package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
)

const testPSK = "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="

func minimalClientYAML() string {
	return `socks_listen: "127.0.0.1:1080"
socks_auth:
  username: local-user
  password: local-password
transport:
  type: tcp
  address: "127.0.0.1:9443"
principal_id: client-01
psk: "` + testPSK + `"
`
}

func minimalServerYAML() string {
	return `transport:
  type: tcp
  listen: "127.0.0.1:9443"
principals:
  - id: client-01
    psk: "` + testPSK + `"
`
}

func TestDecodeClientAndServerDefaultsMatchBudgetVectors(t *testing.T) {
	client, err := DecodeClient([]byte(minimalClientYAML()))
	if err != nil {
		t.Fatal(err)
	}
	if client.Transport.Type != "tcp" || client.Delivery.Mode != protocol.DeliveryAdaptive ||
		client.Delivery.Selection != protocol.PathFastest || client.Status.Enabled || client.SOCKSAuth == nil ||
		*client.SOCKSAuth != (SOCKSAuth{Username: "local-user", Password: "local-password"}) ||
		client.Limits != defaultClientLimits() || client.Deadlines != defaultClientDeadlines() ||
		client.RequiredBytes != DefaultClientRequiredBytes {
		t.Fatalf("client defaults = %#v", client)
	}
	server, err := DecodeServer([]byte(minimalServerYAML()))
	if err != nil {
		t.Fatal(err)
	}
	if server.Transport.Type != "tcp" || server.Status.Enabled ||
		server.Limits != defaultServerLimits() || server.Deadlines != defaultServerDeadlines() ||
		server.RequiredBytes != DefaultServerRequiredBytes || len(server.Principals) != 1 {
		t.Fatalf("server defaults = %#v", server)
	}
}

func TestZeroLimitsMeanNoConfiguredCap(t *testing.T) {
	clientYAML := minimalClientYAML() + `limits:
  flows: 0
  opening_flows: 0
  recovering_flows: 0
  sessions: 0
  auth_in_progress: 0
  socks_connections: 0
  socks_handshakes: 0
  socks_per_source: 0
  memory_budget_bytes: 0
`
	client, err := DecodeClient([]byte(clientYAML))
	if err != nil {
		t.Fatal(err)
	}
	if client.Limits != defaultClientLimits() || client.Limits.MemoryBudgetBytes != 0 {
		t.Fatalf("client unlimited limits = %#v", client.Limits)
	}

	serverYAML := minimalServerYAML() + `limits:
  flows: 0
  per_principal_flows: 0
  opening_flows: 0
  recovering_flows: 0
  sessions: 0
  sessions_per_principal: 0
  auth_in_progress: 0
  target_dials: 0
  tombstones: 0
  tombstones_per_principal: 0
  rate_limit_keys: 0
  memory_budget_bytes: 0
`
	server, err := DecodeServer([]byte(serverYAML))
	if err != nil {
		t.Fatal(err)
	}
	if server.Limits != defaultServerLimits() || server.Limits.MemoryBudgetBytes != 0 {
		t.Fatalf("server unlimited limits = %#v", server.Limits)
	}
}

func TestDecodeCompleteClientDistributedConfiguration(t *testing.T) {
	input := `socks_listen: "[::1]:1080"
socks_auth:
  username: local-user
  password: local-password
transport:
  type: tcp
  address: "Relay.Example:9443"
delivery:
  mode: adaptive
  path_selection: distributed
  constraints:
    max_delivery_delay: "80ms"
    max_delay_gap: "30ms"
    constraint_fallback: pause
interfaces:
  include: ["eth*", "wlan?"]
  exclude: ["*test*"]
principal_id: client-01
psk: "` + testPSK + `"
status:
  enabled: true
  listen: "[::1]:9090"
limits:
  flows: 128
  opening_flows: 64
  recovering_flows: 128
  sessions: 2
  auth_in_progress: 2
  socks_connections: 512
  socks_handshakes: 128
  socks_per_source: 64
  memory_budget_bytes: 536870912
deadlines:
  dial: "9s"
  frame_total: "29s"
  frame_no_progress: "4s"
  socks_greeting: "6s"
  socks_request: "7s"
  drain_cleanup: "8s"
`
	configuration, err := DecodeClient([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Transport.Address != "relay.example:9443" || configuration.SOCKSListen != "[::1]:1080" ||
		configuration.Delivery.Selection != protocol.PathDistributed || configuration.Delivery.Constraints.Fallback != policy.FallbackPause ||
		configuration.Delivery.Constraints.MaxDeliveryDelay != 80*time.Millisecond || configuration.Deadlines.Dial != 9*time.Second ||
		len(configuration.Interfaces.Include) != 2 || !configuration.Status.Enabled {
		t.Fatalf("complete client = %#v", configuration)
	}
}

func TestSOCKSAuthStrictValidation(t *testing.T) {
	secretMarker := "DO_NOT_ECHO_SOCKS_CREDENTIAL"
	withoutAuth := strings.Replace(minimalClientYAML(), "socks_auth:\n  username: local-user\n  password: local-password\n", "", 1)
	assertFieldError := func(name, input, path string, kind error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			_, err := DecodeClient([]byte(input))
			var fieldError *FieldError
			if !errors.Is(err, kind) || !errors.As(err, &fieldError) || fieldError.Path != path || strings.Contains(err.Error(), secretMarker) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	configuration, err := DecodeClient([]byte(withoutAuth))
	if err != nil || configuration.SOCKSAuth != nil {
		t.Fatalf("optional authentication = %#v, %v", configuration.SOCKSAuth, err)
	}
	assertFieldError("mapping type", strings.Replace(minimalClientYAML(), "socks_auth:\n  username: local-user\n  password: local-password", "socks_auth: "+secretMarker, 1), "socks_auth", ErrFieldType)
	assertFieldError("sequence type", strings.Replace(minimalClientYAML(), "socks_auth:\n  username: local-user\n  password: local-password", "socks_auth: []", 1), "socks_auth", ErrFieldType)
	assertFieldError("null type", strings.Replace(minimalClientYAML(), "socks_auth:\n  username: local-user\n  password: local-password", "socks_auth: null", 1), "socks_auth", ErrFieldType)
	assertFieldError("unknown field", strings.Replace(minimalClientYAML(), "  password: local-password", "  password: local-password\n  "+secretMarker+": hidden", 1), "socks_auth", ErrUnknownField)
	assertFieldError("missing username", strings.Replace(minimalClientYAML(), "  username: local-user\n", "", 1), "socks_auth.username", ErrMissingField)
	assertFieldError("missing password", strings.Replace(minimalClientYAML(), "  password: local-password\n", "", 1), "socks_auth.password", ErrMissingField)
	assertFieldError("username type", strings.Replace(minimalClientYAML(), "username: local-user", "username: 123", 1), "socks_auth.username", ErrFieldType)
	assertFieldError("password type", strings.Replace(minimalClientYAML(), "password: local-password", "password: false", 1), "socks_auth.password", ErrFieldType)
	assertFieldError("empty username", strings.Replace(minimalClientYAML(), "username: local-user", "username: \"\"", 1), "socks_auth.username", ErrFieldValue)
	assertFieldError("empty password", strings.Replace(minimalClientYAML(), "password: local-password", "password: \"\"", 1), "socks_auth.password", ErrFieldValue)
	assertFieldError("long username", strings.Replace(minimalClientYAML(), "local-user", strings.Repeat("u", 256), 1), "socks_auth.username", ErrFieldValue)
	assertFieldError("long password", strings.Replace(minimalClientYAML(), "local-password", strings.Repeat("p", 256), 1), "socks_auth.password", ErrFieldValue)

	maxUTF8 := strings.Repeat("€", 85)
	input := strings.Replace(minimalClientYAML(), "local-user", maxUTF8, 1)
	input = strings.Replace(input, "local-password", maxUTF8, 1)
	configuration, err = DecodeClient([]byte(input))
	if err != nil {
		t.Fatalf("255-byte UTF-8 credentials: %v", err)
	}
	if configuration.SOCKSAuth == nil || configuration.SOCKSAuth.Username != maxUTF8 || configuration.SOCKSAuth.Password != maxUTF8 {
		t.Fatalf("credentials = %#v", configuration.SOCKSAuth)
	}
	flowMapping := strings.Replace(minimalClientYAML(), "socks_auth:\n  username: local-user\n  password: local-password", "socks_auth: {username: local-user, password: local-password}", 1)
	if configuration, err = DecodeClient([]byte(flowMapping)); err != nil || configuration.SOCKSAuth == nil {
		t.Fatalf("flow authentication mapping = %#v, %v", configuration.SOCKSAuth, err)
	}
	assertFieldError("UTF-8 byte overflow", strings.Replace(input, maxUTF8, maxUTF8+"€", 1), "socks_auth.username", ErrFieldValue)
}

func TestStatusBasicAuthStrictValidation(t *testing.T) {
	validStatus := `status:
  enabled: true
  listen: "127.0.0.1:9090"
  basic_auth:
    username: manager
    password: status-password
`
	client, err := DecodeClient([]byte(minimalClientYAML() + validStatus))
	if err != nil || client.Status.BasicAuth == nil ||
		*client.Status.BasicAuth != (BasicAuth{Username: "manager", Password: "status-password"}) {
		t.Fatalf("client basic auth = %#v, %v", client.Status.BasicAuth, err)
	}
	server, err := DecodeServer([]byte(minimalServerYAML() + validStatus))
	if err != nil || server.Status.BasicAuth == nil || *server.Status.BasicAuth != *client.Status.BasicAuth {
		t.Fatalf("server basic auth = %#v, %v", server.Status.BasicAuth, err)
	}

	secretMarker := "DO_NOT_ECHO_STATUS_CREDENTIAL"
	tests := []struct {
		name string
		body string
		path string
		kind error
	}{
		{name: "unknown field", body: "    username: manager\n    password: status-password\n    extra: " + secretMarker + "\n", path: "status.basic_auth", kind: ErrUnknownField},
		{name: "missing username", body: "    password: status-password\n", path: "status.basic_auth.username", kind: ErrMissingField},
		{name: "missing password", body: "    username: manager\n", path: "status.basic_auth.password", kind: ErrMissingField},
		{name: "empty username", body: "    username: \"\"\n    password: status-password\n", path: "status.basic_auth.username", kind: ErrFieldValue},
		{name: "colon username", body: "    username: \"manager:admin\"\n    password: status-password\n", path: "status.basic_auth.username", kind: ErrFieldValue},
		{name: "control password", body: "    username: manager\n    password: \"status\\npassword\"\n", path: "status.basic_auth.password", kind: ErrFieldValue},
		{name: "long username", body: "    username: \"" + strings.Repeat("u", 256) + "\"\n    password: status-password\n", path: "status.basic_auth.username", kind: ErrFieldValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := minimalClientYAML() + "status:\n  enabled: true\n  listen: \"127.0.0.1:9090\"\n  basic_auth:\n" + test.body
			_, decodeErr := DecodeClient([]byte(input))
			var fieldError *FieldError
			if !errors.Is(decodeErr, test.kind) || !errors.As(decodeErr, &fieldError) ||
				fieldError.Path != test.path || strings.Contains(decodeErr.Error(), secretMarker) {
				t.Fatalf("error = %v", decodeErr)
			}
		})
	}

	disabled := minimalClientYAML() + `status:
  enabled: false
  basic_auth:
    username: manager
    password: status-password
`
	if _, err := DecodeClient([]byte(disabled)); !errors.Is(err, ErrFieldValue) {
		t.Fatalf("disabled status basic auth error = %v", err)
	}
}

func TestStrictYAMLRejectsUnsupportedSyntax(t *testing.T) {
	tests := map[string]string{
		"JSON document":           `{"socks_listen":"127.0.0.1:1080","transport":{"type":"tcp","address":"127.0.0.1:9443"},"principal_id":"client-01","psk":"` + testPSK + `"}`,
		"flow style root":         `{socks_listen: "127.0.0.1:1080", transport: {type: tcp, address: "127.0.0.1:9443"}, principal_id: client-01, psk: "` + testPSK + `"}`,
		"duplicate root":          minimalClientYAML() + "principal_id: other\n",
		"duplicate nested":        strings.Replace(minimalClientYAML(), "  type: tcp", "  type: tcp\n  type: tcp", 1),
		"non string key":          "1: value\n",
		"complex key":             "? [a, b]\n: value\n",
		"directive":               "%YAML 1.2\n---\n" + minimalClientYAML(),
		"anchor":                  strings.Replace(minimalClientYAML(), "client-01", "&principal client-01", 1),
		"alias":                   minimalClientYAML() + "extra: *principal\n",
		"merge":                   "base: &base {type: tcp}\ntransport:\n  <<: *base\n",
		"explicit builtin tag":    strings.Replace(minimalClientYAML(), "client-01", "!!str client-01", 1),
		"explicit custom tag":     strings.Replace(minimalClientYAML(), "client-01", "!secret client-01", 1),
		"second document":         minimalClientYAML() + "---\n{}\n",
		"trailing empty document": minimalClientYAML() + "---\n",
		"empty document":          "# comment only\n",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeClient([]byte(input)); err == nil {
				t.Fatal("unsupported YAML accepted")
			}
		})
	}
}

func TestStrictYAMLRejectsUnknownAndWrongScalarTypesWithoutEcho(t *testing.T) {
	secretMarker := "DO_NOT_ECHO_ATTACKER_VALUE"
	unknown := minimalClientYAML() + secretMarker + ": true\n"
	if _, err := DecodeClient([]byte(unknown)); !errors.Is(err, ErrUnknownField) || !strings.Contains(err.Error(), "root") || strings.Contains(err.Error(), secretMarker) {
		t.Fatalf("unknown field error = %v", err)
	}
	nestedUnknown := strings.Replace(minimalClientYAML(), "  address: \"127.0.0.1:9443\"", "  address: \"127.0.0.1:9443\"\n  "+secretMarker+": true", 1)
	if _, err := DecodeClient([]byte(nestedUnknown)); !errors.Is(err, ErrUnknownField) || !strings.Contains(err.Error(), "transport") || strings.Contains(err.Error(), secretMarker) {
		t.Fatalf("nested unknown field error = %v", err)
	}
	tests := []string{
		strings.Replace(minimalClientYAML(), "principal_id: client-01", "principal_id: 123", 1),
		minimalClientYAML() + "limits:\n  flows: \"128\"\n",
		minimalClientYAML() + "deadlines:\n  dial: 5\n",
		strings.Replace(minimalClientYAML(), "psk: \""+testPSK+"\"", "psk: false", 1),
		minimalClientYAML() + "interfaces:\n  include: [1]\n",
	}
	for index, input := range tests {
		if _, err := DecodeClient([]byte(input)); err == nil {
			t.Fatalf("wrong scalar type %d accepted", index)
		}
	}
	invalidYAML := []byte("psk: [" + secretMarker)
	if _, err := DecodeClient(invalidYAML); err == nil || strings.Contains(err.Error(), secretMarker) {
		t.Fatalf("syntax error leaked input: %v", err)
	}
}

func TestStrictYAMLAcceptsCoreBooleanAndUnsignedIntegerSpellings(t *testing.T) {
	tests := []string{
		minimalClientYAML() + "limits:\n  flows: +128\n",
		minimalClientYAML() + "limits:\n  flows: 0x80\n",
		minimalClientYAML() + "limits:\n  flows: 0o200\n",
	}
	for index, input := range tests {
		configuration, err := DecodeClient([]byte(input))
		if err != nil {
			t.Fatalf("YAML 1.2 core spelling %d: %v", index, err)
		}
		if configuration.Limits.Flows != 128 {
			t.Fatalf("YAML 1.2 core spelling %d decoded as %#v", index, configuration)
		}
	}

	legacyOrExtended := []string{
		minimalClientYAML() + "limits:\n  flows: 1_28\n",
		minimalClientYAML() + "limits:\n  flows: 0b10000000\n",
	}
	for index, input := range legacyOrExtended {
		if _, err := DecodeClient([]byte(input)); err == nil {
			t.Fatalf("non-core integer spelling %d accepted", index)
		}
	}
}

func TestRoleSpecificFieldsAndUnsupportedTransportsAreRejected(t *testing.T) {
	for _, field := range []string{"target_dials", "rate_limit_keys"} {
		clientServerField := minimalClientYAML() + "limits:\n  " + field + ": 1\n"
		if _, err := DecodeClient([]byte(clientServerField)); !errors.Is(err, ErrUnknownField) {
			t.Fatalf("server-only client field %q error = %v", field, err)
		}
	}
	serverClientDeadline := minimalServerYAML() + "deadlines:\n  socks_greeting: \"5s\"\n"
	if _, err := DecodeServer([]byte(serverClientDeadline)); !errors.Is(err, ErrUnknownField) {
		t.Fatalf("client-only server field error = %v", err)
	}
	for _, removedField := range []string{"allow_insecure_socks", "allow_insecure_relay"} {
		if _, err := DecodeClient([]byte(minimalClientYAML() + removedField + ": true\n")); !errors.Is(err, ErrUnknownField) {
			t.Fatalf("removed client field %q error = %v", removedField, err)
		}
		if _, err := DecodeServer([]byte(minimalServerYAML() + removedField + ": true\n")); !errors.Is(err, ErrUnknownField) {
			t.Fatalf("removed server field %q error = %v", removedField, err)
		}
	}
	for _, transportType := range []string{"udp", "http2", "quic"} {
		input := strings.Replace(minimalClientYAML(), "type: tcp", "type: "+transportType, 1)
		if _, err := DecodeClient([]byte(input)); err == nil {
			t.Fatalf("transport %q accepted", transportType)
		}
	}
}

func TestEndpointRules(t *testing.T) {
	clientInvalid := []string{
		strings.Replace(minimalClientYAML(), "127.0.0.1:1080", "localhost:1080", 1),
		strings.Replace(minimalClientYAML(), "127.0.0.1:1080", "0.0.0.0:0", 1),
		strings.Replace(minimalClientYAML(), "127.0.0.1:9443", "0.0.0.0:9443", 1),
		strings.Replace(minimalClientYAML(), "127.0.0.1:9443", "127.0.0.1:0", 1),
	}
	for index, input := range clientInvalid {
		if _, err := DecodeClient([]byte(input)); err == nil {
			t.Fatalf("invalid client endpoint %d accepted", index)
		}
	}
	for name, address := range map[string]string{
		"IPv4 unspecified": "0.0.0.0:1080",
		"IPv6 unspecified": "[::]:1080",
		"non-loopback":     "192.0.2.1:1080",
	} {
		t.Run("SOCKS "+name, func(t *testing.T) {
			input := strings.Replace(minimalClientYAML(), "127.0.0.1:1080", address, 1)
			configuration, err := DecodeClient([]byte(input))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if configuration.SOCKSListen != address {
				t.Fatalf("configuration = %#v", configuration)
			}
		})
	}
	for name, address := range map[string]string{
		"IPv4 unspecified": "0.0.0.0:9443",
		"IPv6 unspecified": "[::]:9443",
	} {
		t.Run("client target rejects "+name, func(t *testing.T) {
			input := strings.Replace(minimalClientYAML(), "127.0.0.1:9443", address, 1)
			if _, err := DecodeClient([]byte(input)); err == nil {
				t.Fatal("unspecified client target accepted")
			}
		})
	}
	for _, address := range []string{"192.0.2.1:9443", "Relay.Example:9443"} {
		if _, err := DecodeClient([]byte(strings.Replace(minimalClientYAML(), "127.0.0.1:9443", address, 1))); err != nil {
			t.Fatalf("client target %q: %v", address, err)
		}
	}
	for _, address := range []string{"0.0.0.0:9443", "[::]:9443", "192.0.2.1:9443"} {
		if _, err := DecodeServer([]byte(strings.Replace(minimalServerYAML(), "127.0.0.1:9443", address, 1))); err != nil {
			t.Fatalf("server listener %q: %v", address, err)
		}
	}
	for _, address := range []string{"localhost:9443", "0.0.0.0:0"} {
		if _, err := DecodeServer([]byte(strings.Replace(minimalServerYAML(), "127.0.0.1:9443", address, 1))); err == nil {
			t.Fatalf("invalid server listener %q accepted", address)
		}
	}
}

func TestDeliveryInterfaceStatusAndPrincipalValidation(t *testing.T) {
	invalidAdditions := []string{
		"delivery:\n  mode: redundant\n  path_selection: fastest\n",
		"delivery:\n  mode: adaptive\n  path_selection: fastest\n  constraints: {}\n",
		"delivery:\n  mode: adaptive\n  path_selection: distributed\n  constraints:\n    max_delay_gap: \"1ms\"\n",
		"interfaces:\n  include: [\"[broken\"]\n",
		"interfaces:\n  include: [\"\"]\n",
		"status:\n  enabled: true\n",
		"status:\n  enabled: false\n  listen: \"127.0.0.1:9090\"\n",
	}
	statusInput := minimalClientYAML() + "status:\n  enabled: true\n  listen: \"0.0.0.0:9090\"\n"
	if _, err := DecodeClient([]byte(statusInput)); err != nil {
		t.Fatalf("non-loopback status listener: %v", err)
	}
	for index, addition := range invalidAdditions {
		if _, err := DecodeClient([]byte(minimalClientYAML() + addition)); err == nil {
			t.Fatalf("invalid policy/interface/status %d accepted", index)
		}
	}
	duplicatePrincipal := minimalServerYAML() + `  - id: client-01
    psk: "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUE="
`
	if _, err := DecodeServer([]byte(duplicatePrincipal)); err == nil {
		t.Fatal("duplicate principal accepted")
	}
	duplicatePSK := minimalServerYAML() + `  - id: client-02
    psk: "` + testPSK + `"
`
	if _, err := DecodeServer([]byte(duplicatePSK)); err == nil {
		t.Fatal("duplicate PSK accepted")
	}
	zeroPSK := strings.Replace(minimalClientYAML(), testPSK, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", 1)
	if _, err := DecodeClient([]byte(zeroPSK)); err == nil {
		t.Fatal("all-zero PSK accepted")
	}
}

func TestLimitsDeadlinesAndBudgetRelationships(t *testing.T) {
	invalidClientLimits := []string{
		"  flows: 2049\n",
		"  flows: 10\n  opening_flows: 11\n",
		"  sessions: 1\n  auth_in_progress: 2\n",
		"  socks_connections: 10\n  socks_handshakes: 11\n",
		"  memory_budget_bytes: 245314559\n",
	}
	for index, fields := range invalidClientLimits {
		input := minimalClientYAML() + "limits:\n" + fields
		if _, err := DecodeClient([]byte(input)); err == nil {
			t.Fatalf("invalid client limits %d accepted", index)
		}
	}
	invalidServerLimits := []string{
		"  flows: 10\n  per_principal_flows: 11\n",
		"  opening_flows: 10\n  target_dials: 11\n",
		"  sessions: 4\n  sessions_per_principal: 5\n",
		"  tombstones: 10\n  tombstones_per_principal: 11\n",
	}
	for index, fields := range invalidServerLimits {
		input := minimalServerYAML() + "limits:\n" + fields
		if _, err := DecodeServer([]byte(input)); err == nil {
			t.Fatalf("invalid server limits %d accepted", index)
		}
	}
	invalidDeadlines := []string{
		"  dial: \"999ms\"\n",
		"  frame_total: \"4s\"\n",
		"  frame_total: \"5s\"\n  frame_no_progress: \"6s\"\n",
		"  socks_request: \"31s\"\n",
	}
	for index, fields := range invalidDeadlines {
		input := minimalClientYAML() + "deadlines:\n" + fields
		if _, err := DecodeClient([]byte(input)); err == nil {
			t.Fatalf("invalid deadlines %d accepted", index)
		}
	}
}

func TestZeroDependentLimitsFollowExplicitParentLimits(t *testing.T) {
	client, err := DecodeClient([]byte(minimalClientYAML() + `limits:
  flows: 10
  opening_flows: 0
  recovering_flows: 0
  sessions: 2
  auth_in_progress: 0
  socks_connections: 10
  socks_handshakes: 0
  socks_per_source: 0
  memory_budget_bytes: 0
`))
	if err != nil {
		t.Fatal(err)
	}
	wantClient := ClientLimits{
		Flows: 10, OpeningFlows: 10, RecoveringFlows: 10,
		Sessions: 2, AuthInProgress: 2,
		SOCKSConnections: 10, SOCKSHandshakes: 10, SOCKSPerSource: 10,
	}
	if client.Limits != wantClient {
		t.Fatalf("partial client limits = %#v, want %#v", client.Limits, wantClient)
	}

	server, err := DecodeServer([]byte(minimalServerYAML() + `limits:
  flows: 10
  per_principal_flows: 0
  opening_flows: 0
  recovering_flows: 0
  sessions: 4
  sessions_per_principal: 0
  auth_in_progress: 0
  target_dials: 0
  tombstones: 10
  tombstones_per_principal: 0
  rate_limit_keys: 17
  memory_budget_bytes: 0
`))
	if err != nil {
		t.Fatal(err)
	}
	wantServer := ServerLimits{
		Flows: 10, PerPrincipalFlows: 10, OpeningFlows: 10, RecoveringFlows: 10,
		Sessions: 4, SessionsPerPrincipal: 4, AuthInProgress: 4, TargetDials: 10,
		Tombstones: 10, TombstonesPerPrincipal: 10, RateLimitKeys: 17,
	}
	if server.Limits != wantServer {
		t.Fatalf("partial server limits = %#v, want %#v", server.Limits, wantServer)
	}
}

func TestPositiveLimitsKeepInternalSafetyBounds(t *testing.T) {
	assertField := func(name string, server bool, field, path string) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			input := minimalClientYAML() + "limits:\n  " + field + "\n"
			var err error
			if server {
				_, err = DecodeServer([]byte(minimalServerYAML() + "limits:\n  " + field + "\n"))
			} else {
				_, err = DecodeClient([]byte(input))
			}
			var fieldError *FieldError
			if !errors.Is(err, ErrFieldValue) || !errors.As(err, &fieldError) || fieldError.Path != "limits."+path {
				t.Fatalf("error = %v", err)
			}
		})
	}

	clientFields := []struct{ name, field string }{
		{"flows", "flows: 2049"}, {"opening_flows", "opening_flows: 257"},
		{"recovering_flows", "recovering_flows: 1025"}, {"sessions", "sessions: 65"},
		{"auth_in_progress", "auth_in_progress: 65"}, {"socks_connections", "socks_connections: 2049"},
		{"socks_handshakes", "socks_handshakes: 513"}, {"socks_per_source", "socks_per_source: 2049"},
	}
	for _, testCase := range clientFields {
		assertField("client-"+testCase.name, false, testCase.field, testCase.name)
	}

	serverFields := []struct{ name, field string }{
		{"flows", "flows: 8193"}, {"per_principal_flows", "per_principal_flows: 8193"},
		{"opening_flows", "opening_flows: 513"}, {"recovering_flows", "recovering_flows: 2049"},
		{"sessions", "sessions: 4097"}, {"sessions_per_principal", "sessions_per_principal: 4097"},
		{"auth_in_progress", "auth_in_progress: 513"}, {"target_dials", "target_dials: 513"},
		{"tombstones", "tombstones: 32769"}, {"tombstones_per_principal", "tombstones_per_principal: 32769"},
		{"rate_limit_keys", "rate_limit_keys: 16385"},
	}
	for _, testCase := range serverFields {
		assertField("server-"+testCase.name, true, testCase.field, testCase.name)
	}

	client, err := DecodeClient([]byte(minimalClientYAML() + "limits:\n  memory_budget_bytes: 9223372036854775807\n"))
	if err != nil || client.Limits.MemoryBudgetBytes != MaxMemoryBudget {
		t.Fatalf("maximum client memory budget = %d, %v", client.Limits.MemoryBudgetBytes, err)
	}
	server, err := DecodeServer([]byte(minimalServerYAML() + "limits:\n  memory_budget_bytes: 9223372036854775807\n"))
	if err != nil || server.Limits.MemoryBudgetBytes != MaxMemoryBudget {
		t.Fatalf("maximum server memory budget = %d, %v", server.Limits.MemoryBudgetBytes, err)
	}
	tooLarge := "limits:\n  memory_budget_bytes: 18446744073709551616\n"
	if _, err := DecodeClient([]byte(minimalClientYAML() + tooLarge)); err == nil {
		t.Fatal("overflowing client memory budget accepted")
	}
	if _, err := DecodeServer([]byte(minimalServerYAML() + tooLarge)); err == nil {
		t.Fatal("overflowing server memory budget accepted")
	}
}

func TestBudgetVectorsAndOverflow(t *testing.T) {
	clientBytes, err := ClientRequiredBytes(defaultClientLimits())
	if err != nil || clientBytes != DefaultClientRequiredBytes {
		t.Fatalf("client budget = %d, %v", clientBytes, err)
	}
	serverBytes, err := ServerRequiredBytes(defaultServerLimits(), 1)
	if err != nil || serverBytes != DefaultServerRequiredBytes {
		t.Fatalf("server budget = %d, %v", serverBytes, err)
	}
	if _, err := sumBudget([]budgetTerm{{count: ^uint64(0), size: 2}}); !errors.Is(err, ErrBudget) {
		t.Fatalf("multiplication overflow = %v", err)
	}
	if _, err := sumBudget([]budgetTerm{{count: 1, size: ^uint64(0)}, {count: 1, size: 1}}); !errors.Is(err, ErrBudget) {
		t.Fatalf("addition overflow = %v", err)
	}
}

func TestConfigFileLoadChecksTypeAndSize(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "client.yml")
	if err := os.WriteFile(path, []byte(minimalClientYAML()), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadClient(path); err != nil {
		t.Fatalf("regular file: %v", err)
	}
	if _, err := LoadClient(directory); !errors.Is(err, ErrConfigFile) {
		t.Fatalf("directory error = %v", err)
	}
	large := filepath.Join(directory, "large.yml")
	if err := os.WriteFile(large, make([]byte, MaxConfigBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadClient(large); !errors.Is(err, ErrConfigTooLarge) {
		t.Fatalf("large file error = %v", err)
	}
	if _, err := LoadClient(filepath.Join(directory, "missing.yml")); !errors.Is(err, ErrConfigOpen) {
		t.Fatalf("missing file error = %v", err)
	}
}

func TestConfigFileLoadRejectsFIFOWithoutBlocking(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "client.yml")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := LoadClient(path)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConfigFile) {
			t.Fatalf("FIFO error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("configuration loader blocked while opening FIFO")
	}
}

func TestExampleYAMLFilesDecode(t *testing.T) {
	clientData, err := os.ReadFile("../../examples/config/client.yml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeClient(clientData); err != nil {
		t.Fatalf("client example: %v", err)
	}
	serverData, err := os.ReadFile("../../examples/config/server.yml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeServer(serverData); err != nil {
		t.Fatalf("server example: %v", err)
	}
}
