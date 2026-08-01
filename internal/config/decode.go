package config

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	"go.yaml.in/yaml/v3"
)

var (
	ErrSyntax       = errors.New("config: invalid YAML syntax")
	ErrUnknownField = errors.New("config: unknown field")
	ErrMissingField = errors.New("config: missing field")
	ErrFieldType    = errors.New("config: invalid field type")
	ErrFieldValue   = errors.New("config: invalid field value")
)

type FieldError struct {
	Path string
	Kind error
}

func (failure *FieldError) Error() string {
	if failure == nil {
		return "config: invalid field"
	}
	return fmt.Sprintf("config: %s: %s", failure.Path, failure.Kind.Error())
}

func (failure *FieldError) Unwrap() error { return failure.Kind }

func DecodeClient(data []byte) (Client, error) {
	root, err := decodeDocument(data)
	if err != nil {
		return Client{}, err
	}
	fields, err := fieldsOf(root, "", "socks_listen", "socks_auth", "transport", "delivery", "interfaces", "principal_id", "psk", "status", "limits", "deadlines")
	if err != nil {
		return Client{}, err
	}
	configuration := Client{
		Delivery: defaultDelivery(), Deadlines: defaultClientDeadlines(),
	}
	if configuration.SOCKSListen, err = requiredString(fields, "socks_listen"); err != nil {
		return Client{}, err
	}
	if node := fields["socks_auth"]; node != nil {
		authentication, authenticationErr := decodeSOCKSAuth(node)
		if authenticationErr != nil {
			return Client{}, authenticationErr
		}
		configuration.SOCKSAuth = &authentication
	}
	if configuration.Transport, err = decodeTransport(requiredNode(fields, "transport"), true); err != nil {
		return Client{}, err
	}
	if node := fields["delivery"]; node != nil {
		if configuration.Delivery, err = decodeDelivery(node); err != nil {
			return Client{}, err
		}
	}
	if node := fields["interfaces"]; node != nil {
		if configuration.Interfaces, err = decodeInterfaces(node); err != nil {
			return Client{}, err
		}
	}
	if configuration.PrincipalID, err = requiredString(fields, "principal_id"); err != nil {
		return Client{}, err
	}
	if configuration.PSK, err = requiredPSK(fields, "psk"); err != nil {
		return Client{}, err
	}
	if node := fields["status"]; node != nil {
		if configuration.Status, err = decodeStatus(node); err != nil {
			return Client{}, err
		}
	}
	if node := fields["limits"]; node != nil {
		if err := decodeClientLimits(node, &configuration.Limits); err != nil {
			return Client{}, err
		}
	}
	if node := fields["deadlines"]; node != nil {
		if err := decodeDeadlines(node, &configuration.Deadlines, true); err != nil {
			return Client{}, err
		}
	}
	if err := validateClient(&configuration); err != nil {
		return Client{}, err
	}
	return configuration, nil
}

func DecodeServer(data []byte) (Server, error) {
	root, err := decodeDocument(data)
	if err != nil {
		return Server{}, err
	}
	fields, err := fieldsOf(root, "", "transport", "status", "principals", "limits", "deadlines")
	if err != nil {
		return Server{}, err
	}
	configuration := Server{Deadlines: defaultServerDeadlines()}
	if configuration.Transport, err = decodeTransport(requiredNode(fields, "transport"), false); err != nil {
		return Server{}, err
	}
	if node := fields["status"]; node != nil {
		if configuration.Status, err = decodeStatus(node); err != nil {
			return Server{}, err
		}
	}
	if configuration.Principals, err = decodePrincipals(requiredNode(fields, "principals")); err != nil {
		return Server{}, err
	}
	if node := fields["limits"]; node != nil {
		if err := decodeServerLimits(node, &configuration.Limits); err != nil {
			return Server{}, err
		}
	}
	if node := fields["deadlines"]; node != nil {
		if err := decodeDeadlines(node, &configuration.Deadlines, false); err != nil {
			return Server{}, err
		}
	}
	if err := validateServer(&configuration); err != nil {
		return Server{}, err
	}
	return configuration, nil
}

func decodeDocument(data []byte) (*yaml.Node, error) {
	if len(data) == 0 || len(data) > MaxConfigBytes || hasForbiddenYAMLSyntax(data) {
		return nil, ErrSyntax
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil || document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return nil, ErrSyntax
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, ErrSyntax
	}
	if err := validateNode(&document); err != nil {
		return nil, err
	}
	root := document.Content[0]
	if root.Kind != yaml.MappingNode || root.Style&yaml.FlowStyle != 0 {
		return nil, &FieldError{Path: "root", Kind: ErrFieldType}
	}
	return root, nil
}

func hasForbiddenYAMLSyntax(data []byte) bool {
	lines := bytes.Split(data, []byte{'\n'})
	for index, line := range lines {
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if index == 0 {
			line = bytes.TrimPrefix(line, []byte{0xef, 0xbb, 0xbf})
		}
		if len(line) > 0 && line[0] == '%' {
			return true
		}
		trimmed := bytes.TrimLeft(line, " \t")
		if bytes.Equal(trimmed, []byte{'?'}) || bytes.HasPrefix(trimmed, []byte("? ")) || bytes.HasPrefix(trimmed, []byte("?\t")) {
			return true
		}
	}
	return false
}

func validateNode(node *yaml.Node) error {
	if node == nil || node.Anchor != "" || node.Alias != nil || node.Kind == yaml.AliasNode || node.Style&yaml.TaggedStyle != 0 {
		return ErrSyntax
	}
	if node.Kind == yaml.MappingNode {
		if len(node.Content)%2 != 0 {
			return ErrSyntax
		}
		seen := make(map[string]struct{}, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || key.Value == "" || key.Value == "<<" {
				return ErrSyntax
			}
			if _, duplicate := seen[key.Value]; duplicate {
				return ErrSyntax
			}
			seen[key.Value] = struct{}{}
		}
	}
	for _, child := range node.Content {
		if err := validateNode(child); err != nil {
			return err
		}
	}
	return nil
}

func fieldsOf(node *yaml.Node, path string, allowed ...string) (map[string]*yaml.Node, error) {
	if node == nil {
		name := path
		if name == "" {
			name = "root"
		}
		return nil, &FieldError{Path: name, Kind: ErrMissingField}
	}
	if node.Kind != yaml.MappingNode {
		return nil, &FieldError{Path: pathName(path), Kind: ErrFieldType}
	}
	wanted := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		wanted[name] = struct{}{}
	}
	fields := make(map[string]*yaml.Node, len(node.Content)/2)
	for index := 0; index < len(node.Content); index += 2 {
		name := node.Content[index].Value
		if _, ok := wanted[name]; !ok {
			// Report only the trusted container path; never echo an attacker-controlled field name in the error.
			return nil, &FieldError{Path: pathName(path), Kind: ErrUnknownField}
		}
		fields[name] = node.Content[index+1]
	}
	return fields, nil
}

func requiredNode(fields map[string]*yaml.Node, name string) *yaml.Node { return fields[name] }

func requiredString(fields map[string]*yaml.Node, name string) (string, error) {
	node := fields[name]
	if node == nil {
		return "", &FieldError{Path: name, Kind: ErrMissingField}
	}
	return stringValue(node, name)
}

func requiredPSK(fields map[string]*yaml.Node, name string) (PSK, error) {
	value, err := requiredString(fields, name)
	if err != nil {
		return PSK{}, err
	}
	return decodePSK(value, name)
}

func stringValue(node *yaml.Node, path string) (string, error) {
	if node == nil {
		return "", &FieldError{Path: path, Kind: ErrMissingField}
	}
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return "", &FieldError{Path: path, Kind: ErrFieldType}
	}
	return node.Value, nil
}

func boolValue(node *yaml.Node, path string) (bool, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
		return false, &FieldError{Path: path, Kind: ErrFieldType}
	}
	switch node.Value {
	case "true", "True", "TRUE":
		return true, nil
	case "false", "False", "FALSE":
		return false, nil
	default:
		return false, &FieldError{Path: path, Kind: ErrFieldType}
	}
}

func uintValue(node *yaml.Node, path string) (uint64, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" || node.Value == "" {
		return 0, &FieldError{Path: path, Kind: ErrFieldType}
	}
	value := node.Value
	if strings.HasPrefix(value, "-") {
		return 0, &FieldError{Path: path, Kind: ErrFieldValue}
	}
	if strings.HasPrefix(value, "+") {
		value = value[1:]
	}
	base := 10
	switch {
	case strings.HasPrefix(value, "0o"):
		base, value = 8, value[2:]
	case strings.HasPrefix(value, "0x"):
		base, value = 16, value[2:]
	}
	if value == "" || strings.ContainsRune(value, '_') {
		return 0, &FieldError{Path: path, Kind: ErrFieldValue}
	}
	valueNumber, err := strconv.ParseUint(value, base, 64)
	if err != nil {
		return 0, &FieldError{Path: path, Kind: ErrFieldValue}
	}
	return valueNumber, nil
}

func durationValue(node *yaml.Node, path string) (time.Duration, error) {
	value, err := stringValue(node, path)
	if err != nil {
		return 0, err
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, &FieldError{Path: path, Kind: ErrFieldValue}
	}
	return duration, nil
}

func decodePSK(value, path string) (PSK, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(decoded) != 32 || base64.StdEncoding.EncodeToString(decoded) != value {
		return PSK{}, &FieldError{Path: path, Kind: ErrFieldValue}
	}
	var key PSK
	copy(key[:], decoded)
	var aggregate byte
	for _, item := range key {
		aggregate |= item
	}
	if aggregate == 0 {
		return PSK{}, &FieldError{Path: path, Kind: ErrFieldValue}
	}
	return key, nil
}

func decodeTransport(node *yaml.Node, client bool) (Transport, error) {
	path := "transport"
	allowed := []string{"type", "listen"}
	if client {
		allowed = []string{"type", "address"}
	}
	fields, err := fieldsOf(node, path, allowed...)
	if err != nil {
		return Transport{}, err
	}
	typeName, err := requiredStringAt(fields, "type", path+".type")
	if err != nil {
		return Transport{}, err
	}
	if typeName != "tcp" {
		return Transport{}, &FieldError{Path: path + ".type", Kind: ErrFieldValue}
	}
	result := Transport{Type: typeName}
	if client {
		result.Address, err = requiredStringAt(fields, "address", path+".address")
	} else {
		result.Listen, err = requiredStringAt(fields, "listen", path+".listen")
	}
	return result, err
}

func decodeSOCKSAuth(node *yaml.Node) (SOCKSAuth, error) {
	const path = "socks_auth"
	fields, err := fieldsOf(node, path, "username", "password")
	if err != nil {
		return SOCKSAuth{}, err
	}
	result := SOCKSAuth{}
	if result.Username, err = requiredStringAt(fields, "username", path+".username"); err != nil {
		return SOCKSAuth{}, err
	}
	if result.Password, err = requiredStringAt(fields, "password", path+".password"); err != nil {
		return SOCKSAuth{}, err
	}
	return result, nil
}

func requiredStringAt(fields map[string]*yaml.Node, name, path string) (string, error) {
	if fields[name] == nil {
		return "", &FieldError{Path: path, Kind: ErrMissingField}
	}
	return stringValue(fields[name], path)
}

func decodeDelivery(node *yaml.Node) (Delivery, error) {
	fields, err := fieldsOf(node, "delivery", "mode", "path_selection", "constraints")
	if err != nil {
		return Delivery{}, err
	}
	mode, err := requiredStringAt(fields, "mode", "delivery.mode")
	if err != nil {
		return Delivery{}, err
	}
	result := Delivery{}
	switch mode {
	case "redundant":
		result.Mode = protocol.DeliveryRedundant
		result.Selection = protocol.PathNone
		if fields["path_selection"] != nil || fields["constraints"] != nil {
			return Delivery{}, &FieldError{Path: "delivery", Kind: ErrFieldValue}
		}
	case "adaptive":
		result.Mode = protocol.DeliveryAdaptive
		selection := "fastest"
		if fields["path_selection"] != nil {
			selection, err = stringValue(fields["path_selection"], "delivery.path_selection")
			if err != nil {
				return Delivery{}, err
			}
		}
		switch selection {
		case "fastest":
			result.Selection = protocol.PathFastest
			if fields["constraints"] != nil {
				return Delivery{}, &FieldError{Path: "delivery.constraints", Kind: ErrFieldValue}
			}
		case "distributed":
			result.Selection = protocol.PathDistributed
			result.Constraints.Fallback = policy.FallbackFastest
			if fields["constraints"] != nil {
				if result.Constraints, err = decodeConstraints(fields["constraints"]); err != nil {
					return Delivery{}, err
				}
			}
		default:
			return Delivery{}, &FieldError{Path: "delivery.path_selection", Kind: ErrFieldValue}
		}
	default:
		return Delivery{}, &FieldError{Path: "delivery.mode", Kind: ErrFieldValue}
	}
	return result, nil
}

func decodeConstraints(node *yaml.Node) (policy.Constraints, error) {
	fields, err := fieldsOf(node, "delivery.constraints", "max_delivery_delay", "max_delay_gap", "constraint_fallback")
	if err != nil {
		return policy.Constraints{}, err
	}
	result := policy.Constraints{Fallback: policy.FallbackFastest}
	if fields["max_delivery_delay"] != nil {
		result.MaxDeliveryDelay, err = durationValue(fields["max_delivery_delay"], "delivery.constraints.max_delivery_delay")
		if err != nil {
			return policy.Constraints{}, err
		}
	}
	if fields["max_delay_gap"] != nil {
		result.MaxDelayGap, err = durationValue(fields["max_delay_gap"], "delivery.constraints.max_delay_gap")
		if err != nil {
			return policy.Constraints{}, err
		}
	}
	if fields["constraint_fallback"] != nil {
		fallback, valueErr := stringValue(fields["constraint_fallback"], "delivery.constraints.constraint_fallback")
		if valueErr != nil {
			return policy.Constraints{}, valueErr
		}
		switch fallback {
		case "fastest":
			result.Fallback = policy.FallbackFastest
		case "pause":
			result.Fallback = policy.FallbackPause
		default:
			return policy.Constraints{}, &FieldError{Path: "delivery.constraints.constraint_fallback", Kind: ErrFieldValue}
		}
	}
	return result, nil
}

func decodeInterfaces(node *yaml.Node) (Interfaces, error) {
	fields, err := fieldsOf(node, "interfaces", "include", "exclude")
	if err != nil {
		return Interfaces{}, err
	}
	result := Interfaces{}
	if fields["include"] != nil {
		result.Include, err = stringSequence(fields["include"], "interfaces.include")
		if err != nil {
			return Interfaces{}, err
		}
	}
	if fields["exclude"] != nil {
		result.Exclude, err = stringSequence(fields["exclude"], "interfaces.exclude")
	}
	return result, err
}

func stringSequence(node *yaml.Node, path string) ([]string, error) {
	if node.Kind != yaml.SequenceNode {
		return nil, &FieldError{Path: path, Kind: ErrFieldType}
	}
	values := make([]string, len(node.Content))
	for index, child := range node.Content {
		value, err := stringValue(child, path)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

func decodeStatus(node *yaml.Node) (Status, error) {
	fields, err := fieldsOf(node, "status", "enabled", "listen", "basic_auth")
	if err != nil {
		return Status{}, err
	}
	result := Status{}
	if fields["enabled"] != nil {
		result.Enabled, err = boolValue(fields["enabled"], "status.enabled")
		if err != nil {
			return Status{}, err
		}
	}
	if fields["listen"] != nil {
		result.Listen, err = stringValue(fields["listen"], "status.listen")
		if err != nil {
			return Status{}, err
		}
	}
	if fields["basic_auth"] != nil {
		authentication, authenticationErr := decodeBasicAuth(fields["basic_auth"])
		if authenticationErr != nil {
			return Status{}, authenticationErr
		}
		result.BasicAuth = &authentication
	}
	return result, err
}

func decodeBasicAuth(node *yaml.Node) (BasicAuth, error) {
	const path = "status.basic_auth"
	fields, err := fieldsOf(node, path, "username", "password")
	if err != nil {
		return BasicAuth{}, err
	}
	result := BasicAuth{}
	if result.Username, err = requiredStringAt(fields, "username", path+".username"); err != nil {
		return BasicAuth{}, err
	}
	if result.Password, err = requiredStringAt(fields, "password", path+".password"); err != nil {
		return BasicAuth{}, err
	}
	return result, nil
}

func decodePrincipals(node *yaml.Node) ([]Principal, error) {
	if node == nil {
		return nil, &FieldError{Path: "principals", Kind: ErrMissingField}
	}
	if node.Kind != yaml.SequenceNode {
		return nil, &FieldError{Path: "principals", Kind: ErrFieldType}
	}
	if len(node.Content) == 0 || len(node.Content) > MaxPrincipals {
		return nil, &FieldError{Path: "principals", Kind: ErrFieldValue}
	}
	principals := make([]Principal, len(node.Content))
	for index, item := range node.Content {
		path := fmt.Sprintf("principals[%d]", index)
		fields, err := fieldsOf(item, path, "id", "psk")
		if err != nil {
			return nil, err
		}
		if principals[index].ID, err = requiredStringAt(fields, "id", path+".id"); err != nil {
			return nil, err
		}
		value, err := requiredStringAt(fields, "psk", path+".psk")
		if err != nil {
			return nil, err
		}
		if principals[index].PSK, err = decodePSK(value, path+".psk"); err != nil {
			return nil, err
		}
	}
	return principals, nil
}

func decodeClientLimits(node *yaml.Node, limits *ClientLimits) error {
	fields, err := fieldsOf(node, "limits", "flows", "opening_flows", "recovering_flows", "sessions", "auth_in_progress", "socks_connections", "socks_handshakes", "socks_per_source", "memory_budget_bytes")
	if err != nil {
		return err
	}
	values := []struct {
		name string
		dst  *uint64
	}{
		{"flows", &limits.Flows}, {"opening_flows", &limits.OpeningFlows}, {"recovering_flows", &limits.RecoveringFlows},
		{"sessions", &limits.Sessions}, {"auth_in_progress", &limits.AuthInProgress}, {"socks_connections", &limits.SOCKSConnections},
		{"socks_handshakes", &limits.SOCKSHandshakes}, {"socks_per_source", &limits.SOCKSPerSource},
		{"memory_budget_bytes", &limits.MemoryBudgetBytes},
	}
	return decodeUintFields(fields, "limits", values)
}

func decodeServerLimits(node *yaml.Node, limits *ServerLimits) error {
	fields, err := fieldsOf(node, "limits", "flows", "per_principal_flows", "opening_flows", "recovering_flows", "sessions", "sessions_per_principal", "auth_in_progress", "target_dials", "tombstones", "tombstones_per_principal", "rate_limit_keys", "memory_budget_bytes")
	if err != nil {
		return err
	}
	values := []struct {
		name string
		dst  *uint64
	}{
		{"flows", &limits.Flows}, {"per_principal_flows", &limits.PerPrincipalFlows}, {"opening_flows", &limits.OpeningFlows},
		{"recovering_flows", &limits.RecoveringFlows}, {"sessions", &limits.Sessions}, {"sessions_per_principal", &limits.SessionsPerPrincipal},
		{"auth_in_progress", &limits.AuthInProgress}, {"target_dials", &limits.TargetDials}, {"tombstones", &limits.Tombstones},
		{"tombstones_per_principal", &limits.TombstonesPerPrincipal}, {"rate_limit_keys", &limits.RateLimitKeys},
		{"memory_budget_bytes", &limits.MemoryBudgetBytes},
	}
	return decodeUintFields(fields, "limits", values)
}

func decodeUintFields(fields map[string]*yaml.Node, path string, values []struct {
	name string
	dst  *uint64
}) error {
	for _, value := range values {
		if fields[value.name] == nil {
			continue
		}
		parsed, err := uintValue(fields[value.name], path+"."+value.name)
		if err != nil {
			return err
		}
		*value.dst = parsed
	}
	return nil
}

func decodeDeadlines(node *yaml.Node, deadlines *Deadlines, client bool) error {
	allowed := []string{"dial", "frame_total", "frame_no_progress", "drain_cleanup"}
	if client {
		allowed = append(allowed, "socks_greeting", "socks_request")
	}
	fields, err := fieldsOf(node, "deadlines", allowed...)
	if err != nil {
		return err
	}
	values := []struct {
		name string
		dst  *time.Duration
	}{
		{"dial", &deadlines.Dial}, {"frame_total", &deadlines.FrameTotal},
		{"frame_no_progress", &deadlines.FrameNoProgress}, {"drain_cleanup", &deadlines.DrainCleanup},
	}
	if client {
		values = append(values, struct {
			name string
			dst  *time.Duration
		}{"socks_greeting", &deadlines.SOCKSGreeting}, struct {
			name string
			dst  *time.Duration
		}{"socks_request", &deadlines.SOCKSRequest})
	}
	for _, value := range values {
		if fields[value.name] == nil {
			continue
		}
		parsed, valueErr := durationValue(fields[value.name], "deadlines."+value.name)
		if valueErr != nil {
			return valueErr
		}
		*value.dst = parsed
	}
	return nil
}

func pathName(path string) string {
	if path == "" {
		return "root"
	}
	return path
}
