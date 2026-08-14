package config

import (
	"bytes"
	"net"
	"net/netip"
	pathpkg "path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	statusapi "github.com/adrianceding/via/internal/status"
)

func validateClient(configuration *Client) error {
	if configuration == nil || !protocol.ValidPrincipalID(configuration.PrincipalID) {
		return &FieldError{Path: "principal_id", Kind: ErrFieldValue}
	}
	var err error
	if configuration.SOCKSListen, err = validateLiteralEndpoint(configuration.SOCKSListen, "socks_listen"); err != nil {
		return err
	}
	if configuration.SOCKSAuth != nil {
		if err := validateSOCKSAuth(*configuration.SOCKSAuth); err != nil {
			return err
		}
	}
	if configuration.Transport.Type != "tcp" {
		return &FieldError{Path: "transport.type", Kind: ErrFieldValue}
	}
	if configuration.Transport.LanesPerPath == 0 {
		configuration.Transport.LanesPerPath = MinimumLanesPerPath
	}
	if configuration.Transport.LanesPerPath < MinimumLanesPerPath || configuration.Transport.LanesPerPath > MaximumLanesPerPath {
		return &FieldError{Path: "transport.lanes_per_path", Kind: ErrFieldValue}
	}
	if configuration.Transport.WriteBufferBytes == 0 {
		configuration.Transport.WriteBufferBytes = DefaultTCPWriteBufferBytes
	}
	if configuration.Transport.WriteBufferBytes < MinimumTCPWriteBufferBytes || configuration.Transport.WriteBufferBytes > MaximumTCPWriteBufferBytes {
		return &FieldError{Path: "transport.write_buffer_bytes", Kind: ErrFieldValue}
	}
	if err := normalizeAndValidateQueue(&configuration.Transport); err != nil {
		return err
	}
	if configuration.Transport.Address, err = validateClientEndpoint(configuration.Transport.Address, "transport.address"); err != nil {
		return err
	}
	if err := validateDelivery(configuration.Delivery); err != nil {
		return err
	}
	if configuration.Transport.LanesPerPath > 1 &&
		(configuration.Delivery.Mode != protocol.DeliveryAdaptive || configuration.Delivery.Selection != protocol.PathFastest) {
		return &FieldError{Path: "transport.lanes_per_path", Kind: ErrFieldValue}
	}
	if err := validateInterfacePatterns(configuration.Interfaces); err != nil {
		return err
	}
	if err := validateStatus(&configuration.Status); err != nil {
		return err
	}
	normalizeClientLimits(&configuration.Limits, configuration.Transport.LanesPerPath)
	if err := validateClientLimits(configuration.Limits); err != nil {
		return err
	}
	if err := validateDeadlines(configuration.Deadlines, true); err != nil {
		return err
	}
	required, err := ClientRequiredBytes(configuration.Limits, configuration.Transport)
	if err != nil || configuration.Limits.MemoryBudgetBytes != 0 && required > configuration.Limits.MemoryBudgetBytes {
		return &FieldError{Path: "limits.memory_budget_bytes", Kind: ErrBudget}
	}
	configuration.RequiredBytes = required
	return nil
}

func validateSOCKSAuth(authentication SOCKSAuth) error {
	for _, field := range []struct {
		path  string
		value string
	}{
		{path: "socks_auth.username", value: authentication.Username},
		{path: "socks_auth.password", value: authentication.Password},
	} {
		if !utf8.ValidString(field.value) || len(field.value) < 1 || len(field.value) > 255 {
			return &FieldError{Path: field.path, Kind: ErrFieldValue}
		}
	}
	return nil
}

func validateServer(configuration *Server) error {
	if configuration == nil || configuration.Transport.Type != "tcp" {
		return &FieldError{Path: "transport.type", Kind: ErrFieldValue}
	}
	var err error
	if configuration.Transport.Listen, err = validateLiteralEndpoint(configuration.Transport.Listen, "transport.listen"); err != nil {
		return err
	}
	if configuration.Transport.WriteBufferBytes == 0 {
		configuration.Transport.WriteBufferBytes = DefaultTCPWriteBufferBytes
	}
	if configuration.Transport.WriteBufferBytes < MinimumTCPWriteBufferBytes || configuration.Transport.WriteBufferBytes > MaximumTCPWriteBufferBytes {
		return &FieldError{Path: "transport.write_buffer_bytes", Kind: ErrFieldValue}
	}
	if err := normalizeAndValidateQueue(&configuration.Transport); err != nil {
		return err
	}
	if err := validateStatus(&configuration.Status); err != nil {
		return err
	}
	if len(configuration.Principals) < 1 || len(configuration.Principals) > MaxPrincipals {
		return &FieldError{Path: "principals", Kind: ErrFieldValue}
	}
	identities := make(map[string]struct{}, len(configuration.Principals))
	keys := make(map[PSK]struct{}, len(configuration.Principals))
	for _, principal := range configuration.Principals {
		if !protocol.ValidPrincipalID(principal.ID) {
			return &FieldError{Path: "principals.id", Kind: ErrFieldValue}
		}
		if _, duplicate := identities[principal.ID]; duplicate {
			return &FieldError{Path: "principals.id", Kind: ErrFieldValue}
		}
		if _, duplicate := keys[principal.PSK]; duplicate {
			return &FieldError{Path: "principals.psk", Kind: ErrFieldValue}
		}
		identities[principal.ID] = struct{}{}
		keys[principal.PSK] = struct{}{}
	}
	normalizeServerLimits(&configuration.Limits)
	if err := validateServerLimits(configuration.Limits); err != nil {
		return err
	}
	if err := validateDeadlines(configuration.Deadlines, false); err != nil {
		return err
	}
	required, err := ServerRequiredBytes(configuration.Limits, uint64(len(configuration.Principals)), configuration.Transport)
	if err != nil || configuration.Limits.MemoryBudgetBytes != 0 && required > configuration.Limits.MemoryBudgetBytes {
		return &FieldError{Path: "limits.memory_budget_bytes", Kind: ErrBudget}
	}
	configuration.RequiredBytes = required
	return nil
}

func normalizeAndValidateQueue(transport *Transport) error {
	if transport.OutputQueueFrames == 0 {
		transport.OutputQueueFrames = DefaultOutputQueueFrames
	}
	if transport.OutputQueueBytes == 0 {
		transport.OutputQueueBytes = DefaultOutputQueueBytes
	}
	if transport.ControlReserveFrames == 0 {
		transport.ControlReserveFrames = DefaultControlReserveFrames
	}
	if transport.ControlReserveBytes == 0 {
		transport.ControlReserveBytes = DefaultControlReserveBytes
	}
	checks := []limitCheck{
		{"output_queue_frames", transport.OutputQueueFrames, MinimumOutputQueueFrames, MaximumOutputQueueFrames},
		{"output_queue_bytes", transport.OutputQueueBytes, MinimumOutputQueueBytes, MaximumOutputQueueBytes},
		{"control_reserve_frames", transport.ControlReserveFrames, 1, MaximumControlReserveFrames},
		{"control_reserve_bytes", transport.ControlReserveBytes, 1, MaximumControlReserveBytes},
	}
	if err := validateTransportLimitChecks(checks); err != nil {
		return err
	}
	if transport.ControlReserveFrames >= transport.OutputQueueFrames ||
		transport.ControlReserveBytes >= transport.OutputQueueBytes ||
		transport.OutputQueueBytes-transport.ControlReserveBytes < protocol.MaxFrameSize {
		return &FieldError{Path: "transport", Kind: ErrFieldValue}
	}
	return nil
}

func validateDelivery(delivery Delivery) error {
	if delivery.Mode == protocol.DeliveryRedundant {
		if delivery.Selection != protocol.PathNone || delivery.Constraints != (policy.Constraints{}) {
			return &FieldError{Path: "delivery", Kind: ErrFieldValue}
		}
		return nil
	}
	if delivery.Mode != protocol.DeliveryAdaptive || (delivery.Selection != protocol.PathFastest && delivery.Selection != protocol.PathDistributed) {
		return &FieldError{Path: "delivery", Kind: ErrFieldValue}
	}
	if delivery.Selection == protocol.PathFastest {
		if delivery.Constraints != (policy.Constraints{}) {
			return &FieldError{Path: "delivery.constraints", Kind: ErrFieldValue}
		}
		return nil
	}
	if !validConstraint(delivery.Constraints.MaxDeliveryDelay) || !validConstraint(delivery.Constraints.MaxDelayGap) ||
		(delivery.Constraints.Fallback != policy.FallbackFastest && delivery.Constraints.Fallback != policy.FallbackPause) {
		return &FieldError{Path: "delivery.constraints", Kind: ErrFieldValue}
	}
	return nil
}

func validConstraint(value time.Duration) bool {
	return value == 0 || value >= policy.MinimumConstraint && value <= policy.MaximumConstraint
}

func validateInterfacePatterns(interfaces Interfaces) error {
	for name, patterns := range map[string][]string{"include": interfaces.Include, "exclude": interfaces.Exclude} {
		if len(patterns) > 64 {
			return &FieldError{Path: "interfaces." + name, Kind: ErrFieldValue}
		}
		for _, pattern := range patterns {
			if len(pattern) < 1 || len(pattern) > 64 {
				return &FieldError{Path: "interfaces." + name, Kind: ErrFieldValue}
			}
			if _, err := pathpkg.Match(pattern, ""); err != nil {
				return &FieldError{Path: "interfaces." + name, Kind: ErrFieldValue}
			}
		}
	}
	return nil
}

func validateStatus(status *Status) error {
	if status == nil {
		return &FieldError{Path: "status", Kind: ErrFieldValue}
	}
	if !status.Enabled {
		if status.Listen != "" || status.BasicAuth != nil {
			return &FieldError{Path: "status.listen", Kind: ErrFieldValue}
		}
		return nil
	}
	if statusapi.ValidateListenAddress(status.Listen) != nil {
		return &FieldError{Path: "status.listen", Kind: ErrFieldValue}
	}
	value, err := validateLiteralEndpoint(status.Listen, "status.listen")
	if err != nil {
		return err
	}
	status.Listen = value
	if status.BasicAuth != nil {
		if err := validateBasicAuth(*status.BasicAuth); err != nil {
			return err
		}
	}
	return nil
}

func validateBasicAuth(authentication BasicAuth) error {
	fields := []struct {
		path     string
		value    string
		username bool
	}{
		{path: "status.basic_auth.username", value: authentication.Username, username: true},
		{path: "status.basic_auth.password", value: authentication.Password},
	}
	for _, field := range fields {
		if !utf8.ValidString(field.value) || len(field.value) < 1 || len(field.value) > 255 || field.username && strings.ContainsRune(field.value, ':') {
			return &FieldError{Path: field.path, Kind: ErrFieldValue}
		}
		for _, character := range []byte(field.value) {
			if character < 0x20 || character == 0x7f {
				return &FieldError{Path: field.path, Kind: ErrFieldValue}
			}
		}
	}
	return nil
}

func validateClientLimits(limits ClientLimits) error {
	checks := []limitCheck{
		{"flows", limits.Flows, 1, 2048}, {"opening_flows", limits.OpeningFlows, 1, 256},
		{"recovering_flows", limits.RecoveringFlows, 1, 1024},
		{"auth_in_progress", limits.AuthInProgress, 1, MaxClientAuthInProgress}, {"socks_connections", limits.SOCKSConnections, 1, 2048},
		{"socks_handshakes", limits.SOCKSHandshakes, 1, 512}, {"socks_per_source", limits.SOCKSPerSource, 1, 2048},
		{"memory_budget_bytes", limits.MemoryBudgetBytes, 1, MaxMemoryBudget},
		{"flow_send_window_bytes", limits.FlowSendWindowBytes, MinimumFlowWindowBytes, MaximumFlowWindowBytes},
		{"flow_receive_window_bytes", limits.FlowReceiveWindowBytes, MinimumFlowWindowBytes, MaximumFlowWindowBytes},
	}
	if err := validateLimitChecksAllowZero(checks, "memory_budget_bytes"); err != nil {
		return err
	}
	if limits.OpeningFlows > limits.Flows || limits.RecoveringFlows > limits.Flows || limits.AuthInProgress > limits.Sessions ||
		limits.SOCKSHandshakes > limits.SOCKSConnections || limits.SOCKSPerSource > limits.SOCKSConnections {
		return &FieldError{Path: "limits", Kind: ErrFieldValue}
	}
	return nil
}

func validateServerLimits(limits ServerLimits) error {
	checks := []limitCheck{
		{"flows", limits.Flows, 1, 8192}, {"per_principal_flows", limits.PerPrincipalFlows, 1, 8192},
		{"opening_flows", limits.OpeningFlows, 1, 512}, {"recovering_flows", limits.RecoveringFlows, 1, 2048},
		{"transport_connections", limits.Sessions, 1, 4096},
		{"transport_connections_per_principal", limits.SessionsPerPrincipal, 1, 4096},
		{"transport_auth_in_progress", limits.AuthInProgress, 1, 512}, {"target_dials", limits.TargetDials, 1, 512},
		{"tombstones", limits.Tombstones, 1, 32768}, {"tombstones_per_principal", limits.TombstonesPerPrincipal, 1, 32768},
		{"rate_limit_keys", limits.RateLimitKeys, 1, 16384}, {"memory_budget_bytes", limits.MemoryBudgetBytes, 1, MaxMemoryBudget},
		{"open_rate_per_minute_per_principal", limits.OpenRatePerMinutePerPrincipal, 1, 60_000},
		{"open_burst_per_principal", limits.OpenBurstPerPrincipal, 1, 512},
		{"open_rate_per_minute_global", limits.OpenRatePerMinuteGlobal, 1, 60_000},
		{"open_burst_global", limits.OpenBurstGlobal, 1, 512},
		{"flow_send_window_bytes", limits.FlowSendWindowBytes, MinimumFlowWindowBytes, MaximumFlowWindowBytes},
		{"flow_receive_window_bytes", limits.FlowReceiveWindowBytes, MinimumFlowWindowBytes, MaximumFlowWindowBytes},
	}
	if err := validateLimitChecksAllowZero(checks, "memory_budget_bytes"); err != nil {
		return err
	}
	if limits.PerPrincipalFlows > limits.Flows || limits.OpeningFlows > limits.Flows || limits.RecoveringFlows > limits.Flows ||
		limits.SessionsPerPrincipal > limits.Sessions || limits.AuthInProgress > limits.Sessions || limits.TargetDials > limits.OpeningFlows ||
		limits.TombstonesPerPrincipal > limits.Tombstones || limits.OpenBurstPerPrincipal > limits.OpenBurstGlobal ||
		limits.OpenBurstGlobal > limits.OpeningFlows || limits.OpenRatePerMinutePerPrincipal > limits.OpenRatePerMinuteGlobal {
		return &FieldError{Path: "limits", Kind: ErrFieldValue}
	}
	return nil
}

func normalizeClientLimits(limits *ClientLimits, lanesPerPath uint64) {
	if limits.FlowSendWindowBytes == 0 {
		limits.FlowSendWindowBytes = DefaultFlowWindowBytes
	}
	if limits.FlowReceiveWindowBytes == 0 {
		limits.FlowReceiveWindowBytes = DefaultFlowWindowBytes
	}
	if limits.Flows == 0 {
		limits.Flows = 2048
	}
	if limits.OpeningFlows == 0 {
		limits.OpeningFlows = min(limits.Flows, 256)
	}
	if limits.RecoveringFlows == 0 {
		limits.RecoveringFlows = min(limits.Flows, 1024)
	}
	limits.Sessions = 64 * lanesPerPath
	if limits.AuthInProgress == 0 {
		limits.AuthInProgress = min(limits.Sessions, MaxClientAuthInProgress)
	}
	if limits.SOCKSConnections == 0 {
		limits.SOCKSConnections = 2048
	}
	if limits.SOCKSHandshakes == 0 {
		limits.SOCKSHandshakes = min(limits.SOCKSConnections, 512)
	}
	if limits.SOCKSPerSource == 0 {
		limits.SOCKSPerSource = limits.SOCKSConnections
	}
}

func normalizeServerLimits(limits *ServerLimits) {
	if limits.FlowSendWindowBytes == 0 {
		limits.FlowSendWindowBytes = DefaultFlowWindowBytes
	}
	if limits.FlowReceiveWindowBytes == 0 {
		limits.FlowReceiveWindowBytes = DefaultFlowWindowBytes
	}
	if limits.Flows == 0 {
		limits.Flows = 8192
	}
	if limits.PerPrincipalFlows == 0 {
		limits.PerPrincipalFlows = limits.Flows
	}
	if limits.OpeningFlows == 0 {
		limits.OpeningFlows = min(limits.Flows, 512)
	}
	if limits.RecoveringFlows == 0 {
		limits.RecoveringFlows = min(limits.Flows, 2048)
	}
	if limits.Sessions == 0 {
		limits.Sessions = 4096
	}
	if limits.SessionsPerPrincipal == 0 {
		limits.SessionsPerPrincipal = limits.Sessions
	}
	if limits.AuthInProgress == 0 {
		limits.AuthInProgress = min(limits.Sessions, 512)
	}
	if limits.TargetDials == 0 {
		limits.TargetDials = min(limits.OpeningFlows, 512)
	}
	if limits.Tombstones == 0 {
		limits.Tombstones = 32768
	}
	if limits.TombstonesPerPrincipal == 0 {
		limits.TombstonesPerPrincipal = limits.Tombstones
	}
	if limits.RateLimitKeys == 0 {
		limits.RateLimitKeys = 16384
	}
	if limits.OpenRatePerMinutePerPrincipal == 0 {
		limits.OpenRatePerMinutePerPrincipal = 1_000
	}
	if limits.OpenBurstPerPrincipal == 0 {
		limits.OpenBurstPerPrincipal = min(limits.OpeningFlows, 256)
	}
	if limits.OpenRatePerMinuteGlobal == 0 {
		limits.OpenRatePerMinuteGlobal = 10_000
	}
	if limits.OpenBurstGlobal == 0 {
		limits.OpenBurstGlobal = limits.OpeningFlows
	}
}

type limitCheck struct {
	name    string
	value   uint64
	minimum uint64
	maximum uint64
}

func validateTransportLimitChecks(checks []limitCheck) error {
	for _, check := range checks {
		if check.value < check.minimum || check.value > check.maximum {
			return &FieldError{Path: "transport." + check.name, Kind: ErrFieldValue}
		}
	}
	return nil
}

func validateLimitChecks(checks []limitCheck) error {
	for _, check := range checks {
		if check.value < check.minimum || check.value > check.maximum {
			return &FieldError{Path: "limits." + check.name, Kind: ErrFieldValue}
		}
	}
	return nil
}

func validateLimitChecksAllowZero(checks []limitCheck, zeroName string) error {
	for _, check := range checks {
		if check.name == zeroName && check.value == 0 {
			continue
		}
		if check.value < check.minimum || check.value > check.maximum {
			return &FieldError{Path: "limits." + check.name, Kind: ErrFieldValue}
		}
	}
	return nil
}

func validateDeadlines(deadlines Deadlines, client bool) error {
	checks := []durationCheck{
		{"dial", deadlines.Dial, time.Second, 15 * time.Second},
		{"frame_total", deadlines.FrameTotal, 5 * time.Second, 30 * time.Second},
		{"frame_no_progress", deadlines.FrameNoProgress, time.Second, 30 * time.Second},
		{"drain_cleanup", deadlines.DrainCleanup, time.Second, 30 * time.Second},
	}
	if client {
		checks = append(checks,
			durationCheck{"socks_greeting", deadlines.SOCKSGreeting, time.Second, 30 * time.Second},
			durationCheck{"socks_request", deadlines.SOCKSRequest, time.Second, 30 * time.Second},
		)
	} else if deadlines.SOCKSGreeting != 0 || deadlines.SOCKSRequest != 0 {
		return &FieldError{Path: "deadlines", Kind: ErrFieldValue}
	}
	for _, check := range checks {
		if check.value < check.minimum || check.value > check.maximum {
			return &FieldError{Path: "deadlines." + check.name, Kind: ErrFieldValue}
		}
	}
	if deadlines.FrameNoProgress > deadlines.FrameTotal {
		return &FieldError{Path: "deadlines.frame_no_progress", Kind: ErrFieldValue}
	}
	return nil
}

type durationCheck struct {
	name    string
	value   time.Duration
	minimum time.Duration
	maximum time.Duration
}

func validateLiteralEndpoint(value, path string) (string, error) {
	host, port, err := splitEndpoint(value, path)
	if err != nil {
		return "", err
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !address.IsValid() || address.Zone() != "" {
		return "", &FieldError{Path: path, Kind: ErrFieldValue}
	}
	address = address.Unmap()
	return net.JoinHostPort(address.String(), port), nil
}

func validateClientEndpoint(value, path string) (string, error) {
	host, port, err := splitEndpoint(value, path)
	if err != nil {
		return "", err
	}
	if address, parseErr := netip.ParseAddr(host); parseErr == nil {
		if !address.IsValid() || address.Zone() != "" {
			return "", &FieldError{Path: path, Kind: ErrFieldValue}
		}
		address = address.Unmap()
		if address.IsUnspecified() {
			return "", &FieldError{Path: path, Kind: ErrFieldValue}
		}
		return net.JoinHostPort(address.String(), port), nil
	}
	if !validDNSHost(host) {
		return "", &FieldError{Path: path, Kind: ErrFieldValue}
	}
	return net.JoinHostPort(strings.ToLower(host), port), nil
}

func splitEndpoint(value, path string) (string, string, error) {
	host, port, err := net.SplitHostPort(value)
	if err != nil || host == "" || port == "" {
		return "", "", &FieldError{Path: path, Kind: ErrFieldValue}
	}
	parsed, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsed == 0 {
		return "", "", &FieldError{Path: path, Kind: ErrFieldValue}
	}
	return host, strconv.FormatUint(parsed, 10), nil
}

func validDNSHost(host string) bool {
	if len(host) < 1 || len(host) > 253 || strings.HasSuffix(host, ".") {
		return false
	}
	lower := strings.ToLower(host)
	if !bytes.Equal([]byte(lower), []byte(strings.Map(func(character rune) rune {
		if character > 127 {
			return -1
		}
		return character
	}, lower))) {
		return false
	}
	for _, label := range strings.Split(lower, ".") {
		if len(label) < 1 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range []byte(label) {
			if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-') {
				return false
			}
		}
	}
	return true
}
