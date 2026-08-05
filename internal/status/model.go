package status

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"sort"
	"time"

	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

const (
	MaxInterfaces         = 64
	MaxInterfaceAddresses = 8
	MaxSessions           = 4096
	MaxFlows              = 8192
	MaxTerminalSummaries  = 1024
	MaxStatusEvents       = 1024
	MaxStatusConnections  = 8
	MaxStatusResponse     = 256 << 10
	TerminalRetention     = 15 * time.Minute
	HashBytes             = 12
	MaxSafeLabelBytes     = 64
)

var ErrInvalidModel = errors.New("status: invalid model")

type InterfaceReason uint8

const (
	InterfaceEligible InterfaceReason = iota + 1
	InterfaceExcluded
	InterfaceNotIncluded
	InterfaceDown
	InterfaceNoAddress
	InterfaceUnsafeAddress
)

type SessionState uint8

const (
	SessionDialing SessionState = iota + 1
	SessionAuthenticating
	SessionReady
	SessionBackoff
	SessionClosed
)

type FlowState uint8

const (
	FlowOpening FlowState = iota + 1
	FlowAwaitingAttachment
	FlowRelaying
	FlowRecovering
	FlowClosing
	FlowClosed
	FlowResetting
	FlowReset
)

type TransitionReason uint8

const (
	ReasonNone TransitionReason = iota
	ReasonStarted
	ReasonPathAdded
	ReasonPathRemoved
	ReasonAuthenticationFailed
	ReasonProtocolConflict
	ReasonResourceLimit
	ReasonLocalIOFailure
	ReasonDeadlineExceeded
	ReasonCancelled
	ReasonRemoteReset
	ReasonCompleted
	ReasonInternalFailure
)

type AdaptiveState uint8

const (
	AdaptiveSingle AdaptiveState = iota + 1
	AdaptiveTargeted
	AdaptiveFull
	AdaptiveWaiting
)

type AdaptiveTransition uint8

const (
	AdaptiveTransitionNone AdaptiveTransition = iota
	AdaptiveTransitionAcknowledgementGap
	AdaptiveTransitionRetryEscalated
	AdaptiveTransitionStableAcknowledgement
	AdaptiveTransitionAllAttachmentsLost
	AdaptiveTransitionAttachmentRestored
)

type Role uint8

const (
	RoleClient Role = iota + 1
	RoleServer
)

type Resources struct {
	Flows                 uint64 `json:"flows"`
	Sessions              uint64 `json:"sessions"`
	SOCKSConnections      uint64 `json:"socks_connections"`
	OpeningFlows          uint64 `json:"opening_flows"`
	RecoveringFlows       uint64 `json:"recovering_flows"`
	PendingTargetDials    uint64 `json:"pending_target_dials"`
	Tombstones            uint64 `json:"tombstones"`
	ReservedBytes         uint64 `json:"reserved_bytes"`
	MaxSessions           uint64 `json:"max_sessions,omitempty"`
	MaxFlows              uint64 `json:"max_flows,omitempty"`
	MaxSOCKSConnections   uint64 `json:"max_socks_connections,omitempty"`
	MaxPendingTargetDials uint64 `json:"max_pending_target_dials,omitempty"`
	MaxTombstones         uint64 `json:"max_tombstones,omitempty"`
}

// Rejected counts admission denials since daemon start. Each counter
// saturates and is reset only on process restart.
type Rejected struct {
	Flows            uint64 `json:"flows"`
	Sessions         uint64 `json:"sessions"`
	SOCKSConnections uint64 `json:"socks_connections"`
}

type Counters struct {
	FramesSent          uint64 `json:"frames_sent"`
	FramesReceived      uint64 `json:"frames_received"`
	BytesSent           uint64 `json:"bytes_sent"`
	BytesReceived       uint64 `json:"bytes_received"`
	RetransmittedBytes  uint64 `json:"retransmitted_bytes"`
	RedundantBytes      uint64 `json:"redundant_bytes"`
	DroppedStatusEvents uint64 `json:"dropped_status_events"`
}

type Interface struct {
	Index     int             `json:"index"`
	Name      string          `json:"name"`
	Addresses []string        `json:"addresses,omitempty"`
	Reason    InterfaceReason `json:"reason"`
}

type Quality struct {
	SmoothedRTTMicros             uint64  `json:"smoothed_rtt_micros"`
	RetryMicros                   uint64  `json:"retry_micros"`
	CapacityBytesSec              uint64  `json:"capacity_bytes_sec"`
	QueuedBytes                   uint64  `json:"queued_bytes"`
	InFlightBytes                 uint64  `json:"in_flight_bytes"`
	StallPenaltyMicros            uint64  `json:"stall_penalty_micros"`
	DataSampleFresh               bool    `json:"data_sample_fresh"`
	DataSampleAgeMillis           *uint64 `json:"data_sample_age_ms"`
	LastDataCapacityBytesSec      uint64  `json:"last_data_capacity_bytes_sec"`
	ScheduledDataPayloadBytes     uint64  `json:"scheduled_data_payload_bytes"`
	WrittenDataPayloadBytes       uint64  `json:"written_data_payload_bytes"`
	EligibleAckedDataPayloadBytes uint64  `json:"eligible_acked_data_payload_bytes"`
	ReceivedDataPayloadBytes      uint64  `json:"received_data_payload_bytes"`
	DataQueueFrames               uint32  `json:"data_queue_frames"`
	ActiveDataFlows               uint32  `json:"active_data_flows"`
	ProbeSamples                  uint64  `json:"-"`
}

type Session struct {
	IDHash         string           `json:"id"`
	ConnectionID   string           `json:"connection_id,omitempty"`
	Transport      string           `json:"transport"`
	Interface      string           `json:"interface"`
	LocalAddress   string           `json:"local_address"`
	LocalEndpoint  string           `json:"local_endpoint,omitempty"`
	RemoteEndpoint string           `json:"remote_endpoint,omitempty"`
	PrincipalHash  string           `json:"principal,omitempty"`
	State          SessionState     `json:"state"`
	Reason         TransitionReason `json:"reason"`
	StateSince     time.Time        `json:"state_since,omitempty"`
	LastProbeAt    time.Time        `json:"last_probe_at,omitempty"`
	Reconnects     uint64           `json:"reconnects,omitempty"`
	Quality        Quality          `json:"quality"`
	Fastest        bool             `json:"fastest,omitempty"`
}

type Flow struct {
	IDHash                string                 `json:"id"`
	FlowID                string                 `json:"flow_id,omitempty"`
	TargetType            protocol.AddressType   `json:"target_type"`
	TargetHash            string                 `json:"target"`
	DeliveryMode          protocol.DeliveryMode  `json:"delivery_mode"`
	PathSelection         protocol.PathSelection `json:"path_selection"`
	AdaptiveState         AdaptiveState          `json:"adaptive_state"`
	AdaptiveTransition    AdaptiveTransition     `json:"adaptive_transition,omitempty"`
	State                 FlowState              `json:"state"`
	Reason                TransitionReason       `json:"reason"`
	StartedAt             time.Time              `json:"started_at,omitempty"`
	StateSince            time.Time              `json:"state_since,omitempty"`
	PublishedAttachments  uint32                 `json:"published_attachments"`
	PolicyAttachments     uint32                 `json:"policy_attachments"`
	PreferredConnectionID string                 `json:"preferred_connection_id,omitempty"`
	UnacknowledgedBytes   uint64                 `json:"unacknowledged_bytes"`
	TxAllocatedOffset     uint64                 `json:"tx_allocated_offset"`
	TxAcknowledged        uint64                 `json:"tx_acknowledged_offset"`
	RxWrittenOffset       uint64                 `json:"rx_written_offset"`
	RetransmittedBytes    uint64                 `json:"retransmitted_bytes"`
	RedundantBytes        uint64                 `json:"redundant_bytes"`
	RecoveryCount         uint64                 `json:"recovery_count,omitempty"`
	RecoveryMicros        uint64                 `json:"recovery_micros,omitempty"`
}

type Terminal struct {
	IDHash         string                 `json:"id"`
	FlowID         string                 `json:"flow_id,omitempty"`
	State          FlowState              `json:"state"`
	Reason         TransitionReason       `json:"reason"`
	DeliveryMode   protocol.DeliveryMode  `json:"delivery_mode,omitempty"`
	PathSelection  protocol.PathSelection `json:"path_selection,omitempty"`
	StartedAt      time.Time              `json:"started_at,omitempty"`
	FinishedAt     time.Time              `json:"finished_at"`
	RecoveryCount  uint64                 `json:"recovery_count,omitempty"`
	RecoveryMicros uint64                 `json:"recovery_micros,omitempty"`
}

type Snapshot struct {
	GeneratedAt time.Time   `json:"generated_at"`
	Role        Role        `json:"role,omitempty"`
	Healthy     bool        `json:"healthy"`
	Resources   Resources   `json:"resources"`
	Rejected    Rejected    `json:"rejected"`
	Counters    Counters    `json:"counters"`
	Interfaces  []Interface `json:"interfaces,omitempty"`
	Sessions    []Session   `json:"sessions,omitempty"`
	Flows       []Flow      `json:"flows,omitempty"`
	Terminals   []Terminal  `json:"terminals,omitempty"`
}

type Hasher struct {
	key [32]byte
}

func NewHasher(key [32]byte) (*Hasher, error) {
	var aggregate byte
	for _, value := range key {
		aggregate |= value
	}
	if aggregate == 0 {
		return nil, ErrInvalidModel
	}
	return &Hasher{key: key}, nil
}

func (hasher *Hasher) FlowID(flowID protocol.FlowID) string {
	if hasher == nil {
		return ""
	}
	return hasher.sum("via status flow v1\x00", flowID[:])
}

func (hasher *Hasher) SessionID(generation uint64) string {
	if hasher == nil || generation == 0 {
		return ""
	}
	var encoded [8]byte
	for index := 7; index >= 0; index-- {
		encoded[index] = byte(generation)
		generation >>= 8
	}
	return hasher.sum("via status session v1\x00", encoded[:])
}

func (hasher *Hasher) Principal(principalID string) string {
	if hasher == nil || !protocol.ValidPrincipalID(principalID) {
		return ""
	}
	return hasher.sum("via status principal v1\x00", []byte(principalID))
}

func (hasher *Hasher) Target(target protocol.Target) string {
	if hasher == nil || protocol.ValidateTarget(target) != nil {
		return ""
	}
	encoded, err := protocol.EncodeMessage(protocol.Open{
		FlowID: protocol.FlowID{1}, OpenToken: protocol.OpenToken{1},
		DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest, Target: target,
	})
	if err != nil {
		return ""
	}
	return hasher.sum("via status target v1\x00", encoded[8+50:])
}

func (hasher *Hasher) sum(domain string, value []byte) string {
	mac := hmac.New(sha256.New, hasher.key[:])
	_, _ = mac.Write([]byte(domain))
	_, _ = mac.Write(value)
	return hex.EncodeToString(mac.Sum(nil)[:HashBytes])
}

func cloneSnapshot(source Snapshot) Snapshot {
	result := source
	result.Interfaces = append([]Interface(nil), source.Interfaces...)
	for index := range result.Interfaces {
		result.Interfaces[index].Addresses = append([]string(nil), source.Interfaces[index].Addresses...)
	}
	result.Sessions = append([]Session(nil), source.Sessions...)
	for index := range result.Sessions {
		if source.Sessions[index].Quality.DataSampleAgeMillis != nil {
			age := *source.Sessions[index].Quality.DataSampleAgeMillis
			result.Sessions[index].Quality.DataSampleAgeMillis = &age
		}
	}
	result.Flows = append([]Flow(nil), source.Flows...)
	result.Terminals = append([]Terminal(nil), source.Terminals...)
	return result
}

func normalizeInterface(value Interface) (Interface, error) {
	if value.Index < 1 || !safeLabel(value.Name) || value.Reason < InterfaceEligible || value.Reason > InterfaceUnsafeAddress || len(value.Addresses) > MaxInterfaceAddresses {
		return Interface{}, ErrInvalidModel
	}
	seen := make(map[netip.Addr]struct{}, len(value.Addresses))
	addresses := make([]string, 0, len(value.Addresses))
	for _, raw := range value.Addresses {
		address, err := netip.ParseAddr(raw)
		if err != nil || !address.IsValid() || address.Zone() != "" {
			return Interface{}, ErrInvalidModel
		}
		address = address.Unmap()
		if _, duplicate := seen[address]; duplicate {
			continue
		}
		seen[address] = struct{}{}
		addresses = append(addresses, address.String())
	}
	sort.Strings(addresses)
	value.Addresses = addresses
	return value, nil
}

func validSession(value Session) bool {
	if !validHash(value.IDHash) || !transport.ValidName(value.Transport) || !safeLabel(value.Interface) || value.State < SessionDialing || value.State > SessionClosed || !validReason(value.Reason) {
		return false
	}
	address, err := netip.ParseAddr(value.LocalAddress)
	return err == nil && address.IsValid() && address.Zone() == "" && validOptionalHash(value.ConnectionID) &&
		validOptionalHash(value.PrincipalHash) && validOptionalEndpoint(value.LocalEndpoint) &&
		validOptionalEndpoint(value.RemoteEndpoint) &&
		(value.LastProbeAt.IsZero() || value.StateSince.IsZero() || !value.LastProbeAt.Before(value.StateSince))
}

func validFlow(value Flow) bool {
	if !validHash(value.IDHash) || !validOptionalHash(value.FlowID) || !validHash(value.TargetHash) ||
		!validOptionalHash(value.PreferredConnectionID) || !validReason(value.Reason) ||
		value.State < FlowOpening || value.State > FlowReset || value.AdaptiveState < AdaptiveSingle || value.AdaptiveState > AdaptiveWaiting ||
		value.AdaptiveTransition > AdaptiveTransitionAttachmentRestored || value.PolicyAttachments > value.PublishedAttachments ||
		(value.UnacknowledgedBytes != 0 && (value.TxAcknowledged > value.TxAllocatedOffset || value.UnacknowledgedBytes != value.TxAllocatedOffset-value.TxAcknowledged)) ||
		(!value.StartedAt.IsZero() && !value.StateSince.IsZero() && value.StateSince.Before(value.StartedAt)) {
		return false
	}
	if value.TargetType != protocol.AddressIPv4 && value.TargetType != protocol.AddressIPv6 && value.TargetType != protocol.AddressDNS {
		return false
	}
	return value.DeliveryMode == protocol.DeliveryRedundant && value.PathSelection == protocol.PathNone ||
		value.DeliveryMode == protocol.DeliveryAdaptive && (value.PathSelection == protocol.PathFastest || value.PathSelection == protocol.PathDistributed)
}

func validTerminal(value Terminal) bool {
	return validHash(value.IDHash) && validOptionalHash(value.FlowID) &&
		(value.State == FlowClosed || value.State == FlowReset) && validReason(value.Reason) && !value.FinishedAt.IsZero() &&
		(value.StartedAt.IsZero() || !value.FinishedAt.Before(value.StartedAt)) &&
		(value.DeliveryMode == 0 && value.PathSelection == 0 || validDeliveryPolicy(value.DeliveryMode, value.PathSelection))
}

func safeLabel(value string) bool {
	if len(value) < 1 || len(value) > MaxSafeLabelBytes {
		return false
	}
	for _, character := range []byte(value) {
		if character < 0x21 || character > 0x7e || character == '<' || character == '>' || character == '&' || character == '"' || character == '\'' {
			return false
		}
	}
	return true
}

func validHash(value string) bool {
	if len(value) != 2*HashBytes {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validOptionalHash(value string) bool {
	return value == "" || validHash(value)
}

func validOptionalEndpoint(value string) bool {
	if value == "" {
		return true
	}
	address, err := netip.ParseAddrPort(value)
	return err == nil && address.IsValid() && address.Port() != 0 && address.Addr().Zone() == ""
}

func validDeliveryPolicy(mode protocol.DeliveryMode, selection protocol.PathSelection) bool {
	return mode == protocol.DeliveryRedundant && selection == protocol.PathNone ||
		mode == protocol.DeliveryAdaptive && (selection == protocol.PathFastest || selection == protocol.PathDistributed)
}

func validReason(reason TransitionReason) bool {
	return reason <= ReasonInternalFailure
}
