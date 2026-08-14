package config

import (
	"time"

	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
)

const (
	MaxConfigBytes                     = 1 << 20
	MaxPrincipals                      = 4096
	MinimumLanesPerPath                = 1
	MaximumLanesPerPath                = 64
	MaxClientSessions                  = 64 * MaximumLanesPerPath
	MaxClientAuthInProgress            = 64
	MaxMemoryBudget                    = uint64(1<<63 - 1)
	DefaultFlowWindowBytes      uint64 = 320 << 10
	MinimumFlowWindowBytes      uint64 = 65512
	MaximumFlowWindowBytes      uint64 = 4 << 20
	DefaultTCPWriteBufferBytes  uint64 = 128 << 10
	MinimumTCPWriteBufferBytes  uint64 = 65512
	MaximumTCPWriteBufferBytes  uint64 = 16 << 20
	DefaultOutputQueueFrames    uint64 = 256
	MinimumOutputQueueFrames    uint64 = 32
	MaximumOutputQueueFrames    uint64 = 4096
	DefaultOutputQueueBytes     uint64 = 1 << 20
	MinimumOutputQueueBytes     uint64 = 1 << 20
	MaximumOutputQueueBytes     uint64 = 16 << 20
	DefaultControlReserveFrames uint64 = 16
	MaximumControlReserveFrames uint64 = 1024
	DefaultControlReserveBytes  uint64 = 16 << 10
	MaximumControlReserveBytes  uint64 = 1 << 20
)

type PSK [32]byte

type Transport struct {
	Type                 string
	Address              string
	Listen               string
	LanesPerPath         uint64
	WriteBufferBytes     uint64
	OutputQueueFrames    uint64
	OutputQueueBytes     uint64
	ControlReserveFrames uint64
	ControlReserveBytes  uint64
}

type Delivery struct {
	Mode        protocol.DeliveryMode
	Selection   protocol.PathSelection
	Constraints policy.Constraints
}

type Interfaces struct {
	Include []string
	Exclude []string
}

type SOCKSAuth struct {
	Username string
	Password string
}

type Status struct {
	Enabled   bool
	Listen    string
	BasicAuth *BasicAuth
}

type BasicAuth struct {
	Username string
	Password string
}

type ClientLimits struct {
	Flows                  uint64
	OpeningFlows           uint64
	RecoveringFlows        uint64
	Sessions               uint64
	AuthInProgress         uint64
	SOCKSConnections       uint64
	SOCKSHandshakes        uint64
	SOCKSPerSource         uint64
	FlowSendWindowBytes    uint64
	FlowReceiveWindowBytes uint64
	MemoryBudgetBytes      uint64
}

func (limits ClientLimits) flowWindows() (uint64, uint64) {
	return limits.FlowSendWindowBytes, limits.FlowReceiveWindowBytes
}

type ServerLimits struct {
	Flows                         uint64
	PerPrincipalFlows             uint64
	OpeningFlows                  uint64
	RecoveringFlows               uint64
	Sessions                      uint64
	SessionsPerPrincipal          uint64
	AuthInProgress                uint64
	TargetDials                   uint64
	Tombstones                    uint64
	TombstonesPerPrincipal        uint64
	RateLimitKeys                 uint64
	OpenRatePerMinutePerPrincipal uint64
	OpenBurstPerPrincipal         uint64
	OpenRatePerMinuteGlobal       uint64
	OpenBurstGlobal               uint64
	FlowSendWindowBytes           uint64
	FlowReceiveWindowBytes        uint64
	MemoryBudgetBytes             uint64
}

func (limits ServerLimits) flowWindows() (uint64, uint64) {
	return limits.FlowSendWindowBytes, limits.FlowReceiveWindowBytes
}

type Deadlines struct {
	Dial            time.Duration
	FrameTotal      time.Duration
	FrameNoProgress time.Duration
	SOCKSGreeting   time.Duration
	SOCKSRequest    time.Duration
	DrainCleanup    time.Duration
}

type Client struct {
	SOCKSListen   string
	SOCKSAuth     *SOCKSAuth
	Transport     Transport
	Delivery      Delivery
	Interfaces    Interfaces
	PrincipalID   string
	PSK           PSK
	Status        Status
	Limits        ClientLimits
	Deadlines     Deadlines
	RequiredBytes uint64
}

type Principal struct {
	ID  string
	PSK PSK
}

type Server struct {
	Transport     Transport
	Status        Status
	Principals    []Principal
	Limits        ServerLimits
	Deadlines     Deadlines
	RequiredBytes uint64
}

func defaultDelivery() Delivery {
	return Delivery{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest}
}

func defaultClientLimits() ClientLimits {
	return ClientLimits{
		Flows: 2048, OpeningFlows: 256, RecoveringFlows: 1024,
		Sessions: 64, AuthInProgress: MaxClientAuthInProgress,
		SOCKSConnections: 2048, SOCKSHandshakes: 512, SOCKSPerSource: 2048,
		FlowSendWindowBytes: DefaultFlowWindowBytes, FlowReceiveWindowBytes: DefaultFlowWindowBytes,
	}
}

func defaultServerLimits() ServerLimits {
	return ServerLimits{
		Flows: 8192, PerPrincipalFlows: 8192, OpeningFlows: 512, RecoveringFlows: 2048,
		Sessions: 4096, SessionsPerPrincipal: 4096, AuthInProgress: 512, TargetDials: 512,
		Tombstones: 32768, TombstonesPerPrincipal: 32768, RateLimitKeys: 16384,
		OpenRatePerMinutePerPrincipal: 1_000, OpenBurstPerPrincipal: 256,
		OpenRatePerMinuteGlobal: 10_000, OpenBurstGlobal: 512,
		FlowSendWindowBytes: DefaultFlowWindowBytes, FlowReceiveWindowBytes: DefaultFlowWindowBytes,
	}
}

func defaultClientDeadlines() Deadlines {
	return Deadlines{
		Dial: 10 * time.Second, FrameTotal: 30 * time.Second, FrameNoProgress: 5 * time.Second,
		SOCKSGreeting: 5 * time.Second, SOCKSRequest: 5 * time.Second, DrainCleanup: 5 * time.Second,
	}
}

func defaultServerDeadlines() Deadlines {
	return Deadlines{
		Dial: 10 * time.Second, FrameTotal: 30 * time.Second,
		FrameNoProgress: 5 * time.Second, DrainCleanup: 5 * time.Second,
	}
}
