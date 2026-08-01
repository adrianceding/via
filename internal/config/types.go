package config

import (
	"time"

	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
)

const (
	MaxConfigBytes    = 1 << 20
	MaxPrincipals     = 4096
	MaxClientSessions = 64
	MaxMemoryBudget   = uint64(1<<63 - 1)
)

type PSK [32]byte

type Transport struct {
	Type    string
	Address string
	Listen  string
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
	Flows             uint64
	OpeningFlows      uint64
	RecoveringFlows   uint64
	Sessions          uint64
	AuthInProgress    uint64
	SOCKSConnections  uint64
	SOCKSHandshakes   uint64
	SOCKSPerSource    uint64
	MemoryBudgetBytes uint64
}

type ServerLimits struct {
	Flows                  uint64
	PerPrincipalFlows      uint64
	OpeningFlows           uint64
	RecoveringFlows        uint64
	Sessions               uint64
	SessionsPerPrincipal   uint64
	AuthInProgress         uint64
	TargetDials            uint64
	Tombstones             uint64
	TombstonesPerPrincipal uint64
	RateLimitKeys          uint64
	MemoryBudgetBytes      uint64
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
		Sessions: MaxClientSessions, AuthInProgress: MaxClientSessions,
		SOCKSConnections: 2048, SOCKSHandshakes: 512, SOCKSPerSource: 2048,
	}
}

func defaultServerLimits() ServerLimits {
	return ServerLimits{
		Flows: 8192, PerPrincipalFlows: 8192, OpeningFlows: 512, RecoveringFlows: 2048,
		Sessions: 4096, SessionsPerPrincipal: 4096, AuthInProgress: 512, TargetDials: 512,
		Tombstones: 32768, TombstonesPerPrincipal: 32768, RateLimitKeys: 16384,
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
