package client

import (
	"bytes"
	"errors"
	"math"
	"net"
	"net/netip"
	"sort"
	"time"

	networkpath "github.com/adrianceding/via/internal/path"
	"github.com/adrianceding/via/internal/transport"
)

const (
	DefaultSessions          = networkpath.MaxInterfaces
	MaxSessions              = networkpath.MaxInterfaces
	DefaultAuthInProgress    = networkpath.MaxInterfaces
	SessionInitialBackoff    = time.Second
	SessionMaximumBackoff    = 2 * time.Second
	MaxSessionPathCandidates = networkpath.MaxInterfaces * networkpath.MaxAddressesPerInterface
)

var (
	ErrInvalidSessionManager = errors.New("client: invalid session manager configuration")
	ErrInvalidSessionEvent   = errors.New("client: invalid session manager event")
	ErrSessionGeneration     = errors.New("client: session generation exhausted")
)

type ManagedSessionState uint8

const (
	ManagedSessionWaiting ManagedSessionState = iota + 1
	ManagedSessionDialing
	ManagedSessionAuthenticating
	ManagedSessionReady
	ManagedSessionBackoff
)

type SessionManagerConfig struct {
	DesiredSessions int
	AuthInProgress  int
	RemoteEndpoint  string
	QueueLimits     transport.QueueLimits
}

type SessionManagerEventKind uint8

const (
	SessionPathsChanged SessionManagerEventKind = iota + 1
	SessionDialCompleted
	SessionAuthenticationCompleted
	SessionConnectionLost
	SessionBackoffExpired
	SessionShutdown
)

type SessionManagerEvent struct {
	Kind       SessionManagerEventKind
	Candidates []networkpath.Candidate
	Generation uint64
	Connection transport.Connection
	Succeeded  bool
	Err        error
}

type SessionManagerActionKind uint8

const (
	SessionActionDial SessionManagerActionKind = iota + 1
	SessionActionStartAuthentication
	SessionActionReady
	SessionActionLost
	SessionActionCloseConnection
	SessionActionArmBackoff
	SessionActionCancelBackoff
	SessionActionCancelDial
)

type SessionManagerAction struct {
	Kind       SessionManagerActionKind
	Generation uint64
	Candidate  networkpath.Candidate
	Options    transport.DialOptions
	Connection transport.Connection
	After      time.Duration
}

type ManagedSessionSnapshot struct {
	Generation         uint64
	Candidate          networkpath.Candidate
	ActualLocalAddress netip.Addr
	LocalEndpoint      string
	RemoteEndpoint     string
	State              ManagedSessionState
	Failures           uint8
	Reconnects         uint64
}

type SessionManagerSnapshot struct {
	ShuttingDown bool
	Sessions     []ManagedSessionSnapshot
}

type managedSession struct {
	candidate   networkpath.Candidate
	state       ManagedSessionState
	generation  uint64
	failures    uint8
	reconnects  uint64
	connection  transport.Connection
	actualLocal netip.Addr
}

// SessionManager is the sole state owner between path candidates and long-lived
// transport sessions. Handle only emits actions; external executors must perform
// dialing, authentication, and connection closure and feed the results back.
type SessionManager struct {
	config         SessionManagerConfig
	slots          map[networkpath.Candidate]*managedSession
	nextGeneration uint64
	shuttingDown   bool
}

func NewSessionManager(config SessionManagerConfig) (*SessionManager, error) {
	if config.DesiredSessions < 1 || config.DesiredSessions > MaxSessions ||
		config.AuthInProgress < 1 || config.AuthInProgress > config.DesiredSessions ||
		config.RemoteEndpoint == "" || transport.ValidateV1QueueLimits(config.QueueLimits) != nil {
		return nil, ErrInvalidSessionManager
	}
	return &SessionManager{
		config: config,
		slots:  make(map[networkpath.Candidate]*managedSession, config.DesiredSessions),
	}, nil
}

func (manager *SessionManager) Snapshot() SessionManagerSnapshot {
	if manager == nil {
		return SessionManagerSnapshot{ShuttingDown: true}
	}
	snapshot := SessionManagerSnapshot{ShuttingDown: manager.shuttingDown}
	for _, slot := range manager.sortedSlots() {
		entry := ManagedSessionSnapshot{
			Generation:         slot.generation,
			Candidate:          slot.candidate,
			ActualLocalAddress: slot.actualLocal,
			State:              slot.state,
			Failures:           slot.failures,
			Reconnects:         slot.reconnects,
		}
		if slot.connection != nil {
			entry.LocalEndpoint = slot.connection.LocalEndpoint()
			entry.RemoteEndpoint = slot.connection.RemoteEndpoint()
		}
		snapshot.Sessions = append(snapshot.Sessions, entry)
	}
	return snapshot
}

func (manager *SessionManager) Handle(event SessionManagerEvent) ([]SessionManagerAction, error) {
	if manager == nil {
		return nil, ErrInvalidSessionManager
	}
	if manager.shuttingDown && event.Kind != SessionShutdown {
		if event.Kind == SessionDialCompleted && event.Connection != nil {
			return []SessionManagerAction{{Kind: SessionActionCloseConnection, Generation: event.Generation, Connection: event.Connection}}, nil
		}
		return nil, nil
	}
	switch event.Kind {
	case SessionPathsChanged:
		return manager.changePaths(event.Candidates)
	case SessionDialCompleted:
		return manager.completeDial(event)
	case SessionAuthenticationCompleted:
		return manager.completeAuthentication(event)
	case SessionConnectionLost:
		return manager.connectionLost(event.Generation)
	case SessionBackoffExpired:
		return manager.backoffExpired(event.Generation)
	case SessionShutdown:
		return manager.shutdown(), nil
	default:
		return nil, ErrInvalidSessionEvent
	}
}

func (manager *SessionManager) changePaths(candidates []networkpath.Candidate) ([]SessionManagerAction, error) {
	selected, err := selectSessionCandidates(candidates, manager.config.DesiredSessions)
	if err != nil {
		return nil, err
	}
	wanted := make(map[networkpath.Candidate]struct{}, len(selected))
	for _, candidate := range selected {
		wanted[candidate] = struct{}{}
	}
	var actions []SessionManagerAction
	for _, slot := range manager.sortedSlots() {
		if _, keep := wanted[slot.candidate]; keep {
			continue
		}
		actions = append(actions, manager.removeSlot(slot)...)
	}
	for _, candidate := range selected {
		if manager.slots[candidate] == nil {
			manager.slots[candidate] = &managedSession{candidate: candidate, state: ManagedSessionWaiting}
		}
	}
	pumped, pumpErr := manager.pump()
	return append(actions, pumped...), pumpErr
}

func (manager *SessionManager) completeDial(event SessionManagerEvent) ([]SessionManagerAction, error) {
	slot := manager.slotByGeneration(event.Generation)
	if slot == nil || slot.state != ManagedSessionDialing {
		if event.Connection != nil {
			return []SessionManagerAction{{Kind: SessionActionCloseConnection, Generation: event.Generation, Connection: event.Connection}}, nil
		}
		return nil, nil
	}
	actualLocal, endpointErr := connectionLocalAddress(event.Connection)
	if event.Err != nil || event.Connection == nil || endpointErr != nil || actualLocal != slot.candidate.LocalAddress.Unmap() {
		actions := manager.enterBackoff(slot)
		if event.Connection != nil {
			actions = append([]SessionManagerAction{{
				Kind: SessionActionCloseConnection, Generation: slot.generation, Candidate: slot.candidate, Connection: event.Connection,
			}}, actions...)
		}
		pumped, pumpErr := manager.pump()
		return append(actions, pumped...), pumpErr
	}
	slot.connection = event.Connection
	slot.actualLocal = actualLocal
	slot.state = ManagedSessionAuthenticating
	return []SessionManagerAction{{
		Kind: SessionActionStartAuthentication, Generation: slot.generation,
		Candidate: slot.candidate, Connection: slot.connection,
	}}, nil
}

func (manager *SessionManager) completeAuthentication(event SessionManagerEvent) ([]SessionManagerAction, error) {
	slot := manager.slotByGeneration(event.Generation)
	if slot == nil || slot.state != ManagedSessionAuthenticating {
		return nil, nil
	}
	if !event.Succeeded || event.Err != nil {
		connection := slot.connection
		slot.connection = nil
		slot.actualLocal = netip.Addr{}
		actions := []SessionManagerAction{{
			Kind: SessionActionCloseConnection, Generation: slot.generation,
			Candidate: slot.candidate, Connection: connection,
		}}
		actions = append(actions, manager.enterBackoff(slot)...)
		pumped, pumpErr := manager.pump()
		return append(actions, pumped...), pumpErr
	}
	slot.state = ManagedSessionReady
	slot.failures = 0
	actions := []SessionManagerAction{{
		Kind: SessionActionReady, Generation: slot.generation,
		Candidate: slot.candidate, Connection: slot.connection,
	}}
	pumped, pumpErr := manager.pump()
	return append(actions, pumped...), pumpErr
}

func (manager *SessionManager) connectionLost(generation uint64) ([]SessionManagerAction, error) {
	slot := manager.slotByGeneration(generation)
	if slot == nil || slot.state == ManagedSessionBackoff || slot.state == ManagedSessionWaiting {
		return nil, nil
	}
	connection := slot.connection
	wasReady := slot.state == ManagedSessionReady
	slot.connection = nil
	slot.actualLocal = netip.Addr{}
	var actions []SessionManagerAction
	if wasReady {
		if slot.reconnects < math.MaxUint64 {
			slot.reconnects++
		}
		actions = append(actions, SessionManagerAction{
			Kind: SessionActionLost, Generation: slot.generation, Candidate: slot.candidate,
		})
	}
	if connection != nil {
		actions = append(actions, SessionManagerAction{
			Kind: SessionActionCloseConnection, Generation: slot.generation,
			Candidate: slot.candidate, Connection: connection,
		})
	}
	actions = append(actions, manager.enterBackoff(slot)...)
	pumped, pumpErr := manager.pump()
	return append(actions, pumped...), pumpErr
}

func connectionLocalAddress(connection transport.Connection) (netip.Addr, error) {
	if connection == nil {
		return netip.Addr{}, ErrInvalidSessionEvent
	}
	host, _, err := net.SplitHostPort(connection.LocalEndpoint())
	if err != nil {
		return netip.Addr{}, ErrInvalidSessionEvent
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !address.IsValid() || address.Zone() != "" {
		return netip.Addr{}, ErrInvalidSessionEvent
	}
	return address.Unmap(), nil
}

func (manager *SessionManager) backoffExpired(generation uint64) ([]SessionManagerAction, error) {
	slot := manager.slotByGeneration(generation)
	if slot == nil || slot.state != ManagedSessionBackoff {
		return nil, nil
	}
	slot.state = ManagedSessionWaiting
	return manager.pump()
}

func (manager *SessionManager) enterBackoff(slot *managedSession) []SessionManagerAction {
	if slot.failures < math.MaxUint8 {
		slot.failures++
	}
	slot.state = ManagedSessionBackoff
	return []SessionManagerAction{{
		Kind: SessionActionArmBackoff, Generation: slot.generation,
		Candidate: slot.candidate, After: sessionBackoff(slot.failures),
	}}
}

func (manager *SessionManager) pump() ([]SessionManagerAction, error) {
	active := 0
	for _, slot := range manager.slots {
		if slot.state == ManagedSessionDialing || slot.state == ManagedSessionAuthenticating {
			active++
		}
	}
	available := manager.config.AuthInProgress - active
	if available <= 0 {
		return nil, nil
	}
	want := 0
	for _, slot := range manager.slots {
		if slot.state == ManagedSessionWaiting && want < available {
			want++
		}
	}
	if uint64(want) > math.MaxUint64-manager.nextGeneration {
		return nil, ErrSessionGeneration
	}
	var actions []SessionManagerAction
	for _, slot := range manager.sortedSlots() {
		if active >= manager.config.AuthInProgress {
			break
		}
		if slot.state != ManagedSessionWaiting {
			continue
		}
		generation, _ := manager.allocateGeneration()
		slot.generation = generation
		slot.state = ManagedSessionDialing
		active++
		actions = append(actions, SessionManagerAction{
			Kind:       SessionActionDial,
			Generation: generation,
			Candidate:  slot.candidate,
			Options: transport.DialOptions{
				RemoteEndpoint: manager.config.RemoteEndpoint,
				LocalEndpoint:  net.JoinHostPort(slot.candidate.LocalAddress.String(), "0"),
				InterfaceName:  slot.candidate.InterfaceName,
				QueueLimits:    manager.config.QueueLimits,
			},
		})
	}
	return actions, nil
}

func (manager *SessionManager) removeSlot(slot *managedSession) []SessionManagerAction {
	delete(manager.slots, slot.candidate)
	var actions []SessionManagerAction
	if slot.state == ManagedSessionBackoff {
		actions = append(actions, SessionManagerAction{
			Kind: SessionActionCancelBackoff, Generation: slot.generation, Candidate: slot.candidate,
		})
	}
	if slot.state == ManagedSessionDialing {
		actions = append(actions, SessionManagerAction{
			Kind: SessionActionCancelDial, Generation: slot.generation, Candidate: slot.candidate,
		})
	}
	if slot.state == ManagedSessionReady {
		actions = append(actions, SessionManagerAction{
			Kind: SessionActionLost, Generation: slot.generation, Candidate: slot.candidate,
		})
	}
	if slot.connection != nil {
		actions = append(actions, SessionManagerAction{
			Kind: SessionActionCloseConnection, Generation: slot.generation,
			Candidate: slot.candidate, Connection: slot.connection,
		})
	}
	return actions
}

func (manager *SessionManager) shutdown() []SessionManagerAction {
	if manager.shuttingDown {
		return nil
	}
	manager.shuttingDown = true
	var actions []SessionManagerAction
	for _, slot := range manager.sortedSlots() {
		actions = append(actions, manager.removeSlot(slot)...)
	}
	return actions
}

func (manager *SessionManager) sortedSlots() []*managedSession {
	slots := make([]*managedSession, 0, len(manager.slots))
	for _, slot := range manager.slots {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool { return candidateLess(slots[i].candidate, slots[j].candidate) })
	return slots
}

func (manager *SessionManager) slotByGeneration(generation uint64) *managedSession {
	if generation == 0 {
		return nil
	}
	for _, slot := range manager.slots {
		if slot.generation == generation {
			return slot
		}
	}
	return nil
}

func (manager *SessionManager) allocateGeneration() (uint64, bool) {
	if manager.nextGeneration == math.MaxUint64 {
		return 0, false
	}
	manager.nextGeneration++
	return manager.nextGeneration, true
}

func selectSessionCandidates(candidates []networkpath.Candidate, maximum int) ([]networkpath.Candidate, error) {
	if len(candidates) > MaxSessionPathCandidates {
		return nil, ErrInvalidSessionEvent
	}
	unique := make(map[networkpath.Candidate]struct{}, len(candidates))
	groups := make(map[sessionInterfaceKey][]networkpath.Candidate)
	var keys []sessionInterfaceKey
	for _, candidate := range candidates {
		if candidate.InterfaceIndex < 1 || candidate.InterfaceName == "" || !candidate.LocalAddress.IsValid() ||
			candidate.LocalAddress.IsUnspecified() || candidate.LocalAddress.IsMulticast() ||
			candidate.LocalAddress.IsLinkLocalUnicast() || candidate.LocalAddress.IsLinkLocalMulticast() ||
			candidate.LocalAddress.Zone() != "" {
			return nil, ErrInvalidSessionEvent
		}
		candidate.LocalAddress = candidate.LocalAddress.Unmap()
		if _, duplicate := unique[candidate]; duplicate {
			continue
		}
		unique[candidate] = struct{}{}
		key := sessionInterfaceKey{index: candidate.InterfaceIndex, name: candidate.InterfaceName}
		if _, exists := groups[key]; !exists {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], candidate)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].index < keys[j].index
	})
	for _, key := range keys {
		sort.Slice(groups[key], func(i, j int) bool {
			return groups[key][i].LocalAddress.Compare(groups[key][j].LocalAddress) < 0
		})
	}
	selected := make([]networkpath.Candidate, 0, min(maximum, len(keys)))
	for _, key := range keys {
		selected = append(selected, groups[key][0])
		if len(selected) == maximum {
			break
		}
	}
	return selected, nil
}

type sessionInterfaceKey struct {
	index int
	name  string
}

func candidateLess(left, right networkpath.Candidate) bool {
	if left.InterfaceName != right.InterfaceName {
		return left.InterfaceName < right.InterfaceName
	}
	if left.InterfaceIndex != right.InterfaceIndex {
		return left.InterfaceIndex < right.InterfaceIndex
	}
	return bytes.Compare(left.LocalAddress.AsSlice(), right.LocalAddress.AsSlice()) < 0
}

func sessionBackoff(failures uint8) time.Duration {
	if failures <= 1 {
		return SessionInitialBackoff
	}
	return SessionMaximumBackoff
}
