package status

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync/atomic"
	"time"
)

const (
	statusPublicationInterval = 100 * time.Millisecond
	maxStatusEventBatch       = 32
)

var (
	ErrInvalidRepository = errors.New("status: invalid repository")
	ErrRepositoryLimit   = errors.New("status: repository limit exceeded")
)

type Limits struct {
	Interfaces int
	Sessions   int
	Flows      int
}

func DefaultLimits() Limits {
	return Limits{Interfaces: MaxInterfaces, Sessions: MaxSessions, Flows: MaxFlows}
}

type CounterKind uint8

const (
	CounterFramesSent CounterKind = iota + 1
	CounterFramesReceived
	CounterBytesSent
	CounterBytesReceived
	CounterRetransmittedBytes
	CounterRedundantBytes
	CounterDataPayloadBytesSent
	CounterDataPayloadBytesReceived
)

type EventKind uint8

const (
	EventSetHealth EventKind = iota + 1
	EventSetResources
	EventSetRejected
	EventUpsertInterface
	EventRemoveInterface
	EventUpsertSession
	EventRemoveSession
	EventUpsertFlow
	EventRemoveFlow
	EventFlowTerminal
)

type Event struct {
	Kind           EventKind
	Healthy        bool
	Resources      Resources
	Rejected       Rejected
	Interface      Interface
	InterfaceIndex int
	Session        Session
	SessionID      string
	Flow           Flow
	FlowID         string
	Terminal       Terminal
}

type metricCounters struct {
	framesSent               atomic.Uint64
	framesReceived           atomic.Uint64
	bytesSent                atomic.Uint64
	bytesReceived            atomic.Uint64
	dataPayloadBytesSent     atomic.Uint64
	dataPayloadBytesReceived atomic.Uint64
	retransmittedBytes       atomic.Uint64
	redundantBytes           atomic.Uint64
	droppedStatusEvents      atomic.Uint64
}

type repositoryState struct {
	healthy      bool
	resources    Resources
	rejected     Rejected
	interfaces   map[int]Interface
	interfaceIDs []int
	sessions     map[string]Session
	sessionIDs   []string
	flows        map[string]Flow
	flowIDs      []string
	terminals    []Terminal
	terminalAt   map[string]int
	terminalNext int
	lastPrune    time.Time
}

// The Repository event queue is the only mutable boundary between data paths and
// the statistics owner. TryRecord never waits; Run is the sole state writer;
// Snapshot reads only immutable published values.
type Repository struct {
	limits   Limits
	role     Role
	events   chan Event
	now      func() time.Time
	metrics  metricCounters
	snapshot atomic.Pointer[Snapshot]
	state    repositoryState
}

func NewRepository(limits Limits) (*Repository, error) {
	return newRepository(limits, 0, time.Now)
}

func NewRepositoryWithClock(limits Limits, now func() time.Time) (*Repository, error) {
	return newRepository(limits, 0, now)
}

func NewRepositoryForRole(limits Limits, role Role) (*Repository, error) {
	if role != RoleClient && role != RoleServer {
		return nil, ErrInvalidRepository
	}
	return newRepository(limits, role, time.Now)
}

func newRepository(limits Limits, role Role, now func() time.Time) (*Repository, error) {
	if limits.Interfaces < 1 || limits.Interfaces > MaxInterfaces ||
		limits.Sessions < 1 || limits.Sessions > MaxSessions ||
		limits.Flows < 1 || limits.Flows > MaxFlows || now == nil {
		return nil, ErrInvalidRepository
	}
	repository := &Repository{
		limits: limits, role: role,
		events: make(chan Event, MaxStatusEvents),
		now:    now,
		state: repositoryState{
			healthy: true, interfaces: make(map[int]Interface, limits.Interfaces),
			sessions: make(map[string]Session, limits.Sessions), flows: make(map[string]Flow, limits.Flows),
			interfaceIDs: make([]int, 0, limits.Interfaces), sessionIDs: make([]string, 0, limits.Sessions),
			flowIDs:   make([]string, 0, limits.Flows),
			terminals: make([]Terminal, 0, MaxTerminalSummaries), terminalAt: make(map[string]int, MaxTerminalSummaries),
		},
	}
	repository.publish(now())
	return repository, nil
}

func (repository *Repository) TryRecord(event Event) bool {
	if repository == nil || !validEvent(event) {
		return false
	}
	event.Interface.Addresses = append([]string(nil), event.Interface.Addresses...)
	select {
	case repository.events <- event:
		return true
	default:
		saturatingAtomicAdd(&repository.metrics.droppedStatusEvents, 1)
		return false
	}
}

func (repository *Repository) AddCounter(kind CounterKind, delta uint64) bool {
	if repository == nil || delta == 0 {
		return false
	}
	var counter *atomic.Uint64
	switch kind {
	case CounterFramesSent:
		counter = &repository.metrics.framesSent
	case CounterFramesReceived:
		counter = &repository.metrics.framesReceived
	case CounterBytesSent:
		counter = &repository.metrics.bytesSent
	case CounterBytesReceived:
		counter = &repository.metrics.bytesReceived
	case CounterDataPayloadBytesSent:
		counter = &repository.metrics.dataPayloadBytesSent
	case CounterDataPayloadBytesReceived:
		counter = &repository.metrics.dataPayloadBytesReceived
	case CounterRetransmittedBytes:
		counter = &repository.metrics.retransmittedBytes
	case CounterRedundantBytes:
		counter = &repository.metrics.redundantBytes
	default:
		return false
	}
	saturatingAtomicAdd(counter, delta)
	return true
}

// Run consumes events serially. The caller injects ticks for deterministic
// terminal-summary eviction; nil ticks disable active eviction, while subsequent
// business events still trigger eviction based on their receive time. Changes
// for high-volume flow, terminal, and resource updates are coalesced on a fixed
// publication interval. Health and low-volume session/interface control state
// still publish immediately.
func (repository *Repository) Run(ctx context.Context, ticks <-chan time.Time) error {
	publicationTicker := time.NewTicker(statusPublicationInterval)
	defer publicationTicker.Stop()
	return repository.run(ctx, ticks, publicationTicker.C)
}

func (repository *Repository) run(ctx context.Context, ticks, publicationTicks <-chan time.Time) error {
	if repository == nil || ctx == nil {
		return ErrInvalidRepository
	}
	dirty := false
	for {
		select {
		case <-ctx.Done():
			if dirty {
				repository.publish(repository.now())
			}
			return nil
		default:
		}
		select {
		case <-ctx.Done():
			if dirty {
				repository.publish(repository.now())
			}
			return nil
		case now, open := <-ticks:
			if !open {
				ticks = nil
				continue
			}
			before := len(repository.state.terminals)
			repository.prune(now)
			if dirty || len(repository.state.terminals) != before {
				repository.publish(now)
				dirty = false
			}
		case now, open := <-publicationTicks:
			if !open {
				publicationTicks = nil
				continue
			}
			if dirty {
				repository.publish(now)
				dirty = false
			}
		case event := <-repository.events:
			now := repository.now()
			var batch [maxStatusEventBatch]Event
			batch[0] = event
			count := 1
		collect:
			for count < len(batch) {
				select {
				case <-ctx.Done():
					break collect
				case batch[count] = <-repository.events:
					count++
				default:
					break collect
				}
			}
			immediate := false
			for index := 0; index < count; index++ {
				publishNow := repository.eventRequiresImmediatePublication(batch[index])
				if err := repository.apply(batch[index], now); err != nil {
					saturatingAtomicAdd(&repository.metrics.droppedStatusEvents, 1)
					continue
				}
				if publishNow {
					immediate = true
				} else {
					dirty = true
				}
			}
			if immediate {
				repository.publish(now)
				dirty = false
			}
		}
	}
}

func (repository *Repository) eventRequiresImmediatePublication(event Event) bool {
	switch event.Kind {
	case EventUpsertFlow:
		return false
	case EventUpsertSession:
		current, exists := repository.state.sessions[event.Session.IDHash]
		return !exists || !sameSessionControlState(current, event.Session)
	case EventSetResources, EventSetRejected, EventRemoveFlow, EventFlowTerminal:
		return false
	default:
		return true
	}
}

// sameSessionControlState reports whether two session events differ only in
// diagnostic quality fields. Identity, connection, addressing, state and the
// derived fastest marker publish immediately; quality samples coalesce onto the
// publication tick.
func sameSessionControlState(left, right Session) bool {
	return left.IDHash == right.IDHash && left.ConnectionID == right.ConnectionID &&
		left.Transport == right.Transport && left.Interface == right.Interface &&
		left.LocalAddress == right.LocalAddress && left.LocalEndpoint == right.LocalEndpoint &&
		left.RemoteEndpoint == right.RemoteEndpoint && left.PrincipalHash == right.PrincipalHash &&
		left.State == right.State && left.Reason == right.Reason && left.Fastest == right.Fastest
}

func (repository *Repository) Snapshot() Snapshot {
	if repository == nil {
		return Snapshot{}
	}
	published := repository.snapshot.Load()
	if published == nil {
		return Snapshot{}
	}
	result := cloneSnapshot(*published)
	result.Counters = repository.counters()
	return result
}

func (repository *Repository) apply(event Event, now time.Time) error {
	if repository == nil || !validEvent(event) {
		return ErrInvalidModel
	}
	repository.pruneIfDue(now)
	switch event.Kind {
	case EventSetHealth:
		repository.state.healthy = event.Healthy
	case EventSetResources:
		repository.state.resources = event.Resources
	case EventSetRejected:
		repository.state.rejected = event.Rejected
	case EventUpsertInterface:
		value, err := normalizeInterface(event.Interface)
		if err != nil {
			return err
		}
		_, exists := repository.state.interfaces[value.Index]
		if !exists && len(repository.state.interfaces) >= repository.limits.Interfaces {
			return ErrRepositoryLimit
		}
		if !exists {
			repository.state.interfaceIDs = insertSortedInt(repository.state.interfaceIDs, value.Index)
		}
		repository.state.interfaces[value.Index] = value
	case EventRemoveInterface:
		if _, exists := repository.state.interfaces[event.InterfaceIndex]; exists {
			delete(repository.state.interfaces, event.InterfaceIndex)
			repository.state.interfaceIDs = removeSortedInt(repository.state.interfaceIDs, event.InterfaceIndex)
		}
	case EventUpsertSession:
		if !validSession(event.Session) {
			return ErrInvalidModel
		}
		_, exists := repository.state.sessions[event.Session.IDHash]
		if !exists && len(repository.state.sessions) >= repository.limits.Sessions {
			return ErrRepositoryLimit
		}
		if !exists {
			repository.state.sessionIDs = insertSortedString(repository.state.sessionIDs, event.Session.IDHash)
		}
		repository.state.sessions[event.Session.IDHash] = event.Session
	case EventRemoveSession:
		if _, exists := repository.state.sessions[event.SessionID]; exists {
			delete(repository.state.sessions, event.SessionID)
			repository.state.sessionIDs = removeSortedString(repository.state.sessionIDs, event.SessionID)
		}
	case EventUpsertFlow:
		if !validFlow(event.Flow) {
			return ErrInvalidModel
		}
		_, exists := repository.state.flows[event.Flow.IDHash]
		if !exists && len(repository.state.flows) >= repository.limits.Flows {
			return ErrRepositoryLimit
		}
		if !exists {
			repository.state.flowIDs = insertSortedString(repository.state.flowIDs, event.Flow.IDHash)
		}
		repository.state.flows[event.Flow.IDHash] = event.Flow
	case EventRemoveFlow:
		if _, exists := repository.state.flows[event.FlowID]; exists {
			delete(repository.state.flows, event.FlowID)
			repository.state.flowIDs = removeSortedString(repository.state.flowIDs, event.FlowID)
		}
	case EventFlowTerminal:
		if !validTerminal(event.Terminal) {
			return ErrInvalidModel
		}
		if _, exists := repository.state.flows[event.Terminal.IDHash]; exists {
			delete(repository.state.flows, event.Terminal.IDHash)
			repository.state.flowIDs = removeSortedString(repository.state.flowIDs, event.Terminal.IDHash)
		}
		repository.upsertTerminal(event.Terminal)
	default:
		return ErrInvalidModel
	}
	return nil
}

func insertSortedString(values []string, value string) []string {
	index := sort.SearchStrings(values, value)
	values = append(values, "")
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

func removeSortedString(values []string, value string) []string {
	index := sort.SearchStrings(values, value)
	if index == len(values) || values[index] != value {
		return values
	}
	copy(values[index:], values[index+1:])
	return values[:len(values)-1]
}

func insertSortedInt(values []int, value int) []int {
	index := sort.SearchInts(values, value)
	values = append(values, 0)
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}

func removeSortedInt(values []int, value int) []int {
	index := sort.SearchInts(values, value)
	if index == len(values) || values[index] != value {
		return values
	}
	copy(values[index:], values[index+1:])
	return values[:len(values)-1]
}

func (repository *Repository) publish(now time.Time) {
	snapshot := Snapshot{
		GeneratedAt: now,
		Role:        repository.role,
		Healthy:     repository.state.healthy,
		Resources:   repository.state.resources,
		Rejected:    repository.state.rejected,
		Counters:    repository.counters(),
		Interfaces:  make([]Interface, 0, len(repository.state.interfaces)),
		Sessions:    make([]Session, 0, len(repository.state.sessions)),
		Flows:       make([]Flow, 0, len(repository.state.flows)),
		Terminals:   append([]Terminal(nil), repository.state.terminals...),
	}
	for _, id := range repository.state.interfaceIDs {
		value := repository.state.interfaces[id]
		value.Addresses = append([]string(nil), value.Addresses...)
		snapshot.Interfaces = append(snapshot.Interfaces, value)
	}
	for _, id := range repository.state.sessionIDs {
		snapshot.Sessions = append(snapshot.Sessions, repository.state.sessions[id])
	}
	for _, id := range repository.state.flowIDs {
		snapshot.Flows = append(snapshot.Flows, repository.state.flows[id])
	}
	sort.Slice(snapshot.Terminals, func(i, j int) bool {
		if !snapshot.Terminals[i].FinishedAt.Equal(snapshot.Terminals[j].FinishedAt) {
			return snapshot.Terminals[i].FinishedAt.Before(snapshot.Terminals[j].FinishedAt)
		}
		return snapshot.Terminals[i].IDHash < snapshot.Terminals[j].IDHash
	})
	repository.snapshot.Store(&snapshot)
}

func (repository *Repository) counters() Counters {
	return Counters{
		FramesSent:               repository.metrics.framesSent.Load(),
		FramesReceived:           repository.metrics.framesReceived.Load(),
		BytesSent:                repository.metrics.bytesSent.Load(),
		BytesReceived:            repository.metrics.bytesReceived.Load(),
		DataPayloadBytesSent:     repository.metrics.dataPayloadBytesSent.Load(),
		DataPayloadBytesReceived: repository.metrics.dataPayloadBytesReceived.Load(),
		RetransmittedBytes:       repository.metrics.retransmittedBytes.Load(),
		RedundantBytes:           repository.metrics.redundantBytes.Load(),
		DroppedStatusEvents:      repository.metrics.droppedStatusEvents.Load(),
	}
}

func (repository *Repository) prune(now time.Time) {
	if now.IsZero() || len(repository.state.terminals) == 0 {
		return
	}
	cutoff := now.Add(-TerminalRetention)
	kept := repository.state.terminals[:0]
	for _, terminal := range repository.state.terminals {
		if terminal.FinishedAt.After(cutoff) {
			kept = append(kept, terminal)
		}
	}
	repository.state.terminals = kept
	repository.reindexTerminals()
	repository.state.lastPrune = now
}

func (repository *Repository) pruneIfDue(now time.Time) {
	if !repository.state.lastPrune.IsZero() && !now.Before(repository.state.lastPrune) &&
		now.Sub(repository.state.lastPrune) < statusPublicationInterval {
		return
	}
	repository.prune(now)
}

func (repository *Repository) upsertTerminal(terminal Terminal) {
	if index, exists := repository.state.terminalAt[terminal.IDHash]; exists {
		repository.state.terminals[index] = terminal
		return
	}
	if len(repository.state.terminals) < MaxTerminalSummaries {
		repository.state.terminalAt[terminal.IDHash] = len(repository.state.terminals)
		repository.state.terminals = append(repository.state.terminals, terminal)
		return
	}
	index := repository.state.terminalNext
	delete(repository.state.terminalAt, repository.state.terminals[index].IDHash)
	repository.state.terminals[index] = terminal
	repository.state.terminalAt[terminal.IDHash] = index
	repository.state.terminalNext = (index + 1) % MaxTerminalSummaries
}

func (repository *Repository) reindexTerminals() {
	clear(repository.state.terminalAt)
	for index, terminal := range repository.state.terminals {
		repository.state.terminalAt[terminal.IDHash] = index
	}
	repository.state.terminalNext = 0
}

func validEvent(event Event) bool {
	switch event.Kind {
	case EventSetHealth, EventSetResources, EventSetRejected:
		return true
	case EventUpsertInterface:
		_, err := normalizeInterface(event.Interface)
		return err == nil
	case EventRemoveInterface:
		return event.InterfaceIndex > 0
	case EventUpsertSession:
		return validSession(event.Session)
	case EventRemoveSession:
		return validHash(event.SessionID)
	case EventUpsertFlow:
		return validFlow(event.Flow)
	case EventRemoveFlow:
		return validHash(event.FlowID)
	case EventFlowTerminal:
		return validTerminal(event.Terminal)
	default:
		return false
	}
}

func saturatingAtomicAdd(counter *atomic.Uint64, delta uint64) {
	for {
		current := counter.Load()
		next := current + delta
		if next < current {
			next = math.MaxUint64
		}
		if counter.CompareAndSwap(current, next) {
			return
		}
	}
}
