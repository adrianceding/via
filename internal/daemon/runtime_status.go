package daemon

import (
	"crypto/rand"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/adrianceding/via/internal/auth"
	clientcore "github.com/adrianceding/via/internal/client"
	"github.com/adrianceding/via/internal/config"
	"github.com/adrianceding/via/internal/flow"
	pathcore "github.com/adrianceding/via/internal/path"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	statusapi "github.com/adrianceding/via/internal/status"
)

func statusBasicAuth(configuration config.Status) []statusapi.BasicAuth {
	if configuration.BasicAuth == nil {
		return nil
	}
	return []statusapi.BasicAuth{{
		Username: configuration.BasicAuth.Username,
		Password: configuration.BasicAuth.Password,
	}}
}

const (
	statusReadHeaderTimeout = 5 * time.Second
	statusReadTimeout       = 10 * time.Second
	statusWriteTimeout      = 10 * time.Second
	statusIdleTimeout       = 15 * time.Second
	statusMaxHeaderBytes    = 8 << 10
)

type runtimeStatus struct {
	repository *statusapi.Repository
	hasher     *statusapi.Hasher
	maxFlows   int
	now        func() time.Time

	mu          sync.Mutex
	resources   statusapi.Resources
	flows       map[protocol.FlowID]runtimeFlowStatus
	flowTraffic map[protocol.FlowID]runtimeFlowTraffic
	sessions    map[string]statusapi.Session
	connections map[uint64]string
	interfaces  map[int]struct{}
}

type runtimeFlowStatus struct {
	target protocol.Target
	entry  statusapi.Flow
}

type runtimeFlowTraffic struct {
	retransmitted uint64
	redundant     uint64
}

type runtimeFlowObservation struct {
	correlationID        string
	lifecycleState       flow.LifecycleState
	adaptiveState        policy.AdaptiveState
	adaptiveTransition   policy.Transition
	publishedAttachments int
	policyAttachments    int
	preferredAttachment  flow.AttachmentKey
	hasPreferred         bool
	txAllocatedOffset    uint64
	txAcknowledged       uint64
	rxWrittenOffset      uint64
}

type runtimeSessionObservation struct {
	generation     uint64
	transportName  string
	interfaceName  string
	localAddress   netip.Addr
	localEndpoint  string
	remoteEndpoint string
	connectionID   string
	principalID    string
	state          statusapi.SessionState
	reason         statusapi.TransitionReason
	reconnects     uint64
}

func newRuntimeStatus(repository *statusapi.Repository, maxFlows int, reservedBytes uint64) (*runtimeStatus, error) {
	var key [32]byte
	if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
		return nil, err
	}
	return newRuntimeStatusWithKey(repository, maxFlows, reservedBytes, key)
}

func newRuntimeStatusWithKey(repository *statusapi.Repository, maxFlows int, reservedBytes uint64, key [32]byte) (*runtimeStatus, error) {
	return newRuntimeStatusWithClock(repository, maxFlows, reservedBytes, key, time.Now)
}

func newRuntimeStatusWithClock(repository *statusapi.Repository, maxFlows int, reservedBytes uint64, key [32]byte, now func() time.Time) (*runtimeStatus, error) {
	if repository == nil || maxFlows < 1 || maxFlows > statusapi.MaxFlows || now == nil {
		return nil, statusapi.ErrInvalidModel
	}
	hasher, err := statusapi.NewHasher(key)
	if err != nil {
		return nil, err
	}
	observer := &runtimeStatus{
		repository:  repository,
		hasher:      hasher,
		maxFlows:    maxFlows,
		now:         now,
		flows:       make(map[protocol.FlowID]runtimeFlowStatus, maxFlows),
		flowTraffic: make(map[protocol.FlowID]runtimeFlowTraffic, maxFlows),
		sessions:    make(map[string]statusapi.Session, statusapi.MaxSessions),
		connections: make(map[uint64]string, statusapi.MaxSessions),
		interfaces:  make(map[int]struct{}, statusapi.MaxInterfaces),
	}
	observer.resources.ReservedBytes = reservedBytes
	observer.publishResourcesLocked()
	return observer, nil
}

func (observer *runtimeStatus) frameSent(bytes uint64) {
	if observer == nil || bytes == 0 {
		return
	}
	observer.repository.AddCounter(statusapi.CounterFramesSent, 1)
	observer.repository.AddCounter(statusapi.CounterBytesSent, bytes)
}

func (observer *runtimeStatus) frameReceived(bytes uint64) {
	if observer == nil || bytes == 0 {
		return
	}
	observer.repository.AddCounter(statusapi.CounterFramesReceived, 1)
	observer.repository.AddCounter(statusapi.CounterBytesReceived, bytes)
}

func (observer *runtimeStatus) retransmitted(bytes uint64) {
	if observer != nil && bytes != 0 {
		observer.repository.AddCounter(statusapi.CounterRetransmittedBytes, bytes)
	}
}

func (observer *runtimeStatus) redundant(bytes uint64) {
	if observer != nil && bytes != 0 {
		observer.repository.AddCounter(statusapi.CounterRedundantBytes, bytes)
	}
}

func (observer *runtimeStatus) recordFlowTraffic(flowID protocol.FlowID, retransmitted, redundant uint64) {
	if observer == nil || flowID == (protocol.FlowID{}) || retransmitted == 0 && redundant == 0 {
		return
	}
	observer.mu.Lock()
	if _, exists := observer.flows[flowID]; !exists {
		observer.mu.Unlock()
		return
	}
	traffic := observer.flowTraffic[flowID]
	traffic.retransmitted = adjustResource(traffic.retransmitted, int64(retransmitted))
	traffic.redundant = adjustResource(traffic.redundant, int64(redundant))
	observer.flowTraffic[flowID] = traffic
	observer.mu.Unlock()
	if retransmitted != 0 {
		observer.retransmitted(retransmitted)
	}
	if redundant != 0 {
		observer.redundant(redundant)
	}
}

func (observer *runtimeStatus) syncInterfaces(snapshot pathcore.Snapshot) {
	if observer == nil {
		return
	}
	next := make(map[int]struct{}, len(snapshot.Decisions))
	for _, decision := range snapshot.Decisions {
		if decision.InterfaceIndex < 1 {
			continue
		}
		next[decision.InterfaceIndex] = struct{}{}
		addresses := make([]string, 0, len(decision.Candidates))
		for _, candidate := range decision.Candidates {
			addresses = append(addresses, candidate.LocalAddress.String())
		}
		observer.repository.TryRecord(statusapi.Event{
			Kind: statusapi.EventUpsertInterface,
			Interface: statusapi.Interface{
				Index: decision.InterfaceIndex, Name: decision.InterfaceName,
				Addresses: addresses, Reason: statusInterfaceReason(decision.Reason),
			},
		})
	}

	observer.mu.Lock()
	for index := range observer.interfaces {
		if _, exists := next[index]; !exists {
			observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventRemoveInterface, InterfaceIndex: index})
		}
	}
	observer.interfaces = next
	observer.mu.Unlock()
}

func statusInterfaceReason(reason pathcore.DecisionReason) statusapi.InterfaceReason {
	switch reason {
	case pathcore.ReasonEligible:
		return statusapi.InterfaceEligible
	case pathcore.ReasonExcluded:
		return statusapi.InterfaceExcluded
	case pathcore.ReasonNotIncluded:
		return statusapi.InterfaceNotIncluded
	case pathcore.ReasonInterfaceDown:
		return statusapi.InterfaceDown
	case pathcore.ReasonNoEligibleAddress:
		return statusapi.InterfaceNoAddress
	default:
		return statusapi.InterfaceUnsafeAddress
	}
}

func (observer *runtimeStatus) upsertSession(generation uint64, transportName, interfaceName string, local netip.Addr, state statusapi.SessionState, reason statusapi.TransitionReason) {
	observer.upsertSessionObservation(runtimeSessionObservation{
		generation: generation, transportName: transportName, interfaceName: interfaceName,
		localAddress: local, state: state, reason: reason,
	})
}

func (observer *runtimeStatus) upsertSessionObservation(observation runtimeSessionObservation) {
	if observer == nil || observation.generation == 0 || !observation.localAddress.IsValid() {
		return
	}
	id := observer.hasher.SessionID(observation.generation)
	if id == "" {
		return
	}
	observer.mu.Lock()
	previous, exists := observer.sessions[id]
	if !exists {
		observer.resources.Sessions++
		observer.publishResourcesLocked()
	}
	stateSince := previous.StateSince
	if !exists || previous.State != observation.state || stateSince.IsZero() {
		stateSince = observer.now()
	}
	entry := statusapi.Session{
		IDHash: id, ConnectionID: previous.ConnectionID,
		Transport: observation.transportName, Interface: observation.interfaceName,
		LocalAddress: observation.localAddress.Unmap().String(), LocalEndpoint: previous.LocalEndpoint,
		RemoteEndpoint: previous.RemoteEndpoint, PrincipalHash: previous.PrincipalHash,
		State: observation.state, Reason: observation.reason, StateSince: stateSince,
		LastProbeAt: previous.LastProbeAt, Reconnects: previous.Reconnects,
		Quality: previous.Quality, Fastest: previous.Fastest,
	}
	if observation.connectionID != "" {
		entry.ConnectionID = observation.connectionID
		observer.connections[observation.generation] = observation.connectionID
	}
	if observation.principalID != "" {
		entry.PrincipalHash = observer.hasher.Principal(observation.principalID)
	}
	if observation.localEndpoint != "" {
		entry.LocalEndpoint = observation.localEndpoint
	}
	if observation.remoteEndpoint != "" {
		entry.RemoteEndpoint = observation.remoteEndpoint
	}
	if observation.reconnects > entry.Reconnects {
		entry.Reconnects = observation.reconnects
	}
	observer.sessions[id] = entry
	updates := observer.recomputeFastestLocked()
	entry = observer.sessions[id]
	observer.mu.Unlock()
	observer.publishSessionUpdates(updates, id)
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertSession, Session: entry})
}

func (observer *runtimeStatus) observeSessionProbe(generation uint64, rtt time.Duration) {
	if observer == nil || generation == 0 || rtt <= 0 {
		return
	}
	id := observer.hasher.SessionID(generation)
	observer.mu.Lock()
	entry, exists := observer.sessions[id]
	if !exists {
		observer.mu.Unlock()
		return
	}
	sample := uint64(rtt / time.Microsecond)
	if sample == 0 {
		sample = 1
	}
	if entry.Quality.SmoothedRTTMicros == 0 {
		entry.Quality.SmoothedRTTMicros = sample
	} else {
		entry.Quality.SmoothedRTTMicros = (7*entry.Quality.SmoothedRTTMicros + sample) / 8
	}
	entry.Quality.StallPenaltyMicros = 0
	retry := entry.Quality.SmoothedRTTMicros * 2
	minimum := uint64((200 * time.Millisecond) / time.Microsecond)
	maximum := uint64((2 * time.Second) / time.Microsecond)
	if retry < minimum {
		retry = minimum
	}
	if retry > maximum {
		retry = maximum
	}
	entry.Quality.RetryMicros = retry
	entry.LastProbeAt = observer.now()
	observer.sessions[id] = entry
	updates := observer.recomputeFastestLocked()
	entry = observer.sessions[id]
	observer.mu.Unlock()
	observer.publishSessionUpdates(updates, id)
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertSession, Session: entry})
}

func (observer *runtimeStatus) setSessionStallPenalty(generation uint64, penalty time.Duration) {
	if observer == nil || generation == 0 || penalty < 0 {
		return
	}
	id := observer.hasher.SessionID(generation)
	observer.mu.Lock()
	entry, exists := observer.sessions[id]
	if !exists {
		observer.mu.Unlock()
		return
	}
	entry.Quality.StallPenaltyMicros = uint64(penalty / time.Microsecond)
	observer.sessions[id] = entry
	updates := observer.recomputeFastestLocked()
	entry = observer.sessions[id]
	observer.mu.Unlock()
	observer.publishSessionUpdates(updates, id)
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertSession, Session: entry})
}

func (observer *runtimeStatus) removeSession(generation uint64) {
	if observer == nil || generation == 0 {
		return
	}
	id := observer.hasher.SessionID(generation)
	observer.mu.Lock()
	if _, exists := observer.sessions[id]; exists {
		delete(observer.sessions, id)
		if observer.resources.Sessions != 0 {
			observer.resources.Sessions--
		}
		observer.publishResourcesLocked()
	}
	updates := observer.recomputeFastestLocked()
	observer.mu.Unlock()
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventRemoveSession, SessionID: id})
	observer.publishSessionUpdates(updates, "")
}

func (observer *runtimeStatus) syncClientSessions(transportName, principalID string, snapshot clientcore.SessionManagerSnapshot, authenticated map[uint64]*wireSession) {
	if observer == nil {
		return
	}
	next := make(map[string]struct{}, len(snapshot.Sessions))
	for _, session := range snapshot.Sessions {
		if session.Generation == 0 {
			continue
		}
		local := session.ActualLocalAddress
		if !local.IsValid() {
			local = session.Candidate.LocalAddress
		}
		state, reason := statusClientSessionState(session.State)
		observation := runtimeSessionObservation{
			generation: session.Generation, transportName: transportName, interfaceName: session.Candidate.InterfaceName,
			localAddress: local, localEndpoint: session.LocalEndpoint, remoteEndpoint: session.RemoteEndpoint,
			state: state, reason: reason, reconnects: session.Reconnects,
		}
		if wire := authenticated[session.Generation]; wire != nil && wire.connectionID != (auth.CorrelationID{}) {
			observation.connectionID = wire.connectionID.String()
			observation.principalID = principalID
		}
		observer.upsertSessionObservation(observation)
		next[observer.hasher.SessionID(session.Generation)] = struct{}{}
	}
	var removed []string
	observer.mu.Lock()
	for id := range observer.sessions {
		if _, exists := next[id]; !exists {
			delete(observer.sessions, id)
			removed = append(removed, id)
			if observer.resources.Sessions != 0 {
				observer.resources.Sessions--
			}
		}
	}
	if len(removed) != 0 {
		observer.publishResourcesLocked()
	}
	updates := observer.recomputeFastestLocked()
	observer.mu.Unlock()
	for _, id := range removed {
		observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventRemoveSession, SessionID: id})
	}
	observer.publishSessionUpdates(updates, "")
}

func (observer *runtimeStatus) recomputeFastestLocked() []statusapi.Session {
	fastestID := ""
	fastestStall := ^uint64(0)
	fastestRTT := ^uint64(0)
	for id, session := range observer.sessions {
		if session.State != statusapi.SessionReady || session.Quality.SmoothedRTTMicros == 0 {
			continue
		}
		stall := session.Quality.StallPenaltyMicros
		if stall < fastestStall || stall == fastestStall &&
			(session.Quality.SmoothedRTTMicros < fastestRTT ||
				session.Quality.SmoothedRTTMicros == fastestRTT && (fastestID == "" || id < fastestID)) {
			fastestID = id
			fastestStall = stall
			fastestRTT = session.Quality.SmoothedRTTMicros
		}
	}
	updates := make([]statusapi.Session, 0, 2)
	for id, session := range observer.sessions {
		fastest := id == fastestID && fastestID != ""
		if session.Fastest == fastest {
			continue
		}
		session.Fastest = fastest
		observer.sessions[id] = session
		updates = append(updates, session)
	}
	return updates
}

func (observer *runtimeStatus) publishSessionUpdates(updates []statusapi.Session, exceptID string) {
	for _, session := range updates {
		if session.IDHash == exceptID {
			continue
		}
		observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertSession, Session: session})
	}
}

func statusClientSessionState(state clientcore.ManagedSessionState) (statusapi.SessionState, statusapi.TransitionReason) {
	switch state {
	case clientcore.ManagedSessionDialing:
		return statusapi.SessionDialing, statusapi.ReasonStarted
	case clientcore.ManagedSessionAuthenticating:
		return statusapi.SessionAuthenticating, statusapi.ReasonStarted
	case clientcore.ManagedSessionReady:
		return statusapi.SessionReady, statusapi.ReasonPathAdded
	case clientcore.ManagedSessionBackoff:
		return statusapi.SessionBackoff, statusapi.ReasonPathRemoved
	default:
		return statusapi.SessionClosed, statusapi.ReasonNone
	}
}

func (observer *runtimeStatus) upsertFlow(flowID protocol.FlowID, target protocol.Target, mode protocol.DeliveryMode, selection protocol.PathSelection, snapshot flow.FlowSnapshot, policySnapshot policy.Snapshot, reason statusapi.TransitionReason) {
	observer.upsertFlowObservation(flowID, target, mode, selection, runtimeFlowObservation{
		lifecycleState:       snapshot.Lifecycle.State,
		adaptiveState:        policySnapshot.State,
		adaptiveTransition:   policySnapshot.Transition,
		publishedAttachments: len(snapshot.Lifecycle.Published),
		policyAttachments:    len(policySnapshot.Attachments),
		preferredAttachment:  policySnapshot.Preferred,
		hasPreferred:         policySnapshot.HasPreferred,
		txAllocatedOffset:    snapshot.TxAllocatedOffset,
		txAcknowledged:       snapshot.TxAcknowledgedOffset,
		rxWrittenOffset:      snapshot.Rx.WrittenOffset,
	}, reason)
}

func (observer *runtimeStatus) upsertFlowObservation(flowID protocol.FlowID, target protocol.Target, mode protocol.DeliveryMode, selection protocol.PathSelection, observation runtimeFlowObservation, reason statusapi.TransitionReason) {
	if observer == nil || flowID == (protocol.FlowID{}) {
		return
	}
	state := statusFlowState(observation.lifecycleState)
	if state == 0 {
		return
	}
	observer.mu.Lock()
	previous, exists := observer.flows[flowID]
	traffic := observer.flowTraffic[flowID]
	now := observer.now()
	startedAt := previous.entry.StartedAt
	stateSince := previous.entry.StateSince
	if !exists || startedAt.IsZero() {
		startedAt = now
	}
	if !exists || previous.entry.State != state || stateSince.IsZero() {
		stateSince = now
	}
	preferredConnectionID := ""
	if observation.hasPreferred {
		preferredConnectionID = observer.connections[observation.preferredAttachment.SessionGeneration]
	}
	unacknowledged := uint64(0)
	if observation.txAllocatedOffset >= observation.txAcknowledged {
		unacknowledged = observation.txAllocatedOffset - observation.txAcknowledged
	}
	entry := statusapi.Flow{
		FlowID: observation.correlationID, TargetType: statusTargetType(target),
		DeliveryMode: mode, PathSelection: selection,
		AdaptiveState:      statusAdaptiveState(observation.adaptiveState),
		AdaptiveTransition: statusAdaptiveTransition(observation.adaptiveTransition),
		State:              state, Reason: reason, StartedAt: startedAt, StateSince: stateSince,
		PublishedAttachments: uint32(observation.publishedAttachments), PolicyAttachments: uint32(observation.policyAttachments),
		PreferredConnectionID: preferredConnectionID, UnacknowledgedBytes: unacknowledged,
		TxAllocatedOffset: observation.txAllocatedOffset, TxAcknowledged: observation.txAcknowledged,
		RxWrittenOffset: observation.rxWrittenOffset, RetransmittedBytes: traffic.retransmitted, RedundantBytes: traffic.redundant,
	}
	if !exists && len(observer.flows) >= observer.maxFlows {
		observer.mu.Unlock()
		return
	}
	if exists {
		entry.IDHash = previous.entry.IDHash
		if entry.FlowID == "" {
			entry.FlowID = previous.entry.FlowID
		}
		if previous.target == target {
			entry.TargetHash = previous.entry.TargetHash
		}
	}
	if entry.IDHash == "" {
		entry.IDHash = observer.hasher.FlowID(flowID)
	}
	if entry.TargetHash == "" {
		entry.TargetHash = observer.hasher.Target(target)
	}
	if entry.IDHash == "" || entry.TargetHash == "" {
		observer.mu.Unlock()
		return
	}
	if exists && previous.target == target && previous.entry == entry {
		observer.mu.Unlock()
		return
	}
	resourcesBefore := observer.resources
	if !exists {
		observer.resources.Flows++
		observer.adjustFlowState(state, 1)
	} else if previous.entry.State != state {
		observer.adjustFlowState(previous.entry.State, -1)
		observer.adjustFlowState(state, 1)
	}
	observer.flows[flowID] = runtimeFlowStatus{target: target, entry: entry}
	if observer.resources != resourcesBefore {
		observer.publishResourcesLocked()
	}
	observer.mu.Unlock()
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertFlow, Flow: entry})
}

func (observer *runtimeStatus) terminalFlow(flowID protocol.FlowID, state statusapi.FlowState, reason statusapi.TransitionReason) {
	if observer == nil || flowID == (protocol.FlowID{}) || state != statusapi.FlowClosed && state != statusapi.FlowReset {
		return
	}
	observer.mu.Lock()
	previous, exists := observer.flows[flowID]
	id := previous.entry.IDHash
	if exists {
		observer.adjustFlowState(previous.entry.State, -1)
		delete(observer.flows, flowID)
		delete(observer.flowTraffic, flowID)
		if observer.resources.Flows != 0 {
			observer.resources.Flows--
		}
		observer.publishResourcesLocked()
	}
	observer.mu.Unlock()
	if id == "" {
		id = observer.hasher.FlowID(flowID)
	}
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventFlowTerminal, Terminal: statusapi.Terminal{
		IDHash: id, FlowID: previous.entry.FlowID, State: state, Reason: reason,
		DeliveryMode: previous.entry.DeliveryMode, PathSelection: previous.entry.PathSelection,
		StartedAt: previous.entry.StartedAt, FinishedAt: observer.now(),
	}})
}

func (observer *runtimeStatus) addSOCKS(delta int64) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.resources.SOCKSConnections = adjustResource(observer.resources.SOCKSConnections, delta)
	observer.publishResourcesLocked()
	observer.mu.Unlock()
}

func (observer *runtimeStatus) addTargetDial(delta int64) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.resources.PendingTargetDials = adjustResource(observer.resources.PendingTargetDials, delta)
	observer.publishResourcesLocked()
	observer.mu.Unlock()
}

func (observer *runtimeStatus) setTombstones(value uint64) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.resources.Tombstones = value
	observer.publishResourcesLocked()
	observer.mu.Unlock()
}

func (observer *runtimeStatus) setHealthy(value bool) {
	if observer != nil {
		observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventSetHealth, Healthy: value})
	}
}

func (observer *runtimeStatus) adjustFlowState(state statusapi.FlowState, delta int64) {
	switch state {
	case statusapi.FlowOpening, statusapi.FlowAwaitingAttachment:
		observer.resources.OpeningFlows = adjustResource(observer.resources.OpeningFlows, delta)
	case statusapi.FlowRecovering:
		observer.resources.RecoveringFlows = adjustResource(observer.resources.RecoveringFlows, delta)
	}
}

func (observer *runtimeStatus) publishResourcesLocked() {
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventSetResources, Resources: observer.resources})
}

func adjustResource(value uint64, delta int64) uint64 {
	if delta >= 0 {
		increment := uint64(delta)
		if ^uint64(0)-value < increment {
			return ^uint64(0)
		}
		return value + increment
	}
	decrement := uint64(-delta)
	if decrement > value {
		return 0
	}
	return value - decrement
}

func statusFlowState(state flow.LifecycleState) statusapi.FlowState {
	switch state {
	case flow.AwaitingAttachment:
		return statusapi.FlowAwaitingAttachment
	case flow.Relaying:
		return statusapi.FlowRelaying
	case flow.Recovering:
		return statusapi.FlowRecovering
	case flow.Closing:
		return statusapi.FlowClosing
	case flow.Resetting:
		return statusapi.FlowResetting
	case flow.Closed:
		return statusapi.FlowClosed
	case flow.Reset:
		return statusapi.FlowReset
	default:
		return 0
	}
}

func statusAdaptiveState(state policy.AdaptiveState) statusapi.AdaptiveState {
	switch state {
	case policy.AdaptiveTargeted:
		return statusapi.AdaptiveTargeted
	case policy.AdaptiveFull:
		return statusapi.AdaptiveFull
	case policy.AdaptiveWaiting:
		return statusapi.AdaptiveWaiting
	default:
		return statusapi.AdaptiveSingle
	}
}

func statusAdaptiveTransition(transition policy.Transition) statusapi.AdaptiveTransition {
	switch transition {
	case policy.TransitionAcknowledgementGap:
		return statusapi.AdaptiveTransitionAcknowledgementGap
	case policy.TransitionRetryEscalated:
		return statusapi.AdaptiveTransitionRetryEscalated
	case policy.TransitionStableAcknowledgement:
		return statusapi.AdaptiveTransitionStableAcknowledgement
	case policy.TransitionAllAttachmentsLost:
		return statusapi.AdaptiveTransitionAllAttachmentsLost
	case policy.TransitionAttachmentRestored:
		return statusapi.AdaptiveTransitionAttachmentRestored
	default:
		return statusapi.AdaptiveTransitionNone
	}
}

func statusTargetType(target protocol.Target) protocol.AddressType {
	if target.DNSName != "" {
		return protocol.AddressDNS
	}
	if target.Address.Unmap().Is4() {
		return protocol.AddressIPv4
	}
	return protocol.AddressIPv6
}

func clientStatusReasonForReset(reason protocol.ResetReason) statusapi.TransitionReason {
	switch reason {
	case protocol.ResetProtocolConflict:
		return statusapi.ReasonProtocolConflict
	case protocol.ResetResourceLimit:
		return statusapi.ReasonResourceLimit
	case protocol.ResetLocalIOFailure:
		return statusapi.ReasonLocalIOFailure
	case protocol.ResetInternalFailure:
		return statusapi.ReasonInternalFailure
	case protocol.ResetDeadlineExceeded:
		return statusapi.ReasonDeadlineExceeded
	default:
		return statusapi.ReasonCancelled
	}
}

type limitedListener struct {
	net.Listener
	slots     chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newLimitedListener(listener net.Listener, maximum int) (net.Listener, error) {
	if listener == nil || maximum < 1 {
		return nil, statusapi.ErrInvalidHTTP
	}
	return &limitedListener{Listener: listener, slots: make(chan struct{}, maximum), closed: make(chan struct{})}, nil
}

func (listener *limitedListener) Accept() (net.Conn, error) {
	select {
	case listener.slots <- struct{}{}:
	case <-listener.closed:
		return nil, net.ErrClosed
	}
	connection, err := listener.Listener.Accept()
	if err != nil {
		<-listener.slots
		return nil, err
	}
	return &limitedConnection{Conn: connection, release: func() { <-listener.slots }}, nil
}

func (listener *limitedListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return listener.Listener.Close()
}

type limitedConnection struct {
	net.Conn
	once    sync.Once
	release func()
}

func (connection *limitedConnection) Close() error {
	connection.once.Do(connection.release)
	return connection.Conn.Close()
}
