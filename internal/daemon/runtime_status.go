package daemon

import (
	"crypto/rand"
	"io"
	"math"
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
	rejections  statusapi.Rejected
	flows       map[protocol.FlowID]runtimeFlowStatus
	flowTraffic map[protocol.FlowID]runtimeFlowTraffic
	sessions    map[string]statusapi.Session
	fastestID   string
	runtime     map[uint64]sessionRuntimeSnapshot
	connections map[uint64]string
	interfaces  map[int]struct{}
}

type runtimeFlowStatus struct {
	target          protocol.Target
	entry           statusapi.Flow
	recoveringSince time.Time
	recoveryCount   uint64
	recoveryMicros  uint64
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
	pathGroupID    protocol.PathGroupID
	lane           uint16
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
		runtime:     make(map[uint64]sessionRuntimeSnapshot, statusapi.MaxSessions),
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

// observeDataPayloadSent records a DATA payload only after the transport write
// has completed successfully. The summary counter is independent of the
// per-session snapshot and uses the repository's atomic saturation boundary.
func (observer *runtimeStatus) observeDataPayloadSent(payloadBytes uint64) {
	if observer == nil || payloadBytes == 0 {
		return
	}
	observer.repository.AddCounter(statusapi.CounterDataPayloadBytesSent, payloadBytes)
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
	observer.mu.Lock()
	id, entry, ok := observer.mergeSessionObservationLocked(observation)
	if !ok {
		observer.mu.Unlock()
		return
	}
	updates := observer.recomputeFastestLocked()
	entry = observer.sessions[id]
	observer.mu.Unlock()
	observer.publishSessionUpdates(updates, id)
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertSession, Session: entry})
}

func (observer *runtimeStatus) mergeSessionObservationLocked(observation runtimeSessionObservation) (string, statusapi.Session, bool) {
	id := observer.hasher.SessionID(observation.generation)
	if id == "" {
		return "", statusapi.Session{}, false
	}
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
		IDHash: id, ConnectionID: previous.ConnectionID, PathGroupID: previous.PathGroupID, Lane: previous.Lane,
		Transport: observation.transportName, Interface: observation.interfaceName,
		LocalAddress: observation.localAddress.Unmap().String(), LocalEndpoint: previous.LocalEndpoint,
		RemoteEndpoint: previous.RemoteEndpoint, PrincipalHash: previous.PrincipalHash,
		State: observation.state, Reason: observation.reason, StateSince: stateSince,
		LastProbeAt: previous.LastProbeAt, Reconnects: previous.Reconnects,
		Quality: previous.Quality, Fastest: previous.Fastest,
	}
	if runtimeSnapshot, exists := observer.runtime[observation.generation]; exists {
		entry.Quality = statusQualityFromRuntime(runtimeSnapshot, entry.Quality)
		delete(observer.runtime, observation.generation)
	}
	if observation.connectionID != "" {
		entry.ConnectionID = observation.connectionID
		observer.connections[observation.generation] = observation.connectionID
	}
	if observation.principalID != "" {
		entry.PrincipalHash = observer.hasher.Principal(observation.principalID)
	}
	if protocol.ValidPathGroupID(observation.pathGroupID) {
		entry.PathGroupID = observer.hasher.PathGroupID(observation.pathGroupID)
	}
	if observation.lane != 0 {
		entry.Lane = observation.lane
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
	return id, entry, true
}

// observeSessionDataReceived accumulates received DATA payload bytes per wire
// session. It is the receiving-direction counterpart of WrittenDataPayloadBytes
// and is observed directly from the wire read path because receiving does not
// traverse the session runtime. The update is local to the status entry and
// deliberately does not publish a status event or recompute fastest: the next
// throttled runtime snapshot carries the value out, bounding event volume.
func (observer *runtimeStatus) observeSessionDataReceived(generation uint64, payloadBytes uint64) {
	if observer == nil || generation == 0 || payloadBytes == 0 {
		return
	}
	id := observer.hasher.SessionID(generation)
	if id == "" {
		return
	}
	observer.mu.Lock()
	entry, exists := observer.sessions[id]
	if !exists {
		observer.mu.Unlock()
		return
	}
	entry.Quality.ReceivedDataPayloadBytes += payloadBytes
	observer.sessions[id] = entry
	observer.mu.Unlock()
	observer.repository.AddCounter(statusapi.CounterDataPayloadBytesReceived, payloadBytes)
}

func (observer *runtimeStatus) observeSessionPeerSendCapacity(generation uint64, snapshot peerSendCapacitySnapshot) {
	if observer == nil || generation == 0 {
		return
	}
	id := observer.hasher.SessionID(generation)
	if id == "" {
		return
	}
	observer.mu.Lock()
	entry, exists := observer.sessions[id]
	if !exists {
		observer.mu.Unlock()
		return
	}
	entry.Quality.PeerSendCapacityBytesSec = 0
	entry.Quality.PeerSendDataSampleAt = nil
	entry.Quality.PeerSendDataSampleExpiresAt = nil
	if snapshot.Measured && snapshot.CapacityBytesSec != 0 && snapshot.SampleAge >= 0 && snapshot.SampleFreshness > 0 {
		observedAt := observer.now()
		sampleAt := observedAt.Add(-snapshot.SampleAge)
		expiresAt := sampleAt.Add(snapshot.SampleFreshness)
		entry.Quality.PeerSendCapacityBytesSec = snapshot.CapacityBytesSec
		entry.Quality.PeerSendDataSampleAt = &sampleAt
		entry.Quality.PeerSendDataSampleExpiresAt = &expiresAt
	}
	observer.sessions[id] = entry
	observer.mu.Unlock()
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertSession, Session: entry})
}

func (observer *runtimeStatus) observeSessionRuntime(generation uint64, snapshot sessionRuntimeSnapshot) {
	if observer == nil || generation == 0 {
		return
	}
	id := observer.hasher.SessionID(generation)
	observer.mu.Lock()
	entry, exists := observer.sessions[id]
	if !exists {
		if snapshot.Closed || len(observer.runtime) >= statusapi.MaxSessions {
			observer.mu.Unlock()
			return
		}
		observer.runtime[generation] = snapshot
		observer.mu.Unlock()
		return
	}
	previousProbeSamples := entry.Quality.ProbeSamples
	previous := entry
	entry.Quality = statusQualityFromRuntime(snapshot, entry.Quality)
	if snapshot.Quality.ProbeSamples > previousProbeSamples {
		entry.LastProbeAt = observer.now()
	}
	observer.sessions[id] = entry
	var updates []statusapi.Session
	if sessionRankChanged(previous, entry) {
		updates = observer.updateFastestForSessionLocked(id, previous)
		entry = observer.sessions[id]
	}
	observer.mu.Unlock()
	observer.publishSessionUpdates(updates, id)
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertSession, Session: entry})
}

func statusQualityFromRuntime(snapshot sessionRuntimeSnapshot, previous statusapi.Quality) statusapi.Quality {
	quality := snapshot.Quality
	result := previous
	result.SmoothedRTTMicros = durationMicros(quality.SRTT)
	result.RetryMicros = durationMicros(quality.RetryEstimate)
	result.CapacityBytesSec = finiteRate(quality.CapacityBytesSec)
	result.QueuedBytes = quality.QueuedBytes
	result.InFlightBytes = quality.InFlightBytes
	result.StallPenaltyMicros = durationMicros(quality.StallPenalty)
	result.DataSampleFresh = quality.DataSampleFresh
	result.DataSampleAgeMillis = nil
	if quality.DataSamples != 0 && quality.DataSampleAge >= 0 {
		age := uint64(quality.DataSampleAge / time.Millisecond)
		result.DataSampleAgeMillis = &age
	}
	result.LastDataCapacityBytesSec = finiteRate(quality.LastDataCapacity)
	result.ScheduledDataPayloadBytes = snapshot.ScheduledData
	result.WrittenDataPayloadBytes = snapshot.WrittenData
	result.EligibleAckedDataPayloadBytes = snapshot.EligibleAckedData
	result.DataQueueFrames = snapshot.DataQueueFrames
	result.ActiveDataFlows = uint32(maxInt(snapshot.ActiveDataFlows, 0))
	result.ProbeSamples = quality.ProbeSamples
	return result
}

func durationMicros(value time.Duration) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value / time.Microsecond)
}

func finiteRate(value float64) uint64 {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) || value >= float64(^uint64(0)) {
		if value >= float64(^uint64(0)) {
			return ^uint64(0)
		}
		return 0
	}
	return uint64(value)
}

func maxInt(value, minimum int) int {
	if value < minimum {
		return minimum
	}
	return value
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
	delete(observer.runtime, generation)
	updates := observer.recomputeFastestLocked()
	observer.mu.Unlock()
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventRemoveSession, SessionID: id})
	observer.publishSessionUpdates(updates, "")
}

func (observer *runtimeStatus) syncClientSessions(transportName, principalID string, snapshot clientcore.SessionManagerSnapshot, authenticated map[uint64]*wireSession) {
	if observer == nil {
		return
	}
	observations := make([]runtimeSessionObservation, 0, len(snapshot.Sessions))
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
			pathGroupID: session.PathGroupID, lane: session.Lane + 1,
			state: state, reason: reason, reconnects: session.Reconnects,
		}
		if wire := authenticated[session.Generation]; wire != nil && wire.connectionID != (auth.CorrelationID{}) {
			observation.connectionID = wire.connectionID.String()
			observation.principalID = principalID
		}
		observations = append(observations, observation)
	}

	next := make(map[string]struct{}, len(snapshot.Sessions))
	var removed []string
	entries := make([]statusapi.Session, 0, len(observations))
	observer.mu.Lock()
	for _, observation := range observations {
		id, _, ok := observer.mergeSessionObservationLocked(observation)
		if ok {
			next[id] = struct{}{}
		}
	}
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
	for id := range next {
		entries = append(entries, observer.sessions[id])
	}
	observer.mu.Unlock()
	for _, id := range removed {
		observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventRemoveSession, SessionID: id})
	}
	observer.publishSessionUpdates(updates, "")
	for _, entry := range entries {
		observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventUpsertSession, Session: entry})
	}
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
	observer.fastestID = fastestID
	return updates
}

func (observer *runtimeStatus) updateFastestForSessionLocked(id string, previous statusapi.Session) []statusapi.Session {
	entry, exists := observer.sessions[id]
	if !exists || observer.fastestID == "" {
		return observer.recomputeFastestLocked()
	}
	if id == observer.fastestID {
		if sessionRankEligible(entry) && !sessionRankLess(previous, entry) {
			return nil
		}
		return observer.recomputeFastestLocked()
	}
	fastest, exists := observer.sessions[observer.fastestID]
	if !exists || !sessionRankEligible(fastest) {
		return observer.recomputeFastestLocked()
	}
	if !sessionRankLess(entry, fastest) {
		return nil
	}
	fastest.Fastest = false
	entry.Fastest = true
	observer.sessions[observer.fastestID] = fastest
	observer.sessions[id] = entry
	observer.fastestID = id
	return []statusapi.Session{fastest, entry}
}

func sessionRankChanged(previous, current statusapi.Session) bool {
	return previous.State != current.State ||
		previous.Quality.StallPenaltyMicros != current.Quality.StallPenaltyMicros ||
		previous.Quality.SmoothedRTTMicros != current.Quality.SmoothedRTTMicros
}

func sessionRankEligible(session statusapi.Session) bool {
	return session.State == statusapi.SessionReady && session.Quality.SmoothedRTTMicros != 0
}

func sessionRankLess(candidate, current statusapi.Session) bool {
	if !sessionRankEligible(candidate) {
		return false
	}
	if !sessionRankEligible(current) {
		return true
	}
	if candidate.Quality.StallPenaltyMicros != current.Quality.StallPenaltyMicros {
		return candidate.Quality.StallPenaltyMicros < current.Quality.StallPenaltyMicros
	}
	if candidate.Quality.SmoothedRTTMicros != current.Quality.SmoothedRTTMicros {
		return candidate.Quality.SmoothedRTTMicros < current.Quality.SmoothedRTTMicros
	}
	return candidate.IDHash < current.IDHash
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
	// Recovery accounting: entering Recovering starts the timer; leaving it
	// completes one recovery and accumulates its duration. The counters survive
	// further Relaying/Recovering cycles and are carried into the terminal entry.
	recoveringSince := previous.recoveringSince
	recoveryCount := previous.recoveryCount
	recoveryMicros := previous.recoveryMicros
	enteringRecovery := state == statusapi.FlowRecovering && (!exists || previous.entry.State != statusapi.FlowRecovering)
	leavingRecovery := exists && previous.entry.State == statusapi.FlowRecovering && state != statusapi.FlowRecovering
	if enteringRecovery {
		recoveringSince = now
	} else if leavingRecovery && !recoveringSince.IsZero() {
		recoveryCount++
		recoveryMicros += uint64(now.Sub(recoveringSince) / time.Microsecond)
		recoveringSince = time.Time{}
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
		RecoveryCount: recoveryCount, RecoveryMicros: recoveryMicros,
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
	observer.flows[flowID] = runtimeFlowStatus{
		target: target, entry: entry,
		recoveringSince: recoveringSince, recoveryCount: recoveryCount, recoveryMicros: recoveryMicros,
	}
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
		RecoveryCount: previous.recoveryCount, RecoveryMicros: previous.recoveryMicros,
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

// setResourceLimits records the configured admission ceilings once at startup.
// The daemon passes its effective limits after construction; later calls are
// allowed but not expected.
func (observer *runtimeStatus) setResourceLimits(sessions, flows, socksConnections, pendingTargetDials, tombstones uint64) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.resources.MaxSessions = sessions
	observer.resources.MaxFlows = flows
	observer.resources.MaxSOCKSConnections = socksConnections
	observer.resources.MaxPendingTargetDials = pendingTargetDials
	observer.resources.MaxTombstones = tombstones
	observer.publishResourcesLocked()
	observer.mu.Unlock()
}

type flowRejection uint8

const (
	flowRejectionRateLimited flowRejection = iota + 1
	flowRejectionOpeningCapacity
	flowRejectionTargetDialCapacity
)

// rejectSession, rejectFlow and rejectSOCKS saturate the matching admission
// denial counter and publish it immediately. They are safe to call from any
// daemon path and are not throttled: denials are rare, control-plane events.
func (observer *runtimeStatus) rejectSession() {
	observer.reject(1, 0, 0)
}

func (observer *runtimeStatus) rejectFlow(classification ...flowRejection) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.rejections.Flows = adjustResource(observer.rejections.Flows, 1)
	if len(classification) == 1 {
		switch classification[0] {
		case flowRejectionRateLimited:
			observer.rejections.FlowRateLimited = adjustResource(observer.rejections.FlowRateLimited, 1)
		case flowRejectionOpeningCapacity:
			observer.rejections.FlowOpeningCapacity = adjustResource(observer.rejections.FlowOpeningCapacity, 1)
		case flowRejectionTargetDialCapacity:
			observer.rejections.FlowTargetDialCapacity = adjustResource(observer.rejections.FlowTargetDialCapacity, 1)
		}
	}
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventSetRejected, Rejected: observer.rejections})
	observer.mu.Unlock()
}

func (observer *runtimeStatus) rejectSOCKS() {
	observer.reject(0, 0, 1)
}

func (observer *runtimeStatus) reject(sessions, flows, socks uint64) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	observer.rejections.Sessions = adjustResource(observer.rejections.Sessions, int64(sessions))
	observer.rejections.Flows = adjustResource(observer.rejections.Flows, int64(flows))
	observer.rejections.SOCKSConnections = adjustResource(observer.rejections.SOCKSConnections, int64(socks))
	observer.repository.TryRecord(statusapi.Event{Kind: statusapi.EventSetRejected, Rejected: observer.rejections})
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
