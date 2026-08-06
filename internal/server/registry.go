package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
)

const (
	OpenDeadline      = 15 * time.Second
	TombstoneLifetime = 60 * time.Second

	HardMaxServerFlows                  = 8192
	HardMaxServerFlowsPerPrincipal      = HardMaxServerFlows
	HardMaxServerOpeningFlows           = 512
	HardMaxServerTargetDials            = 512
	HardMaxServerTombstones             = 32768
	HardMaxServerTombstonesPerPrincipal = HardMaxServerTombstones
)

var ErrInvalidRegistryConfig = errors.New("server: invalid registry configuration")

// RegistryLimits defines hard runtime bounds for the server registry. A terminal
// slot is reserved during OPEN so every active flow can converge within the
// terminal record limit.
type RegistryLimits struct {
	Flows                  int
	FlowsPerPrincipal      int
	OpeningFlows           int
	TargetDials            int
	Tombstones             int
	TombstonesPerPrincipal int
	FlowSendWindowBytes    uint64
	FlowReceiveWindowBytes uint64
}

// FlowKey is the unique server registry key. The same FlowID under different principals is unrelated.
type FlowKey struct {
	PrincipalID string
	FlowID      protocol.FlowID
}

// RegistryEntryState covers OPEN preflight, mirrored active flow state, and terminal records.
type RegistryEntryState uint8

const (
	RegistryOpeningCapability RegistryEntryState = iota + 1
	RegistryOpeningTarget
	RegistryAwaitingAttachment
	RegistryRelaying
	RegistryRecovering
	RegistryClosing
	RegistryResetting
	RegistryTombstoneClosed
	RegistryTombstoneReset
)

type TerminalKind uint8

const (
	TerminalClosed TerminalKind = iota + 1
	TerminalReset
)

// GenerateCapabilityAction is the only random action allowed after OPEN reserves all resources.
type GenerateCapabilityAction struct {
	Key        FlowKey
	Generation uint64
	Deadline   time.Time
}

// OpenDialAction describes the unique target dial request. It contains values only, without a connection or network executor.
type OpenDialAction struct {
	Key        FlowKey
	Generation uint64
	Target     protocol.Target
	Deadline   time.Time
}

// OpenOutcome is the deterministic output of an OPEN, random action, dial result,
// or deadline event. When Ready is true, Result can be sent immediately;
// CloseLateTarget requires the executor to close the newly completed target
// connection. At most one of the other two actions is non-nil at a time.
type OpenOutcome struct {
	Ready              bool
	Result             protocol.OpenResult
	Operation          *OpenOperation
	GenerateCapability *GenerateCapabilityAction
	Dial               *OpenDialAction
	Owner              *flow.Flow
	CloseLateTarget    bool
}

// OpenOperation lets identical concurrent OPEN requests share one bounded
// completion signal instead of allocating a queue per waiter. Result is available
// only after Done closes.
type OpenOperation struct {
	done   chan struct{}
	mu     sync.RWMutex
	ready  bool
	result protocol.OpenResult
}

func newOpenOperation() *OpenOperation {
	return &OpenOperation{done: make(chan struct{})}
}

func (operation *OpenOperation) Done() <-chan struct{} {
	if operation == nil {
		return nil
	}
	return operation.done
}

func (operation *OpenOperation) Result() (protocol.OpenResult, bool) {
	if operation == nil {
		return protocol.OpenResult{}, false
	}
	operation.mu.RLock()
	result, ready := operation.result, operation.ready
	operation.mu.RUnlock()
	return result, ready
}

func (operation *OpenOperation) complete(result protocol.OpenResult) {
	if operation == nil {
		return
	}
	operation.mu.Lock()
	if operation.ready {
		operation.mu.Unlock()
		return
	}
	operation.result = result
	operation.ready = true
	close(operation.done)
	operation.mu.Unlock()
}

// RegistryEntrySnapshot excludes OpenToken, capability tokens, digests, and target addresses.
type RegistryEntrySnapshot struct {
	Key                FlowKey
	State              RegistryEntryState
	DeliveryMode       protocol.DeliveryMode
	PathSelection      protocol.PathSelection
	Constraints        protocol.DeliveryConstraints
	Owner              *flow.Flow
	ActionGeneration   uint64
	OpenExpiresAt      time.Time
	TerminalKind       TerminalKind
	TerminalResult     protocol.OpenResultCode
	TombstoneExpiresAt time.Time
}

type RegistrySnapshot struct {
	Entries                int
	Flows                  int
	OpeningFlows           int
	TargetDialReservations int
	Tombstones             int
	TombstoneReservations  int
}

type PrincipalUsageSnapshot struct {
	Flows                 int
	Tombstones            int
	TombstoneReservations int
}

type registryKey struct {
	principalID string
	flowID      protocol.FlowID
}

type registryEntry struct {
	key                registryKey
	state              RegistryEntryState
	openToken          protocol.OpenToken
	target             protocol.Target
	deliveryMode       protocol.DeliveryMode
	pathSelection      protocol.PathSelection
	constraints        protocol.DeliveryConstraints
	capability         protocol.Capability
	capabilityDigest   [sha256.Size]byte
	capabilityInFlight bool
	owner              *flow.Flow
	actionGeneration   uint64
	openExpiresAt      time.Time
	operation          *OpenOperation
	terminalKind       TerminalKind
	terminalResult     protocol.OpenResultCode
	tombstoneExpiresAt time.Time
}

type principalUsage struct {
	flows                 int
	tombstones            int
	tombstoneReservations int
}

// Registry stores a bounded (PrincipalID, FlowID) index. It performs no network
// I/O; callers must execute returned random and target dial actions and feed
// results back with the original generation.
type Registry struct {
	mu                     sync.Mutex
	randomMu               sync.Mutex
	clockMu                sync.Mutex
	limits                 RegistryLimits
	random                 io.Reader
	now                    func() time.Time
	entries                map[registryKey]*registryEntry
	principal              map[string]*principalUsage
	nextGeneration         uint64
	flows                  int
	openingFlows           int
	targetDialReservations int
	tombstones             int
	tombstoneReservations  int
}

var _ JoinDirectory = (*Registry)(nil)

func NewRegistry(limits RegistryLimits, random io.Reader, now func() time.Time) (*Registry, error) {
	if random == nil || now == nil || !validRegistryLimits(limits) {
		return nil, ErrInvalidRegistryConfig
	}
	return &Registry{
		limits:    limits,
		random:    random,
		now:       now,
		entries:   make(map[registryKey]*registryEntry),
		principal: make(map[string]*principalUsage),
	}, nil
}

func validRegistryLimits(limits RegistryLimits) bool {
	return limits.Flows > 0 && limits.Flows <= HardMaxServerFlows &&
		limits.FlowsPerPrincipal > 0 && limits.FlowsPerPrincipal <= HardMaxServerFlowsPerPrincipal && limits.FlowsPerPrincipal <= limits.Flows &&
		limits.OpeningFlows > 0 && limits.OpeningFlows <= HardMaxServerOpeningFlows && limits.OpeningFlows <= limits.Flows &&
		limits.TargetDials > 0 && limits.TargetDials <= HardMaxServerTargetDials && limits.TargetDials <= limits.OpeningFlows &&
		limits.Tombstones > 0 && limits.Tombstones <= HardMaxServerTombstones &&
		limits.TombstonesPerPrincipal > 0 && limits.TombstonesPerPrincipal <= HardMaxServerTombstonesPerPrincipal && limits.TombstonesPerPrincipal <= limits.Tombstones
}

// HandleOpen atomically validates and reserves flow, target dial, and future
// terminal record capacity. It returns only a GenerateCapability action and does
// not read randomness or dial a target while holding the lock.
func (registry *Registry) HandleOpen(principalID string, request protocol.Open) OpenOutcome {
	if registry == nil {
		return openFailure(request.FlowID, protocol.OpenInternalFailure)
	}
	if !protocol.ValidPrincipalID(principalID) || request.FlowID == (protocol.FlowID{}) {
		return openFailure(request.FlowID, protocol.OpenInternalFailure)
	}
	now := registry.currentTime()
	key := registryKey{principalID: principalID, flowID: request.FlowID}

	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	if entry := registry.entries[key]; entry != nil {
		outcome := registry.duplicateOpenLocked(entry, request)
		registry.mu.Unlock()
		return outcome
	}
	normalizedTarget, failure := validateOpen(principalID, request)
	if failure != protocol.OpenSuccess {
		registry.mu.Unlock()
		return openFailure(request.FlowID, failure)
	}
	if registry.nextGeneration == math.MaxUint64 {
		registry.mu.Unlock()
		return openFailure(request.FlowID, protocol.OpenInternalFailure)
	}
	if !registry.reserveLocked(principalID) {
		registry.mu.Unlock()
		return openFailure(request.FlowID, protocol.OpenResourceLimit)
	}
	registry.nextGeneration++
	generation := registry.nextGeneration
	operation := newOpenOperation()
	entry := &registryEntry{
		key:              key,
		state:            RegistryOpeningCapability,
		openToken:        request.OpenToken,
		target:           normalizedTarget,
		deliveryMode:     request.DeliveryMode,
		pathSelection:    request.PathSelection,
		constraints:      request.Constraints,
		owner:            flow.NewFlowWithWindows(registry.limits.FlowSendWindowBytes, registry.limits.FlowReceiveWindowBytes),
		actionGeneration: generation,
		openExpiresAt:    now.Add(OpenDeadline),
		operation:        operation,
	}
	registry.entries[key] = entry
	action := &GenerateCapabilityAction{
		Key:        exportKey(key),
		Generation: generation,
		Deadline:   entry.openExpiresAt,
	}
	registry.mu.Unlock()
	return OpenOutcome{Operation: operation, GenerateCapability: action}
}

// GenerateCapability reads the injected random source and advances the matching
// generation to its unique target dial action. Concurrent duplicate calls for the
// same generation do not read a second random value.
func (registry *Registry) GenerateCapability(action GenerateCapabilityAction) OpenOutcome {
	if registry == nil || action.Generation == 0 {
		return OpenOutcome{}
	}
	now := registry.currentTime()
	key := importKey(action.Key)
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	entry := registry.entries[key]
	if entry == nil || entry.state != RegistryOpeningCapability || entry.actionGeneration != action.Generation || entry.openExpiresAt != action.Deadline {
		outcome := registry.staleOpeningOutcomeLocked(entry)
		registry.mu.Unlock()
		return outcome
	}
	if !now.Before(entry.openExpiresAt) {
		outcome := registry.terminalizeLocked(entry, TerminalReset, protocol.OpenInternalFailure, now)
		registry.mu.Unlock()
		return outcome
	}
	if entry.capabilityInFlight {
		operation := entry.operation
		registry.mu.Unlock()
		return OpenOutcome{Operation: operation}
	}
	entry.capabilityInFlight = true
	registry.mu.Unlock()

	var capability protocol.Capability
	registry.randomMu.Lock()
	_, randomErr := io.ReadFull(registry.random, capability[:])
	registry.randomMu.Unlock()
	if capability == (protocol.Capability{}) {
		randomErr = io.ErrUnexpectedEOF
	}

	now = registry.currentTime()
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	entry = registry.entries[key]
	if entry == nil || entry.state != RegistryOpeningCapability || entry.actionGeneration != action.Generation {
		outcome := registry.staleOpeningOutcomeLocked(entry)
		registry.mu.Unlock()
		return outcome
	}
	if randomErr != nil || !now.Before(entry.openExpiresAt) {
		outcome := registry.terminalizeLocked(entry, TerminalReset, protocol.OpenInternalFailure, now)
		registry.mu.Unlock()
		return outcome
	}
	entry.capability = capability
	entry.capabilityDigest = sha256.Sum256(capability[:])
	entry.capabilityInFlight = false
	entry.state = RegistryOpeningTarget
	dial := &OpenDialAction{
		Key:        exportKey(key),
		Generation: action.Generation,
		Target:     entry.target,
		Deadline:   entry.openExpiresAt,
	}
	operation := entry.operation
	registry.mu.Unlock()
	return OpenOutcome{Operation: operation, Dial: dial}
}

// CompleteDial accepts only the unique result for the current OPEN generation. A
// late successful result requires the executor to close its connection; this
// method neither receives nor operates on a net.Conn itself.
func (registry *Registry) CompleteDial(action OpenDialAction, succeeded bool) OpenOutcome {
	if registry == nil || action.Generation == 0 {
		return OpenOutcome{CloseLateTarget: succeeded}
	}
	now := registry.currentTime()
	key := importKey(action.Key)
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	entry := registry.entries[key]
	if entry == nil || entry.state != RegistryOpeningTarget || entry.actionGeneration != action.Generation || entry.target != action.Target || entry.openExpiresAt != action.Deadline {
		outcome := registry.staleOpeningOutcomeLocked(entry)
		outcome.CloseLateTarget = succeeded
		registry.mu.Unlock()
		return outcome
	}
	if !now.Before(entry.openExpiresAt) {
		outcome := registry.terminalizeLocked(entry, TerminalReset, protocol.OpenConnectFailed, now)
		outcome.CloseLateTarget = succeeded
		registry.mu.Unlock()
		return outcome
	}
	if !succeeded {
		outcome := registry.terminalizeLocked(entry, TerminalReset, protocol.OpenConnectFailed, now)
		registry.mu.Unlock()
		return outcome
	}

	registry.finishOpeningLocked(entry)
	entry.state = RegistryAwaitingAttachment
	result := successResult(entry)
	entry.operation.complete(result)
	entry.operation = nil
	outcome := OpenOutcome{Ready: true, Result: result, Owner: entry.owner}
	registry.mu.Unlock()
	return outcome
}

// ExpireOpen handles the fixed, non-refreshable deadline measured from the first OPEN. Stale generations have no effect.
func (registry *Registry) ExpireOpen(key FlowKey, generation uint64) OpenOutcome {
	if registry == nil || generation == 0 {
		return OpenOutcome{}
	}
	now := registry.currentTime()
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	entry := registry.entries[importKey(key)]
	if entry == nil || entry.actionGeneration != generation ||
		(entry.state != RegistryOpeningCapability && entry.state != RegistryOpeningTarget) {
		registry.mu.Unlock()
		return OpenOutcome{}
	}
	result := protocol.OpenInternalFailure
	if entry.state == RegistryOpeningTarget {
		result = protocol.OpenConnectFailed
	}
	outcome := registry.terminalizeLocked(entry, TerminalReset, result, now)
	registry.mu.Unlock()
	return outcome
}

// RejectOpen converges the current OPEN generation with an explicit resource
// result before external work begins. It does not accept success or rewrite a
// stale or completed generation.
func (registry *Registry) RejectOpen(key FlowKey, generation uint64, result protocol.OpenResultCode) OpenOutcome {
	if registry == nil || generation == 0 || result != protocol.OpenResourceLimit {
		return OpenOutcome{}
	}
	now := registry.currentTime()
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	entry := registry.entries[importKey(key)]
	if entry == nil || entry.actionGeneration != generation || entry.state != RegistryOpeningCapability {
		registry.mu.Unlock()
		return OpenOutcome{}
	}
	outcome := registry.terminalizeLocked(entry, TerminalReset, result, now)
	registry.mu.Unlock()
	return outcome
}

// AuthorizeJoin performs fixed-length digest and constant-time comparisons for
// both existing and missing keys. It returns a flow owner only in an attachable
// lifecycle state and creates neither provisional nor published attachments.
func (registry *Registry) AuthorizeJoin(principalID string, flowID protocol.FlowID, capability protocol.Capability) (JoinAuthorization, bool) {
	providedDigest := sha256.Sum256(capability[:])
	expectedDigest := dummyCapabilityDigest
	var entry *registryEntry
	if registry != nil {
		now := registry.currentTime()
		registry.mu.Lock()
		registry.pruneExpiredLocked(now)
		entry = registry.entries[registryKey{principalID: principalID, flowID: flowID}]
		if entry != nil {
			expectedDigest = entry.capabilityDigest
		}
		matches := subtle.ConstantTimeCompare(providedDigest[:], expectedDigest[:]) == 1
		if matches && entry != nil && entry.owner != nil && joinableRegistryState(entry.state) {
			authorization := JoinAuthorization{
				PrincipalID: principalID,
				FlowID:      flowID,
				Flow:        entry.owner,
			}
			registry.mu.Unlock()
			return authorization, true
		}
		registry.mu.Unlock()
		return JoinAuthorization{}, false
	}
	_ = subtle.ConstantTimeCompare(providedDigest[:], expectedDigest[:])
	return JoinAuthorization{}, false
}

// UpdateLifecycle mirrors a lifecycle transition already completed by the same
// flow owner. It does not modify attachment sets itself. After entering Closing,
// duplicate OPEN requests can no longer retrieve the capability token, but its
// digest remains until residency ends so the original token can still authorize JOIN.
func (registry *Registry) UpdateLifecycle(key FlowKey, owner *flow.Flow, state flow.LifecycleState) bool {
	if registry == nil || owner == nil {
		return false
	}
	wantState, valid := registryStateForLifecycle(state)
	if !valid {
		return false
	}
	registry.mu.Lock()
	entry := registry.entries[importKey(key)]
	if entry == nil || entry.owner != owner || !validLifecycleTransition(entry.state, wantState) {
		registry.mu.Unlock()
		return false
	}
	entry.state = wantState
	if wantState == RegistryClosing {
		entry.openToken = protocol.OpenToken{}
		entry.capability = protocol.Capability{}
		entry.target = protocol.Target{}
	}
	if wantState == RegistryResetting {
		entry.openToken = protocol.OpenToken{}
		entry.capability = protocol.Capability{}
		entry.capabilityDigest = [sha256.Size]byte{}
		entry.target = protocol.Target{}
	}
	registry.mu.Unlock()
	return true
}

// MarkTerminal replaces an active flow with a fixed terminal record containing no sensitive values, target, or owner.
func (registry *Registry) MarkTerminal(key FlowKey, owner *flow.Flow, terminal TerminalKind) bool {
	if registry == nil || owner == nil || (terminal != TerminalClosed && terminal != TerminalReset) {
		return false
	}
	now := registry.currentTime()
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	entry := registry.entries[importKey(key)]
	if entry == nil || entry.owner != owner || entry.state == RegistryOpeningCapability || entry.state == RegistryOpeningTarget || entryIsTombstone(entry.state) {
		registry.mu.Unlock()
		return false
	}
	registry.terminalizeLocked(entry, terminal, protocol.OpenInternalFailure, now)
	registry.mu.Unlock()
	return true
}

func (registry *Registry) Lookup(key FlowKey) (RegistryEntrySnapshot, bool) {
	if registry == nil {
		return RegistryEntrySnapshot{}, false
	}
	now := registry.currentTime()
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	entry := registry.entries[importKey(key)]
	if entry == nil {
		registry.mu.Unlock()
		return RegistryEntrySnapshot{}, false
	}
	snapshot := snapshotEntry(entry)
	registry.mu.Unlock()
	return snapshot, true
}

func (registry *Registry) Snapshot() RegistrySnapshot {
	if registry == nil {
		return RegistrySnapshot{}
	}
	now := registry.currentTime()
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	snapshot := RegistrySnapshot{
		Entries:                len(registry.entries),
		Flows:                  registry.flows,
		OpeningFlows:           registry.openingFlows,
		TargetDialReservations: registry.targetDialReservations,
		Tombstones:             registry.tombstones,
		TombstoneReservations:  registry.tombstoneReservations,
	}
	registry.mu.Unlock()
	return snapshot
}

func (registry *Registry) PrincipalUsage(principalID string) PrincipalUsageSnapshot {
	if registry == nil {
		return PrincipalUsageSnapshot{}
	}
	now := registry.currentTime()
	registry.mu.Lock()
	registry.pruneExpiredLocked(now)
	usage := registry.principal[principalID]
	snapshot := PrincipalUsageSnapshot{}
	if usage != nil {
		snapshot = PrincipalUsageSnapshot{
			Flows:                 usage.flows,
			Tombstones:            usage.tombstones,
			TombstoneReservations: usage.tombstoneReservations,
		}
	}
	registry.mu.Unlock()
	return snapshot
}

func validateOpen(principalID string, request protocol.Open) (protocol.Target, protocol.OpenResultCode) {
	if !protocol.ValidPrincipalID(principalID) || request.FlowID == (protocol.FlowID{}) || request.OpenToken == (protocol.OpenToken{}) {
		return protocol.Target{}, protocol.OpenInternalFailure
	}
	if !protocol.ValidDeliveryPolicy(request.DeliveryMode, request.PathSelection, request.Constraints) {
		return protocol.Target{}, protocol.OpenUnsupportedPolicy
	}
	if err := protocol.ValidateTarget(request.Target); err != nil {
		return protocol.Target{}, protocol.OpenInvalidTarget
	}
	target := request.Target
	if target.Address.IsValid() {
		target.Address = target.Address.Unmap()
	}
	return target, protocol.OpenSuccess
}

func (registry *Registry) duplicateOpenLocked(entry *registryEntry, request protocol.Open) OpenOutcome {
	if entryIsTombstone(entry.state) {
		return openFailure(request.FlowID, entry.terminalResult)
	}
	if entry.state == RegistryClosing || entry.state == RegistryResetting {
		return openFailure(request.FlowID, protocol.OpenInternalFailure)
	}
	if !sameOpen(entry, request) {
		return openFailure(request.FlowID, protocol.OpenInternalFailure)
	}
	if entry.state == RegistryOpeningCapability || entry.state == RegistryOpeningTarget {
		return OpenOutcome{Operation: entry.operation}
	}
	return OpenOutcome{Ready: true, Result: successResult(entry), Owner: entry.owner}
}

func sameOpen(entry *registryEntry, request protocol.Open) bool {
	return entry != nil &&
		subtle.ConstantTimeCompare(entry.openToken[:], request.OpenToken[:]) == 1 &&
		entry.target == normalizedTarget(request.Target) &&
		entry.deliveryMode == request.DeliveryMode && entry.pathSelection == request.PathSelection &&
		entry.constraints == request.Constraints
}

func normalizedTarget(target protocol.Target) protocol.Target {
	if target.Address.IsValid() {
		target.Address = target.Address.Unmap()
	}
	return target
}

func (registry *Registry) reserveLocked(principalID string) bool {
	usage := registry.principal[principalID]
	if usage == nil {
		usage = &principalUsage{}
	}
	// Check per-principal terminal record capacity before global terminal record capacity.
	if usage.tombstoneReservations >= registry.limits.TombstonesPerPrincipal ||
		registry.tombstoneReservations >= registry.limits.Tombstones ||
		usage.flows >= registry.limits.FlowsPerPrincipal || registry.flows >= registry.limits.Flows ||
		registry.openingFlows >= registry.limits.OpeningFlows ||
		registry.targetDialReservations >= registry.limits.TargetDials {
		return false
	}
	if registry.principal[principalID] == nil {
		registry.principal[principalID] = usage
	}
	usage.flows++
	usage.tombstoneReservations++
	registry.flows++
	registry.openingFlows++
	registry.targetDialReservations++
	registry.tombstoneReservations++
	return true
}

func (registry *Registry) finishOpeningLocked(entry *registryEntry) {
	usage := registry.principal[entry.key.principalID]
	registry.openingFlows--
	registry.targetDialReservations--
	if usage == nil || registry.openingFlows < 0 || registry.targetDialReservations < 0 {
		panic("server registry opening counters corrupted")
	}
}

func (registry *Registry) terminalizeLocked(entry *registryEntry, terminal TerminalKind, result protocol.OpenResultCode, now time.Time) OpenOutcome {
	if entry.state == RegistryOpeningCapability || entry.state == RegistryOpeningTarget {
		registry.finishOpeningLocked(entry)
	}
	usage := registry.principal[entry.key.principalID]
	if usage == nil || usage.flows < 1 || registry.flows < 1 {
		panic("server registry flow counters corrupted")
	}
	usage.flows--
	registry.flows--
	usage.tombstones++
	registry.tombstones++
	entry.state = RegistryTombstoneReset
	if terminal == TerminalClosed {
		entry.state = RegistryTombstoneClosed
	}
	entry.openToken = protocol.OpenToken{}
	entry.target = protocol.Target{}
	entry.deliveryMode = 0
	entry.pathSelection = 0
	entry.constraints = protocol.DeliveryConstraints{}
	entry.capability = protocol.Capability{}
	entry.capabilityDigest = [sha256.Size]byte{}
	entry.capabilityInFlight = false
	entry.owner = nil
	entry.actionGeneration = 0
	entry.openExpiresAt = time.Time{}
	entry.terminalKind = terminal
	entry.terminalResult = result
	entry.tombstoneExpiresAt = now.Add(TombstoneLifetime)
	stable := openFailure(entry.key.flowID, result)
	entry.operation.complete(stable.Result)
	entry.operation = nil
	return stable
}

func (registry *Registry) staleOpeningOutcomeLocked(entry *registryEntry) OpenOutcome {
	if entry != nil && entryIsTombstone(entry.state) {
		return openFailure(entry.key.flowID, entry.terminalResult)
	}
	return OpenOutcome{}
}

func (registry *Registry) pruneExpiredLocked(now time.Time) {
	for key, entry := range registry.entries {
		if !entryIsTombstone(entry.state) || now.Before(entry.tombstoneExpiresAt) {
			continue
		}
		usage := registry.principal[key.principalID]
		if usage == nil || usage.tombstones < 1 || usage.tombstoneReservations < 1 || registry.tombstones < 1 || registry.tombstoneReservations < 1 {
			panic("server registry tombstone counters corrupted")
		}
		delete(registry.entries, key)
		usage.tombstones--
		usage.tombstoneReservations--
		registry.tombstones--
		registry.tombstoneReservations--
		if usage.flows == 0 && usage.tombstones == 0 && usage.tombstoneReservations == 0 {
			delete(registry.principal, key.principalID)
		}
	}
}

func (registry *Registry) currentTime() time.Time {
	registry.clockMu.Lock()
	now := registry.now()
	registry.clockMu.Unlock()
	return now
}

func openFailure(flowID protocol.FlowID, result protocol.OpenResultCode) OpenOutcome {
	return OpenOutcome{
		Ready: true,
		Result: protocol.OpenResult{
			FlowID: flowID,
			Result: result,
		},
	}
}

func successResult(entry *registryEntry) protocol.OpenResult {
	return protocol.OpenResult{
		FlowID:        entry.key.flowID,
		Result:        protocol.OpenSuccess,
		Capability:    entry.capability,
		DeliveryMode:  entry.deliveryMode,
		PathSelection: entry.pathSelection,
		Constraints:   entry.constraints,
	}
}

func snapshotEntry(entry *registryEntry) RegistryEntrySnapshot {
	return RegistryEntrySnapshot{
		Key:                exportKey(entry.key),
		State:              entry.state,
		DeliveryMode:       entry.deliveryMode,
		PathSelection:      entry.pathSelection,
		Constraints:        entry.constraints,
		Owner:              entry.owner,
		ActionGeneration:   entry.actionGeneration,
		OpenExpiresAt:      entry.openExpiresAt,
		TerminalKind:       entry.terminalKind,
		TerminalResult:     entry.terminalResult,
		TombstoneExpiresAt: entry.tombstoneExpiresAt,
	}
}

func exportKey(key registryKey) FlowKey {
	return FlowKey{PrincipalID: key.principalID, FlowID: key.flowID}
}

func importKey(key FlowKey) registryKey {
	return registryKey{principalID: key.PrincipalID, flowID: key.FlowID}
}

func entryIsTombstone(state RegistryEntryState) bool {
	return state == RegistryTombstoneClosed || state == RegistryTombstoneReset
}

func joinableRegistryState(state RegistryEntryState) bool {
	return state == RegistryAwaitingAttachment || state == RegistryRelaying ||
		state == RegistryRecovering || state == RegistryClosing
}

func registryStateForLifecycle(state flow.LifecycleState) (RegistryEntryState, bool) {
	switch state {
	case flow.AwaitingAttachment:
		return RegistryAwaitingAttachment, true
	case flow.Relaying:
		return RegistryRelaying, true
	case flow.Recovering:
		return RegistryRecovering, true
	case flow.Closing:
		return RegistryClosing, true
	case flow.Resetting:
		return RegistryResetting, true
	default:
		return 0, false
	}
}

func validLifecycleTransition(from, to RegistryEntryState) bool {
	if from == to {
		return true
	}
	switch from {
	case RegistryAwaitingAttachment:
		return to == RegistryRelaying || to == RegistryClosing || to == RegistryResetting
	case RegistryRelaying:
		return to == RegistryRecovering || to == RegistryClosing || to == RegistryResetting
	case RegistryRecovering:
		return to == RegistryRelaying || to == RegistryClosing || to == RegistryResetting
	case RegistryClosing:
		return to == RegistryResetting
	default:
		return false
	}
}

var dummyCapabilityDigest = sha256.Sum256([]byte("via server dummy capability v1"))
