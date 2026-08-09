package policy

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
	"sort"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
)

const (
	MaxAttachments               = flow.MaxAttachments
	StableACKCount               = 8
	StableAcknowledged           = 64 << 10
	MinimumConstraint            = protocol.MinimumDeliveryConstraint
	MaximumConstraint            = protocol.MaximumDeliveryConstraint
	fastestImprovementPercent    = 15
	fastestImprovementAbsolute   = 5 * time.Millisecond
	fastestChallengeDuration     = 300 * time.Millisecond
	fastestHoldDownDuration      = time.Second
	maximumAdaptiveDataCopies    = 2
	MaximumDeliveryRetryEstimate = 10 * time.Second
)

var (
	ErrInvalidPolicy     = errors.New("policy: invalid configuration")
	ErrAttachmentLimit   = errors.New("policy: attachment limit exceeded")
	ErrUnknownAttachment = errors.New("policy: unknown attachment")
)

type ConstraintFallback = protocol.DeliveryConstraintFallback

const (
	FallbackFastest = protocol.DeliveryFallbackFastest
	FallbackPause   = protocol.DeliveryFallbackPause
)

type Constraints = protocol.DeliveryConstraints

type Config struct {
	Mode        protocol.DeliveryMode
	Selection   protocol.PathSelection
	Constraints Constraints
}

func (config Config) normalize() (Config, error) {
	if config.Selection == protocol.PathDistributed {
		if config.Constraints.Fallback == 0 {
			config.Constraints.Fallback = FallbackFastest
		}
	}
	if !protocol.ValidDeliveryPolicy(config.Mode, config.Selection, config.Constraints) {
		return Config{}, ErrInvalidPolicy
	}
	return config, nil
}

type AdaptiveState uint8

const (
	AdaptiveSingle AdaptiveState = iota + 1
	AdaptiveTargeted
	AdaptiveFull
	AdaptiveWaiting
)

type Transition uint8

const (
	TransitionNone Transition = iota
	TransitionAcknowledgementGap
	TransitionRetryEscalated
	TransitionStableAcknowledgement
	TransitionAllAttachmentsLost
	TransitionAttachmentRestored
)

type PlacementRequest struct {
	ItemID            uint64
	AttemptGeneration uint64
	Bytes             uint64
	Attempted         []flow.AttachmentKey
}

type Placement struct {
	Attachment        flow.AttachmentKey
	RetryAfter        time.Duration
	EstimatedDelivery time.Duration
}

type AttachmentSnapshot struct {
	Attachment flow.AttachmentKey
	Quality    QualitySnapshot
	Assigned   uint64
}

type Snapshot struct {
	Config         Config
	State          AdaptiveState
	ReturnState    AdaptiveState
	Transition     Transition
	Pending        bool
	StableACKs     uint64
	StableBytes    uint64
	Preferred      flow.AttachmentKey
	HasPreferred   bool
	Incumbent      flow.AttachmentKey
	HasIncumbent   bool
	Candidate      flow.AttachmentKey
	HasCandidate   bool
	IncumbentSince time.Time
	CandidateSince time.Time
	HoldDownUntil  time.Time
	Attachments    []AttachmentSnapshot
}

type StatusSnapshot struct {
	State          AdaptiveState
	Transition     Transition
	Attachments    int
	Preferred      flow.AttachmentKey
	HasPreferred   bool
	Incumbent      flow.AttachmentKey
	HasIncumbent   bool
	Candidate      flow.AttachmentKey
	HasCandidate   bool
	IncumbentSince time.Time
	CandidateSince time.Time
	HoldDownUntil  time.Time
}

type attachmentState struct {
	quality         *Quality
	qualitySnapshot QualitySnapshot
	hasSnapshot     bool
	assigned        uint64
	tie             uint64
}

type Policy struct {
	flowID         protocol.FlowID
	config         Config
	now            func() time.Time
	state          AdaptiveState
	returnState    AdaptiveState
	transition     Transition
	pending        bool
	stableACKs     uint64
	stableBytes    uint64
	attachments    map[flow.AttachmentKey]*attachmentState
	ordered        []flow.AttachmentKey
	incumbent      flow.AttachmentKey
	hasIncumbent   bool
	incumbentSince time.Time
	candidate      flow.AttachmentKey
	hasCandidate   bool
	candidateSince time.Time
	holdDownUntil  time.Time
}

func New(flowID protocol.FlowID, config Config) (*Policy, error) {
	return NewWithClock(flowID, config, time.Now)
}

func NewWithClock(flowID protocol.FlowID, config Config, now func() time.Time) (*Policy, error) {
	normalized, err := config.normalize()
	if err != nil || flowID == (protocol.FlowID{}) || now == nil {
		return nil, ErrInvalidPolicy
	}
	return &Policy{
		flowID: flowID, config: normalized, now: now, state: AdaptiveSingle,
		attachments: make(map[flow.AttachmentKey]*attachmentState, MaxAttachments),
	}, nil
}

func (policy *Policy) AddAttachment(attachment flow.AttachmentKey) error {
	if !validAttachment(attachment) {
		return ErrUnknownAttachment
	}
	if _, exists := policy.attachments[attachment]; exists {
		return nil
	}
	if len(policy.attachments) >= MaxAttachments {
		return ErrAttachmentLimit
	}
	policy.attachments[attachment] = &attachmentState{quality: NewQuality(), tie: stableTie(policy.flowID, attachment)}
	policy.ordered = append(policy.ordered, attachment)
	policy.sortAttachments()
	policy.resetStable()
	if policy.state == AdaptiveWaiting {
		if policy.pending {
			policy.state = AdaptiveFull
		} else if policy.returnState != 0 && policy.returnState != AdaptiveWaiting {
			policy.state = policy.returnState
		} else {
			policy.state = AdaptiveSingle
		}
		policy.returnState = 0
		policy.transition = TransitionAttachmentRestored
	}
	return nil
}

func (policy *Policy) RemoveAttachment(attachment flow.AttachmentKey) {
	if _, exists := policy.attachments[attachment]; !exists {
		return
	}
	delete(policy.attachments, attachment)
	for index, current := range policy.ordered {
		if current == attachment {
			copy(policy.ordered[index:], policy.ordered[index+1:])
			policy.ordered = policy.ordered[:len(policy.ordered)-1]
			break
		}
	}
	if policy.hasIncumbent && policy.incumbent == attachment {
		policy.clearIncumbent()
	}
	if policy.hasCandidate && policy.candidate == attachment {
		policy.clearCandidate()
	}
	policy.resetStable()
	if len(policy.attachments) == 0 && policy.state != AdaptiveWaiting {
		policy.returnState = policy.state
		policy.state = AdaptiveWaiting
		policy.transition = TransitionAllAttachmentsLost
	}
}

func (policy *Policy) SetPending(pending bool) { policy.pending = pending }

// IncumbentStalled reports whether pending delivery is currently assigned to
// an incumbent attachment whose latest quality sample marks it as stalled.
// This query is used by the quality-update hot path and must remain allocation
// free.
func (policy *Policy) IncumbentStalled() bool {
	if policy == nil || !policy.pending || !policy.hasIncumbent {
		return false
	}
	state, ok := policy.attachments[policy.incumbent]
	return ok && policy.qualitySnapshot(state).StallPenalty > 0
}

// SetInitialIncumbent gives a fastest-path flow affinity to a known attachment
// until normal challenge, stall, or recovery rules move it. It is intentionally
// a no-op once an incumbent exists.
func (policy *Policy) SetInitialIncumbent(attachment flow.AttachmentKey) error {
	if policy == nil {
		return ErrUnknownAttachment
	}
	if _, ok := policy.attachments[attachment]; !ok {
		return ErrUnknownAttachment
	}
	if policy.config.Selection != protocol.PathFastest || policy.hasIncumbent {
		return nil
	}
	policy.setIncumbent(attachment, policy.currentNow(), false)
	return nil
}

// Attachments returns the current attachment keys in stable order without
// building a full policy snapshot. Callers that only need the key set for
// refresh loops should prefer this over Snapshot.
func (policy *Policy) Attachments() []flow.AttachmentKey {
	if policy == nil {
		return nil
	}
	return policy.sortedAttachments()
}

func (policy *Policy) State() AdaptiveState {
	if policy == nil {
		return AdaptiveWaiting
	}
	return policy.state
}

func (policy *Policy) Mode() protocol.DeliveryMode {
	if policy == nil {
		return 0
	}
	return policy.config.Mode
}

func (policy *Policy) Quality(attachment flow.AttachmentKey) (*Quality, error) {
	state, ok := policy.attachments[attachment]
	if !ok {
		return nil, ErrUnknownAttachment
	}
	return state.quality, nil
}

func (policy *Policy) SetQualitySnapshot(attachment flow.AttachmentKey, snapshot QualitySnapshot) error {
	state, ok := policy.attachments[attachment]
	if !ok {
		return ErrUnknownAttachment
	}
	state.qualitySnapshot = snapshot
	state.hasSnapshot = true
	return nil
}

func (policy *Policy) SetQualitySnapshots(snapshots map[flow.AttachmentKey]QualitySnapshot) error {
	for attachment := range snapshots {
		if _, ok := policy.attachments[attachment]; !ok {
			return ErrUnknownAttachment
		}
	}
	for attachment, next := range snapshots {
		state := policy.attachments[attachment]
		state.qualitySnapshot = next
		state.hasSnapshot = true
	}
	return nil
}

func (policy *Policy) SetAttachmentLoad(attachment flow.AttachmentKey, queuedBytes, inFlightBytes uint64) error {
	state, ok := policy.attachments[attachment]
	if !ok {
		return ErrUnknownAttachment
	}
	if state.hasSnapshot {
		state.qualitySnapshot.QueuedBytes = queuedBytes
		state.qualitySnapshot.InFlightBytes = inFlightBytes
	} else {
		state.quality.SetLoad(queuedBytes, inFlightBytes)
	}
	return nil
}

func (policy *Policy) SetStallPenalty(attachment flow.AttachmentKey, penalty time.Duration) error {
	if penalty < 0 {
		return ErrInvalidSample
	}
	state, ok := policy.attachments[attachment]
	if !ok {
		return ErrUnknownAttachment
	}
	if state.hasSnapshot {
		state.qualitySnapshot.StallPenalty = penalty
		return nil
	}
	return state.quality.SetStallPenalty(penalty)
}

func (policy *Policy) ResolveAssigned(attachment flow.AttachmentKey, bytes uint64) error {
	state, ok := policy.attachments[attachment]
	if !ok {
		return ErrUnknownAttachment
	}
	if bytes >= state.assigned {
		state.assigned = 0
	} else {
		state.assigned -= bytes
	}
	return nil
}

func (policy *Policy) PlaceNew(request PlacementRequest) []Placement {
	if policy.state == AdaptiveWaiting || len(policy.attachments) == 0 {
		return nil
	}
	// Adaptive recovery duplicates the outstanding gap, but new DATA should
	// continue on one selected path. Keeping every new segment duplicated while
	// a single gap is unresolved amplifies reordering and exhausts path queues.
	if policy.config.Mode == protocol.DeliveryRedundant || policy.state == AdaptiveFull && request.Bytes == 0 {
		return policy.placeAll(request.Attempted, request.Bytes)
	}
	return policy.placeOne(request, nil)
}

func (policy *Policy) GapDue(request PlacementRequest) []Placement {
	policy.resetStable()
	if policy.state == AdaptiveWaiting || len(policy.attachments) == 0 {
		return nil
	}
	if policy.config.Mode == protocol.DeliveryRedundant {
		return policy.placeAll(request.Attempted, request.Bytes)
	}
	if policy.state == AdaptiveSingle {
		policy.state = AdaptiveTargeted
		policy.transition = TransitionAcknowledgementGap
	}
	if policy.state == AdaptiveFull {
		placements := policy.placeAll(request.Attempted, request.Bytes)
		policy.setRecoveryIncumbent(placements, request.Bytes)
		return placements
	}
	placements := policy.placeOne(request, attemptedSet(request.Attempted))
	if policy.config.Selection == protocol.PathFastest && len(placements) != 0 {
		// The incumbent just failed to advance the cumulative ACK. Keep new
		// DATA on the alternate that received the targeted recovery instead of
		// immediately sending the next segment back to the failed path.
		policy.setIncumbent(placements[0].Attachment, policy.currentNow(), true)
	}
	return placements
}

func (policy *Policy) RetryDue(request PlacementRequest) []Placement {
	policy.resetStable()
	if policy.state == AdaptiveWaiting || len(policy.attachments) == 0 {
		return nil
	}
	if policy.config.Mode == protocol.DeliveryAdaptive {
		if policy.state != AdaptiveFull {
			policy.state = AdaptiveFull
			policy.transition = TransitionRetryEscalated
		}
	}
	placements := policy.placeAll(request.Attempted, request.Bytes)
	policy.setRecoveryIncumbent(placements, request.Bytes)
	return placements
}

func (policy *Policy) ControlPlacements() []Placement {
	return policy.placeAll(nil, 0)
}

func (policy *Policy) RecordCumulativeProgress(acknowledgedBytes uint64) {
	if acknowledgedBytes == 0 || policy.config.Mode != protocol.DeliveryAdaptive ||
		(policy.state != AdaptiveTargeted && policy.state != AdaptiveFull) {
		return
	}
	policy.stableACKs++
	policy.stableBytes = saturatingSum(policy.stableBytes, acknowledgedBytes)
	if policy.stableACKs < StableACKCount || policy.stableBytes < StableAcknowledged {
		return
	}
	if policy.state == AdaptiveFull {
		policy.state = AdaptiveTargeted
	} else {
		policy.state = AdaptiveSingle
	}
	policy.transition = TransitionStableAcknowledgement
	policy.resetStable()
}

func (policy *Policy) Snapshot() Snapshot {
	if policy == nil {
		return Snapshot{State: AdaptiveWaiting}
	}
	snapshot := Snapshot{
		Config:      policy.config,
		State:       policy.state,
		ReturnState: policy.returnState,
		Transition:  policy.transition,
		Pending:     policy.pending,
		StableACKs:  policy.stableACKs,
		StableBytes: policy.stableBytes,
		Incumbent:   policy.incumbent, HasIncumbent: policy.hasIncumbent,
		Candidate: policy.candidate, HasCandidate: policy.hasCandidate,
		IncumbentSince: policy.incumbentSince, CandidateSince: policy.candidateSince,
		HoldDownUntil: policy.holdDownUntil,
	}
	if candidates := policy.candidates(0, nil); len(candidates) != 0 {
		preferred := candidates[0].attachment
		if policy.config.Selection == protocol.PathFastest && policy.hasIncumbent {
			if _, exists := policy.attachments[policy.incumbent]; exists {
				preferred = policy.incumbent
			}
		}
		snapshot.Preferred = preferred
		snapshot.HasPreferred = true
	}
	keys := policy.ordered
	for _, attachment := range keys {
		state := policy.attachments[attachment]
		snapshot.Attachments = append(snapshot.Attachments, AttachmentSnapshot{
			Attachment: attachment,
			Quality:    policy.qualitySnapshot(state),
			Assigned:   state.assigned,
		})
	}
	return snapshot
}

func (policy *Policy) StatusSnapshot() StatusSnapshot {
	if policy == nil {
		return StatusSnapshot{State: AdaptiveWaiting}
	}
	status := StatusSnapshot{
		State: policy.state, Transition: policy.transition, Attachments: len(policy.attachments),
		Incumbent: policy.incumbent, HasIncumbent: policy.hasIncumbent,
		Candidate: policy.candidate, HasCandidate: policy.hasCandidate,
		IncumbentSince: policy.incumbentSince, CandidateSince: policy.candidateSince,
		HoldDownUntil: policy.holdDownUntil,
	}
	var bestEstimate time.Duration
	var bestTie uint64
	for _, attachment := range policy.ordered {
		state := policy.attachments[attachment]
		estimate := deliveryEstimate(policy.qualitySnapshot(state), 0)
		tie := state.tie
		if !status.HasPreferred || estimate < bestEstimate || estimate == bestEstimate && tie < bestTie {
			status.Preferred = attachment
			status.HasPreferred = true
			bestEstimate = estimate
			bestTie = tie
		}
	}
	if policy.config.Selection == protocol.PathFastest && policy.hasIncumbent {
		if _, exists := policy.attachments[policy.incumbent]; exists {
			status.Preferred = policy.incumbent
			status.HasPreferred = true
		}
	}
	return status
}

func (policy *Policy) placeAll(attempted []flow.AttachmentKey, payloadBytes uint64) []Placement {
	excluded := attemptedSet(attempted)
	keys := policy.ordered
	placementsKeys := make([]flow.AttachmentKey, 0, len(keys))
	for _, attachment := range keys {
		if _, exists := excluded[attachment]; !exists {
			placementsKeys = append(placementsKeys, attachment)
		}
	}
	if payloadBytes != 0 {
		eligible := make([]flow.AttachmentKey, 0, len(placementsKeys))
		for _, attachment := range placementsKeys {
			quality := policy.qualitySnapshot(policy.attachments[attachment])
			if quality.StallPenalty == 0 {
				eligible = append(eligible, attachment)
			}
		}
		if len(eligible) != 0 {
			placementsKeys = eligible
		}
		copyLimit := maximumAdaptiveDataCopies
		if policy.config.Selection == protocol.PathFastest {
			copyLimit = 1
		}
		if policy.config.Mode == protocol.DeliveryAdaptive && policy.state == AdaptiveFull && len(placementsKeys) > copyLimit {
			sort.SliceStable(placementsKeys, func(left, right int) bool {
				leftQuality := policy.qualitySnapshot(policy.attachments[placementsKeys[left]])
				rightQuality := policy.qualitySnapshot(policy.attachments[placementsKeys[right]])
				leftEstimate := deliveryEstimate(leftQuality, payloadBytes)
				rightEstimate := deliveryEstimate(rightQuality, payloadBytes)
				if leftEstimate != rightEstimate {
					return leftEstimate < rightEstimate
				}
				return policy.attachments[placementsKeys[left]].tie < policy.attachments[placementsKeys[right]].tie
			})
			placementsKeys = placementsKeys[:copyLimit]
		}
	}
	placements := make([]Placement, 0, len(placementsKeys))
	for _, attachment := range placementsKeys {
		quality := policy.qualitySnapshot(policy.attachments[attachment])
		estimatedDelivery := deliveryEstimate(quality, payloadBytes)
		placements = append(placements, Placement{
			Attachment:        attachment,
			RetryAfter:        boundedRetryAfter(quality.RetryEstimate, estimatedDelivery),
			EstimatedDelivery: estimatedDelivery,
		})
	}
	return placements
}

func (policy *Policy) setRecoveryIncumbent(placements []Placement, payloadBytes uint64) {
	if policy.config.Selection != protocol.PathFastest || payloadBytes == 0 || len(placements) == 0 {
		return
	}
	policy.setIncumbent(placements[0].Attachment, policy.currentNow(), true)
}

func (policy *Policy) placeOne(request PlacementRequest, excluded map[flow.AttachmentKey]struct{}) []Placement {
	candidates := policy.candidates(request.Bytes, excluded)
	if len(candidates) == 0 {
		return nil
	}
	if policy.config.Selection == protocol.PathFastest {
		chosen := candidates[0]
		if len(excluded) == 0 {
			chosen = policy.selectFastest(candidates)
		}
		return []Placement{policy.placementFor(chosen)}
	}
	filtered := policy.applyConstraints(candidates)
	if len(filtered) == 0 {
		if policy.config.Constraints.Fallback == FallbackPause {
			return nil
		}
		filtered = candidates[:1]
	}
	chosen := filtered[0]
	bestFinish := policy.virtualFinish(chosen, request.Bytes)
	for _, candidate := range filtered[1:] {
		finish := policy.virtualFinish(candidate, request.Bytes)
		if finish < bestFinish || finish == bestFinish && candidate.tie < chosen.tie {
			chosen = candidate
			bestFinish = finish
		}
	}
	state := policy.attachments[chosen.attachment]
	state.assigned = saturatingSum(state.assigned, request.Bytes)
	policy.normalizeAssigned()
	return []Placement{policy.placementFor(chosen)}
}

func (policy *Policy) placementFor(candidate candidate) Placement {
	quality := policy.qualitySnapshot(policy.attachments[candidate.attachment])
	return Placement{
		Attachment:        candidate.attachment,
		RetryAfter:        boundedRetryAfter(quality.RetryEstimate, candidate.estimate),
		EstimatedDelivery: candidate.estimate,
	}
}

func boundedRetryAfter(retryEstimate, estimatedDelivery time.Duration) time.Duration {
	return min(max(retryEstimate, estimatedDelivery), MaximumDeliveryRetryEstimate)
}

type candidate struct {
	attachment flow.AttachmentKey
	estimate   time.Duration
	capacity   float64
	tie        uint64
}

func (policy *Policy) candidates(payloadBytes uint64, excluded map[flow.AttachmentKey]struct{}) []candidate {
	result := make([]candidate, 0, len(policy.attachments))
	for _, attachment := range policy.ordered {
		state := policy.attachments[attachment]
		if _, exists := excluded[attachment]; exists {
			continue
		}
		snapshot := policy.qualitySnapshot(state)
		result = append(result, candidate{
			attachment: attachment,
			estimate:   deliveryEstimate(snapshot, payloadBytes),
			capacity:   effectiveCapacity(snapshot),
			tie:        state.tie,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].estimate != result[j].estimate {
			return result[i].estimate < result[j].estimate
		}
		return result[i].tie < result[j].tie
	})
	return result
}

func (policy *Policy) applyConstraints(candidates []candidate) []candidate {
	constraints := policy.config.Constraints
	fastest := candidates[0].estimate
	filtered := make([]candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if constraints.MaxDeliveryDelay != 0 && candidate.estimate > constraints.MaxDeliveryDelay {
			continue
		}
		if constraints.MaxDelayGap != 0 && candidate.estimate-fastest > constraints.MaxDelayGap {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}

func (policy *Policy) virtualFinish(candidate candidate, payloadBytes uint64) float64 {
	snapshot := policy.qualitySnapshot(policy.attachments[candidate.attachment])
	capacity := effectiveCapacity(snapshot)
	assigned := policy.attachments[candidate.attachment].assigned
	base := snapshot.SRTT
	if base == 0 {
		base = InitialRetryEstimate
	}
	return base.Seconds() + snapshot.StallPenalty.Seconds() +
		float64(saturatingSum(snapshot.QueuedBytes, snapshot.InFlightBytes, assigned, payloadBytes))/capacity
}

func (policy *Policy) normalizeAssigned() {
	if len(policy.attachments) < 2 {
		return
	}
	minimum := uint64(math.MaxUint64)
	for _, state := range policy.attachments {
		if state.assigned < minimum {
			minimum = state.assigned
		}
	}
	if minimum < 1<<30 {
		return
	}
	for _, state := range policy.attachments {
		state.assigned -= minimum
	}
}

func (policy *Policy) selectFastest(candidates []candidate) candidate {
	best := candidates[0]
	now := policy.currentNow()
	if !policy.hasIncumbent {
		policy.setIncumbent(best.attachment, now, false)
		return best
	}
	incumbentIndex := -1
	for index, candidate := range candidates {
		if candidate.attachment == policy.incumbent {
			incumbentIndex = index
			break
		}
	}
	if incumbentIndex < 0 {
		policy.setIncumbent(best.attachment, now, false)
		return best
	}
	incumbent := candidates[incumbentIndex]
	// Keep a continuous byte stream on one healthy path. Queue depth changes
	// faster than delivery quality and must not turn fastest mode into striping;
	// explicit gap/retry recovery still moves away from a stalled incumbent.
	incumbentQuality := policy.qualitySnapshot(policy.attachments[incumbent.attachment])
	if policy.pending && incumbentQuality.StallPenalty == 0 {
		policy.clearCandidate()
		return incumbent
	}
	if best.attachment == incumbent.attachment || !fastestImprovement(incumbent.estimate, best.estimate) {
		policy.clearCandidate()
		return incumbent
	}
	if !policy.hasCandidate || policy.candidate != best.attachment {
		policy.candidate = best.attachment
		policy.hasCandidate = true
		policy.candidateSince = now
		return incumbent
	}
	if now.Before(policy.candidateSince) {
		policy.candidateSince = now
		return incumbent
	}
	if now.Sub(policy.candidateSince) < fastestChallengeDuration || now.Before(policy.holdDownUntil) {
		return incumbent
	}
	policy.setIncumbent(best.attachment, now, true)
	return best
}

func fastestImprovement(incumbent, candidate time.Duration) bool {
	if incumbent <= candidate || incumbent <= 0 {
		return false
	}
	improvement := incumbent - candidate
	return improvement >= fastestImprovementAbsolute &&
		float64(improvement) >= float64(incumbent)*fastestImprovementPercent/100
}

func (policy *Policy) setIncumbent(attachment flow.AttachmentKey, now time.Time, normalSwitch bool) {
	policy.incumbent = attachment
	policy.hasIncumbent = true
	policy.incumbentSince = now
	policy.clearCandidate()
	if normalSwitch {
		policy.holdDownUntil = now.Add(fastestHoldDownDuration)
	} else {
		policy.holdDownUntil = time.Time{}
	}
}

func (policy *Policy) clearIncumbent() {
	policy.incumbent = flow.AttachmentKey{}
	policy.hasIncumbent = false
	policy.incumbentSince = time.Time{}
	policy.holdDownUntil = time.Time{}
}

func (policy *Policy) clearCandidate() {
	policy.candidate = flow.AttachmentKey{}
	policy.hasCandidate = false
	policy.candidateSince = time.Time{}
}

func (policy *Policy) currentNow() time.Time {
	if policy == nil || policy.now == nil {
		return time.Now()
	}
	return policy.now()
}

func (policy *Policy) sortedAttachments() []flow.AttachmentKey {
	return append([]flow.AttachmentKey(nil), policy.ordered...)
}

func (policy *Policy) sortAttachments() {
	sort.Slice(policy.ordered, func(i, j int) bool {
		left := policy.attachments[policy.ordered[i]].tie
		right := policy.attachments[policy.ordered[j]].tie
		if left != right {
			return left < right
		}
		if policy.ordered[i].SessionGeneration != policy.ordered[j].SessionGeneration {
			return policy.ordered[i].SessionGeneration < policy.ordered[j].SessionGeneration
		}
		return policy.ordered[i].AttachmentGeneration < policy.ordered[j].AttachmentGeneration
	})
}

func (policy *Policy) resetStable() {
	policy.stableACKs = 0
	policy.stableBytes = 0
}

func (policy *Policy) qualitySnapshot(state *attachmentState) QualitySnapshot {
	if state == nil {
		return QualitySnapshot{CapacityBytesSec: defaultCapacity, RetryEstimate: InitialRetryEstimate}
	}
	if state.hasSnapshot {
		return state.qualitySnapshot
	}
	return state.quality.Snapshot()
}

func attemptedSet(attempted []flow.AttachmentKey) map[flow.AttachmentKey]struct{} {
	set := make(map[flow.AttachmentKey]struct{}, len(attempted))
	for _, attachment := range attempted {
		set[attachment] = struct{}{}
	}
	return set
}

func stableTie(flowID protocol.FlowID, attachment flow.AttachmentKey) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write(flowID[:])
	var encoded [16]byte
	binary.BigEndian.PutUint64(encoded[:8], attachment.SessionGeneration)
	binary.BigEndian.PutUint64(encoded[8:], attachment.AttachmentGeneration)
	_, _ = hash.Write(encoded[:])
	return hash.Sum64()
}

func validAttachment(attachment flow.AttachmentKey) bool {
	return attachment.SessionGeneration != 0 && attachment.AttachmentGeneration != 0
}
