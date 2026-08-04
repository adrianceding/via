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
	MaxAttachments             = flow.MaxAttachments
	StableACKCount             = 8
	StableAcknowledged         = 64 << 10
	MinimumConstraint          = protocol.MinimumDeliveryConstraint
	MaximumConstraint          = protocol.MaximumDeliveryConstraint
	fastestImprovementPercent  = 15
	fastestImprovementAbsolute = 5 * time.Millisecond
	fastestChallengeDuration   = 300 * time.Millisecond
	fastestHoldDownDuration    = time.Second
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
	policy.attachments[attachment] = &attachmentState{quality: NewQuality()}
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
	if len(snapshots) != len(policy.attachments) {
		return ErrUnknownAttachment
	}
	for attachment := range snapshots {
		if _, ok := policy.attachments[attachment]; !ok {
			return ErrUnknownAttachment
		}
	}
	for attachment, state := range policy.attachments {
		next := snapshots[attachment]
		state.qualitySnapshot = next
		state.hasSnapshot = true
	}
	return nil
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
	if policy.config.Mode == protocol.DeliveryRedundant || policy.state == AdaptiveFull {
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
		return policy.placeAll(request.Attempted, request.Bytes)
	}
	return policy.placeOne(request, attemptedSet(request.Attempted))
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
	return policy.placeAll(request.Attempted, request.Bytes)
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
	keys := policy.sortedAttachments()
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
	for attachment, state := range policy.attachments {
		estimate := deliveryEstimate(policy.qualitySnapshot(state), 0)
		tie := stableTie(policy.flowID, attachment)
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
	keys := policy.sortedAttachments()
	placements := make([]Placement, 0, len(keys))
	for _, attachment := range keys {
		if _, exists := excluded[attachment]; !exists {
			quality := policy.qualitySnapshot(policy.attachments[attachment])
			estimatedDelivery := deliveryEstimate(quality, payloadBytes)
			placements = append(placements, Placement{
				Attachment:        attachment,
				RetryAfter:        boundedRetryAfter(quality.RetryEstimate, estimatedDelivery),
				EstimatedDelivery: estimatedDelivery,
			})
		}
	}
	return placements
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
	retryAfter := max(retryEstimate, estimatedDelivery)
	if retryAfter > MaximumRetryEstimate {
		return MaximumRetryEstimate
	}
	return retryAfter
}

type candidate struct {
	attachment flow.AttachmentKey
	estimate   time.Duration
	capacity   float64
	tie        uint64
}

func (policy *Policy) candidates(payloadBytes uint64, excluded map[flow.AttachmentKey]struct{}) []candidate {
	result := make([]candidate, 0, len(policy.attachments))
	for attachment, state := range policy.attachments {
		if _, exists := excluded[attachment]; exists {
			continue
		}
		snapshot := policy.qualitySnapshot(state)
		result = append(result, candidate{
			attachment: attachment,
			estimate:   deliveryEstimate(snapshot, payloadBytes),
			capacity:   snapshot.CapacityBytesSec,
			tie:        stableTie(policy.flowID, attachment),
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
	capacity := snapshot.CapacityBytesSec
	if !finitePositive(capacity) {
		capacity = defaultCapacity
	}
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
	keys := make([]flow.AttachmentKey, 0, len(policy.attachments))
	for attachment := range policy.attachments {
		keys = append(keys, attachment)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := stableTie(policy.flowID, keys[i])
		right := stableTie(policy.flowID, keys[j])
		if left != right {
			return left < right
		}
		if keys[i].SessionGeneration != keys[j].SessionGeneration {
			return keys[i].SessionGeneration < keys[j].SessionGeneration
		}
		return keys[i].AttachmentGeneration < keys[j].AttachmentGeneration
	})
	return keys
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
