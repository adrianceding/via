package policy

import (
	"errors"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
)

var (
	testFlowID      = protocol.FlowID{1}
	testAttachmentA = flow.AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1}
	testAttachmentB = flow.AttachmentKey{SessionGeneration: 2, AttachmentGeneration: 1}
)

func TestPolicyConfigurationMatrix(t *testing.T) {
	valid := []Config{
		{Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone},
		{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest},
		{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed},
		{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed, Constraints: Constraints{
			MaxDeliveryDelay: 80 * time.Millisecond,
			MaxDelayGap:      30 * time.Millisecond,
			Fallback:         FallbackPause,
		}},
	}
	for _, config := range valid {
		if _, err := New(testFlowID, config); err != nil {
			t.Fatalf("valid config %#v: %v", config, err)
		}
	}
	invalid := []Config{
		{},
		{Mode: protocol.DeliveryRedundant, Selection: protocol.PathFastest},
		{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathNone},
		{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest, Constraints: Constraints{MaxDelayGap: time.Second}},
		{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed, Constraints: Constraints{MaxDelayGap: time.Millisecond}},
		{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed, Constraints: Constraints{Fallback: 99}},
	}
	for _, config := range invalid {
		if _, err := New(testFlowID, config); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("invalid config %#v error = %v", config, err)
		}
	}
	if _, err := New(protocol.FlowID{}, valid[0]); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("zero flow id error = %v", err)
	}
}

func TestRedundantPolicyPlacesEveryCopyAndControl(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone})
	addTestAttachments(t, policy)
	placements := policy.PlaceNew(PlacementRequest{Bytes: 100})
	assertPlacementSet(t, placements, testAttachmentA, testAttachmentB)
	for _, placement := range placements {
		if placement.RetryAfter != InitialRetryEstimate || placement.EstimatedDelivery <= 0 {
			t.Fatalf("placement estimates = %#v", placement)
		}
	}
	assertPlacementSet(t, policy.ControlPlacements(), testAttachmentA, testAttachmentB)
	placements = policy.RetryDue(PlacementRequest{Attempted: []flow.AttachmentKey{testAttachmentA}})
	assertPlacementSet(t, placements, testAttachmentB)
}

func TestFastestPolicyIncludesCongestionAndStall(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	qualityA, _ := policy.Quality(testAttachmentA)
	qualityB, _ := policy.Quality(testAttachmentB)
	if err := qualityA.ObserveData(20*time.Millisecond, 1<<20, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := qualityB.ObserveData(50*time.Millisecond, 1<<20, time.Second); err != nil {
		t.Fatal(err)
	}
	if got := policy.PlaceNew(PlacementRequest{Bytes: 100})[0].Attachment; got != testAttachmentA {
		t.Fatalf("fastest placement = %+v", got)
	}
	qualityA.SetLoad(2<<20, 0)
	if err := qualityA.SetStallPenalty(time.Second); err != nil {
		t.Fatal(err)
	}
	if got := policy.PlaceNew(PlacementRequest{Bytes: 100})[0].Attachment; got != testAttachmentB {
		t.Fatalf("congested placement = %+v", got)
	}
}

func TestDistributedPolicyWeightsByCapacityAndIsDeterministic(t *testing.T) {
	build := func() *Policy {
		policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed})
		addTestAttachments(t, policy)
		qualityA, _ := policy.Quality(testAttachmentA)
		qualityB, _ := policy.Quality(testAttachmentB)
		if err := qualityA.ObserveData(20*time.Millisecond, 2<<20, time.Second); err != nil {
			t.Fatal(err)
		}
		if err := qualityB.ObserveData(20*time.Millisecond, 1<<20, time.Second); err != nil {
			t.Fatal(err)
		}
		return policy
	}
	first := build()
	second := build()
	var firstSequence, secondSequence []flow.AttachmentKey
	counts := map[flow.AttachmentKey]int{}
	for index := 0; index < 120; index++ {
		request := PlacementRequest{ItemID: uint64(index + 1), Bytes: 16 << 10}
		one := first.PlaceNew(request)[0].Attachment
		two := second.PlaceNew(request)[0].Attachment
		firstSequence = append(firstSequence, one)
		secondSequence = append(secondSequence, two)
		counts[one]++
	}
	for index := range firstSequence {
		if firstSequence[index] != secondSequence[index] {
			t.Fatalf("nondeterministic placement at %d", index)
		}
	}
	if counts[testAttachmentA] <= counts[testAttachmentB] || counts[testAttachmentB] == 0 {
		t.Fatalf("weighted counts = %#v", counts)
	}
}

func TestDistributedConstraintsAndFallback(t *testing.T) {
	config := Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed, Constraints: Constraints{
		MaxDeliveryDelay: 100 * time.Millisecond,
		MaxDelayGap:      30 * time.Millisecond,
		Fallback:         FallbackPause,
	}}
	policy := newTestPolicy(t, config)
	addTestAttachments(t, policy)
	qualityA, _ := policy.Quality(testAttachmentA)
	qualityB, _ := policy.Quality(testAttachmentB)
	_ = qualityA.ObserveData(50*time.Millisecond, 10<<20, time.Second)
	_ = qualityB.ObserveData(200*time.Millisecond, 10<<20, time.Second)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("constraint placement = %+v", got)
	}
	qualityA.SetStallPenalty(time.Second)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1}); len(got) != 0 {
		t.Fatalf("pause fallback placements = %#v", got)
	}

	config.Constraints.Fallback = FallbackFastest
	policy = newTestPolicy(t, config)
	addTestAttachments(t, policy)
	qualityA, _ = policy.Quality(testAttachmentA)
	qualityB, _ = policy.Quality(testAttachmentB)
	_ = qualityA.ObserveData(200*time.Millisecond, 1<<20, time.Second)
	_ = qualityB.ObserveData(300*time.Millisecond, 1<<20, time.Second)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1}); len(got) != 1 || got[0].Attachment != testAttachmentA {
		t.Fatalf("fastest fallback placements = %#v", got)
	}
}

func TestAdaptiveEscalationRecoveryAndStableDeescalation(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	request := PlacementRequest{Bytes: 100, Attempted: []flow.AttachmentKey{testAttachmentA}}
	placements := policy.GapDue(request)
	if policy.Snapshot().State != AdaptiveTargeted || policy.Snapshot().Transition != TransitionAcknowledgementGap || len(placements) != 1 || placements[0].Attachment != testAttachmentB {
		t.Fatalf("targeted recovery = %#v / %#v", policy.Snapshot(), placements)
	}
	placements = policy.RetryDue(PlacementRequest{Bytes: 100})
	if policy.Snapshot().State != AdaptiveFull || policy.Snapshot().Transition != TransitionRetryEscalated {
		t.Fatalf("full escalation = %#v", policy.Snapshot())
	}
	assertPlacementSet(t, placements, testAttachmentA, testAttachmentB)
	assertPlacementSet(t, policy.PlaceNew(PlacementRequest{Bytes: 100}), testAttachmentA, testAttachmentB)

	for index := 0; index < StableACKCount; index++ {
		policy.RecordCumulativeProgress(StableAcknowledged / StableACKCount)
	}
	if policy.Snapshot().State != AdaptiveTargeted || policy.Snapshot().Transition != TransitionStableAcknowledgement {
		t.Fatalf("first stable window = %#v", policy.Snapshot())
	}
	for index := 0; index < StableACKCount; index++ {
		policy.RecordCumulativeProgress(StableAcknowledged / StableACKCount)
	}
	if policy.Snapshot().State != AdaptiveSingle || policy.Snapshot().Transition != TransitionStableAcknowledgement {
		t.Fatalf("second stable window = %#v", policy.Snapshot())
	}
}

func TestAdaptiveWaitsForRecoveryAndUsesFullWhenPending(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	policy.SetPending(true)
	policy.RemoveAttachment(testAttachmentA)
	policy.RemoveAttachment(testAttachmentB)
	if policy.Snapshot().State != AdaptiveWaiting || policy.Snapshot().Transition != TransitionAllAttachmentsLost || len(policy.PlaceNew(PlacementRequest{Bytes: 1})) != 0 {
		t.Fatalf("waiting policy = %#v", policy.Snapshot())
	}
	if err := policy.AddAttachment(testAttachmentA); err != nil {
		t.Fatal(err)
	}
	if policy.Snapshot().State != AdaptiveFull || policy.Snapshot().Transition != TransitionAttachmentRestored {
		t.Fatalf("pending recovery = %#v", policy.Snapshot())
	}

	policy.RemoveAttachment(testAttachmentA)
	policy.SetPending(false)
	if err := policy.AddAttachment(testAttachmentB); err != nil {
		t.Fatal(err)
	}
	if policy.Snapshot().State != AdaptiveFull {
		t.Fatalf("saved full state without pending = %v", policy.Snapshot().State)
	}
}

func TestPolicySnapshotReportsPreferredAttachmentWithoutMutatingPlacement(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	qualityA, _ := policy.Quality(testAttachmentA)
	qualityB, _ := policy.Quality(testAttachmentB)
	_ = qualityA.ObserveData(20*time.Millisecond, 1<<20, time.Second)
	_ = qualityB.ObserveData(50*time.Millisecond, 1<<20, time.Second)

	snapshot := policy.Snapshot()
	if !snapshot.HasPreferred || snapshot.Preferred != testAttachmentA || snapshot.Attachments[0].Assigned != 0 || snapshot.Attachments[1].Assigned != 0 {
		t.Fatalf("initial preferred snapshot = %#v", snapshot)
	}
	qualityA.SetLoad(2<<20, 0)
	if err := qualityA.SetStallPenalty(time.Second); err != nil {
		t.Fatal(err)
	}
	snapshot = policy.Snapshot()
	if !snapshot.HasPreferred || snapshot.Preferred != testAttachmentB || snapshot.Attachments[0].Assigned != 0 || snapshot.Attachments[1].Assigned != 0 {
		t.Fatalf("updated preferred snapshot = %#v", snapshot)
	}
}

func TestPolicyStatusSnapshotIsCompactAndReadOnly(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	qualityA, _ := policy.Quality(testAttachmentA)
	qualityB, _ := policy.Quality(testAttachmentB)
	_ = qualityA.ObserveData(10*time.Millisecond, 1<<20, time.Second)
	_ = qualityB.ObserveData(20*time.Millisecond, 1<<20, time.Second)
	before := policy.Snapshot()
	status := policy.StatusSnapshot()
	after := policy.Snapshot()
	if status.State != AdaptiveSingle || status.Attachments != 2 || !status.HasPreferred || status.Preferred != testAttachmentA {
		t.Fatalf("status snapshot = %#v", status)
	}
	if before.Attachments[0].Assigned != after.Attachments[0].Assigned || before.Attachments[1].Assigned != after.Attachments[1].Assigned {
		t.Fatalf("status snapshot mutated assigned bytes: %#v / %#v", before, after)
	}
	if allocations := testing.AllocsPerRun(100, func() { _ = policy.StatusSnapshot() }); allocations != 0 {
		t.Fatalf("status snapshot allocations = %.0f, want 0", allocations)
	}
}

func TestPolicyAttachmentLimitsAndSnapshotIsolation(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	for sessionGeneration := uint64(3); sessionGeneration <= uint64(MaxAttachments); sessionGeneration++ {
		if err := policy.AddAttachment(flow.AttachmentKey{SessionGeneration: sessionGeneration, AttachmentGeneration: 1}); err != nil {
			t.Fatalf("attachment %d error = %v", sessionGeneration, err)
		}
	}
	if got := len(policy.Snapshot().Attachments); got != MaxAttachments {
		t.Fatalf("attachment count = %d, want %d", got, MaxAttachments)
	}
	if err := policy.AddAttachment(flow.AttachmentKey{SessionGeneration: uint64(MaxAttachments) + 1, AttachmentGeneration: 1}); !errors.Is(err, ErrAttachmentLimit) {
		t.Fatalf("attachment limit error = %v", err)
	}
	if _, err := policy.Quality(flow.AttachmentKey{SessionGeneration: 9, AttachmentGeneration: 9}); !errors.Is(err, ErrUnknownAttachment) {
		t.Fatalf("unknown quality error = %v", err)
	}
	snapshot := policy.Snapshot()
	snapshot.Attachments[0].Assigned = 99
	if policy.Snapshot().Attachments[0].Assigned == 99 {
		t.Fatal("mutable snapshot escaped")
	}
}

func newTestPolicy(t *testing.T, config Config) *Policy {
	t.Helper()
	policy, err := New(testFlowID, config)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func addTestAttachments(t *testing.T, policy *Policy) {
	t.Helper()
	if err := policy.AddAttachment(testAttachmentA); err != nil {
		t.Fatal(err)
	}
	if err := policy.AddAttachment(testAttachmentB); err != nil {
		t.Fatal(err)
	}
}

func assertPlacementSet(t *testing.T, placements []Placement, attachments ...flow.AttachmentKey) {
	t.Helper()
	if len(placements) != len(attachments) {
		t.Fatalf("placements = %#v, want %d", placements, len(attachments))
	}
	want := make(map[flow.AttachmentKey]struct{}, len(attachments))
	for _, attachment := range attachments {
		want[attachment] = struct{}{}
	}
	for _, placement := range placements {
		if _, ok := want[placement.Attachment]; !ok {
			t.Fatalf("unexpected placement %+v in %#v", placement.Attachment, placements)
		}
		delete(want, placement.Attachment)
	}
	if len(want) != 0 {
		t.Fatalf("missing placements %#v", want)
	}
}
