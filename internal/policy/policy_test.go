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
		if placement.RetryAfter != placement.EstimatedDelivery || placement.RetryAfter <= InitialRetryEstimate {
			t.Fatalf("placement estimates = %#v", placement)
		}
	}
	assertPlacementSet(t, policy.ControlPlacements(), testAttachmentA, testAttachmentB)
	placements = policy.RetryDue(PlacementRequest{Attempted: []flow.AttachmentKey{testAttachmentA}})
	assertPlacementSet(t, placements, testAttachmentB)
}

func TestPolicyBoundsRetryAfterByEstimatedDelivery(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	quality := QualitySnapshot{
		SRTT:             20 * time.Millisecond,
		CapacityBytesSec: 1 << 20,
		QueuedBytes:      256 << 10,
		RetryEstimate:    MinimumRetryEstimate,
	}
	if err := policy.SetQualitySnapshot(testAttachmentA, quality); err != nil {
		t.Fatal(err)
	}
	if err := policy.SetQualitySnapshot(testAttachmentB, quality); err != nil {
		t.Fatal(err)
	}

	placement := policy.PlaceNew(PlacementRequest{Bytes: 1024})[0]
	if placement.RetryAfter != placement.EstimatedDelivery || placement.RetryAfter <= MinimumRetryEstimate {
		t.Fatalf("delivery-aware placement = %#v", placement)
	}

	quality.QueuedBytes = 32 << 20
	if err := policy.SetQualitySnapshot(testAttachmentA, quality); err != nil {
		t.Fatal(err)
	}
	if err := policy.SetQualitySnapshot(testAttachmentB, quality); err != nil {
		t.Fatal(err)
	}
	placement = policy.PlaceNew(PlacementRequest{Bytes: 1024})[0]
	if placement.EstimatedDelivery <= MaximumDeliveryRetryEstimate || placement.RetryAfter != MaximumDeliveryRetryEstimate {
		t.Fatalf("delivery-aware placement did not cap retry below no-progress timeout = %#v", placement)
	}
}

func TestFastestPolicyDoesNotPreferUnmeasuredCapacityOverKnownPath(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	if err := policy.SetQualitySnapshot(testAttachmentA, QualitySnapshot{
		DataSamples: 1, SRTT: 20 * time.Millisecond, CapacityBytesSec: 512 << 10,
		LastDataCapacity: 512 << 10, DataSampleFresh: true, QueuedBytes: 128 << 10,
		RetryEstimate: MinimumRetryEstimate,
	}); err != nil {
		t.Fatal(err)
	}
	if err := policy.SetQualitySnapshot(testAttachmentB, QualitySnapshot{
		ProbeSamples: 1, SRTT: 20 * time.Millisecond, CapacityBytesSec: defaultCapacity,
		RetryEstimate: MinimumRetryEstimate,
	}); err != nil {
		t.Fatal(err)
	}
	placements := policy.PlaceNew(PlacementRequest{Bytes: 64 << 10})
	if len(placements) != 1 || placements[0].Attachment != testAttachmentA {
		t.Fatalf("placement preferred unmeasured path: %#v", placements)
	}
}

func TestPolicyTelemetryMutatesActiveQualitySnapshot(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	if err := policy.AddAttachment(testAttachmentA); err != nil {
		t.Fatal(err)
	}
	if err := policy.SetQualitySnapshot(testAttachmentA, QualitySnapshot{
		SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	if err := policy.SetAttachmentLoad(testAttachmentA, 4096, 2048); err != nil {
		t.Fatal(err)
	}
	if err := policy.SetStallPenalty(testAttachmentA, time.Second); err != nil {
		t.Fatal(err)
	}
	snapshot := policy.Snapshot().Attachments[0].Quality
	if snapshot.QueuedBytes != 4096 || snapshot.InFlightBytes != 2048 || snapshot.StallPenalty != time.Second {
		t.Fatalf("telemetry did not mutate active quality snapshot = %#v", snapshot)
	}
}

func TestPolicyIncumbentStalled(t *testing.T) {
	if (*Policy)(nil).IncumbentStalled() {
		t.Fatal("nil policy reported a stalled incumbent")
	}
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	if err := policy.AddAttachment(testAttachmentA); err != nil {
		t.Fatal(err)
	}
	policy.incumbent = testAttachmentA
	policy.hasIncumbent = true
	if policy.IncumbentStalled() {
		t.Fatal("policy without pending delivery reported a stalled incumbent")
	}
	policy.SetPending(true)
	if policy.IncumbentStalled() {
		t.Fatal("healthy incumbent reported as stalled")
	}
	if err := policy.SetQualitySnapshot(testAttachmentA, QualitySnapshot{
		SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20, StallPenalty: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if !policy.IncumbentStalled() {
		t.Fatal("stalled incumbent was not reported")
	}
	if allocations := testing.AllocsPerRun(100, func() { _ = policy.IncumbentStalled() }); allocations != 0 {
		t.Fatalf("incumbent stall query allocations = %v, want 0", allocations)
	}
	delete(policy.attachments, testAttachmentA)
	if policy.IncumbentStalled() {
		t.Fatal("removed incumbent reported as stalled")
	}
}

func TestPolicySetInitialIncumbent(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	if err := policy.SetInitialIncumbent(testAttachmentB); err != nil {
		t.Fatal(err)
	}
	if snapshot := policy.Snapshot(); !snapshot.HasIncumbent || snapshot.Incumbent != testAttachmentB {
		t.Fatalf("initial incumbent = %#v", snapshot)
	}
	if err := policy.SetInitialIncumbent(testAttachmentA); err != nil {
		t.Fatal(err)
	}
	if got := policy.Snapshot().Incumbent; got != testAttachmentB {
		t.Fatalf("existing incumbent changed to %#v", got)
	}
	unknown := flow.AttachmentKey{SessionGeneration: 99, AttachmentGeneration: 1}
	if err := policy.SetInitialIncumbent(unknown); !errors.Is(err, ErrUnknownAttachment) {
		t.Fatalf("unknown initial incumbent error = %v", err)
	}

	distributed := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed})
	addTestAttachments(t, distributed)
	if err := distributed.SetInitialIncumbent(testAttachmentA); err != nil {
		t.Fatal(err)
	}
	if distributed.Snapshot().HasIncumbent {
		t.Fatal("distributed policy retained an incumbent")
	}
}

func TestFastestPolicyIncludesCongestionAndStall(t *testing.T) {
	now := time.Unix(100, 0)
	policy := newTestPolicyWithClock(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest}, func() time.Time { return now })
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
	if got := policy.PlaceNew(PlacementRequest{Bytes: 100})[0].Attachment; got != testAttachmentA {
		t.Fatalf("fastest placement during challenge = %+v", got)
	}
	now = now.Add(fastestChallengeDuration - time.Nanosecond)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 100})[0].Attachment; got != testAttachmentA {
		t.Fatalf("fastest placement before challenge = %+v", got)
	}
	now = now.Add(time.Nanosecond)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 100})[0].Attachment; got != testAttachmentB {
		t.Fatalf("congested placement = %+v", got)
	}
}

func TestFastestPolicyHoldDownAndImmediateFailureSwitch(t *testing.T) {
	now := time.Unix(200, 0)
	policy := newTestPolicyWithClock(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest}, func() time.Time { return now })
	addTestAttachments(t, policy)
	if err := policy.SetQualitySnapshot(testAttachmentA, QualitySnapshot{SRTT: 100 * time.Millisecond, CapacityBytesSec: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if err := policy.SetQualitySnapshot(testAttachmentB, QualitySnapshot{SRTT: 200 * time.Millisecond, CapacityBytesSec: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("initial fastest placement = %+v", got)
	}
	if err := policy.SetQualitySnapshot(testAttachmentB, QualitySnapshot{SRTT: 50 * time.Millisecond, CapacityBytesSec: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("candidate switched before challenge = %+v", got)
	}
	now = now.Add(fastestChallengeDuration)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentB {
		t.Fatalf("candidate did not switch after challenge = %+v", got)
	}
	if err := policy.SetQualitySnapshot(testAttachmentA, QualitySnapshot{SRTT: 10 * time.Millisecond, CapacityBytesSec: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(fastestChallengeDuration)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentB {
		t.Fatalf("hold-down switched early = %+v", got)
	}
	policy.RemoveAttachment(testAttachmentB)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("failure switch = %+v", got)
	}
}

func TestFastestPolicyPinsHealthyIncumbentWhileDataIsPending(t *testing.T) {
	now := time.Unix(300, 0)
	policy := newTestPolicyWithClock(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest}, func() time.Time { return now })
	addTestAttachments(t, policy)
	quality := QualitySnapshot{CapacityBytesSec: 1 << 20}
	quality.SRTT = 100 * time.Millisecond
	if err := policy.SetQualitySnapshot(testAttachmentA, quality); err != nil {
		t.Fatal(err)
	}
	quality.SRTT = 200 * time.Millisecond
	if err := policy.SetQualitySnapshot(testAttachmentB, quality); err != nil {
		t.Fatal(err)
	}
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("initial fastest placement = %+v", got)
	}

	policy.SetPending(true)
	if err := policy.SetQualitySnapshot(testAttachmentB, QualitySnapshot{SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("pending flow switched to a quality candidate = %+v", got)
	}
	if snapshot := policy.Snapshot(); snapshot.HasCandidate {
		t.Fatalf("pending flow retained a switch candidate = %#v", snapshot)
	}
	now = now.Add(fastestChallengeDuration)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("pending flow switched after challenge = %+v", got)
	}
}

func TestFastestPolicyMovesFromStalledIncumbentWhileDataIsPending(t *testing.T) {
	now := time.Unix(400, 0)
	policy := newTestPolicyWithClock(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest}, func() time.Time { return now })
	addTestAttachments(t, policy)
	if err := policy.SetQualitySnapshots(map[flow.AttachmentKey]QualitySnapshot{
		testAttachmentA: {SRTT: 100 * time.Millisecond, CapacityBytesSec: 1 << 20},
		testAttachmentB: {SRTT: 200 * time.Millisecond, CapacityBytesSec: 1 << 20},
	}); err != nil {
		t.Fatal(err)
	}
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("initial fastest placement = %+v", got)
	}

	policy.SetPending(true)
	if err := policy.SetQualitySnapshots(map[flow.AttachmentKey]QualitySnapshot{
		testAttachmentA: {SRTT: 100 * time.Millisecond, CapacityBytesSec: 1 << 20, StallPenalty: 3 * time.Second},
		testAttachmentB: {SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20},
	}); err != nil {
		t.Fatal(err)
	}
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentA {
		t.Fatalf("stalled incumbent switched before challenge = %+v", got)
	}
	now = now.Add(fastestChallengeDuration)
	if got := policy.PlaceNew(PlacementRequest{Bytes: 1})[0].Attachment; got != testAttachmentB {
		t.Fatalf("stalled incumbent did not switch after challenge = %+v", got)
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

func TestDistributedPolicyApproximatesCapacityRatios(t *testing.T) {
	for _, test := range []struct {
		name      string
		capacityA float64
		capacityB float64
		wantMinB  int
		wantMaxB  int
	}{
		{name: "one-to-two", capacityA: 1, capacityB: 2, wantMinB: 55, wantMaxB: 70},
		{name: "one-to-four", capacityA: 1, capacityB: 4, wantMinB: 75, wantMaxB: 85},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed})
			addTestAttachments(t, policy)
			if err := policy.SetQualitySnapshot(testAttachmentA, QualitySnapshot{SRTT: time.Millisecond, CapacityBytesSec: test.capacityA}); err != nil {
				t.Fatal(err)
			}
			if err := policy.SetQualitySnapshot(testAttachmentB, QualitySnapshot{SRTT: time.Millisecond, CapacityBytesSec: test.capacityB}); err != nil {
				t.Fatal(err)
			}
			counts := map[flow.AttachmentKey]int{}
			for index := 0; index < 100; index++ {
				placement := policy.PlaceNew(PlacementRequest{ItemID: uint64(index + 1), Bytes: 1})
				if len(placement) != 1 {
					t.Fatalf("placement %d = %#v", index, placement)
				}
				counts[placement[0].Attachment]++
			}
			if counts[testAttachmentB] < test.wantMinB || counts[testAttachmentB] > test.wantMaxB {
				t.Fatalf("capacity ratio counts = %#v", counts)
			}
		})
	}
}

func TestDistributedPolicyRetainsDecisionAssignedUntilAdmissionResolution(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed})
	addTestAttachments(t, policy)
	quality := QualitySnapshot{SRTT: time.Millisecond, CapacityBytesSec: 1 << 20}
	if err := policy.SetQualitySnapshots(map[flow.AttachmentKey]QualitySnapshot{
		testAttachmentA: quality,
		testAttachmentB: quality,
	}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 8; index++ {
		if placements := policy.PlaceNew(PlacementRequest{ItemID: uint64(index + 1), Bytes: 4096}); len(placements) != 1 {
			t.Fatalf("placement %d = %#v", index, placements)
		}
	}
	before := policy.Snapshot()
	if before.Attachments[0].Assigned == 0 && before.Attachments[1].Assigned == 0 {
		t.Fatal("placements did not create temporary assigned bytes")
	}
	if err := policy.SetQualitySnapshots(map[flow.AttachmentKey]QualitySnapshot{
		testAttachmentA: quality,
		testAttachmentB: quality,
	}); err != nil {
		t.Fatal(err)
	}
	unabsorbed := policy.Snapshot()
	if unabsorbed.Attachments[0].Assigned != before.Attachments[0].Assigned ||
		unabsorbed.Attachments[1].Assigned != before.Attachments[1].Assigned {
		t.Fatalf("unabsorbed assigned bytes were discarded: before=%#v after=%#v", before.Attachments, unabsorbed.Attachments)
	}
	if err := policy.SetQualitySnapshots(map[flow.AttachmentKey]QualitySnapshot{
		testAttachmentA: {SRTT: time.Millisecond, CapacityBytesSec: 1 << 20, QueuedBytes: before.Attachments[0].Assigned},
		testAttachmentB: {SRTT: time.Millisecond, CapacityBytesSec: 1 << 20, QueuedBytes: before.Attachments[1].Assigned},
	}); err != nil {
		t.Fatal(err)
	}
	afterLoad := policy.Snapshot()
	if afterLoad.Attachments[0].Assigned != before.Attachments[0].Assigned ||
		afterLoad.Attachments[1].Assigned != before.Attachments[1].Assigned {
		t.Fatalf("runtime load implicitly resolved assigned bytes: before=%#v after=%#v", before.Attachments, afterLoad.Attachments)
	}
	for _, attachment := range before.Attachments {
		if err := policy.ResolveAssigned(attachment.Attachment, attachment.Assigned); err != nil {
			t.Fatal(err)
		}
	}
	for _, attachment := range policy.Snapshot().Attachments {
		if attachment.Assigned != 0 {
			t.Fatalf("assigned bytes after admission resolution = %#v", policy.Snapshot().Attachments)
		}
	}
}

func TestDistributedPolicyResolvesAssignedAfterInvisibleLoadPulse(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed})
	addTestAttachments(t, policy)
	quality := QualitySnapshot{SRTT: time.Millisecond, CapacityBytesSec: 1 << 20}
	if err := policy.SetQualitySnapshots(map[flow.AttachmentKey]QualitySnapshot{
		testAttachmentA: quality,
		testAttachmentB: quality,
	}); err != nil {
		t.Fatal(err)
	}
	placement := policy.PlaceNew(PlacementRequest{ItemID: 1, Bytes: 32 << 10})
	if len(placement) != 1 {
		t.Fatalf("placement = %#v", placement)
	}
	unknown := flow.AttachmentKey{SessionGeneration: 99, AttachmentGeneration: 99}
	if err := policy.ResolveAssigned(unknown, 32<<10); !errors.Is(err, ErrUnknownAttachment) {
		t.Fatalf("unknown attachment resolve error = %v", err)
	}
	if err := policy.ResolveAssigned(placement[0].Attachment, 64<<10); err != nil {
		t.Fatal(err)
	}
	for _, attachment := range policy.Snapshot().Attachments {
		if attachment.Assigned != 0 {
			t.Fatalf("assigned bytes after completed load pulse = %#v", policy.Snapshot().Attachments)
		}
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
	placements = policy.PlaceNew(PlacementRequest{Bytes: 100})
	if len(placements) != 1 || placements[0].Attachment != testAttachmentB {
		t.Fatalf("targeted recovery did not move new DATA = %#v", placements)
	}
	placements = policy.RetryDue(PlacementRequest{Bytes: 100})
	if policy.Snapshot().State != AdaptiveFull || policy.Snapshot().Transition != TransitionRetryEscalated {
		t.Fatalf("full escalation = %#v", policy.Snapshot())
	}
	if len(placements) != 1 {
		t.Fatalf("fastest full recovery copy count = %#v", placements)
	}
	placements = policy.PlaceNew(PlacementRequest{Bytes: 100})
	if len(placements) != 1 {
		t.Fatalf("new DATA remained duplicated during recovery = %#v", placements)
	}

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

func TestAdaptiveRecoverySkipsStalledDataAttachments(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	quality := QualitySnapshot{
		SRTT:             10 * time.Millisecond,
		CapacityBytesSec: 1 << 20,
		RetryEstimate:    MinimumRetryEstimate,
		DataSampleFresh:  true,
	}
	if err := policy.SetQualitySnapshot(testAttachmentA, quality); err != nil {
		t.Fatal(err)
	}
	quality.StallPenalty = time.Second
	if err := policy.SetQualitySnapshot(testAttachmentB, quality); err != nil {
		t.Fatal(err)
	}

	placements := policy.RetryDue(PlacementRequest{Bytes: 100})
	if len(placements) != 1 || placements[0].Attachment != testAttachmentA {
		t.Fatalf("stalled recovery placements = %#v", placements)
	}
	assertPlacementSet(t, policy.ControlPlacements(), testAttachmentA, testAttachmentB)
}

func TestAdaptiveFullRecoveryLimitsDataCopiesToBestAttachments(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	third := flow.AttachmentKey{SessionGeneration: 3, AttachmentGeneration: 1}
	if err := policy.AddAttachment(third); err != nil {
		t.Fatal(err)
	}
	for attachment, rtt := range map[flow.AttachmentKey]time.Duration{
		testAttachmentA: 10 * time.Millisecond,
		testAttachmentB: 20 * time.Millisecond,
		third:           30 * time.Millisecond,
	} {
		if err := policy.SetQualitySnapshot(attachment, QualitySnapshot{
			SRTT:             rtt,
			CapacityBytesSec: 1 << 20,
			RetryEstimate:    MinimumRetryEstimate,
			DataSampleFresh:  true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	placements := policy.RetryDue(PlacementRequest{Bytes: 100})
	if len(placements) != 1 {
		t.Fatalf("fastest adaptive full copy count = %#v, want 1", placements)
	}
	placements = policy.PlaceNew(PlacementRequest{Bytes: 100})
	if len(placements) != 1 || placements[0].Attachment != testAttachmentA {
		t.Fatalf("new DATA recovery placement = %#v", placements)
	}
	assertPlacementSet(t, policy.ControlPlacements(), testAttachmentA, testAttachmentB, third)
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

func TestPolicyUsesImmutableSessionQualitySnapshotForAttachments(t *testing.T) {
	policy := newTestPolicy(t, Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	addTestAttachments(t, policy)
	physical := QualitySnapshot{
		ProbeSamples:     3,
		SRTT:             25 * time.Millisecond,
		RTTVariation:     5 * time.Millisecond,
		CapacityBytesSec: 2 << 20,
		QueuedBytes:      4096,
		InFlightBytes:    8192,
		StallPenalty:     0,
		RetryEstimate:    100 * time.Millisecond,
		DataSampleFresh:  true,
		DataSampleAge:    time.Second,
		LastDataCapacity: 2 << 20,
	}
	if err := policy.SetQualitySnapshot(testAttachmentA, physical); err != nil {
		t.Fatal(err)
	}
	if err := policy.SetQualitySnapshot(testAttachmentB, physical); err != nil {
		t.Fatal(err)
	}

	physical.QueuedBytes = 1 << 20
	snapshot := policy.Snapshot()
	if len(snapshot.Attachments) != 2 || snapshot.Attachments[0].Quality != snapshot.Attachments[1].Quality {
		t.Fatalf("shared physical snapshot = %#v", snapshot.Attachments)
	}
	if snapshot.Attachments[0].Quality.QueuedBytes != 4096 {
		t.Fatalf("published snapshot changed through source value = %#v", snapshot.Attachments[0].Quality)
	}
	placements := policy.PlaceNew(PlacementRequest{Bytes: 1024})
	if len(placements) != 1 || placements[0].EstimatedDelivery <= 0 {
		t.Fatalf("placement from immutable snapshot = %#v", placements)
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
	return newTestPolicyWithClock(t, config, time.Now)
}

func newTestPolicyWithClock(t *testing.T, config Config, now func() time.Time) *Policy {
	t.Helper()
	policy, err := NewWithClock(testFlowID, config, now)
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
