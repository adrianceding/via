package status

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestRepositoryAppliesBoundedStateAndPublishesImmutableSnapshot(t *testing.T) {
	now := time.Unix(2_000, 0).UTC()
	repository, err := NewRepositoryWithClock(Limits{Interfaces: 1, Sessions: 1, Flows: 1}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if generatedAt := repository.Snapshot().GeneratedAt; generatedAt != now {
		t.Fatalf("initial snapshot generated at %v, want %v", generatedAt, now)
	}
	interfaceValue := Interface{Index: 2, Name: "eth0", Addresses: []string{"192.0.2.2"}, Reason: InterfaceEligible}
	sessionValue := validTestSession(1)
	flowValue := validTestFlow(2)
	for _, event := range []Event{
		{Kind: EventSetResources, Resources: Resources{Flows: 1, Sessions: 1, ReservedBytes: 99}},
		{Kind: EventUpsertInterface, Interface: interfaceValue},
		{Kind: EventUpsertSession, Session: sessionValue},
		{Kind: EventUpsertFlow, Flow: flowValue},
	} {
		if err := repository.apply(event, now); err != nil {
			t.Fatal(err)
		}
	}
	repository.publish(now)
	repository.AddCounter(CounterBytesSent, 7)
	snapshot := repository.Snapshot()
	if !snapshot.Healthy || snapshot.GeneratedAt != now || snapshot.Resources.ReservedBytes != 99 ||
		len(snapshot.Interfaces) != 1 || len(snapshot.Sessions) != 1 || len(snapshot.Flows) != 1 || snapshot.Counters.BytesSent != 7 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	snapshot.Interfaces[0].Addresses[0] = "198.51.100.1"
	snapshot.Flows[0].TargetHash = testHash(9)
	second := repository.Snapshot()
	if second.Interfaces[0].Addresses[0] != "192.0.2.2" || second.Flows[0].TargetHash != flowValue.TargetHash {
		t.Fatalf("published snapshot mutated = %#v", second)
	}

	if err := repository.apply(Event{Kind: EventUpsertInterface, Interface: Interface{Index: 3, Name: "wlan0", Reason: InterfaceEligible}}, now); !errors.Is(err, ErrRepositoryLimit) {
		t.Fatalf("interface overflow error = %v", err)
	}
	if err := repository.apply(Event{Kind: EventUpsertSession, Session: validTestSession(3)}, now); !errors.Is(err, ErrRepositoryLimit) {
		t.Fatalf("session overflow error = %v", err)
	}
	if err := repository.apply(Event{Kind: EventUpsertFlow, Flow: validTestFlow(4)}, now); !errors.Is(err, ErrRepositoryLimit) {
		t.Fatalf("flow overflow error = %v", err)
	}
}

func TestRepositoryRoleIsImmutableSnapshotMetadata(t *testing.T) {
	repository, err := NewRepositoryForRole(DefaultLimits(), RoleClient)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Snapshot().Role != RoleClient {
		t.Fatalf("initial role = %v", repository.Snapshot().Role)
	}
	if err := repository.apply(Event{Kind: EventSetHealth, Healthy: false}, time.Now()); err != nil {
		t.Fatal(err)
	}
	repository.publish(time.Now())
	if repository.Snapshot().Role != RoleClient {
		t.Fatalf("updated role = %v", repository.Snapshot().Role)
	}
	for _, role := range []Role{0, RoleServer + 1} {
		if _, err := NewRepositoryForRole(DefaultLimits(), role); !errors.Is(err, ErrInvalidRepository) {
			t.Fatalf("invalid role %v error = %v", role, err)
		}
	}
}

func TestRepositoryEventQueueIsExactlyBoundedAndNonBlocking(t *testing.T) {
	repository, err := NewRepository(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < MaxStatusEvents; index++ {
		if !repository.TryRecord(Event{Kind: EventSetHealth, Healthy: true}) {
			t.Fatalf("event %d rejected before queue limit", index+1)
		}
	}
	if repository.TryRecord(Event{Kind: EventSetHealth, Healthy: false}) {
		t.Fatal("event above queue limit accepted")
	}
	if got := repository.Snapshot().Counters.DroppedStatusEvents; got != 1 {
		t.Fatalf("dropped events = %d", got)
	}
	if repository.TryRecord(Event{}) {
		t.Fatal("invalid event accepted")
	}
}

func TestRepositoryCountersSaturateInsteadOfWrapping(t *testing.T) {
	repository, err := NewRepository(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !repository.AddCounter(CounterBytesSent, math.MaxUint64-1) || !repository.AddCounter(CounterBytesSent, 10) {
		t.Fatal("valid counter increment rejected")
	}
	if got := repository.Snapshot().Counters.BytesSent; got != math.MaxUint64 {
		t.Fatalf("saturated counter = %d", got)
	}
	if repository.AddCounter(0, 1) || repository.AddCounter(CounterBytesSent, 0) {
		t.Fatal("invalid counter increment accepted")
	}
}

func TestRepositoryTerminalRingAndRetentionAreDeterministic(t *testing.T) {
	now := time.Unix(3_000, 0).UTC()
	repository, err := NewRepositoryWithClock(DefaultLimits(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index <= MaxTerminalSummaries; index++ {
		terminal := Terminal{
			IDHash: testHash(uint64(index + 1)), State: FlowClosed,
			Reason: ReasonCompleted, FinishedAt: now.Add(time.Duration(index) * time.Millisecond),
		}
		if err := repository.apply(Event{Kind: EventFlowTerminal, Terminal: terminal}, now); err != nil {
			t.Fatal(err)
		}
	}
	repository.publish(now)
	snapshot := repository.Snapshot()
	if len(snapshot.Terminals) != MaxTerminalSummaries || snapshot.Terminals[0].IDHash == testHash(1) {
		t.Fatalf("terminal ring = first %q count %d", snapshot.Terminals[0].IDHash, len(snapshot.Terminals))
	}
	repository.prune(now.Add(TerminalRetention + 2*time.Second))
	repository.publish(now.Add(TerminalRetention + 2*time.Second))
	if got := len(repository.Snapshot().Terminals); got != 0 {
		t.Fatalf("expired terminals = %d", got)
	}
}

func TestRepositoryRunStopsOnContextAndHandlesClosedTickChannel(t *testing.T) {
	repository, err := NewRepository(DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	close(ticks)
	done := make(chan error, 1)
	go func() { done <- repository.Run(ctx, ticks) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("repository did not stop")
	}
}

func TestRepositoryRunCoalescesFlowProgressUntilPublicationTick(t *testing.T) {
	now := time.Unix(4_000, 0).UTC()
	repository, err := NewRepositoryWithClock(DefaultLimits(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	initial := validTestFlow(1)
	if err := repository.apply(Event{Kind: EventUpsertFlow, Flow: initial}, now); err != nil {
		t.Fatal(err)
	}
	repository.publish(now)

	ctx, cancel := context.WithCancel(context.Background())
	publicationTicks := make(chan time.Time, 1)
	done := make(chan error, 1)
	go func() { done <- repository.run(ctx, nil, publicationTicks) }()

	latest := initial
	latest.TxAllocatedOffset = 300
	latest.TxAcknowledged = 200
	latest.RxWrittenOffset = 100
	for offset := uint64(100); offset <= 300; offset += 100 {
		progress := initial
		progress.TxAllocatedOffset = offset
		progress.TxAcknowledged = offset - 100
		progress.RxWrittenOffset = offset / 3
		if !repository.TryRecord(Event{Kind: EventUpsertFlow, Flow: progress}) {
			t.Fatalf("progress event at offset %d rejected", offset)
		}
	}

	waitForStatus(t, time.Second, func() bool {
		return len(repository.events) == 0
	}, "repository to consume coalesced progress")
	if got := repository.Snapshot().Flows[0].TxAllocatedOffset; got != 0 {
		t.Fatalf("progress published before tick = %d", got)
	}

	publicationAt := now.Add(100 * time.Millisecond)
	publicationTicks <- publicationAt
	waitForStatus(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Flows) == 1 && snapshot.Flows[0].TxAllocatedOffset == latest.TxAllocatedOffset &&
			snapshot.GeneratedAt == publicationAt
	}, "coalesced progress publication")

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryRunPublishesTerminalAheadOfPendingProgress(t *testing.T) {
	now := time.Unix(5_000, 0).UTC()
	repository, err := NewRepositoryWithClock(DefaultLimits(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	initial := validTestFlow(1)
	if err := repository.apply(Event{Kind: EventUpsertFlow, Flow: initial}, now); err != nil {
		t.Fatal(err)
	}
	repository.publish(now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- repository.run(ctx, nil, make(chan time.Time)) }()

	progress := initial
	progress.TxAllocatedOffset = 100
	terminal := Terminal{IDHash: initial.IDHash, State: FlowClosed, Reason: ReasonCompleted, FinishedAt: now}
	if !repository.TryRecord(Event{Kind: EventUpsertFlow, Flow: progress}) ||
		!repository.TryRecord(Event{Kind: EventFlowTerminal, Terminal: terminal}) {
		t.Fatal("status event rejected")
	}
	waitForStatus(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Flows) == 0 && len(snapshot.Terminals) == 1 && snapshot.Terminals[0].IDHash == initial.IDHash
	}, "terminal publication")

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryRunPublishesFlowControlChangesImmediately(t *testing.T) {
	now := time.Unix(6_000, 0).UTC()
	repository, err := NewRepositoryWithClock(DefaultLimits(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	initial := validTestFlow(1)
	if err := repository.apply(Event{Kind: EventUpsertFlow, Flow: initial}, now); err != nil {
		t.Fatal(err)
	}
	repository.publish(now)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- repository.run(ctx, nil, make(chan time.Time)) }()

	recovering := initial
	recovering.State = FlowRecovering
	recovering.Reason = ReasonPathRemoved
	if !repository.TryRecord(Event{Kind: EventUpsertFlow, Flow: recovering}) {
		t.Fatal("control event rejected")
	}
	waitForStatus(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Flows) == 1 && snapshot.Flows[0].State == FlowRecovering &&
			snapshot.Flows[0].Reason == ReasonPathRemoved
	}, "flow control publication")

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryRunIgnoresPublicationTickWithoutProgress(t *testing.T) {
	now := time.Unix(7_000, 0).UTC()
	repository, err := NewRepositoryWithClock(DefaultLimits(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	publicationTicks := make(chan time.Time)
	done := make(chan error, 1)
	go func() { done <- repository.run(ctx, nil, publicationTicks) }()

	publicationTicks <- now.Add(time.Second)
	if generatedAt := repository.Snapshot().GeneratedAt; generatedAt != now {
		t.Fatalf("empty tick generated snapshot at %v, want %v", generatedAt, now)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryRunFlushesPendingProgressOnCancellation(t *testing.T) {
	now := time.Unix(8_000, 0).UTC()
	repository, err := NewRepositoryWithClock(DefaultLimits(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	initial := validTestFlow(1)
	if err := repository.apply(Event{Kind: EventUpsertFlow, Flow: initial}, now); err != nil {
		t.Fatal(err)
	}
	repository.publish(now)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- repository.run(ctx, nil, make(chan time.Time)) }()
	progress := initial
	progress.TxAcknowledged = 200
	if !repository.TryRecord(Event{Kind: EventUpsertFlow, Flow: progress}) {
		t.Fatal("progress event rejected")
	}
	waitForStatus(t, time.Second, func() bool {
		return len(repository.events) == 0
	}, "repository to consume pending progress")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := repository.Snapshot().Flows[0].TxAcknowledged; got != progress.TxAcknowledged {
		t.Fatalf("flushed acknowledged offset = %d, want %d", got, progress.TxAcknowledged)
	}
}

func TestRepositoryRunPreservesFIFOAcrossRejectedBatchEvent(t *testing.T) {
	now := time.Unix(9_000, 0).UTC()
	repository, err := NewRepositoryWithClock(Limits{Interfaces: 1, Sessions: 1, Flows: 1}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	initial := validTestFlow(1)
	if err := repository.apply(Event{Kind: EventUpsertFlow, Flow: initial}, now); err != nil {
		t.Fatal(err)
	}
	repository.publish(now)

	progress := initial
	progress.TxAllocatedOffset = 100
	recovering := progress
	recovering.State = FlowRecovering
	recovering.Reason = ReasonPathRemoved
	for _, event := range []Event{
		{Kind: EventUpsertFlow, Flow: progress},
		{Kind: EventUpsertFlow, Flow: validTestFlow(2)},
		{Kind: EventUpsertFlow, Flow: recovering},
	} {
		if !repository.TryRecord(event) {
			t.Fatal("batch event rejected before repository processing")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- repository.run(ctx, nil, make(chan time.Time)) }()
	waitForStatus(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Flows) == 1 && snapshot.Flows[0].TxAllocatedOffset == progress.TxAllocatedOffset &&
			snapshot.Flows[0].State == FlowRecovering && snapshot.Counters.DroppedStatusEvents == 1
	}, "ordered batch publication after rejected event")

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func waitForStatus(t *testing.T, maximum time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(maximum)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func validTestSession(value uint64) Session {
	return Session{
		IDHash: testHash(value), Transport: "tcp", Interface: "eth0", LocalAddress: "192.0.2.1",
		State: SessionReady, Reason: ReasonStarted,
	}
}

func validTestFlow(value uint64) Flow {
	return Flow{
		IDHash: testHash(value), TargetType: 3, TargetHash: testHash(value + 100),
		DeliveryMode: 2, PathSelection: 1, AdaptiveState: AdaptiveSingle,
		State: FlowRelaying, Reason: ReasonStarted,
	}
}
