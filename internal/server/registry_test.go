package server

import (
	"bytes"
	"errors"
	"io"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
)

func TestRegistryOpenIsIdempotentAndProducesOneTargetDial(t *testing.T) {
	clock := newRegistryTestClock()
	random := &countingCapabilityReader{value: 0x61}
	registry := newTestRegistry(t, testRegistryLimits(), random, clock.Now)
	request := testOpen(1)

	first := registry.HandleOpen("client-a", request)
	requireCapabilityAction(t, first)
	if first.Ready || first.Operation == nil {
		t.Fatalf("first OPEN outcome = %#v, want pending operation", first)
	}
	wantUsage := RegistrySnapshot{
		Entries:                1,
		Flows:                  1,
		OpeningFlows:           1,
		TargetDialReservations: 1,
		TombstoneReservations:  1,
	}
	if got := registry.Snapshot(); got != wantUsage {
		t.Fatalf("reserved usage = %#v, want %#v", got, wantUsage)
	}

	duplicate := registry.HandleOpen("client-a", request)
	if duplicate.Ready || duplicate.Operation != first.Operation || duplicate.GenerateCapability != nil || duplicate.Dial != nil {
		t.Fatalf("duplicate OPEN outcome = %#v, want shared pending operation", duplicate)
	}
	conflict := request
	conflict.OpenToken[0] ^= 0xff
	requireOpenFailure(t, registry.HandleOpen("client-a", conflict), protocol.OpenInternalFailure)
	invalidConflict := request
	invalidConflict.PathSelection = protocol.PathNone
	requireOpenFailure(t, registry.HandleOpen("client-a", invalidConflict), protocol.OpenInternalFailure)

	generated := registry.GenerateCapability(*first.GenerateCapability)
	if generated.Dial == nil || generated.Operation != first.Operation || generated.Ready {
		t.Fatalf("capability outcome = %#v, want one target dial", generated)
	}
	if random.Calls() != 1 {
		t.Fatalf("random calls = %d, want 1", random.Calls())
	}
	duringDial := registry.HandleOpen("client-a", request)
	if duringDial.Operation != first.Operation || duringDial.Dial != nil || duringDial.Ready {
		t.Fatalf("OPEN during dial = %#v, want shared pending operation", duringDial)
	}

	completed := registry.CompleteDial(*generated.Dial, true)
	if !completed.Ready || completed.Result.Result != protocol.OpenSuccess || completed.Owner == nil || completed.CloseLateTarget {
		t.Fatalf("dial completion = %#v, want published success", completed)
	}
	if completed.Result.Capability == (protocol.Capability{}) || completed.Result.DeliveryMode != request.DeliveryMode || completed.Result.PathSelection != request.PathSelection {
		t.Fatalf("success result = %#v, want capability and accepted policy", completed.Result)
	}
	select {
	case <-first.Operation.Done():
	default:
		t.Fatal("shared OPEN operation did not complete")
	}
	if result, ready := first.Operation.Result(); !ready || result != completed.Result {
		t.Fatalf("operation result = %#v, %v; want %#v, true", result, ready, completed.Result)
	}

	replayed := registry.HandleOpen("client-a", request)
	if !replayed.Ready || replayed.Result != completed.Result || replayed.Owner != completed.Owner {
		t.Fatalf("completed duplicate OPEN = %#v, want identical success", replayed)
	}
	if random.Calls() != 1 {
		t.Fatalf("duplicate success consumed random calls = %d, want 1", random.Calls())
	}
	if got := registry.Snapshot(); got != (RegistrySnapshot{
		Entries:               1,
		Flows:                 1,
		TombstoneReservations: 1,
	}) {
		t.Fatalf("active usage = %#v", got)
	}
}

func TestRegistryDistributedConstraintsAreStoredEchoedAndCompared(t *testing.T) {
	clock := newRegistryTestClock()
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x4d}, clock.Now)
	request := testOpen(0x24)
	request.PathSelection = protocol.PathDistributed
	request.Constraints = protocol.DeliveryConstraints{
		MaxDeliveryDelay: 80 * time.Millisecond,
		MaxDelayGap:      30 * time.Millisecond,
		Fallback:         protocol.DeliveryFallbackPause,
	}

	opening := registry.HandleOpen("client-a", request)
	requireCapabilityAction(t, opening)
	key := FlowKey{PrincipalID: "client-a", FlowID: request.FlowID}
	openingSnapshot, ok := registry.Lookup(key)
	if !ok || openingSnapshot.Constraints != request.Constraints {
		t.Fatalf("opening snapshot = %#v, %t; want constraints %#v", openingSnapshot, ok, request.Constraints)
	}

	conflict := request
	conflict.Constraints.MaxDelayGap += 10 * time.Millisecond
	requireOpenFailure(t, registry.HandleOpen("client-a", conflict), protocol.OpenInternalFailure)

	generated := registry.GenerateCapability(*opening.GenerateCapability)
	if generated.Dial == nil {
		t.Fatalf("capability outcome = %#v, want target dial", generated)
	}
	completed := registry.CompleteDial(*generated.Dial, true)
	if !completed.Ready || completed.Result.Result != protocol.OpenSuccess || completed.Result.Constraints != request.Constraints {
		t.Fatalf("completed OPEN = %#v, want echoed constraints %#v", completed, request.Constraints)
	}
	requireOpenFailure(t, registry.HandleOpen("client-a", conflict), protocol.OpenInternalFailure)
	activeSnapshot, ok := registry.Lookup(key)
	if !ok || activeSnapshot.Constraints != request.Constraints {
		t.Fatalf("active snapshot = %#v, %t; want constraints %#v", activeSnapshot, ok, request.Constraints)
	}
	duplicate := registry.HandleOpen("client-a", request)
	if !duplicate.Ready || duplicate.Result != completed.Result || duplicate.Owner != completed.Owner {
		t.Fatalf("duplicate OPEN = %#v, want identical accepted policy", duplicate)
	}
}

func TestRegistryRejectOpenPublishesStableResourceLimit(t *testing.T) {
	clock := newRegistryTestClock()
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x31}, clock.Now)
	request := testOpen(91)
	opening := registry.HandleOpen("client-a", request)
	if opening.GenerateCapability == nil || opening.Operation == nil {
		t.Fatalf("opening outcome = %#v", opening)
	}
	rejected := registry.RejectOpen(opening.GenerateCapability.Key, opening.GenerateCapability.Generation, protocol.OpenResourceLimit)
	if !rejected.Ready || rejected.Result.FlowID != request.FlowID || rejected.Result.Result != protocol.OpenResourceLimit {
		t.Fatalf("rejected OPEN = %#v", rejected)
	}
	if result, ready := opening.Operation.Result(); !ready || result != rejected.Result {
		t.Fatalf("shared rejection = %#v, %v", result, ready)
	}
	if stale := registry.RejectOpen(opening.GenerateCapability.Key, opening.GenerateCapability.Generation, protocol.OpenResourceLimit); stale.Ready {
		t.Fatalf("stale rejection = %#v", stale)
	}
	if invalid := registry.RejectOpen(opening.GenerateCapability.Key, opening.GenerateCapability.Generation, protocol.OpenInternalFailure); invalid.Ready {
		t.Fatalf("invalid rejection = %#v", invalid)
	}
}

func TestRegistryConcurrentDuplicateOpenSharesOneOperation(t *testing.T) {
	clock := newRegistryTestClock()
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x21}, clock.Now)
	request := testOpen(2)
	const callers = 32
	start := make(chan struct{})
	outcomes := make(chan OpenOutcome, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			outcomes <- registry.HandleOpen("client-a", request)
		}()
	}
	close(start)
	group.Wait()
	close(outcomes)

	var operation *OpenOperation
	var capabilityAction *GenerateCapabilityAction
	for outcome := range outcomes {
		if outcome.Ready || outcome.Operation == nil {
			t.Fatalf("concurrent outcome = %#v, want pending", outcome)
		}
		if operation == nil {
			operation = outcome.Operation
		} else if outcome.Operation != operation {
			t.Fatal("concurrent duplicate OPEN did not share operation")
		}
		if outcome.GenerateCapability != nil {
			if capabilityAction != nil {
				t.Fatal("more than one capability action was produced")
			}
			capabilityAction = outcome.GenerateCapability
		}
	}
	if capabilityAction == nil {
		t.Fatal("no capability action was produced")
	}
	generated := registry.GenerateCapability(*capabilityAction)
	if generated.Dial == nil {
		t.Fatalf("GenerateCapability() = %#v, want dial", generated)
	}
}

func TestRegistryCapabilityGenerationIsSingleFlight(t *testing.T) {
	clock := newRegistryTestClock()
	random := newBlockingCapabilityReader(0x42)
	registry := newTestRegistry(t, testRegistryLimits(), random, clock.Now)
	opening := registry.HandleOpen("client-a", testOpen(3))
	action := *opening.GenerateCapability
	result := make(chan OpenOutcome, 1)
	go func() { result <- registry.GenerateCapability(action) }()
	<-random.started

	duplicate := registry.GenerateCapability(action)
	if duplicate.Operation != opening.Operation || duplicate.Dial != nil || duplicate.Ready {
		t.Fatalf("duplicate capability execution = %#v, want shared pending operation", duplicate)
	}
	close(random.release)
	generated := <-result
	if generated.Dial == nil || random.Calls() != 1 {
		t.Fatalf("single-flight result = %#v, random calls %d", generated, random.Calls())
	}
	if stale := registry.GenerateCapability(action); stale.Dial != nil {
		t.Fatalf("stale capability execution produced second dial: %#v", stale)
	}
	if random.Calls() != 1 {
		t.Fatalf("stale execution consumed random calls = %d, want 1", random.Calls())
	}
}

func TestRegistryEntropyFailureCreatesStableTombstoneWithoutDial(t *testing.T) {
	tests := []struct {
		name   string
		random io.Reader
	}{
		{name: "reader error", random: errorCapabilityReader{}},
		{name: "all zero", random: bytes.NewReader(make([]byte, len(protocol.Capability{})))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newRegistryTestClock()
			registry := newTestRegistry(t, testRegistryLimits(), test.random, clock.Now)
			request := testOpen(4)
			opening := registry.HandleOpen("client-a", request)
			failed := registry.GenerateCapability(*opening.GenerateCapability)
			requireOpenFailure(t, failed, protocol.OpenInternalFailure)
			if failed.Dial != nil || failed.Owner != nil {
				t.Fatalf("entropy failure produced external resource: %#v", failed)
			}
			if result, ready := opening.Operation.Result(); !ready || result.Result != protocol.OpenInternalFailure {
				t.Fatalf("shared failure = %#v, %v", result, ready)
			}
			snapshot, ok := registry.Lookup(FlowKey{PrincipalID: "client-a", FlowID: request.FlowID})
			if !ok || snapshot.State != RegistryTombstoneReset || snapshot.Owner != nil || snapshot.TerminalResult != protocol.OpenInternalFailure {
				t.Fatalf("terminal snapshot = %#v, %v", snapshot, ok)
			}
			entry := registry.entries[registryKey{principalID: "client-a", flowID: request.FlowID}]
			if entry.openToken != (protocol.OpenToken{}) || entry.capability != (protocol.Capability{}) || entry.capabilityDigest != ([32]byte{}) || entry.target != (protocol.Target{}) {
				t.Fatalf("tombstone retained secret or target: %#v", entry)
			}
			requireOpenFailure(t, registry.HandleOpen("client-a", request), protocol.OpenInternalFailure)
			invalidReplay := request
			invalidReplay.PathSelection = protocol.PathNone
			requireOpenFailure(t, registry.HandleOpen("client-a", invalidReplay), protocol.OpenInternalFailure)
			if got := registry.Snapshot(); got != (RegistrySnapshot{
				Entries:               1,
				Tombstones:            1,
				TombstoneReservations: 1,
			}) {
				t.Fatalf("failure usage = %#v", got)
			}
		})
	}
}

func TestRegistryOpenDeadlineAndLateDialSuccess(t *testing.T) {
	clock := newRegistryTestClock()
	random := &countingCapabilityReader{value: 0x53}
	registry := newTestRegistry(t, testRegistryLimits(), random, clock.Now)
	request := testOpen(5)
	opening := registry.HandleOpen("client-a", request)
	generated := registry.GenerateCapability(*opening.GenerateCapability)
	clock.Advance(OpenDeadline)

	late := registry.CompleteDial(*generated.Dial, true)
	requireOpenFailure(t, late, protocol.OpenConnectFailed)
	if !late.CloseLateTarget {
		t.Fatal("late target success was not marked for close")
	}
	if result, ready := opening.Operation.Result(); !ready || result.Result != protocol.OpenConnectFailed {
		t.Fatalf("deadline operation result = %#v, %v", result, ready)
	}
	if repeated := registry.CompleteDial(*generated.Dial, true); !repeated.CloseLateTarget || repeated.Result.Result != protocol.OpenConnectFailed {
		t.Fatalf("repeated late success = %#v", repeated)
	}
	if got := registry.Snapshot(); got.Flows != 0 || got.OpeningFlows != 0 || got.TargetDialReservations != 0 || got.Tombstones != 1 {
		t.Fatalf("deadline usage = %#v", got)
	}
}

func TestRegistryDialFailureCreatesStableConnectFailure(t *testing.T) {
	clock := newRegistryTestClock()
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x62}, clock.Now)
	request := testOpen(20)
	opening := registry.HandleOpen("client-a", request)
	generated := registry.GenerateCapability(*opening.GenerateCapability)
	failed := registry.CompleteDial(*generated.Dial, false)
	requireOpenFailure(t, failed, protocol.OpenConnectFailed)
	if failed.CloseLateTarget {
		t.Fatal("failed dial without a connection requested a close")
	}
	if result, ready := opening.Operation.Result(); !ready || result.Result != protocol.OpenConnectFailed {
		t.Fatalf("failed dial operation = %#v, %v", result, ready)
	}
	requireOpenFailure(t, registry.HandleOpen("client-a", request), protocol.OpenConnectFailed)
}

func TestRegistryDeadlineBeforeCapabilityDoesNotReadEntropy(t *testing.T) {
	clock := newRegistryTestClock()
	random := &countingCapabilityReader{value: 0x31}
	registry := newTestRegistry(t, testRegistryLimits(), random, clock.Now)
	request := testOpen(6)
	opening := registry.HandleOpen("client-a", request)
	expired := registry.ExpireOpen(opening.GenerateCapability.Key, opening.GenerateCapability.Generation)
	requireOpenFailure(t, expired, protocol.OpenInternalFailure)

	stale := registry.GenerateCapability(*opening.GenerateCapability)
	requireOpenFailure(t, stale, protocol.OpenInternalFailure)
	if random.Calls() != 0 || stale.Dial != nil {
		t.Fatalf("expired capability action read entropy or dialed: calls=%d outcome=%#v", random.Calls(), stale)
	}
}

func TestRegistryRejectsStaleDialGenerationWithoutChangingOpening(t *testing.T) {
	clock := newRegistryTestClock()
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x74}, clock.Now)
	request := testOpen(7)
	opening := registry.HandleOpen("client-a", request)
	generated := registry.GenerateCapability(*opening.GenerateCapability)
	staleAction := *generated.Dial
	staleAction.Generation++
	stale := registry.CompleteDial(staleAction, true)
	if !stale.CloseLateTarget || stale.Ready {
		t.Fatalf("stale dial outcome = %#v, want close only", stale)
	}
	snapshot, _ := registry.Lookup(generated.Dial.Key)
	if snapshot.State != RegistryOpeningTarget {
		t.Fatalf("state after stale dial = %v, want OpeningTarget", snapshot.State)
	}
	success := registry.CompleteDial(*generated.Dial, true)
	if success.Result.Result != protocol.OpenSuccess {
		t.Fatalf("matching dial result = %#v", success)
	}
	second := registry.CompleteDial(*generated.Dial, true)
	if !second.CloseLateTarget || second.Ready {
		t.Fatalf("second success = %#v, want close only", second)
	}
}

func TestRegistryJoinAuthorizationAndLifecycleSecretRetention(t *testing.T) {
	clock := newRegistryTestClock()
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x45}, clock.Now)
	request := testOpen(8)
	success := completeTestOpen(t, registry, "client-a", request)
	key := FlowKey{PrincipalID: "client-a", FlowID: request.FlowID}

	authorization, ok := registry.AuthorizeJoin("client-a", request.FlowID, success.Result.Capability)
	if !ok || authorization.PrincipalID != "client-a" || authorization.FlowID != request.FlowID || authorization.Flow != success.Owner {
		t.Fatalf("authorization = %#v, %v", authorization, ok)
	}
	wrong := success.Result.Capability
	wrong[0] ^= 0xff
	if _, ok := registry.AuthorizeJoin("client-a", request.FlowID, wrong); ok {
		t.Fatal("wrong capability was authorized")
	}
	if _, ok := registry.AuthorizeJoin("client-b", request.FlowID, success.Result.Capability); ok {
		t.Fatal("wrong principal namespace was authorized")
	}

	if !registry.UpdateLifecycle(key, success.Owner, flow.Relaying) ||
		!registry.UpdateLifecycle(key, success.Owner, flow.Recovering) ||
		!registry.UpdateLifecycle(key, success.Owner, flow.Relaying) ||
		!registry.UpdateLifecycle(key, success.Owner, flow.Closing) {
		t.Fatal("valid lifecycle transition was rejected")
	}
	if registry.UpdateLifecycle(key, success.Owner, flow.Relaying) {
		t.Fatal("Closing transitioned back to Relaying")
	}
	entry := registry.entries[importKey(key)]
	if entry.openToken != (protocol.OpenToken{}) || entry.capability != (protocol.Capability{}) || entry.target != (protocol.Target{}) || entry.capabilityDigest == ([32]byte{}) {
		t.Fatalf("Closing secret retention = token %x capability %x target %#v digest %x", entry.openToken, entry.capability, entry.target, entry.capabilityDigest)
	}
	if _, ok := registry.AuthorizeJoin("client-a", request.FlowID, success.Result.Capability); !ok {
		t.Fatal("Closing did not authorize original capability through digest")
	}
	requireOpenFailure(t, registry.HandleOpen("client-a", request), protocol.OpenInternalFailure)

	if !registry.UpdateLifecycle(key, success.Owner, flow.Resetting) {
		t.Fatal("Closing -> Resetting was rejected")
	}
	if _, ok := registry.AuthorizeJoin("client-a", request.FlowID, success.Result.Capability); ok {
		t.Fatal("Resetting authorized JOIN")
	}
	if !registry.MarkTerminal(key, success.Owner, TerminalReset) {
		t.Fatal("MarkTerminal() rejected matching owner")
	}
	snapshot, ok := registry.Lookup(key)
	if !ok || snapshot.State != RegistryTombstoneReset || snapshot.Owner != nil || snapshot.TerminalKind != TerminalReset {
		t.Fatalf("reset tombstone = %#v, %v", snapshot, ok)
	}
}

func TestRegistryClosedTerminalRequiresMatchingOwnerAndIsStable(t *testing.T) {
	clock := newRegistryTestClock()
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x29}, clock.Now)
	request := testOpen(9)
	success := completeTestOpen(t, registry, "client-a", request)
	key := FlowKey{PrincipalID: "client-a", FlowID: request.FlowID}
	if registry.MarkTerminal(key, flow.NewFlow(), TerminalClosed) {
		t.Fatal("wrong owner terminalized flow")
	}
	if !registry.MarkTerminal(key, success.Owner, TerminalClosed) {
		t.Fatal("matching owner did not terminalize flow")
	}
	snapshot, ok := registry.Lookup(key)
	if !ok || snapshot.State != RegistryTombstoneClosed || snapshot.TerminalKind != TerminalClosed || snapshot.TerminalResult != protocol.OpenInternalFailure {
		t.Fatalf("closed tombstone = %#v, %v", snapshot, ok)
	}
	requireOpenFailure(t, registry.HandleOpen("client-a", request), protocol.OpenInternalFailure)
}

func TestRegistryTombstoneExpiryReleasesAllReservations(t *testing.T) {
	clock := newRegistryTestClock()
	random := &countingCapabilityReader{value: 0}
	registry := newTestRegistry(t, testRegistryLimits(), random, clock.Now)
	request := testOpen(10)
	opening := registry.HandleOpen("client-a", request)
	registry.GenerateCapability(*opening.GenerateCapability)
	clock.Advance(TombstoneLifetime - time.Nanosecond)
	if _, ok := registry.Lookup(FlowKey{PrincipalID: "client-a", FlowID: request.FlowID}); !ok {
		t.Fatal("tombstone expired before fixed lifetime")
	}
	clock.Advance(time.Nanosecond)
	if _, ok := registry.Lookup(FlowKey{PrincipalID: "client-a", FlowID: request.FlowID}); ok {
		t.Fatal("tombstone retained at expiry boundary")
	}
	if got := registry.Snapshot(); got != (RegistrySnapshot{}) {
		t.Fatalf("usage after tombstone expiry = %#v", got)
	}
	if got := registry.PrincipalUsage("client-a"); got != (PrincipalUsageSnapshot{}) {
		t.Fatalf("principal usage after expiry = %#v", got)
	}
}

func TestRegistryQuotaReservationsAreHardAndPreserveExistingFlows(t *testing.T) {
	t.Run("opening and target dial", func(t *testing.T) {
		clock := newRegistryTestClock()
		limits := RegistryLimits{Flows: 2, FlowsPerPrincipal: 2, OpeningFlows: 1, TargetDials: 1, Tombstones: 2, TombstonesPerPrincipal: 2}
		registry := newTestRegistry(t, limits, &countingCapabilityReader{value: 0x11}, clock.Now)
		firstRequest := testOpen(11)
		first := registry.HandleOpen("client-a", firstRequest)
		requireCapabilityAction(t, first)
		rejected := registry.HandleOpen("client-a", testOpen(12))
		requireOpenFailure(t, rejected, protocol.OpenResourceLimit)
		if rejected.Rejection != OpenRejectedOpeningCapacity {
			t.Fatalf("opening rejection = %d", rejected.Rejection)
		}
		if snapshot, _ := registry.Lookup(FlowKey{PrincipalID: "client-a", FlowID: firstRequest.FlowID}); snapshot.State != RegistryOpeningCapability {
			t.Fatalf("resource refusal changed existing flow: %#v", snapshot)
		}
	})

	t.Run("target dial", func(t *testing.T) {
		clock := newRegistryTestClock()
		limits := RegistryLimits{Flows: 2, FlowsPerPrincipal: 2, OpeningFlows: 2, TargetDials: 1, Tombstones: 2, TombstonesPerPrincipal: 2}
		registry := newTestRegistry(t, limits, &countingCapabilityReader{value: 0x12}, clock.Now)
		requireCapabilityAction(t, registry.HandleOpen("client-a", testOpen(18)))
		rejected := registry.HandleOpen("client-a", testOpen(19))
		requireOpenFailure(t, rejected, protocol.OpenResourceLimit)
		if rejected.Rejection != OpenRejectedTargetDialCapacity {
			t.Fatalf("target dial rejection = %d", rejected.Rejection)
		}
	})

	t.Run("per principal terminal reservation", func(t *testing.T) {
		clock := newRegistryTestClock()
		limits := RegistryLimits{Flows: 2, FlowsPerPrincipal: 2, OpeningFlows: 2, TargetDials: 2, Tombstones: 2, TombstonesPerPrincipal: 1}
		registry := newTestRegistry(t, limits, &countingCapabilityReader{value: 0x22}, clock.Now)
		completeTestOpen(t, registry, "client-a", testOpen(13))
		requireOpenFailure(t, registry.HandleOpen("client-a", testOpen(14)), protocol.OpenResourceLimit)
		other := registry.HandleOpen("client-b", testOpen(15))
		requireCapabilityAction(t, other)
		if got := registry.PrincipalUsage("client-a"); got.Flows != 1 || got.TombstoneReservations != 1 {
			t.Fatalf("client-a usage = %#v", got)
		}
	})

	t.Run("global flow and tombstone reservation", func(t *testing.T) {
		clock := newRegistryTestClock()
		limits := RegistryLimits{Flows: 1, FlowsPerPrincipal: 1, OpeningFlows: 1, TargetDials: 1, Tombstones: 1, TombstonesPerPrincipal: 1}
		registry := newTestRegistry(t, limits, &countingCapabilityReader{value: 0x33}, clock.Now)
		firstRequest := testOpen(16)
		completeTestOpen(t, registry, "client-a", firstRequest)
		requireOpenFailure(t, registry.HandleOpen("client-b", testOpen(17)), protocol.OpenResourceLimit)
		if got := registry.Snapshot(); got.Flows != 1 || got.TombstoneReservations != 1 {
			t.Fatalf("global usage = %#v", got)
		}
	})
}

func TestRegistryValidatesBeforeReservingOrReadingRandom(t *testing.T) {
	clock := newRegistryTestClock()
	random := &countingCapabilityReader{value: 0x17}
	registry := newTestRegistry(t, testRegistryLimits(), random, clock.Now)
	base := testOpen(18)
	tests := []struct {
		name      string
		principal string
		request   protocol.Open
		result    protocol.OpenResultCode
	}{
		{name: "principal", principal: "bad principal", request: base, result: protocol.OpenInternalFailure},
		{name: "zero flow", principal: "client-a", request: withZeroFlow(base), result: protocol.OpenInternalFailure},
		{name: "zero token", principal: "client-a", request: withZeroToken(base), result: protocol.OpenInternalFailure},
		{name: "policy", principal: "client-a", request: withInvalidPolicy(base), result: protocol.OpenUnsupportedPolicy},
		{name: "constraints", principal: "client-a", request: withInvalidConstraints(base), result: protocol.OpenUnsupportedPolicy},
		{name: "target", principal: "client-a", request: withInvalidTarget(base), result: protocol.OpenInvalidTarget},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireOpenFailure(t, registry.HandleOpen(test.principal, test.request), test.result)
		})
	}
	if random.Calls() != 0 || registry.Snapshot() != (RegistrySnapshot{}) {
		t.Fatalf("invalid OPEN consumed resources: random=%d snapshot=%#v", random.Calls(), registry.Snapshot())
	}
}

func TestRegistryCanonicalizesMappedIPv4ForDuplicateOpen(t *testing.T) {
	clock := newRegistryTestClock()
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x35}, clock.Now)
	request := testOpen(19)
	request.Target = protocol.Target{Address: netip.MustParseAddr("::ffff:192.0.2.9"), Port: 443}
	first := registry.HandleOpen("client-a", request)
	requireCapabilityAction(t, first)
	unmapped := request
	unmapped.Target.Address = netip.MustParseAddr("192.0.2.9")
	duplicate := registry.HandleOpen("client-a", unmapped)
	if duplicate.Operation != first.Operation || duplicate.Ready {
		t.Fatalf("canonical target duplicate = %#v, want shared operation", duplicate)
	}
}

func TestRegistrySameFlowIDUsesIndependentPrincipalNamespaces(t *testing.T) {
	clock := newRegistryTestClock()
	entropy := append(bytes.Repeat([]byte{0x51}, len(protocol.Capability{})), bytes.Repeat([]byte{0x72}, len(protocol.Capability{}))...)
	registry := newTestRegistry(t, testRegistryLimits(), bytes.NewReader(entropy), clock.Now)
	request := testOpen(21)
	first := completeTestOpen(t, registry, "client-a", request)
	second := completeTestOpen(t, registry, "client-b", request)
	if first.Owner == second.Owner || first.Result.Capability == second.Result.Capability {
		t.Fatalf("principal namespaces shared owner or capability: first=%#v second=%#v", first, second)
	}
	if _, ok := registry.AuthorizeJoin("client-a", request.FlowID, second.Result.Capability); ok {
		t.Fatal("client-a authorized client-b capability")
	}
	if _, ok := registry.AuthorizeJoin("client-b", request.FlowID, first.Result.Capability); ok {
		t.Fatal("client-b authorized client-a capability")
	}
	if got := registry.Snapshot(); got.Flows != 2 || got.Entries != 2 {
		t.Fatalf("namespace usage = %#v", got)
	}
}

func TestRegistryRejectsInvalidConstruction(t *testing.T) {
	valid := testRegistryLimits()
	tests := []struct {
		name   string
		limits RegistryLimits
	}{
		{name: "zero", limits: RegistryLimits{}},
		{name: "flows hard maximum", limits: withLimits(valid, func(value *RegistryLimits) { value.Flows = HardMaxServerFlows + 1 })},
		{name: "per principal above flows", limits: withLimits(valid, func(value *RegistryLimits) { value.FlowsPerPrincipal = value.Flows + 1 })},
		{name: "opening above flows", limits: withLimits(valid, func(value *RegistryLimits) { value.OpeningFlows = value.Flows + 1 })},
		{name: "dials above opening", limits: withLimits(valid, func(value *RegistryLimits) { value.TargetDials = value.OpeningFlows + 1 })},
		{name: "tombstones hard maximum", limits: withLimits(valid, func(value *RegistryLimits) { value.Tombstones = HardMaxServerTombstones + 1 })},
		{name: "per principal tombstones above total", limits: withLimits(valid, func(value *RegistryLimits) { value.TombstonesPerPrincipal = value.Tombstones + 1 })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRegistry(test.limits, &countingCapabilityReader{value: 1}, time.Now); !errors.Is(err, ErrInvalidRegistryConfig) {
				t.Fatalf("NewRegistry() error = %v, want ErrInvalidRegistryConfig", err)
			}
		})
	}
	if _, err := NewRegistry(valid, nil, time.Now); !errors.Is(err, ErrInvalidRegistryConfig) {
		t.Fatalf("nil random error = %v", err)
	}
	if _, err := NewRegistry(valid, &countingCapabilityReader{value: 1}, nil); !errors.Is(err, ErrInvalidRegistryConfig) {
		t.Fatalf("nil clock error = %v", err)
	}
}

func newTestRegistry(t *testing.T, limits RegistryLimits, random io.Reader, now func() time.Time) *Registry {
	t.Helper()
	registry, err := NewRegistry(limits, random, now)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	return registry
}

func testRegistryLimits() RegistryLimits {
	return RegistryLimits{
		Flows:                  4,
		FlowsPerPrincipal:      2,
		OpeningFlows:           2,
		TargetDials:            2,
		Tombstones:             4,
		TombstonesPerPrincipal: 2,
	}
}

func testOpen(value byte) protocol.Open {
	request := protocol.Open{
		DeliveryMode:  protocol.DeliveryAdaptive,
		PathSelection: protocol.PathFastest,
		Target: protocol.Target{
			DNSName: "example.test",
			Port:    443,
		},
	}
	request.FlowID[15] = value
	request.OpenToken[31] = value
	return request
}

func completeTestOpen(t *testing.T, registry *Registry, principalID string, request protocol.Open) OpenOutcome {
	t.Helper()
	opening := registry.HandleOpen(principalID, request)
	requireCapabilityAction(t, opening)
	generated := registry.GenerateCapability(*opening.GenerateCapability)
	if generated.Dial == nil {
		t.Fatalf("GenerateCapability() = %#v, want dial", generated)
	}
	completed := registry.CompleteDial(*generated.Dial, true)
	if !completed.Ready || completed.Result.Result != protocol.OpenSuccess || completed.Owner == nil {
		t.Fatalf("CompleteDial() = %#v, want success", completed)
	}
	return completed
}

func requireCapabilityAction(t *testing.T, outcome OpenOutcome) {
	t.Helper()
	if outcome.Ready || outcome.Operation == nil || outcome.GenerateCapability == nil || outcome.Dial != nil {
		t.Fatalf("OPEN outcome = %#v, want GenerateCapability action", outcome)
	}
	if outcome.GenerateCapability.Generation == 0 || outcome.GenerateCapability.Deadline.IsZero() {
		t.Fatalf("GenerateCapability action = %#v, want generation and deadline", outcome.GenerateCapability)
	}
}

func requireOpenFailure(t *testing.T, outcome OpenOutcome, result protocol.OpenResultCode) {
	t.Helper()
	if !outcome.Ready || outcome.Result.Result != result || outcome.Result.Capability != (protocol.Capability{}) ||
		outcome.Result.DeliveryMode != 0 || outcome.Result.PathSelection != 0 ||
		outcome.Result.Constraints != (protocol.DeliveryConstraints{}) || outcome.Owner != nil {
		t.Fatalf("OPEN outcome = %#v, want failure %d", outcome, result)
	}
}

type registryTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newRegistryTestClock() *registryTestClock {
	return &registryTestClock{now: time.Unix(1_700_000_000, 0)}
}

func (clock *registryTestClock) Now() time.Time {
	clock.mu.Lock()
	now := clock.now
	clock.mu.Unlock()
	return now
}

func (clock *registryTestClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

type countingCapabilityReader struct {
	mu    sync.Mutex
	calls int
	value byte
}

func (reader *countingCapabilityReader) Read(buffer []byte) (int, error) {
	reader.mu.Lock()
	reader.calls++
	value := reader.value
	reader.mu.Unlock()
	for index := range buffer {
		buffer[index] = value
	}
	return len(buffer), nil
}

func (reader *countingCapabilityReader) Calls() int {
	reader.mu.Lock()
	calls := reader.calls
	reader.mu.Unlock()
	return calls
}

type blockingCapabilityReader struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
	value   byte
}

func newBlockingCapabilityReader(value byte) *blockingCapabilityReader {
	return &blockingCapabilityReader{started: make(chan struct{}), release: make(chan struct{}), value: value}
}

func (reader *blockingCapabilityReader) Read(buffer []byte) (int, error) {
	if reader.calls.Add(1) == 1 {
		close(reader.started)
	}
	<-reader.release
	for index := range buffer {
		buffer[index] = reader.value
	}
	return len(buffer), nil
}

func (reader *blockingCapabilityReader) Calls() int { return int(reader.calls.Load()) }

type errorCapabilityReader struct{}

func (errorCapabilityReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func withZeroFlow(request protocol.Open) protocol.Open {
	request.FlowID = protocol.FlowID{}
	return request
}

func withZeroToken(request protocol.Open) protocol.Open {
	request.OpenToken = protocol.OpenToken{}
	return request
}

func withInvalidPolicy(request protocol.Open) protocol.Open {
	request.PathSelection = protocol.PathNone
	return request
}

func withInvalidConstraints(request protocol.Open) protocol.Open {
	request.Constraints = protocol.DeliveryConstraints{Fallback: protocol.DeliveryFallbackFastest}
	return request
}

func withInvalidTarget(request protocol.Open) protocol.Open {
	request.Target.Port = 0
	return request
}

func withLimits(limits RegistryLimits, mutate func(*RegistryLimits)) RegistryLimits {
	mutate(&limits)
	return limits
}
