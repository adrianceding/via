package daemon

import (
	"context"
	"errors"
	"math"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/config"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	servercore "github.com/adrianceding/via/internal/server"
	statusapi "github.com/adrianceding/via/internal/status"
	"github.com/adrianceding/via/internal/transport"
)

var errTestJoinWrite = errors.New("test JOIN_RESULT write failed")

func TestAllocateDaemonGenerationNeverWraps(t *testing.T) {
	var counter atomic.Uint64
	counter.Store(math.MaxUint64 - 1)
	value, ok := allocateDaemonGeneration(&counter)
	if !ok || value != math.MaxUint64 {
		t.Fatalf("last generation = %d, %t", value, ok)
	}
	if value, ok = allocateDaemonGeneration(&counter); ok || value != 0 || counter.Load() != math.MaxUint64 {
		t.Fatalf("exhausted generation = %d, %t; counter = %d", value, ok, counter.Load())
	}

	counter.Store(math.MaxUint64 - 1)
	start := make(chan struct{})
	results := make(chan bool, 2)
	for range 2 {
		go func() {
			<-start
			_, allocated := allocateDaemonGeneration(&counter)
			results <- allocated
		}()
	}
	close(start)
	allocated := 0
	for range 2 {
		if <-results {
			allocated++
		}
	}
	if allocated != 1 || counter.Load() != math.MaxUint64 {
		t.Fatalf("concurrent exhaustion allocated = %d; counter = %d", allocated, counter.Load())
	}
}

func TestServerDuplicateJoinReusesPublishedAttachment(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	request := protocol.Join{FlowID: harness.flowID, Capability: harness.capability}
	if err := harness.daemon.handleJoin(harness.session, request); err != nil {
		t.Fatal(err)
	}
	first, ok := harness.session.attachment(harness.flowID)
	if !ok {
		t.Fatal("first JOIN did not publish its attachment")
	}
	if err := harness.daemon.handleJoin(harness.session, request); err != nil {
		t.Fatal(err)
	}
	second, ok := harness.session.attachment(harness.flowID)
	if !ok || second != first {
		t.Fatalf("duplicate JOIN attachment = %#v, %t; want %#v", second, ok, first)
	}
	snapshot := harness.instance.snapshot().Flow.Lifecycle
	if len(snapshot.Published) != 1 || len(snapshot.Provisional) != 0 || harness.daemon.nextAttachment.Load() != 1 {
		t.Fatalf("duplicate JOIN lifecycle = %#v; next attachment = %d", snapshot, harness.daemon.nextAttachment.Load())
	}
	if got := harness.connection.joinSuccesses(); got != 2 {
		t.Fatalf("JOIN success results = %d", got)
	}
}

func TestServerDuplicateOpenReturnsExistingFlowWithoutDial(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	request := protocol.Open{
		FlowID: harness.flowID, OpenToken: protocol.OpenToken{2}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 9},
	}
	// The harness intentionally has no TargetDialExecutor. A duplicate OPEN can
	// only pass if the runtime reuses the Registry result without another dial.
	if err := harness.daemon.handleOpen(harness.session, request); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		_, ready := harness.connection.lastOpenResult()
		return ready
	}, "duplicate OPEN result")
	result, ok := harness.connection.lastOpenResult()
	if !ok || result.Result != protocol.OpenSuccess || result.Capability != harness.capability {
		t.Fatalf("duplicate OPEN result = %#v, %t", result, ok)
	}
	if snapshot := harness.daemon.registry.Snapshot(); snapshot.Flows != 1 || snapshot.TargetDialReservations != 0 {
		t.Fatalf("registry after duplicate OPEN = %#v", snapshot)
	}
	now := time.Now()
	remaining := 0
	for harness.daemon.limiter.AllowOpen(now, harness.session.principal) {
		remaining++
	}
	if remaining != 128 {
		t.Fatalf("duplicate OPEN consumed flow rate-limit capacity: remaining %d, want 128", remaining)
	}
}

func TestServerRedundantOpenPublishesOriginatingSession(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	request := protocol.Open{
		FlowID: harness.flowID, OpenToken: protocol.OpenToken{2}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 9},
	}
	if err := harness.daemon.handleOpen(harness.session, request); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		_, attached := harness.session.attachment(harness.flowID)
		result, ok := harness.connection.lastOpenResult()
		return attached && ok && result.ImplicitAttachment
	}, "redundant OPEN implicit originating attachment")
	if published := harness.instance.snapshot().Flow.Lifecycle.Published; len(published) != 1 {
		t.Fatalf("redundant OPEN published attachments = %#v", published)
	}
	if joins := harness.connection.joinSuccesses(); joins != 0 {
		t.Fatalf("redundant OPEN extra JOIN_RESULT sends = %d", joins)
	}
}

func TestServerAdaptiveOpenPublishesOriginatingSession(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	flowID := protocol.FlowID{0x11}
	request := protocol.Open{
		FlowID: flowID, OpenToken: protocol.OpenToken{0x12}, DeliveryMode: protocol.DeliveryAdaptive,
		PathSelection: protocol.PathFastest,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 10},
	}
	// Bypass dial by completing the registry operation directly, then create
	// the flow with a pipe — matching the pattern used by the harness setup.
	outcome := harness.daemon.registry.HandleOpen(harness.session.principal, request)
	if outcome.GenerateCapability == nil {
		t.Fatalf("adaptive OPEN capability outcome = %#v", outcome)
	}
	outcome = harness.daemon.registry.GenerateCapability(*outcome.GenerateCapability)
	if outcome.Dial == nil {
		t.Fatalf("adaptive OPEN dial outcome = %#v", outcome)
	}
	outcome = harness.daemon.registry.CompleteDial(*outcome.Dial, true)
	if !outcome.Ready || outcome.Result.Result != protocol.OpenSuccess {
		t.Fatalf("adaptive OPEN completion = %#v", outcome)
	}
	targetConn, _ := net.Pipe()
	key := servercore.FlowKey{PrincipalID: harness.session.principal, FlowID: flowID}
	instance, err := newServerFlow(
		harness.daemon, key, flowID, request.Target, request.DeliveryMode, request.PathSelection,
		request.Constraints, outcome.Owner, targetConn,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !harness.daemon.addFlow(key, instance) {
		t.Fatal("addFlow failed")
	}
	if err := instance.start(); err != nil {
		t.Fatal(err)
	}
	result := outcome.Result
	if err := harness.daemon.completeOpenSession(harness.session, request, result); err != nil {
		t.Fatal(err)
	}
	_, attached := harness.session.attachment(flowID)
	if !attached {
		t.Fatal("session was not attached to the adaptive flow")
	}
	openResult, ok := harness.connection.lastOpenResult()
	if !ok || !openResult.ImplicitAttachment {
		t.Fatalf("adaptive OPEN_RESULT did not set ImplicitAttachment = %#v, %t", openResult, ok)
	}
	if published := instance.snapshot().Flow.Lifecycle.Published; len(published) != 1 {
		t.Fatalf("adaptive OPEN published attachments = %#v", published)
	}
	if joins := harness.connection.joinSuccesses(); joins != 0 {
		t.Fatalf("adaptive OPEN extra JOIN_RESULT sends = %d", joins)
	}
}

func TestServerOpenAppliesDistributedConstraintsToDownlinkRelay(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	request := protocol.Open{
		FlowID:        protocol.FlowID{0x31},
		OpenToken:     protocol.OpenToken{0x32},
		DeliveryMode:  protocol.DeliveryAdaptive,
		PathSelection: protocol.PathDistributed,
		Constraints: protocol.DeliveryConstraints{
			MaxDeliveryDelay: 90 * time.Millisecond,
			MaxDelayGap:      40 * time.Millisecond,
			Fallback:         protocol.DeliveryFallbackPause,
		},
		Target: protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 10},
	}
	opening := harness.daemon.registry.HandleOpen(harness.session.principal, request)
	if opening.GenerateCapability == nil {
		t.Fatalf("OPEN capability outcome = %#v", opening)
	}
	generated := harness.daemon.registry.GenerateCapability(*opening.GenerateCapability)
	if generated.Dial == nil {
		t.Fatalf("OPEN dial outcome = %#v", generated)
	}
	target, peer := net.Pipe()
	defer peer.Close()
	completed := harness.daemon.completeOpenDial(request, *generated.Dial, target)
	if !completed.Ready || completed.Result.Result != protocol.OpenSuccess || completed.Result.Constraints != request.Constraints {
		t.Fatalf("OPEN completion = %#v", completed)
	}
	key := servercore.FlowKey{PrincipalID: harness.session.principal, FlowID: request.FlowID}
	instance := harness.daemon.flow(key)
	if instance == nil {
		t.Fatal("accepted OPEN did not install a server flow")
	}
	want := policy.Config{
		Mode:        request.DeliveryMode,
		Selection:   request.PathSelection,
		Constraints: request.Constraints,
	}
	if got := instance.snapshot().Policy.Config; got != want {
		t.Fatalf("downlink relay policy = %#v, want %#v", got, want)
	}
}

func TestServerSlowOpenDoesNotBlockExistingFlowJoin(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	connector := newBlockingTargetConnector(false)
	executor, err := servercore.NewTargetDialExecutor(net.DefaultResolver, connector)
	if err != nil {
		t.Fatal(err)
	}
	harness.daemon.targets = executor
	request := protocol.Open{
		FlowID: protocol.FlowID{3}, OpenToken: protocol.OpenToken{4}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 10},
	}
	if err := harness.daemon.handleOpen(harness.session, request); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connector.started:
	case <-time.After(time.Second):
		t.Fatal("target dial did not start")
	}
	joinDone := make(chan error, 1)
	go func() {
		joinDone <- harness.daemon.handleJoin(harness.session, protocol.Join{
			FlowID: harness.flowID, Capability: harness.capability,
		})
	}()
	select {
	case err := <-joinDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow target dial blocked JOIN on the shared session")
	}
	connector.unblock()
	waitFor(t, time.Second, func() bool {
		return len(harness.connection.openResults(request.FlowID)) == 1
	}, "failed slow OPEN result")
}

func TestServerOpenInstallRollbackIsStableForDuplicateWaiter(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	harness.daemon.openWorkerLimit = 1

	connector := newBlockingTargetConnector(true)
	defer connector.closePeer()
	executor, err := servercore.NewTargetDialExecutor(net.DefaultResolver, connector)
	if err != nil {
		t.Fatal(err)
	}
	harness.daemon.targets = executor
	request := protocol.Open{
		FlowID: protocol.FlowID{5}, OpenToken: protocol.OpenToken{6}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 11},
	}
	if err := harness.daemon.handleOpen(harness.session, request); err != nil {
		t.Fatal(err)
	}
	if err := harness.daemon.handleOpen(harness.session, request); err != nil {
		t.Fatal(err)
	}
	harness.daemon.openWaitersMu.Lock()
	waiters := len(harness.daemon.openWaiters)
	harness.daemon.openWaitersMu.Unlock()
	if waiters != 1 {
		t.Fatalf("coalesced duplicate OPEN waiters = %d, want 1", waiters)
	}
	select {
	case <-connector.started:
	case <-time.After(time.Second):
		t.Fatal("target dial did not start")
	}
	if err := harness.daemon.handleOpen(harness.session, request); err != nil {
		t.Fatal(err)
	}
	key := servercore.FlowKey{PrincipalID: harness.session.principal, FlowID: request.FlowID}
	harness.daemon.flowsMu.Lock()
	harness.daemon.flows[key] = harness.instance
	harness.daemon.flowsMu.Unlock()
	connector.unblock()
	waitFor(t, time.Second, func() bool {
		return len(harness.connection.openResults(request.FlowID)) == 2
	}, "duplicate rollback OPEN results")
	results := harness.connection.openResults(request.FlowID)
	for index, result := range results {
		if result.Result != protocol.OpenConnectFailed {
			t.Fatalf("rollback result[%d] = %#v", index, result)
		}
	}
	if connector.calls.Load() != 1 {
		t.Fatalf("duplicate OPEN target dials = %d", connector.calls.Load())
	}
	harness.daemon.flowsMu.Lock()
	if harness.daemon.flows[key] == harness.instance {
		delete(harness.daemon.flows, key)
	}
	harness.daemon.flowsMu.Unlock()
}

func TestServerDrainingRejectsOpenBeforeRegistryReservation(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	harness.daemon.draining.Store(true)
	request := protocol.Open{
		FlowID: protocol.FlowID{9}, OpenToken: protocol.OpenToken{8}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 9},
	}
	before := harness.daemon.registry.Snapshot()
	if err := harness.daemon.handleOpen(harness.session, request); err != nil {
		t.Fatal(err)
	}
	after := harness.daemon.registry.Snapshot()
	if after != before {
		t.Fatalf("draining OPEN changed registry: before=%#v after=%#v", before, after)
	}
	result, ok := harness.connection.lastOpenResult()
	if !ok || result.FlowID != request.FlowID || result.Result != protocol.OpenResourceLimit {
		t.Fatalf("draining OPEN result = %#v, %t", result, ok)
	}
}

func TestServerDuplicateOpenWaiterUsesOriginalAbsoluteDeadline(t *testing.T) {
	now := time.Unix(100, 0)
	registry, err := servercore.NewRegistry(servercore.RegistryLimits{
		Flows: 2, FlowsPerPrincipal: 2, OpeningFlows: 2, TargetDials: 2,
		Tombstones: 4, TombstonesPerPrincipal: 4,
	}, zeroFreeReader{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := protocol.Open{
		FlowID: protocol.FlowID{12}, OpenToken: protocol.OpenToken{13}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 14},
	}
	first := registry.HandleOpen("client-01", request)
	duplicate := registry.HandleOpen("client-01", request)
	if first.GenerateCapability == nil || duplicate.Operation == nil {
		t.Fatalf("OPEN outcomes = %#v, %#v", first, duplicate)
	}
	now = now.Add(servercore.OpenDeadline)
	daemon := &serverDaemon{registry: registry}
	outcome := daemon.expireOpenWaiter(first.GenerateCapability.Key, first.GenerateCapability.Generation, duplicate)
	if !outcome.Ready || outcome.Result.FlowID != request.FlowID || outcome.Result.Result != protocol.OpenInternalFailure {
		t.Fatalf("expired duplicate OPEN = %#v", outcome)
	}
	if result, ready := first.Operation.Result(); !ready || result != outcome.Result {
		t.Fatalf("authoritative OPEN operation = %#v, %t", result, ready)
	}
}

func TestServerDuplicateJoinSendFailureKeepsPublishedAttachment(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	request := protocol.Join{FlowID: harness.flowID, Capability: harness.capability}
	if err := harness.daemon.handleJoin(harness.session, request); err != nil {
		t.Fatal(err)
	}
	want, ok := harness.session.attachment(harness.flowID)
	if !ok {
		t.Fatal("first JOIN did not publish its attachment")
	}
	harness.connection.failJoin.Store(true)
	if err := harness.daemon.handleJoin(harness.session, request); !errors.Is(err, errTestJoinWrite) {
		t.Fatalf("duplicate JOIN error = %v", err)
	}
	got, ok := harness.session.attachment(harness.flowID)
	if !ok || got != want {
		t.Fatalf("published attachment after failed duplicate = %#v, %t; want %#v", got, ok, want)
	}
	if snapshot := harness.instance.snapshot().Flow.Lifecycle; len(snapshot.Published) != 1 || len(snapshot.Provisional) != 0 {
		t.Fatalf("lifecycle after failed duplicate = %#v", snapshot)
	}
}

func TestServerJoinCompletionAfterTerminalReleasesReservation(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	writeStarted := make(chan struct{})
	writeRelease := make(chan struct{})
	harness.connection.mu.Lock()
	harness.connection.blockJoinStart = writeStarted
	harness.connection.blockJoinRelease = writeRelease
	harness.connection.mu.Unlock()
	result := make(chan error, 1)
	go func() {
		result <- harness.daemon.handleJoin(harness.session, protocol.Join{
			FlowID: harness.flowID, Capability: harness.capability,
		})
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("JOIN_RESULT write did not start")
	}
	harness.instance.close()
	close(writeRelease)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	harness.session.attachmentsMu.RLock()
	attachments := len(harness.session.attachments)
	reservations := len(harness.session.reservations)
	harness.session.attachmentsMu.RUnlock()
	if attachments != 0 || reservations != 0 {
		t.Fatalf("late JOIN completion left attachments=%d reservations=%d", attachments, reservations)
	}
}

func TestServerFlowSemanticFailureDoesNotCloseSharedSession(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	request := protocol.Join{FlowID: harness.flowID, Capability: harness.capability}
	if err := harness.daemon.handleJoin(harness.session, request); err != nil {
		t.Fatal(err)
	}
	attachment, _ := harness.session.attachment(harness.flowID)
	if err := harness.instance.remote(protocol.ACK{FlowID: harness.flowID, NextOffset: 1}, attachment); err != nil {
		t.Fatalf("data-flow semantic error escaped to transport session: %v", err)
	}
	state := harness.instance.snapshot().Flow.Lifecycle.State
	if state != flow.Resetting && state != flow.Reset {
		t.Fatalf("semantic conflict state = %v", state)
	}
	if harness.connection.isClosed() {
		t.Fatal("shared transport session was closed by a Flow semantic error")
	}
}

func TestServerFlowKeepsOneTimerPerKind(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	harness.instance.armTimer(relayTimerRetry, 101, time.Hour)
	harness.instance.armTimer(relayTimerRetry, 102, time.Hour)
	harness.instance.cancelTimer(relayTimerRetry, 101)
	harness.instance.timersMu.Lock()
	current, ok := harness.instance.timers[relayTimerRetry]
	count := len(harness.instance.timers)
	harness.instance.timersMu.Unlock()
	if !ok || count != 2 || current.generation != 102 {
		// RelayStart owns the recovery timer; the explicit retry kind must add
		// exactly one more slot and replace only its own prior generation.
		t.Fatalf("timers = %d; retry = %#v, %t", count, current, ok)
	}
	harness.instance.cancelTimer(relayTimerRetry, 102)
	harness.instance.timersMu.Lock()
	_, remains := harness.instance.timers[relayTimerRetry]
	harness.instance.timersMu.Unlock()
	if remains {
		t.Fatal("matching timer cancellation did not remove retry timer")
	}
}

func TestServerFlowDrainWakesWhenLastFlowLeaves(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	done := make(chan struct{})
	go func() {
		harness.daemon.waitForFlowDrain(time.Hour)
		close(done)
	}()
	harness.daemon.removeFlow(harness.key, harness.instance)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flow drain did not observe last-flow removal")
	}
	harness.instance.close()
}

func TestServerForcedCloseReleasesRegistryAndTimers(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	harness.instance.close()
	harness.daemon.cancelRuntime()
	harness.peer.Close()
	harness.daemon.waitWorkers()
	harness.closed.Store(true)

	entry, ok := harness.daemon.registry.Lookup(harness.key)
	if !ok || entry.State != servercore.RegistryTombstoneReset || harness.daemon.flow(harness.key) != nil {
		t.Fatalf("forced close registry = %#v, %t; active flow = %p", entry, ok, harness.daemon.flow(harness.key))
	}
	harness.instance.timersMu.Lock()
	timers := len(harness.instance.timers)
	harness.instance.timersMu.Unlock()
	if timers != 0 {
		t.Fatalf("timers after forced close = %d", timers)
	}
}

func TestServerWorkerLimitRejectsWithoutBlocking(t *testing.T) {
	daemon := &serverDaemon{workerLimit: 1}
	started := make(chan struct{})
	release := make(chan struct{})
	if !daemon.startWorker(func() {
		close(started)
		<-release
	}) {
		t.Fatal("first worker was rejected below the limit")
	}
	<-started
	if daemon.startWorker(func() {}) {
		t.Fatal("worker above the configured hard limit was accepted")
	}
	close(release)
	daemon.waitWorkers()
}

func TestServerOpenWorkersAreIsolatedFromFlowWorkers(t *testing.T) {
	daemon := &serverDaemon{workerLimit: 1, openWorkerLimit: 1}
	openStarted := make(chan struct{})
	openRelease := make(chan struct{})
	if !daemon.startOpenWorker(func() {
		close(openStarted)
		<-openRelease
	}) {
		t.Fatal("first OPEN worker was rejected")
	}
	<-openStarted
	flowDone := make(chan struct{})
	if !daemon.startWorker(func() { close(flowDone) }) {
		t.Fatal("OPEN worker consumed the Flow worker pool")
	}
	<-flowDone
	if daemon.startOpenWorker(func() {}) {
		t.Fatal("OPEN worker exceeded its independent hard limit")
	}
	close(openRelease)
	daemon.waitOpenWorkers()
	daemon.waitWorkers()
}

func TestServerOpenWaitersAreCoalescedAndBounded(t *testing.T) {
	daemon := &serverDaemon{openWaiters: make(map[openWaiterKey]struct{}, 1), openWaiterLimit: 1}
	daemon.openWaitersCond = sync.NewCond(&daemon.openWaitersMu)
	started := make(chan struct{})
	release := make(chan struct{})
	key := openWaiterKey{sessionGeneration: 1, flowID: protocol.FlowID{1}}
	if !daemon.startOpenWaiter(key, func() {
		close(started)
		<-release
	}) {
		t.Fatal("first OPEN waiter was rejected")
	}
	<-started
	if !daemon.startOpenWaiter(key, func() { t.Error("coalesced waiter executed twice") }) {
		t.Fatal("duplicate OPEN waiter was not coalesced")
	}
	if daemon.startOpenWaiter(openWaiterKey{sessionGeneration: 2, flowID: protocol.FlowID{2}}, func() {}) {
		t.Fatal("OPEN waiter exceeded its hard limit")
	}
	close(release)
	daemon.waitOpenWaiters()
	if daemon.startOpenWaiter(key, func() {}) {
		t.Fatal("OPEN waiter started after shutdown")
	}
}

func TestServerOpenWorkerExhaustionKeepsSharedSessionUsable(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	harness.daemon.openWorkerLimit = 1
	started := make(chan struct{})
	release := make(chan struct{})
	if !harness.daemon.startOpenWorker(func() {
		close(started)
		<-release
	}) {
		t.Fatal("could not occupy OPEN worker")
	}
	<-started
	request := protocol.Open{
		FlowID: protocol.FlowID{7}, OpenToken: protocol.OpenToken{8}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 12},
	}
	if err := harness.daemon.handleOpen(harness.session, request); err != nil {
		t.Fatal(err)
	}
	close(release)
	harness.daemon.waitOpenWorkers()
	results := harness.connection.openResults(request.FlowID)
	if len(results) != 1 || results[0].Result != protocol.OpenResourceLimit {
		t.Fatalf("OPEN exhaustion results = %#v", results)
	}
	if harness.connection.isClosed() {
		t.Fatal("OPEN worker exhaustion closed the shared session")
	}
	if err := harness.daemon.handleJoin(harness.session, protocol.Join{
		FlowID: harness.flowID, Capability: harness.capability,
	}); err != nil {
		t.Fatalf("existing Flow JOIN after OPEN exhaustion: %v", err)
	}
}

func TestServerOpenWaiterExhaustionReturnsResourceLimit(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	connector := newBlockingTargetConnector(true)
	defer connector.closePeer()
	executor, err := servercore.NewTargetDialExecutor(net.DefaultResolver, connector)
	if err != nil {
		t.Fatal(err)
	}
	harness.daemon.targets = executor
	harness.daemon.openWaiterLimit = 1
	first := protocol.Open{
		FlowID: protocol.FlowID{0x41}, OpenToken: protocol.OpenToken{0x42}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 41},
	}
	second := protocol.Open{
		FlowID: protocol.FlowID{0x43}, OpenToken: protocol.OpenToken{0x44}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 43},
	}
	if err := harness.daemon.handleOpen(harness.session, first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connector.started:
	case <-time.After(time.Second):
		t.Fatal("first target dial did not start")
	}
	if err := harness.daemon.handleOpen(harness.session, first); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		harness.daemon.openWaitersMu.Lock()
		defer harness.daemon.openWaitersMu.Unlock()
		return len(harness.daemon.openWaiters) == 1
	}, "first OPEN waiter")
	if err := harness.daemon.handleOpen(harness.session, second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return connector.calls.Load() >= 2 }, "second target dial")
	if err := harness.daemon.handleOpen(harness.session, second); err != nil {
		t.Fatalf("OPEN waiter capacity closed the shared session: %v", err)
	}
	results := harness.connection.openResults(second.FlowID)
	if len(results) != 1 || results[0].Result != protocol.OpenResourceLimit {
		t.Fatalf("OPEN waiter capacity results = %#v", results)
	}
	if harness.connection.isClosed() {
		t.Fatal("OPEN waiter capacity closed the shared session")
	}
}

func TestServerRecoveringFlowLimitResetsOnlyExcessFlow(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	if err := harness.daemon.handleJoin(harness.session, protocol.Join{
		FlowID: harness.flowID, Capability: harness.capability,
	}); err != nil {
		t.Fatal(err)
	}
	harness.daemon.recoveringSlots = make(chan struct{}, 1)
	harness.daemon.recoveringSlots <- struct{}{}
	harness.instance.sessionClosed(harness.session.generation)
	state := harness.instance.snapshot().Flow.Lifecycle.State
	if state != flow.Resetting && state != flow.Reset {
		t.Fatalf("excess recovering Flow state = %v", state)
	}
	harness.instance.mu.Lock()
	reason := harness.instance.lastReason
	harness.instance.mu.Unlock()
	if reason != statusapi.ReasonResourceLimit {
		t.Fatalf("excess recovering Flow reason = %v", reason)
	}
	if len(harness.daemon.recoveringSlots) != 1 {
		t.Fatalf("recovering slots = %d", len(harness.daemon.recoveringSlots))
	}
	<-harness.daemon.recoveringSlots
}

func TestServerRuntimeStatusTracksSessionFlowAndTerminal(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- harness.daemon.statusRepository.Run(ctx, nil) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()

	waitFor(t, time.Second, func() bool {
		snapshot := harness.daemon.statusRepository.Snapshot()
		return len(snapshot.Sessions) == 1 && len(snapshot.Flows) == 1 &&
			snapshot.Flows[0].State == statusapi.FlowAwaitingAttachment
	}, "initial server runtime status")
	if err := harness.daemon.handleJoin(harness.session, protocol.Join{
		FlowID: harness.flowID, Capability: harness.capability,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		snapshot := harness.daemon.statusRepository.Snapshot()
		return len(snapshot.Flows) == 1 && snapshot.Flows[0].State == statusapi.FlowRelaying
	}, "relaying server runtime status")
	harness.instance.sessionClosed(harness.session.generation)
	waitFor(t, time.Second, func() bool {
		snapshot := harness.daemon.statusRepository.Snapshot()
		return len(snapshot.Flows) == 1 && snapshot.Flows[0].State == statusapi.FlowRecovering &&
			snapshot.Resources.RecoveringFlows == 1
	}, "recovering server runtime status")
	harness.instance.close()
	waitFor(t, time.Second, func() bool {
		snapshot := harness.daemon.statusRepository.Snapshot()
		return len(snapshot.Flows) == 0 && len(snapshot.Terminals) == 1 &&
			snapshot.Resources.RecoveringFlows == 0 && snapshot.Resources.Tombstones == 1
	}, "terminal server runtime status")
}

type serverRuntimeHarness struct {
	daemon     *serverDaemon
	instance   *serverFlow
	owner      *flow.Flow
	session    *wireSession
	connection *recordingTransportConnection
	peer       net.Conn
	key        servercore.FlowKey
	flowID     protocol.FlowID
	capability protocol.Capability
	closed     atomic.Bool
}

func newServerRuntimeHarness(t *testing.T) *serverRuntimeHarness {
	t.Helper()
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	dialCtx, cancelDials := context.WithCancel(runtimeCtx)
	registry, err := servercore.NewRegistry(servercore.RegistryLimits{
		Flows: 8, FlowsPerPrincipal: 8, OpeningFlows: 8, TargetDials: 8,
		Tombstones: 16, TombstonesPerPrincipal: 16,
	}, zeroFreeReader{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := servercore.NewRateLimiter(128)
	if err != nil {
		t.Fatal(err)
	}
	statusRepository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 8, Flows: 8})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 1
	statusObserver, err := newRuntimeStatusWithKey(statusRepository, 8, 12345, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	daemon := &serverDaemon{
		configuration: config.Server{
			Transport: config.Transport{Type: "tcp"},
			Limits:    config.ServerLimits{Flows: 8, SessionsPerPrincipal: 4},
			Deadlines: config.Deadlines{Dial: time.Second, DrainCleanup: time.Second},
		},
		runtimeCtx: runtimeCtx, cancelRuntime: cancelRuntime,
		dialCtx: dialCtx, cancelDials: cancelDials,
		registry: registry, limiter: limiter, principalKeys: map[string]auth.Key{"client-01": {1}},
		sessions: make(map[uint64]*wireSession), principalSessions: make(map[string]int),
		flows: make(map[servercore.FlowKey]*serverFlow), waiters: make(map[servercore.FlowKey]*flowWaiter),
		flowChanged:      make(chan struct{}, 1),
		workerLimit:      (servercore.MaxRelayPendingSends + 8) * 8,
		openWorkerLimit:  8,
		openWaiters:      make(map[openWaiterKey]struct{}, 8),
		openWaiterLimit:  8,
		recoveringSlots:  make(chan struct{}, 8),
		statusRepository: statusRepository,
		statusObserver:   statusObserver,
	}
	daemon.openWaitersCond = sync.NewCond(&daemon.openWaitersMu)

	flowID := protocol.FlowID{1}
	request := protocol.Open{
		FlowID: flowID, OpenToken: protocol.OpenToken{2}, DeliveryMode: protocol.DeliveryRedundant,
		PathSelection: protocol.PathNone,
		Target:        protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 9},
	}
	outcome := daemon.registry.HandleOpen("client-01", request)
	if outcome.GenerateCapability == nil {
		t.Fatalf("OPEN capability outcome = %#v", outcome)
	}
	outcome = daemon.registry.GenerateCapability(*outcome.GenerateCapability)
	if outcome.Dial == nil {
		t.Fatalf("OPEN dial outcome = %#v", outcome)
	}
	outcome = daemon.registry.CompleteDial(*outcome.Dial, true)
	if !outcome.Ready || outcome.Owner == nil || outcome.Result.Result != protocol.OpenSuccess {
		t.Fatalf("OPEN completion = %#v", outcome)
	}

	target, peer := net.Pipe()
	key := servercore.FlowKey{PrincipalID: "client-01", FlowID: flowID}
	instance, err := newServerFlow(daemon, key, flowID, request.Target, request.DeliveryMode, request.PathSelection, request.Constraints, outcome.Owner, target)
	if err != nil {
		t.Fatal(err)
	}
	if !daemon.addFlow(key, instance) {
		t.Fatal("could not install server Flow")
	}
	if err := instance.start(); err != nil {
		t.Fatal(err)
	}
	instance.activateStatus()
	connection := newRecordingTransportConnection(t)
	session, err := newWireSession(runtimeCtx, 1, connection)
	if err != nil {
		t.Fatal(err)
	}
	session.principal = "client-01"
	session.status = statusObserver
	if !daemon.addSession(session) {
		t.Fatal("could not install server session")
	}
	return &serverRuntimeHarness{
		daemon: daemon, instance: instance, owner: outcome.Owner, session: session,
		connection: connection, peer: peer, key: key, flowID: flowID, capability: outcome.Result.Capability,
	}
}

func (harness *serverRuntimeHarness) close() {
	if harness == nil || harness.closed.Swap(true) {
		return
	}
	harness.daemon.closeAll()
	harness.daemon.cancelRuntime()
	_ = harness.peer.Close()
	harness.daemon.waitOpenWaiters()
	harness.daemon.waitOpenWorkers()
	harness.daemon.waitWorkers()
}

type zeroFreeReader struct{}

func (zeroFreeReader) Read(target []byte) (int, error) {
	for index := range target {
		target[index] = byte(index + 1)
	}
	return len(target), nil
}

type recordingTransportConnection struct {
	capabilities     transport.Capabilities
	mu               sync.Mutex
	messages         []protocol.Message
	closed           bool
	done             chan struct{}
	closeOnce        sync.Once
	failJoin         atomic.Bool
	blockJoinStart   chan struct{}
	blockJoinRelease chan struct{}
}

func newRecordingTransportConnection(t *testing.T) *recordingTransportConnection {
	t.Helper()
	capabilities, err := transport.NewCapabilities(transport.CapabilitySpec{
		MaxEncodedFrame: protocol.MaxFrameSize, Reliable: true, Ordered: true, HalfClose: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &recordingTransportConnection{capabilities: capabilities, done: make(chan struct{})}
}

func (connection *recordingTransportConnection) Capabilities() transport.Capabilities {
	return connection.capabilities
}

func (connection *recordingTransportConnection) QueueLimits() transport.QueueLimits {
	return transport.V1QueueLimits()
}

func (*recordingTransportConnection) LocalEndpoint() string  { return "127.0.0.1:10000" }
func (*recordingTransportConnection) RemoteEndpoint() string { return "127.0.0.1:20000" }

func (connection *recordingTransportConnection) ReadFrame(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-connection.done:
		return nil, transport.ErrClosed
	}
}

func (connection *recordingTransportConnection) WriteFrame(_ context.Context, request transport.WriteRequest) error {
	_, message, err := protocol.DecodeEncodedFrame(request.Encoded)
	if err != nil {
		return err
	}
	connection.mu.Lock()
	var blockStart, blockRelease chan struct{}
	if _, ok := message.(protocol.JoinResult); ok && connection.blockJoinStart != nil {
		blockStart, blockRelease = connection.blockJoinStart, connection.blockJoinRelease
		connection.blockJoinStart = nil
		connection.blockJoinRelease = nil
	}
	connection.mu.Unlock()
	if blockStart != nil {
		close(blockStart)
		<-blockRelease
	}
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.closed {
		return transport.ErrClosed
	}
	if result, ok := message.(protocol.JoinResult); ok && connection.failJoin.Load() {
		_ = result
		return errTestJoinWrite
	}
	connection.messages = append(connection.messages, message)
	return nil
}

func (*recordingTransportConnection) CloseWrite() error { return nil }

func (connection *recordingTransportConnection) Close() error {
	connection.closeOnce.Do(func() {
		connection.mu.Lock()
		connection.closed = true
		connection.mu.Unlock()
		close(connection.done)
	})
	return nil
}

func (connection *recordingTransportConnection) joinSuccesses() int {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	count := 0
	for _, message := range connection.messages {
		if result, ok := message.(protocol.JoinResult); ok && result.Result == protocol.JoinSuccess {
			count++
		}
	}
	return count
}

func (connection *recordingTransportConnection) lastOpenResult() (protocol.OpenResult, bool) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	for index := len(connection.messages) - 1; index >= 0; index-- {
		if result, ok := connection.messages[index].(protocol.OpenResult); ok {
			return result, true
		}
	}
	return protocol.OpenResult{}, false
}

func (connection *recordingTransportConnection) openResults(flowID protocol.FlowID) []protocol.OpenResult {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	var results []protocol.OpenResult
	for _, message := range connection.messages {
		if result, ok := message.(protocol.OpenResult); ok && result.FlowID == flowID {
			results = append(results, result)
		}
	}
	return results
}

func (connection *recordingTransportConnection) isClosed() bool {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.closed
}

type blockingTargetConnector struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	succeed bool
	calls   atomic.Int32
	peerMu  sync.Mutex
	peer    net.Conn
}

func newBlockingTargetConnector(succeed bool) *blockingTargetConnector {
	return &blockingTargetConnector{started: make(chan struct{}), release: make(chan struct{}), succeed: succeed}
}

func (connector *blockingTargetConnector) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	if connector.calls.Add(1) == 1 {
		close(connector.started)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-connector.release:
	}
	if !connector.succeed {
		return nil, errors.New("test target dial failed")
	}
	connection, peer := net.Pipe()
	connector.peerMu.Lock()
	connector.peer = peer
	connector.peerMu.Unlock()
	return connection, nil
}

func (connector *blockingTargetConnector) unblock() {
	connector.once.Do(func() { close(connector.release) })
}

func (connector *blockingTargetConnector) closePeer() {
	connector.unblock()
	connector.peerMu.Lock()
	if connector.peer != nil {
		_ = connector.peer.Close()
	}
	connector.peerMu.Unlock()
}
