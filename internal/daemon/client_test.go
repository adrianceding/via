package daemon

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	clientcore "github.com/adrianceding/via/internal/client"
	"github.com/adrianceding/via/internal/config"
	"github.com/adrianceding/via/internal/flow"
	pathcore "github.com/adrianceding/via/internal/path"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/socks5"
	"github.com/adrianceding/via/internal/transport"
)

func TestClientRelayNeedsSessionRefresh(t *testing.T) {
	tests := []struct {
		kind clientcore.ApplicationRelayEventKind
		want bool
	}{
		{clientcore.ApplicationRelayApplyFlowActions, true},
		{clientcore.ApplicationRelayPublishAttachment, true},
		{clientcore.ApplicationRelayReadResult, true},
		{clientcore.ApplicationRelaySendResult, true},
		{clientcore.ApplicationRelayRetryDeadline, true},
		{clientcore.ApplicationRelayRemoteMessage, false},
		{clientcore.ApplicationRelayWriteResult, false},
		{clientcore.ApplicationRelaySendAdmitted, false},
		{clientcore.ApplicationRelaySetSessionQuality, false},
	}
	for _, test := range tests {
		if got := clientRelayNeedsSessionRefresh(test.kind); got != test.want {
			t.Fatalf("clientRelayNeedsSessionRefresh(%v) = %v, want %v", test.kind, got, test.want)
		}
	}
}

func TestClientFlowEventBackpressureDoesNotCancelFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	instance := &clientFlow{ctx: ctx, cancel: cancel, events: make(chan any, 1)}
	instance.events <- "sentinel"

	want := "event"
	done := make(chan struct{})
	go func() {
		instance.emit(want)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("full flow event queue did not apply backpressure")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-ctx.Done():
		t.Fatal("full flow event queue cancelled the flow")
	default:
	}
	if got := <-instance.events; got != "sentinel" {
		t.Fatalf("queued event = %#v, want sentinel", got)
	}
	select {
	case got := <-instance.events:
		if got != want {
			t.Fatalf("backpressured event = %#v, want %#v", got, want)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("backpressured event was not delivered")
	}
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("event sender did not return after queue space became available")
	}
}

func TestClientFlowRemoteOverflowDoesNotBlockSharedSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	instance := &clientFlow{ctx: ctx, cancel: cancel, remoteEvents: make(chan clientFlowRemote, 1)}
	instance.remoteEvents <- clientFlowRemote{}

	done := make(chan struct{})
	go func() {
		instance.emitRemote(clientFlowRemote{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("full remote event queue blocked the shared session reader")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("remote queue overflow did not cancel only the overloaded flow")
	}
	if got := protocol.ResetReason(instance.cancelCode.Load()); got != protocol.ResetResourceLimit {
		t.Fatalf("overflow reset reason = %d, want %d", got, protocol.ResetResourceLimit)
	}
}

func TestClientFlowDefersRemoteMessagesUntilAttachmentPublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flowID := protocol.FlowID{1}
	attachment := flow.AttachmentKey{SessionGeneration: 7, AttachmentGeneration: 9}
	machine := flow.NewFlowWithWindows(1024, 1024)
	relay, err := clientcore.NewApplicationRelay(flowID, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, machine)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayStart}); err != nil {
		t.Fatal(err)
	}
	coordinator := pendingOpenCoordinator(t, flowID, attachment.SessionGeneration)
	instance := &clientFlow{
		ctx: ctx, cancel: cancel, flowID: flowID, relay: relay, openJoin: coordinator,
		host:        &clientDaemon{configuration: config.Client{Limits: config.ClientLimits{FlowReceiveWindowBytes: 1024}}},
		relayTimers: make(map[relayTimerKey]*time.Timer),
	}
	defer func() {
		for _, timer := range instance.relayTimers {
			timer.Stop()
		}
	}()
	data := protocol.Data{FlowID: flowID, Offset: 0, Bytes: []byte("banner")}
	instance.handleRemote(clientFlowRemote{message: data, sessionGeneration: attachment.SessionGeneration})
	if len(instance.deferredRemote) != 1 || instance.deferredBytes != uint64(len(data.Bytes)) {
		t.Fatalf("deferred remote = %#v, bytes %d", instance.deferredRemote, instance.deferredBytes)
	}

	if _, err := relay.Handle(clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelayReserveAttachment, Generation: 11, Attachment: attachment,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelayPublishAttachment, Generation: 11, Attachment: attachment,
	}); err != nil {
		t.Fatal(err)
	}
	instance.replayDeferredRemote(attachment)
	instance.drainOwnedEvents()
	if len(instance.deferredRemote) != 0 || instance.deferredBytes != 0 {
		t.Fatalf("deferred remote after publication = %#v, bytes %d", instance.deferredRemote, instance.deferredBytes)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Rx.BufferedBytes != uint64(len(data.Bytes)) {
		t.Fatalf("replayed receive snapshot = %#v", snapshot.Flow.Rx)
	}
}

func TestClientFlowDeferredRemoteMessagesAreBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	instance := &clientFlow{
		ctx: ctx, cancel: cancel, relay: &clientcore.ApplicationRelay{},
		openJoin: pendingOpenCoordinator(t, protocol.FlowID{1}, 7),
		host:     &clientDaemon{configuration: config.Client{Limits: config.ClientLimits{FlowReceiveWindowBytes: 4}}},
	}
	instance.deferRemote(clientFlowRemote{
		message: protocol.Data{FlowID: protocol.FlowID{1}, Bytes: []byte("12345")}, sessionGeneration: 7,
	})
	select {
	case <-ctx.Done():
	default:
		t.Fatal("deferred remote overflow did not cancel the flow")
	}
	if got := protocol.ResetReason(instance.cancelCode.Load()); got != protocol.ResetResourceLimit {
		t.Fatalf("overflow reset reason = %d, want %d", got, protocol.ResetResourceLimit)
	}
}

func pendingOpenCoordinator(t *testing.T, flowID protocol.FlowID, sessionGeneration uint64) *clientcore.OpenJoinCoordinator {
	t.Helper()
	coordinator, err := clientcore.NewOpenJoinCoordinator(clientcore.OpenJoinSpec{
		Target:       protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 80},
		DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	actions, err := coordinator.Handle(clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinStart})
	if err != nil || len(actions) == 0 {
		t.Fatalf("start actions = %#v, %v", actions, err)
	}
	identityGeneration := actions[0].Generation
	if _, err := coordinator.Handle(clientcore.OpenJoinEvent{
		Kind: clientcore.OpenJoinIdentityGenerated, Generation: identityGeneration,
		FlowID: flowID, OpenToken: protocol.OpenToken{1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Handle(clientcore.OpenJoinEvent{
		Kind: clientcore.OpenJoinSessionReady, SessionGeneration: sessionGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := coordinator.PendingAttempt(sessionGeneration, clientcore.OpenJoinAttemptOpen); !ok {
		t.Fatal("coordinator did not start pending OPEN")
	}
	return coordinator
}

func TestClientFlowInitialOpenHeadStartUsesBoundedSessionRTT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host := &clientDaemon{sessions: make(map[uint64]*wireSession)}
	instance := &clientFlow{host: host}
	if got := instance.initialOpenHeadStart(1); got != minimumInitialOpenHeadStart {
		t.Fatalf("missing session head start = %s", got)
	}

	session, err := newWireSession(ctx, 1, &clientTestTransportConnection{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	host.sessions[1] = session
	session.probeSRTT = 100 * time.Millisecond
	if got := instance.initialOpenHeadStart(1); got != 100*time.Millisecond {
		t.Fatalf("measured session head start = %s", got)
	}
	session.probeSRTT = time.Second
	if got := instance.initialOpenHeadStart(1); got != maximumInitialOpenHeadStart {
		t.Fatalf("bounded session head start = %s", got)
	}
}

func TestClientFlowBlockedPreferredOpenDoesNotDelayFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	preferred := &gate11BlockingConnection{started: make(chan struct{}), release: make(chan struct{})}
	fallback := &clientOpenWriteConnection{written: make(chan struct{}, 1)}
	preferredSession, err := newWireSession(ctx, 1, preferred)
	if err != nil {
		t.Fatal(err)
	}
	fallbackSession, err := newWireSession(ctx, 2, fallback)
	if err != nil {
		preferredSession.close()
		t.Fatal(err)
	}
	host := &clientDaemon{
		runtimeCtx: ctx, poolEvents: make(chan poolEvent, 2),
		sessions: map[uint64]*wireSession{1: preferredSession, 2: fallbackSession},
	}
	instance := &clientFlow{ctx: ctx, host: host}
	defer func() {
		close(preferred.release)
		cancel()
		preferredSession.close()
		fallbackSession.close()
		host.wg.Wait()
	}()

	open := protocol.Open{
		FlowID: protocol.FlowID{1}, OpenToken: protocol.OpenToken{1}, DeliveryMode: protocol.DeliveryAdaptive,
		PathSelection: protocol.PathFastest, Target: protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 80},
	}
	instance.sendOrderedOpens([]clientcore.OpenJoinAction{
		{Kind: clientcore.OpenJoinActionSendOpen, SessionGeneration: 1, Open: open},
		{Kind: clientcore.OpenJoinActionSendOpen, SessionGeneration: 2, Open: open},
	})
	select {
	case <-preferred.started:
	case <-time.After(time.Second):
		t.Fatal("preferred OPEN write did not start")
	}
	select {
	case <-fallback.written:
	case <-time.After(maximumInitialOpenHeadStart + 250*time.Millisecond):
		t.Fatal("fallback OPEN waited for the preferred write to finish")
	}
}

func TestClientFlowConsumesOwnedTimerRegistrations(t *testing.T) {
	openTimer := time.NewTimer(time.Hour)
	relayTimer := time.NewTimer(time.Hour)
	defer openTimer.Stop()
	defer relayTimer.Stop()
	openKey := openJoinTimerKey{kind: openJoinTimerOpenRetry, generation: 7}
	relayKey := relayTimerKey{kind: relayTimerRetry, generation: 9}
	instance := &clientFlow{
		openJoinTimers: map[openJoinTimerKey]*time.Timer{openKey: openTimer},
		relayTimers:    map[relayTimerKey]*time.Timer{relayKey: relayTimer},
	}

	instance.consumeOpenJoinTimer(openKey)
	instance.consumeRelayTimer(relayKey)
	if len(instance.openJoinTimers) != 0 || len(instance.relayTimers) != 0 {
		t.Fatalf("timer registrations remain: open=%d relay=%d", len(instance.openJoinTimers), len(instance.relayTimers))
	}
}

func TestClientDaemonClosesStaleAuthenticationResult(t *testing.T) {
	manager, err := clientcore.NewSessionManager(clientcore.SessionManagerConfig{
		DesiredSessions: 1,
		AuthInProgress:  1,
		RemoteEndpoint:  "127.0.0.1:9443",
		QueueLimits:     transport.V1QueueLimits(),
		PathNamespace:   clientcore.PathGroupNamespace{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := pathcore.Candidate{InterfaceIndex: 1, InterfaceName: "lo", LocalAddress: netip.MustParseAddr("127.0.0.1")}
	actions, err := manager.Handle(clientcore.SessionManagerEvent{Kind: clientcore.SessionPathsChanged, Candidates: []pathcore.Candidate{candidate}})
	if err != nil || len(actions) != 1 || actions[0].Kind != clientcore.SessionActionDial {
		t.Fatalf("initial actions = %#v, %v", actions, err)
	}
	generation := actions[0].Generation
	if _, err := manager.Handle(clientcore.SessionManagerEvent{Kind: clientcore.SessionPathsChanged}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := &clientTestTransportConnection{}
	session, err := newWireSession(ctx, generation, connection)
	if err != nil {
		t.Fatal(err)
	}
	daemon := &clientDaemon{
		runtimeCtx:     ctx,
		sessionManager: manager,
		pending:        make(map[uint64]*wireSession),
		dialCancel:     make(map[uint64]context.CancelFunc),
		backoffs:       make(map[uint64]*time.Timer),
	}
	daemon.handlePoolEvent(poolEvent{wire: session, manager: clientcore.SessionManagerEvent{
		Kind: clientcore.SessionAuthenticationCompleted, Generation: generation, Succeeded: true,
	}})
	if !connection.closed.Load() {
		t.Fatal("stale authenticated connection was not closed")
	}
	if len(daemon.pending) != 0 {
		t.Fatalf("stale pending sessions = %d", len(daemon.pending))
	}
}

func TestClientDaemonWithNoSOCKSConnectionsDoesNotWaitForDrainDeadline(t *testing.T) {
	relayAddress := reserveAddress(t)
	socksAddress := reserveAddress(t)
	configuration := decodeClientForDaemonTest(t, relayAddress, socksAddress)
	daemon, err := newClientDaemon(configuration)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := make(chan error, 1)
	go func() { result <- daemon.run(ctx) }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		daemon.cancelRuntime()
		daemon.closeAll()
		<-result
		t.Fatal("empty client waited for the drain deadline")
	}
}

func TestClientDaemonNotifiesFlowBeforeIdentityIsGenerated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	instance := &clientFlow{ctx: ctx, cancel: cancel, events: make(chan any, 2)}
	daemon := &clientDaemon{actors: map[*clientFlow]struct{}{instance: {}}}

	daemon.notifySessionReady(11)
	daemon.notifySessionLost(11)
	ready, ok := (<-instance.events).(clientFlowSessionReady)
	if !ok || ready.generation != 11 || ready.rtt != 0 {
		t.Fatalf("ready notification = %#v", ready)
	}
	lost, ok := (<-instance.events).(clientFlowSessionLost)
	if !ok || lost.generation != 11 {
		t.Fatalf("lost notification = %#v", lost)
	}
}

func TestClientDaemonAggregatesPathGroupLaneLifecycle(t *testing.T) {
	pathGroupID := protocol.PathGroupID{1}
	daemon := &clientDaemon{
		configuration: config.Client{PrincipalID: "client-01"},
		sessions:      make(map[uint64]*wireSession), pathGroups: make(map[protocol.PathGroupID]*wirePathGroup),
		pathGroupGenerations: make(map[uint64]*wirePathGroup), laneGroups: make(map[uint64]*wirePathGroup),
	}
	newLane := func(generation uint64) *wireSession {
		session, err := newWireSession(context.Background(), generation, &clientTestTransportConnection{})
		if err != nil {
			t.Fatal(err)
		}
		session.principal = "client-01"
		session.pathGroupID = pathGroupID
		return session
	}
	first := newLane(11)
	second := newLane(12)
	pathGeneration, ready, err := daemon.addReadySession(first)
	if err != nil || !ready || pathGeneration != 11 {
		t.Fatalf("first lane = generation %d, ready %t, %v", pathGeneration, ready, err)
	}
	if secondGeneration, secondReady, err := daemon.addReadySession(second); err != nil || secondReady || secondGeneration != pathGeneration {
		t.Fatalf("second lane = generation %d, ready %t, %v", secondGeneration, secondReady, err)
	}
	flowID := protocol.FlowID{2}
	attachment := flow.AttachmentKey{SessionGeneration: pathGeneration, AttachmentGeneration: 3}
	if err := first.reserve(flowID, attachment); err != nil {
		t.Fatal(err)
	}
	if err := first.publish(flowID, attachment); err != nil {
		t.Fatal(err)
	}
	if got, ok := second.attachment(flowID); !ok || got != attachment {
		t.Fatalf("second lane attachment = %#v, %t", got, ok)
	}
	if removed, generation, lost := daemon.removeReadySession(first.generation); removed != first || generation != pathGeneration || lost {
		t.Fatalf("first removal = %#v, generation %d, lost %t", removed, generation, lost)
	}
	if daemon.session(pathGeneration) != second {
		t.Fatal("remaining lane did not become path group representative")
	}
	if removed, generation, lost := daemon.removeReadySession(second.generation); removed != second || generation != pathGeneration || !lost {
		t.Fatalf("last removal = %#v, generation %d, lost %t", removed, generation, lost)
	}
}

func TestClientFlowSessionQualityNotificationIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	instance := &clientFlow{ctx: ctx, cancel: cancel, events: make(chan any, 1)}

	instance.tryEmitSessionQuality(11, 170*time.Millisecond, probeTimeout)
	quality, ok := (<-instance.events).(clientFlowSessionQuality)
	if !ok || quality.generation != 11 || quality.rtt != 170*time.Millisecond || quality.stall != probeTimeout {
		t.Fatalf("quality notification = %#v", quality)
	}
}

func TestClientFlowTerminalQualityNotificationsAreDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	instance := &clientFlow{
		ctx: ctx, cancel: cancel, events: make(chan any, 1), qualityWake: make(chan struct{}, 1),
		pendingSessionQuality: make(map[uint64]clientFlowSessionQuality),
		pendingRelayQuality:   make(map[flow.AttachmentKey]clientcore.ApplicationRelayEvent),
	}
	instance.tryEmitSessionQuality(11, 25*time.Millisecond, 0)
	instance.stopTerminalQuality(flow.Closing)
	instance.tryEmitSessionQuality(11, 50*time.Millisecond, 0)
	instance.tryEmitQuality(clientcore.ApplicationRelayEvent{
		Kind:       clientcore.ApplicationRelaySetSessionQuality,
		Attachment: flow.AttachmentKey{SessionGeneration: 11, AttachmentGeneration: 1},
		Quality:    policy.QualitySnapshot{SRTT: 50 * time.Millisecond},
	})
	if pending := instance.takeQualityEvents(); len(pending) != 0 {
		t.Fatalf("terminal quality notifications = %#v", pending)
	}
	select {
	case <-instance.qualityWake:
	default:
	}
}

func TestClientFlowQualityNotificationCoalescesWhenQueueFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attachment := flow.AttachmentKey{SessionGeneration: 7, AttachmentGeneration: 9}
	instance := &clientFlow{
		ctx: ctx, cancel: cancel, events: make(chan any, 1), qualityWake: make(chan struct{}, 1),
	}
	instance.events <- "sentinel"

	instance.tryEmitSessionQuality(11, 170*time.Millisecond, probeTimeout)
	instance.tryEmitSessionQuality(11, 25*time.Millisecond, 0)
	instance.tryEmitQuality(clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelaySetSessionQuality, Attachment: attachment,
		Quality: policy.QualitySnapshot{SRTT: 170 * time.Millisecond, StallPenalty: probeTimeout},
	})
	instance.tryEmitQuality(clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelaySetSessionQuality, Attachment: attachment,
		Quality: policy.QualitySnapshot{SRTT: 25 * time.Millisecond},
	})

	pending := instance.takeQualityEvents()
	var sessionQuality *clientFlowSessionQuality
	var relayQuality *clientcore.ApplicationRelayEvent
	for _, raw := range pending {
		switch event := raw.(type) {
		case clientFlowSessionQuality:
			copy := event
			sessionQuality = &copy
		case clientcore.ApplicationRelayEvent:
			copy := event
			relayQuality = &copy
		}
	}
	if sessionQuality == nil || sessionQuality.generation != 11 || sessionQuality.rtt != 25*time.Millisecond || sessionQuality.stall != 0 {
		t.Fatalf("coalesced session quality = %#v", sessionQuality)
	}
	if relayQuality == nil || relayQuality.Attachment != attachment || relayQuality.Quality.SRTT != 25*time.Millisecond || relayQuality.Quality.StallPenalty != 0 {
		t.Fatalf("coalesced relay quality = %#v", relayQuality)
	}
}

func TestClientProbeTimeoutPenaltyAndRecoveryReachPublishedFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flowID := protocol.FlowID{1}
	attachment := flow.AttachmentKey{SessionGeneration: 7, AttachmentGeneration: 9}
	instance := &clientFlow{ctx: ctx, cancel: cancel, events: make(chan any, 5)}
	session, err := newWireSession(ctx, attachment.SessionGeneration, &clientTestTransportConnection{})
	if err != nil {
		t.Fatal(err)
	}
	session.attachments[flowID] = attachment
	daemon := &clientDaemon{
		flows:                map[protocol.FlowID]*clientFlow{flowID: instance},
		sessions:             map[uint64]*wireSession{session.generation: session},
		pathGroupGenerations: make(map[uint64]*wirePathGroup), laneGroups: make(map[uint64]*wirePathGroup),
	}

	session.runtime.setStallPenalty(probeTimeout)
	daemon.notifyProbeStallPenalty(session, probeTimeout)
	stalledQuality := receiveClientFlowEvent[clientFlowSessionQuality](t, instance.events)
	penalty := receiveClientFlowEvent[clientcore.ApplicationRelayEvent](t, instance.events)
	if stalledQuality.generation != session.generation || stalledQuality.stall != probeTimeout {
		t.Fatalf("stalled session quality = %#v", stalledQuality)
	}
	if penalty.Kind != clientcore.ApplicationRelaySetSessionQuality || penalty.Attachment != attachment || penalty.Quality.StallPenalty != probeTimeout {
		t.Fatalf("probe penalty event = %#v", penalty)
	}
	session.runtime.observeProbe(25 * time.Millisecond)
	session.runtime.setStallPenalty(0)
	daemon.notifyProbeQuality(session, 25*time.Millisecond)
	recoveredQuality := receiveClientFlowEvent[clientFlowSessionQuality](t, instance.events)
	clear := receiveClientFlowEvent[clientcore.ApplicationRelayEvent](t, instance.events)
	if recoveredQuality.generation != session.generation || recoveredQuality.stall != 0 {
		t.Fatalf("recovered session quality = %#v", recoveredQuality)
	}
	if clear.Kind != clientcore.ApplicationRelaySetSessionQuality || clear.Attachment != attachment ||
		clear.Quality.SRTT != 25*time.Millisecond || clear.Quality.StallPenalty != 0 {
		t.Fatalf("probe quality event = %#v", clear)
	}
}

func receiveClientFlowEvent[T any](t *testing.T, events <-chan any) T {
	t.Helper()
	select {
	case raw := <-events:
		event, ok := raw.(T)
		if !ok {
			t.Fatalf("client flow event type = %T", raw)
		}
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for client flow event")
		var zero T
		return zero
	}
}

func TestClientProbeWithoutInboundProgressReportsConnectionLost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session, err := newWireSession(ctx, 7, &clientTestTransportConnection{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, expired, dead := session.startProbe(time.Now().Add(-probeTimeout)); !ok || expired || dead {
		t.Fatalf("initial probe = ok=%t expired=%t dead=%t", ok, expired, dead)
	}
	daemon := &clientDaemon{runtimeCtx: ctx, poolEvents: make(chan poolEvent, 1)}

	daemon.probeClientSession(session)

	select {
	case event := <-daemon.poolEvents:
		if event.manager.Kind != clientcore.SessionConnectionLost || event.manager.Generation != session.generation {
			t.Fatalf("probe loss event = %#v", event.manager)
		}
	default:
		t.Fatal("half-open client session did not report connection loss")
	}
}

func TestSelectRelayAddressPrefersIPv4RegardlessOfResolverOrder(t *testing.T) {
	ipv4 := netip.MustParseAddr("149.28.139.39")
	ipv6 := netip.MustParseAddr("2001:db8::39")
	if got, err := selectRelayAddress([]netip.Addr{ipv6, ipv4}); err != nil || got != ipv4 {
		t.Fatalf("selected relay address = %v, %v; want IPv4 %v", got, err, ipv4)
	}
}

func TestSelectRelayAddressFallsBackToIPv6(t *testing.T) {
	ipv6 := netip.MustParseAddr("2001:db8::39")
	if got, err := selectRelayAddress([]netip.Addr{netip.IPv6Unspecified(), ipv6}); err != nil || got != ipv6 {
		t.Fatalf("selected relay address = %v, %v; want IPv6 %v", got, err, ipv6)
	}
}

func TestWriteSOCKSReplyArmsFreshBoundedDeadline(t *testing.T) {
	connection := &clientTestNetConnection{}
	before := time.Now()
	if err := writeSOCKSReply(connection, socks5.ReplyGeneralFailure); err != nil {
		t.Fatal(err)
	}
	want, _ := socks5.Reply(socks5.ReplyGeneralFailure)
	if !bytes.Equal(connection.Bytes(), want[:]) {
		t.Fatalf("reply = %v, want %v", connection.Bytes(), want)
	}
	if !connection.writeDeadline.After(before) || connection.writeDeadline.After(before.Add(sendTimeout+time.Second)) {
		t.Fatalf("write deadline = %v", connection.writeDeadline)
	}
}

func TestClientDaemonOpeningLimitIsExact(t *testing.T) {
	daemon := &clientDaemon{openingSlots: make(chan struct{}, 1)}
	if !daemon.acquireOpening() {
		t.Fatal("first opening slot was rejected")
	}
	if daemon.acquireOpening() {
		t.Fatal("opening slot above the limit was accepted")
	}
	daemon.releaseOpening()
	if !daemon.acquireOpening() {
		t.Fatal("released opening slot was not reusable")
	}
	daemon.releaseOpening()
}

func TestClientFlowRecoveringLimitPreservesExistingRecoveringFlow(t *testing.T) {
	host := &clientDaemon{recoveringSlots: make(chan struct{}, 1)}
	first := &clientFlow{host: host}
	second := &clientFlow{host: host}
	if !first.syncRecoveringSlot(flow.Recovering) {
		t.Fatal("first recovering flow was rejected")
	}
	if second.syncRecoveringSlot(flow.Recovering) {
		t.Fatal("recovering flow above the limit was accepted")
	}
	if len(host.recoveringSlots) != 1 || !first.recoveringSlot {
		t.Fatal("existing recovering flow lost its reserved slot")
	}
	if !first.syncRecoveringSlot(flow.Relaying) {
		t.Fatal("leaving recovery failed")
	}
	if !second.syncRecoveringSlot(flow.Recovering) {
		t.Fatal("released recovering slot was not reusable")
	}
	second.syncRecoveringSlot(flow.Resetting)
}

type clientTestTransportConnection struct {
	closed atomic.Bool
}

type clientOpenWriteConnection struct {
	written chan struct{}
}

type clientTestNetConnection struct {
	bytes.Buffer
	writeDeadline time.Time
}

func (*clientTestNetConnection) Read([]byte) (int, error)        { return 0, net.ErrClosed }
func (*clientTestNetConnection) Close() error                    { return nil }
func (*clientTestNetConnection) LocalAddr() net.Addr             { return clientTestAddress("local") }
func (*clientTestNetConnection) RemoteAddr() net.Addr            { return clientTestAddress("remote") }
func (*clientTestNetConnection) SetDeadline(time.Time) error     { return nil }
func (*clientTestNetConnection) SetReadDeadline(time.Time) error { return nil }
func (connection *clientTestNetConnection) SetWriteDeadline(deadline time.Time) error {
	connection.writeDeadline = deadline
	return nil
}

type clientTestAddress string

func (address clientTestAddress) Network() string { return "test" }
func (address clientTestAddress) String() string  { return string(address) }

func (*clientTestTransportConnection) Capabilities() transport.Capabilities {
	capabilities, _ := transport.NewCapabilities(transport.CapabilitySpec{MaxEncodedFrame: protocol.MaxFrameSize})
	return capabilities
}
func (*clientTestTransportConnection) QueueLimits() transport.QueueLimits {
	return transport.V1QueueLimits()
}
func (*clientTestTransportConnection) LocalEndpoint() string  { return "127.0.0.1:1" }
func (*clientTestTransportConnection) RemoteEndpoint() string { return "127.0.0.1:2" }
func (*clientTestTransportConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, net.ErrClosed
}
func (*clientTestTransportConnection) WriteFrame(context.Context, transport.WriteRequest) error {
	return net.ErrClosed
}
func (*clientTestTransportConnection) CloseWrite() error { return nil }
func (connection *clientTestTransportConnection) Close() error {
	connection.closed.Store(true)
	return nil
}

func (*clientOpenWriteConnection) Capabilities() transport.Capabilities {
	capabilities, _ := transport.NewCapabilities(transport.CapabilitySpec{MaxEncodedFrame: protocol.MaxFrameSize})
	return capabilities
}
func (*clientOpenWriteConnection) QueueLimits() transport.QueueLimits {
	return transport.V1QueueLimits()
}
func (*clientOpenWriteConnection) LocalEndpoint() string  { return "127.0.0.1:1" }
func (*clientOpenWriteConnection) RemoteEndpoint() string { return "127.0.0.1:2" }
func (*clientOpenWriteConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, net.ErrClosed
}
func (connection *clientOpenWriteConnection) WriteFrame(context.Context, transport.WriteRequest) error {
	connection.written <- struct{}{}
	return nil
}
func (*clientOpenWriteConnection) CloseWrite() error { return nil }
func (*clientOpenWriteConnection) Close() error      { return nil }
