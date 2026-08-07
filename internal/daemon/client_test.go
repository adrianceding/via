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
	"github.com/adrianceding/via/internal/flow"
	pathcore "github.com/adrianceding/via/internal/path"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/socks5"
	"github.com/adrianceding/via/internal/transport"
)

func TestClientFlowEventOverflowCancelsOnlyFlowWithoutBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	instance := &clientFlow{ctx: ctx, cancel: cancel, events: make(chan any, 1)}
	instance.events <- struct{}{}

	done := make(chan struct{})
	go func() {
		instance.emit(struct{}{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("full flow event queue blocked its caller")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("full flow event queue did not cancel the over-limit flow")
	}
	if got := protocol.ResetReason(instance.cancelCode.Load()); got != protocol.ResetResourceLimit {
		t.Fatalf("overflow reset reason = %d, want %d", got, protocol.ResetResourceLimit)
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
	daemon := &clientDaemon{flows: map[protocol.FlowID]*clientFlow{flowID: instance}}

	session.runtime.setStallPenalty(probeTimeout)
	daemon.notifyProbeStallPenalty(session, probeTimeout)
	stalledQuality := (<-instance.events).(clientFlowSessionQuality)
	penalty := (<-instance.events).(clientcore.ApplicationRelayEvent)
	if stalledQuality.generation != session.generation || stalledQuality.stall != probeTimeout {
		t.Fatalf("stalled session quality = %#v", stalledQuality)
	}
	if penalty.Kind != clientcore.ApplicationRelaySetSessionQuality || penalty.Attachment != attachment || penalty.Quality.StallPenalty != probeTimeout {
		t.Fatalf("probe penalty event = %#v", penalty)
	}
	session.runtime.observeProbe(25 * time.Millisecond)
	session.runtime.setStallPenalty(0)
	daemon.notifyProbeQuality(session, 25*time.Millisecond)
	recoveredQuality := (<-instance.events).(clientFlowSessionQuality)
	clear := (<-instance.events).(clientcore.ApplicationRelayEvent)
	if recoveredQuality.generation != session.generation || recoveredQuality.stall != 0 {
		t.Fatalf("recovered session quality = %#v", recoveredQuality)
	}
	if clear.Kind != clientcore.ApplicationRelaySetSessionQuality || clear.Attachment != attachment ||
		clear.Quality.SRTT != 25*time.Millisecond || clear.Quality.StallPenalty != 0 {
		t.Fatalf("probe quality event = %#v", clear)
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
