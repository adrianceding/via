package daemon

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
	statusapi "github.com/adrianceding/via/internal/status"
	"github.com/adrianceding/via/internal/transport"
)

func TestAuthenticatePublishesSameConnectionIDOnBothSides(t *testing.T) {
	key := auth.Key{1, 2, 3}
	client, server := newAuthenticationSessionPair(t)
	challenges, err := auth.NewChallengeGenerator(bytes.NewReader(make([]byte, 48)))
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(map[string]auth.Key{"edge-1": key}, auth.Key{9})
	pathGroupID := protocol.PathGroupID{1, 2, 3, 4}

	serverResult := make(chan error, 1)
	go func() { serverResult <- authenticateServer(context.Background(), server, challenges, verifier) }()
	if err := authenticateClient(context.Background(), client, "edge-1", pathGroupID, key, bytes.NewReader(make([]byte, auth.NonceSize))); err != nil {
		t.Fatalf("authenticateClient() error = %v", err)
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("authenticateServer() error = %v", err)
	}
	if client.connectionID == (auth.CorrelationID{}) || client.connectionID != server.connectionID {
		t.Fatalf("connection IDs = %q / %q", client.connectionID, server.connectionID)
	}
	if client.pathGroupID != pathGroupID || server.pathGroupID != pathGroupID {
		t.Fatalf("path group IDs = %x / %x", client.pathGroupID, server.pathGroupID)
	}
}

func TestAuthenticateFailureDoesNotPublishConnectionID(t *testing.T) {
	client, server := newAuthenticationSessionPair(t)
	challenges, err := auth.NewChallengeGenerator(bytes.NewReader(make([]byte, 48)))
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(map[string]auth.Key{"edge-1": {1}}, auth.Key{9})
	pathGroupID := protocol.PathGroupID{1, 2, 3, 4}

	serverResult := make(chan error, 1)
	go func() { serverResult <- authenticateServer(context.Background(), server, challenges, verifier) }()
	if err := authenticateClient(context.Background(), client, "edge-1", pathGroupID, auth.Key{2}, bytes.NewReader(make([]byte, auth.NonceSize))); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("authenticateClient() error = %v", err)
	}
	if err := <-serverResult; !errors.Is(err, ErrAuthentication) {
		t.Fatalf("authenticateServer() error = %v", err)
	}
	if client.connectionID != (auth.CorrelationID{}) || server.connectionID != (auth.CorrelationID{}) {
		t.Fatalf("failed connection IDs = %q / %q", client.connectionID, server.connectionID)
	}
	if client.pathGroupID != (protocol.PathGroupID{}) || server.pathGroupID != (protocol.PathGroupID{}) {
		t.Fatalf("failed path group IDs = %x / %x", client.pathGroupID, server.pathGroupID)
	}
}

func newAuthenticationSessionPair(t *testing.T) (*wireSession, *wireSession) {
	t.Helper()
	clientConnection, serverConnection := newFramedConnectionPair()
	client, err := newWireSession(context.Background(), 1, clientConnection)
	if err != nil {
		t.Fatal(err)
	}
	server, err := newWireSession(context.Background(), 1, serverConnection)
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

func TestWireSessionsSharePathGroupAttachments(t *testing.T) {
	first, err := newWireSession(context.Background(), 11, &clientTestTransportConnection{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := newWireSession(context.Background(), 12, &clientTestTransportConnection{})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newWireAttachmentRegistry(101)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.bindAttachmentRegistry(registry); err != nil {
		t.Fatal(err)
	}
	if err := second.bindAttachmentRegistry(registry); err != nil {
		t.Fatal(err)
	}
	flowID := protocol.FlowID{1}
	attachment := flow.AttachmentKey{SessionGeneration: 101, AttachmentGeneration: 7}
	if err := first.reserve(flowID, attachment); err != nil {
		t.Fatal(err)
	}
	if err := first.publish(flowID, attachment); err != nil {
		t.Fatal(err)
	}
	if got, ok := second.attachment(flowID); !ok || got != attachment {
		t.Fatalf("second lane attachment = %#v, %t", got, ok)
	}
	second.release(flowID, attachment)
	if _, ok := first.attachment(flowID); ok {
		t.Fatal("attachment remained after release through second lane")
	}
}

func TestWireSendContextHonorsParentDeadline(t *testing.T) {
	session, err := newWireSession(context.Background(), 1, blockingWireConnection{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- session.sendContext(ctx, protocol.Probe{Token: 1}) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("send error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not stop wire send")
	}
}

func TestWireControlSendWaitsForQueueCapacity(t *testing.T) {
	limits := transport.V1QueueLimits()
	limits.MaxFrames = 2
	limits.ReservedControlFrames = 1
	connection := newRuntimeTestConnectionWithLimits(limits)
	session, err := newWireSession(context.Background(), 1, connection)
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()

	control := transport.WriteRequest{Class: transport.FrameControl, Encoded: []byte{1}}
	if _, err := session.runtime.admit(context.Background(), control, protocol.FlowID{}, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	connection.waitEntered(t)
	if _, err := session.runtime.admit(context.Background(), control, protocol.FlowID{}, 0, 0, 0); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() { result <- session.send(protocol.Probe{Token: 3}) }()
	select {
	case err := <-result:
		t.Fatalf("control send returned before queue capacity was available: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	connection.release()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("control send after capacity returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("control send did not resume after queue capacity became available")
	}
}

func TestWireCloseKeepsSelectedEncodedBorrowedUntilWriteReturns(t *testing.T) {
	connection := newBorrowedWriteConnection()
	defer connection.release()
	session, err := newWireSession(context.Background(), 1, connection)
	if err != nil {
		t.Fatal(err)
	}
	encoded := []byte{7, 8, 9}
	pending, err := session.admitEncodedContextMetadata(context.Background(), transport.FrameData, encoded, protocol.FlowID{1}, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	waitWireSignal(t, connection.writeStarted, "selected WriteFrame start")

	type writeResult struct {
		completedAt time.Time
		err         error
	}
	result := make(chan writeResult, 1)
	go func() {
		completedAt, _, waitErr := pending.wait()
		result <- writeResult{completedAt: completedAt, err: waitErr}
	}()
	closed := make(chan struct{})
	go func() {
		session.close()
		close(closed)
	}()
	waitWireSignal(t, connection.closeCalled, "transport Close")
	select {
	case got := <-result:
		t.Fatalf("selected request completed before WriteFrame returned: %#v", got)
	case <-time.After(20 * time.Millisecond):
	}
	if snapshot := session.runtime.snapshot(); snapshot.InFlightFrames != 1 || snapshot.InFlightEncoded != uint64(len(encoded)) {
		t.Fatalf("closing selected request accounting = %#v", snapshot)
	}

	connection.release()
	var got writeResult
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("selected request did not complete after WriteFrame returned")
	}
	encoded[0] = 99
	if !errors.Is(got.err, transport.ErrClosed) || !got.completedAt.IsZero() {
		t.Fatalf("selected close result = %#v", got)
	}
	waitWireSignal(t, connection.writeReturned, "selected WriteFrame return")
	waitWireSignal(t, closed, "wire session close")
	if marker := <-connection.observedMarker; marker != 7 {
		t.Fatalf("transport observed encoded marker %d, want 7", marker)
	}
}

func TestWireProbeRateAndGenerationAreBounded(t *testing.T) {
	session, err := newWireSession(context.Background(), 1, blockingWireConnection{})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(100, 0)
	first, ok, expired, dead := session.startProbe(start)
	if !ok || expired || dead || first.Token != 1 {
		t.Fatalf("first probe = %#v, %t, expired=%t dead=%t", first, ok, expired, dead)
	}
	if _, ok, expired, dead := session.startProbe(start.Add(time.Second)); ok || expired || dead {
		t.Fatal("second probe replaced an in-flight sample")
	}
	rtt, ok := session.completeProbe(first.Token, start.Add(1050*time.Millisecond))
	if !ok || rtt != 1050*time.Millisecond {
		t.Fatalf("first probe RTT = %v, %t", rtt, ok)
	}
	if _, ok, _, _ := session.startProbe(start.Add(probeInterval - time.Nanosecond)); ok {
		t.Fatal("probe rate limit accepted an early probe")
	}
	second, ok, expired, dead := session.startProbe(start.Add(probeInterval))
	if !ok || expired || dead || second.Token != 2 {
		t.Fatalf("second probe = %#v, %t", second, ok)
	}
	rtt, ok = session.completeProbe(second.Token, start.Add(probeInterval+100*time.Millisecond))
	if !ok || rtt != 100*time.Millisecond {
		t.Fatalf("probe RTT = %v, %t", rtt, ok)
	}
	if got, want := session.smoothedProbeRTT(), 931250*time.Microsecond; got != want {
		t.Fatalf("smoothed probe RTT = %v, want %v", got, want)
	}
	if _, ok := session.completeProbe(second.Token, start.Add(probeInterval+200*time.Millisecond)); ok {
		t.Fatal("duplicate probe acknowledgement updated quality")
	}
	third, ok, expired, dead := session.startProbe(start.Add(3 * probeInterval))
	if !ok || expired || dead || third.Token != 3 {
		t.Fatalf("third probe = %#v, %t", third, ok)
	}
	if _, ok, expired, dead := session.startProbe(start.Add(3*probeInterval + probeTimeout - time.Nanosecond)); ok || expired || dead {
		t.Fatal("in-flight probe was replaced before its timeout")
	}
	session.noteInboundProgress()
	replacement, ok, expired, dead := session.startProbe(start.Add(3*probeInterval + probeTimeout))
	if ok || !expired || dead || replacement.Token != 0 {
		t.Fatalf("timed-out probe = %#v, %t, expired=%t dead=%t", replacement, ok, expired, dead)
	}
	if _, ok := session.completeProbe(third.Token, start.Add(3*probeInterval+probeTimeout+50*time.Millisecond)); ok {
		t.Fatal("timed-out probe acknowledgement updated quality")
	}
	fourth, ok, expired, dead := session.startProbe(start.Add(4 * probeInterval))
	if !ok || expired || dead || fourth.Token != 4 {
		t.Fatalf("replacement probe = %#v, %t, expired=%t dead=%t", fourth, ok, expired, dead)
	}
	if _, ok := session.completeProbe(fourth.Token, start.Add(4*probeInterval+100*time.Millisecond)); !ok {
		t.Fatal("replacement probe acknowledgement was ignored")
	}
	session.probeMu.Lock()
	session.nextProbe = ^uint64(0)
	session.probeMu.Unlock()
	if _, ok, _, _ := session.startProbe(start.Add(5 * probeInterval)); ok {
		t.Fatal("exhausted probe generation wrapped")
	}
}

func TestWireReadCountsReceivedDataPayload(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 7
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)

	left, right := newFramedConnectionPair()
	session, err := newWireSession(context.Background(), 1, left)
	if err != nil {
		t.Fatal(err)
	}
	defer session.cancel()
	session.status = observer

	encoded, err := protocol.EncodeMessage(protocol.Data{FlowID: protocol.FlowID{1}, Offset: 0, Bytes: make([]byte, 512)})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case right.outbound <- encoded:
	default:
		t.Fatal("could not queue encoded DATA frame")
	}

	message, err := session.read(context.Background())
	if err != nil {
		t.Fatalf("read() error = %v", err)
	}
	if _, ok := message.(protocol.Data); !ok {
		t.Fatalf("read() message = %T, want Data", message)
	}
	id := observer.hasher.SessionID(1)
	if got := observer.sessions[id].Quality.ReceivedDataPayloadBytes; got != 512 {
		t.Fatalf("received payload = %d, want 512", got)
	}
	if got := repository.Snapshot().Counters.DataPayloadBytesReceived; got != 512 {
		t.Fatalf("summary received payload = %d, want 512", got)
	}

	// 非 DATA 帧（例如 Probe）不得计入接收字节。
	probe, err := protocol.EncodeMessage(protocol.Probe{Token: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case right.outbound <- probe:
	default:
		t.Fatal("could not queue encoded probe frame")
	}
	if _, err := session.read(context.Background()); err != nil {
		t.Fatalf("read() probe error = %v", err)
	}
	if got := observer.sessions[id].Quality.ReceivedDataPayloadBytes; got != 512 {
		t.Fatalf("probe changed received payload to %d, want 512", got)
	}
	if got := repository.Snapshot().Counters.DataPayloadBytesReceived; got != 512 {
		t.Fatalf("probe changed summary received payload to %d, want 512", got)
	}
}

func TestWireWriteCountsSuccessfulDataPayloadOnly(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 10
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	left, right := newFramedConnectionPair()
	session, err := newWireSession(context.Background(), 1, left)
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	session.status = observer
	encoded, err := protocol.EncodeMessage(protocol.Data{FlowID: protocol.FlowID{1}, Offset: 0, Bytes: make([]byte, 384)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.sendEncodedContextWithCompletion(context.Background(), transport.FrameData, encoded, protocol.FlowID{1}, 1, 1, 384); err != nil {
		t.Fatalf("successful DATA write = %v", err)
	}
	if got := repository.Snapshot().Counters.DataPayloadBytesSent; got != 384 {
		t.Fatalf("summary sent payload = %d, want 384", got)
	}
	if err := session.send(protocol.Probe{Token: 1}); err != nil {
		t.Fatalf("control write = %v", err)
	}
	if got := repository.Snapshot().Counters.DataPayloadBytesSent; got != 384 {
		t.Fatalf("control write changed summary sent payload to %d", got)
	}
	select {
	case <-right.inbound:
	case <-time.After(time.Second):
		t.Fatal("successful DATA frame was not written")
	}
}

func TestWireWriteDoesNotCountFailedDataPayload(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 11
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	session, err := newWireSession(context.Background(), 1, failingWireConnection{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	session.status = observer
	encoded, err := protocol.EncodeMessage(protocol.Data{FlowID: protocol.FlowID{1}, Offset: 0, Bytes: make([]byte, 128)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.sendEncodedContextWithCompletion(context.Background(), transport.FrameData, encoded, protocol.FlowID{1}, 1, 1, 128); err == nil {
		t.Fatal("failed DATA write unexpectedly succeeded")
	}
	if got := repository.Snapshot().Counters.DataPayloadBytesSent; got != 0 {
		t.Fatalf("failed DATA write changed summary sent payload to %d", got)
	}
}

func TestWireProbePublishesSessionQualitySnapshot(t *testing.T) {
	session, err := newWireSession(context.Background(), 1, blockingWireConnection{})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(200, 0)
	probe, ok, expired, dead := session.startProbe(start)
	if !ok || expired || dead {
		t.Fatalf("probe = %#v, ok=%t, expired=%t dead=%t", probe, ok, expired, dead)
	}
	session.noteInboundProgress()
	replacement, ok, expired, dead := session.startProbe(start.Add(probeTimeout))
	if ok || !expired || dead || replacement.Token != 0 {
		t.Fatalf("probe timeout = %#v, ok=%t expired=%t dead=%t", replacement, ok, expired, dead)
	}
	if snapshot := session.qualitySnapshot(); snapshot.StallPenalty != probeTimeout {
		t.Fatalf("stalled session quality = %#v", snapshot)
	}
	if _, ok := session.completeProbe(probe.Token, start.Add(probeTimeout+25*time.Millisecond)); ok {
		t.Fatal("timed-out probe acknowledgement was accepted")
	}
	latest, ok, expired, dead := session.startProbe(start.Add(probeInterval))
	if !ok || expired || dead {
		t.Fatalf("latest probe = %#v, ok=%t, expired=%t dead=%t", latest, ok, expired, dead)
	}
	rtt, ok := session.completeProbe(latest.Token, start.Add(probeInterval+25*time.Millisecond))
	if !ok || rtt != 25*time.Millisecond {
		t.Fatalf("replacement RTT = %v, ok=%t", rtt, ok)
	}
	snapshot := session.qualitySnapshot()
	if snapshot.SRTT != 25*time.Millisecond || snapshot.StallPenalty != 0 || snapshot.ProbeSamples != 1 {
		t.Fatalf("recovered session quality = %#v", snapshot)
	}
}

func TestWireProbeACKCarriesLocalSendCapacity(t *testing.T) {
	now := time.Unix(300, 0)
	session, err := newWireSessionWithClock(context.Background(), 1, blockingWireConnection{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()

	session.runtime.mu.Lock()
	if err := session.runtime.quality.ObserveData(0, 64<<10, time.Second); err != nil {
		session.runtime.mu.Unlock()
		t.Fatal(err)
	}
	session.runtime.mu.Unlock()

	ack := session.probeACK(7)
	if ack.Token != 7 || ack.SendCapacityBytesSec != 64<<10 || ack.SendCapacitySampleAgeNanos != 0 ||
		ack.SendCapacityFreshForNanos != uint64(3*time.Second) {
		t.Fatalf("probe ack = %#v", ack)
	}
}

func TestWirePeerSendCapacityFollowsMatchingProbeACK(t *testing.T) {
	now := time.Unix(400, 0)
	session, err := newWireSessionWithClock(context.Background(), 1, blockingWireConnection{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer session.close()
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 8
	observer, err := newRuntimeStatusWithClock(repository, 1, 1, statusKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	session.setStatusObserver(observer)

	probe, ok, expired, dead := session.startProbe(now)
	if !ok || expired || dead {
		t.Fatalf("probe = %#v, ok=%t expired=%t dead=%t", probe, ok, expired, dead)
	}
	ack := protocol.ProbeACK{
		Token: probe.Token, SendCapacityBytesSec: 8 << 20,
		SendCapacitySampleAgeNanos: uint64(time.Second), SendCapacityFreshForNanos: uint64(3 * time.Second),
	}
	now = now.Add(100 * time.Millisecond)
	if rtt, accepted := session.completeProbeACK(ack, now); !accepted || rtt != 100*time.Millisecond {
		t.Fatalf("complete probe = %v, %t", rtt, accepted)
	}
	fresh := session.peerSendCapacitySnapshot(now.Add(2 * time.Second))
	if !fresh.Measured || !fresh.Fresh || fresh.CapacityBytesSec != 8<<20 || fresh.SampleAge != 3*time.Second {
		t.Fatalf("fresh peer capacity = %#v", fresh)
	}
	statusQuality := observer.sessions[observer.hasher.SessionID(1)].Quality
	if statusQuality.PeerSendCapacityBytesSec != 8<<20 || statusQuality.PeerSendDataSampleExpiresAt == nil {
		t.Fatalf("peer capacity was not published = %#v", statusQuality)
	}
	stale := session.peerSendCapacitySnapshot(now.Add(2*time.Second + time.Nanosecond))
	if !stale.Measured || stale.Fresh || stale.CapacityBytesSec != 8<<20 {
		t.Fatalf("stale peer capacity = %#v", stale)
	}
	if _, accepted := session.completeProbeACK(protocol.ProbeACK{Token: probe.Token}, now.Add(time.Second)); accepted {
		t.Fatal("duplicate probe ack was accepted")
	}
	if after := session.peerSendCapacitySnapshot(now.Add(time.Second)); after.CapacityBytesSec != 8<<20 {
		t.Fatalf("duplicate probe ack changed peer capacity = %#v", after)
	}

	now = now.Add(probeInterval)
	next, ok, expired, dead := session.startProbe(now)
	if !ok || expired || dead {
		t.Fatalf("next probe = %#v, ok=%t expired=%t dead=%t", next, ok, expired, dead)
	}
	now = now.Add(time.Millisecond)
	if _, accepted := session.completeProbeACK(protocol.ProbeACK{Token: next.Token}, now); !accepted {
		t.Fatal("unmeasured probe ack was rejected")
	}
	if cleared := session.peerSendCapacitySnapshot(now); cleared.Measured || cleared.CapacityBytesSec != 0 {
		t.Fatalf("unmeasured peer capacity = %#v", cleared)
	}
}

type blockingWireConnection struct{}

type borrowedWriteConnection struct {
	writeStarted   chan struct{}
	writeReturned  chan struct{}
	closeCalled    chan struct{}
	releaseWrite   chan struct{}
	observedMarker chan byte
	writeOnce      sync.Once
	closeOnce      sync.Once
	releaseOnce    sync.Once
}

func newBorrowedWriteConnection() *borrowedWriteConnection {
	return &borrowedWriteConnection{
		writeStarted:   make(chan struct{}),
		writeReturned:  make(chan struct{}),
		closeCalled:    make(chan struct{}),
		releaseWrite:   make(chan struct{}),
		observedMarker: make(chan byte, 1),
	}
}

func (connection *borrowedWriteConnection) Capabilities() transport.Capabilities {
	return blockingWireConnection{}.Capabilities()
}
func (*borrowedWriteConnection) QueueLimits() transport.QueueLimits {
	return transport.V1QueueLimits()
}
func (*borrowedWriteConnection) LocalEndpoint() string  { return "127.0.0.1:1" }
func (*borrowedWriteConnection) RemoteEndpoint() string { return "127.0.0.1:2" }
func (*borrowedWriteConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, net.ErrClosed
}
func (connection *borrowedWriteConnection) WriteFrame(_ context.Context, request transport.WriteRequest) error {
	connection.writeOnce.Do(func() { close(connection.writeStarted) })
	<-connection.releaseWrite
	connection.observedMarker <- request.Encoded[0]
	close(connection.writeReturned)
	return transport.ErrClosed
}
func (*borrowedWriteConnection) CloseWrite() error { return nil }
func (connection *borrowedWriteConnection) Close() error {
	connection.closeOnce.Do(func() { close(connection.closeCalled) })
	return nil
}
func (connection *borrowedWriteConnection) release() {
	connection.releaseOnce.Do(func() { close(connection.releaseWrite) })
}

func waitWireSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type failingWireConnection struct{}

func (failingWireConnection) Capabilities() transport.Capabilities {
	return blockingWireConnection{}.Capabilities()
}
func (failingWireConnection) QueueLimits() transport.QueueLimits { return transport.V1QueueLimits() }
func (failingWireConnection) LocalEndpoint() string              { return "127.0.0.1:1" }
func (failingWireConnection) RemoteEndpoint() string             { return "127.0.0.1:2" }
func (failingWireConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, net.ErrClosed
}
func (failingWireConnection) WriteFrame(context.Context, transport.WriteRequest) error {
	return errors.New("write failed")
}
func (failingWireConnection) CloseWrite() error { return nil }
func (failingWireConnection) Close() error      { return nil }

func (blockingWireConnection) Capabilities() transport.Capabilities {
	capabilities, err := transport.NewCapabilities(transport.CapabilitySpec{MaxEncodedFrame: protocol.MaxFrameSize})
	if err != nil {
		panic(err)
	}
	return capabilities
}
func (blockingWireConnection) QueueLimits() transport.QueueLimits { return transport.V1QueueLimits() }
func (blockingWireConnection) LocalEndpoint() string              { return "127.0.0.1:1" }
func (blockingWireConnection) RemoteEndpoint() string             { return "127.0.0.1:2" }
func (blockingWireConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, net.ErrClosed
}
func (blockingWireConnection) WriteFrame(ctx context.Context, _ transport.WriteRequest) error {
	<-ctx.Done()
	return ctx.Err()
}
func (blockingWireConnection) CloseWrite() error { return nil }
func (blockingWireConnection) Close() error      { return nil }

type framedConnection struct {
	inbound   chan []byte
	outbound  chan []byte
	done      chan struct{}
	closeOnce *sync.Once
}

func newFramedConnectionPair() (*framedConnection, *framedConnection) {
	leftToRight := make(chan []byte, 4)
	rightToLeft := make(chan []byte, 4)
	done := make(chan struct{})
	closeOnce := &sync.Once{}
	return &framedConnection{inbound: rightToLeft, outbound: leftToRight, done: done, closeOnce: closeOnce},
		&framedConnection{inbound: leftToRight, outbound: rightToLeft, done: done, closeOnce: closeOnce}
}

func (*framedConnection) Capabilities() transport.Capabilities {
	capabilities, err := transport.NewCapabilities(transport.CapabilitySpec{
		MaxEncodedFrame: protocol.MaxFrameSize, Reliable: true, Ordered: true,
	})
	if err != nil {
		panic(err)
	}
	return capabilities
}
func (*framedConnection) QueueLimits() transport.QueueLimits { return transport.V1QueueLimits() }
func (*framedConnection) LocalEndpoint() string              { return "127.0.0.1:1" }
func (*framedConnection) RemoteEndpoint() string             { return "127.0.0.1:2" }
func (connection *framedConnection) ReadFrame(ctx context.Context) ([]byte, error) {
	select {
	case encoded := <-connection.inbound:
		return encoded, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-connection.done:
		return nil, net.ErrClosed
	}
}
func (connection *framedConnection) WriteFrame(ctx context.Context, request transport.WriteRequest) error {
	select {
	case connection.outbound <- append([]byte(nil), request.Encoded...):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-connection.done:
		return net.ErrClosed
	}
}
func (*framedConnection) CloseWrite() error { return nil }
func (connection *framedConnection) Close() error {
	connection.closeOnce.Do(func() { close(connection.done) })
	return nil
}
