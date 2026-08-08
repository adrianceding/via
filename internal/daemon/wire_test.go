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

	serverResult := make(chan error, 1)
	go func() { serverResult <- authenticateServer(context.Background(), server, challenges, verifier) }()
	if err := authenticateClient(context.Background(), client, "edge-1", key, bytes.NewReader(make([]byte, auth.NonceSize))); err != nil {
		t.Fatalf("authenticateClient() error = %v", err)
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("authenticateServer() error = %v", err)
	}
	if client.connectionID == (auth.CorrelationID{}) || client.connectionID != server.connectionID {
		t.Fatalf("connection IDs = %q / %q", client.connectionID, server.connectionID)
	}
}

func TestAuthenticateFailureDoesNotPublishConnectionID(t *testing.T) {
	client, server := newAuthenticationSessionPair(t)
	challenges, err := auth.NewChallengeGenerator(bytes.NewReader(make([]byte, 48)))
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(map[string]auth.Key{"edge-1": {1}}, auth.Key{9})

	serverResult := make(chan error, 1)
	go func() { serverResult <- authenticateServer(context.Background(), server, challenges, verifier) }()
	if err := authenticateClient(context.Background(), client, "edge-1", auth.Key{2}, bytes.NewReader(make([]byte, auth.NonceSize))); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("authenticateClient() error = %v", err)
	}
	if err := <-serverResult; !errors.Is(err, ErrAuthentication) {
		t.Fatalf("authenticateServer() error = %v", err)
	}
	if client.connectionID != (auth.CorrelationID{}) || server.connectionID != (auth.CorrelationID{}) {
		t.Fatalf("failed connection IDs = %q / %q", client.connectionID, server.connectionID)
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

func TestWireProbeRateAndGenerationAreBounded(t *testing.T) {
	session, err := newWireSession(context.Background(), 1, blockingWireConnection{})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(100, 0)
	first, ok, expired := session.startProbe(start)
	if !ok || expired || first.Token != 1 {
		t.Fatalf("first probe = %#v, %t, expired=%t", first, ok, expired)
	}
	if _, ok, _ := session.startProbe(start.Add(time.Second - 1)); ok {
		t.Fatal("probe rate limit accepted an early probe")
	}
	if _, ok, expired := session.startProbe(start.Add(probeInterval)); ok || expired {
		t.Fatal("second probe replaced an in-flight sample")
	}
	rtt, ok := session.completeProbe(first.Token, start.Add(1050*time.Millisecond))
	if !ok || rtt != 1050*time.Millisecond {
		t.Fatalf("first probe RTT = %v, %t", rtt, ok)
	}
	second, ok, expired := session.startProbe(start.Add(2 * probeInterval))
	if !ok || expired || second.Token != 2 {
		t.Fatalf("second probe = %#v, %t", second, ok)
	}
	rtt, ok = session.completeProbe(second.Token, start.Add(2100*time.Millisecond))
	if !ok || rtt != 100*time.Millisecond {
		t.Fatalf("probe RTT = %v, %t", rtt, ok)
	}
	if got, want := session.smoothedProbeRTT(), 931250*time.Microsecond; got != want {
		t.Fatalf("smoothed probe RTT = %v, want %v", got, want)
	}
	if _, ok := session.completeProbe(second.Token, start.Add(2200*time.Millisecond)); ok {
		t.Fatal("duplicate probe acknowledgement updated quality")
	}
	third, ok, expired := session.startProbe(start.Add(3 * probeInterval))
	if !ok || expired || third.Token != 3 {
		t.Fatalf("third probe = %#v, %t", third, ok)
	}
	if _, ok, expired := session.startProbe(start.Add(3*probeInterval + probeTimeout - time.Nanosecond)); ok || expired {
		t.Fatal("in-flight probe was replaced before its timeout")
	}
	fourth, ok, expired := session.startProbe(start.Add(3*probeInterval + probeTimeout))
	if !ok || !expired || fourth.Token != 4 {
		t.Fatalf("timed-out probe replacement = %#v, %t, expired=%t", fourth, ok, expired)
	}
	if _, ok := session.completeProbe(third.Token, start.Add(3*probeInterval+probeTimeout+50*time.Millisecond)); ok {
		t.Fatal("timed-out probe acknowledgement updated quality")
	}
	if _, ok := session.completeProbe(fourth.Token, start.Add(3*probeInterval+probeTimeout+100*time.Millisecond)); !ok {
		t.Fatal("replacement probe acknowledgement was ignored")
	}
	session.probeMu.Lock()
	session.nextProbe = ^uint64(0)
	session.probeMu.Unlock()
	if _, ok, _ := session.startProbe(start.Add(3*probeInterval + probeTimeout + probeInterval)); ok {
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
}

func TestWireProbePublishesSessionQualitySnapshot(t *testing.T) {
	session, err := newWireSession(context.Background(), 1, blockingWireConnection{})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(200, 0)
	probe, ok, expired := session.startProbe(start)
	if !ok || expired {
		t.Fatalf("probe = %#v, ok=%t, expired=%t", probe, ok, expired)
	}
	replacement, ok, expired := session.startProbe(start.Add(probeTimeout))
	if !ok || !expired {
		t.Fatalf("probe timeout = %#v, ok=%t expired=%t", replacement, ok, expired)
	}
	if snapshot := session.qualitySnapshot(); snapshot.StallPenalty != probeTimeout {
		t.Fatalf("stalled session quality = %#v", snapshot)
	}
	if _, ok := session.completeProbe(probe.Token, start.Add(probeTimeout+25*time.Millisecond)); ok {
		t.Fatal("timed-out probe acknowledgement was accepted")
	}
	latest, ok, expired := session.startProbe(start.Add(2 * probeTimeout))
	if !ok || !expired {
		t.Fatalf("latest probe = %#v, ok=%t, expired=%t", latest, ok, expired)
	}
	rtt, ok := session.completeProbe(latest.Token, start.Add(2*probeTimeout+25*time.Millisecond))
	if !ok || rtt != 25*time.Millisecond {
		t.Fatalf("replacement RTT = %v, ok=%t", rtt, ok)
	}
	snapshot := session.qualitySnapshot()
	if snapshot.SRTT != 25*time.Millisecond || snapshot.StallPenalty != 0 || snapshot.ProbeSamples != 1 {
		t.Fatalf("recovered session quality = %#v", snapshot)
	}
}

type blockingWireConnection struct{}

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
