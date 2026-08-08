package daemon

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
	servercore "github.com/adrianceding/via/internal/server"
	"github.com/adrianceding/via/internal/transport"
)

func TestServerJoinTerminalRaceExplicitlyRetiresSession(t *testing.T) {
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

	harness.connection.mu.Lock()
	messages := append([]protocol.Message(nil), harness.connection.messages...)
	harness.connection.mu.Unlock()
	if len(messages) < 2 {
		t.Fatalf("terminal JOIN messages = %#v", messages)
	}
	join, joinOK := messages[len(messages)-2].(protocol.JoinResult)
	reset, resetOK := messages[len(messages)-1].(protocol.Reset)
	if !joinOK || join.Result != protocol.JoinSuccess || !resetOK ||
		reset.FlowID != harness.flowID || reset.Reason != protocol.ResetCancelled {
		t.Fatalf("terminal JOIN tail = %#v", messages[len(messages)-2:])
	}
	if !harness.connection.isClosed() || harness.session.ctx.Err() == nil {
		t.Fatal("terminal JOIN race left the transport session usable")
	}
}

func TestServerFlowTerminalDoesNotCancelSelectedSessionSend(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	if err := harness.daemon.handleJoin(harness.session, protocol.Join{
		FlowID: harness.flowID, Capability: harness.capability,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		return harness.instance.snapshot().PendingSends == 0
	}, "initial server Flow sends")
	attachment, ok := harness.session.attachment(harness.flowID)
	if !ok {
		t.Fatal("JOIN did not publish the server attachment")
	}

	connection := &gate11BlockingConnection{started: make(chan struct{}), release: make(chan struct{})}
	harness.session.connection = connection
	done := make(chan struct{})
	go func() {
		harness.instance.executeSend(servercore.RelayAction{
			Kind: servercore.RelayActionSendMessage, Generation: 999,
			Attachment: attachment,
			Message:    protocol.ACK{FlowID: harness.flowID},
		})
		close(done)
	}()
	select {
	case <-connection.started:
	case <-time.After(time.Second):
		t.Fatal("server Flow send did not block in the transport")
	}

	harness.instance.close()
	select {
	case <-done:
		t.Fatal("terminal server Flow canceled an already selected session write")
	default:
	}
	close(connection.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("selected session send did not report its real write result")
	}
	if harness.session.ctx.Err() != nil {
		t.Fatal("terminating one Flow cancelled the shared transport session")
	}
	if connection.closed.Load() {
		t.Fatal("terminating one Flow closed the shared transport connection")
	}
}

func TestServerProbeTimeoutPenaltyAndRecoveryUpdatePublishedFlow(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	if err := harness.daemon.handleJoin(harness.session, protocol.Join{
		FlowID: harness.flowID, Capability: harness.capability,
	}); err != nil {
		t.Fatal(err)
	}
	attachment, ok := harness.session.attachment(harness.flowID)
	if !ok {
		t.Fatal("JOIN did not publish the server attachment")
	}

	harness.session.runtime.setStallPenalty(probeTimeout)
	harness.daemon.notifyServerProbeStallPenalty(harness.session, probeTimeout)
	snapshot := harness.instance.snapshot().Policy
	if len(snapshot.Attachments) != 1 || snapshot.Attachments[0].Attachment != attachment || snapshot.Attachments[0].Quality.StallPenalty != probeTimeout {
		t.Fatalf("server probe penalty snapshot = %#v", snapshot)
	}
	harness.session.runtime.observeProbe(25 * time.Millisecond)
	harness.session.runtime.setStallPenalty(0)
	harness.daemon.notifyServerProbeQuality(harness.session, 25*time.Millisecond)
	snapshot = harness.instance.snapshot().Policy
	if len(snapshot.Attachments) != 1 || snapshot.Attachments[0].Quality.StallPenalty != 0 || snapshot.Attachments[0].Quality.SRTT != 25*time.Millisecond {
		t.Fatalf("server recovered probe snapshot = %#v", snapshot)
	}
}

func TestServerTerminalFlowIgnoresProbeQuality(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	if err := harness.daemon.handleJoin(harness.session, protocol.Join{
		FlowID: harness.flowID, Capability: harness.capability,
	}); err != nil {
		t.Fatal(err)
	}
	before := harness.instance.snapshot().Policy
	harness.instance.qualityStopped.Store(true)
	harness.session.runtime.observeProbe(75 * time.Millisecond)
	harness.daemon.notifyServerProbeQuality(harness.session, 75*time.Millisecond)
	after := harness.instance.snapshot().Policy
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("terminal probe quality changed policy: before=%#v after=%#v", before, after)
	}
}

func TestServerShutdownWaitsForRunningTimerCallback(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()

	harness.instance.mu.Lock()
	harness.instance.armTimer(relayTimerRetry, 777, time.Millisecond)
	waitFor(t, time.Second, func() bool {
		harness.instance.timersMu.Lock()
		_, active := harness.instance.timers[relayTimerRetry]
		harness.instance.timersMu.Unlock()
		return !active
	}, "running server timer callback")
	harness.instance.stopTimers()

	done := make(chan struct{})
	go func() {
		harness.daemon.timerCallbacks.Wait()
		close(done)
	}()
	select {
	case <-done:
		harness.instance.mu.Unlock()
		t.Fatal("timer callback wait returned while callback was still running")
	case <-time.After(20 * time.Millisecond):
	}
	harness.instance.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timer callback wait did not finish after callback returned")
	}
}

type gate11BlockingConnection struct {
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	closed    atomic.Bool
}

func (*gate11BlockingConnection) Capabilities() transport.Capabilities {
	capabilities, err := transport.NewCapabilities(transport.CapabilitySpec{MaxEncodedFrame: protocol.MaxFrameSize})
	if err != nil {
		panic(err)
	}
	return capabilities
}
func (*gate11BlockingConnection) QueueLimits() transport.QueueLimits {
	return transport.V1QueueLimits()
}
func (*gate11BlockingConnection) LocalEndpoint() string  { return "127.0.0.1:1" }
func (*gate11BlockingConnection) RemoteEndpoint() string { return "127.0.0.1:2" }
func (*gate11BlockingConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, transport.ErrClosed
}
func (connection *gate11BlockingConnection) WriteFrame(ctx context.Context, _ transport.WriteRequest) error {
	connection.startOnce.Do(func() { close(connection.started) })
	select {
	case <-connection.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*gate11BlockingConnection) CloseWrite() error { return nil }
func (connection *gate11BlockingConnection) Close() error {
	connection.closed.Store(true)
	return nil
}
