package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

func TestSessionRuntimeControlPriorityAndFlowRoundRobin(t *testing.T) {
	connection := newRuntimeTestConnection()
	runtime, err := newSessionRuntime(context.Background(), connection, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	first := runtimeRequestData(1, 1, []byte("a"))
	firstRequest, err := runtime.admit(context.Background(), first, protocol.FlowID{1}, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	connection.waitEntered(t)
	if snapshot := runtime.snapshot(); snapshot.QueuedFrames != 0 || snapshot.QueuedEncodedBytes != 0 ||
		snapshot.InFlightFrames != 1 || snapshot.InFlightEncoded != uint64(len(first.Encoded)) {
		t.Fatalf("selected request accounting = %#v", snapshot)
	}
	second := runtimeRequestData(2, 1, []byte("b"))
	third := runtimeRequestData(1, 2, []byte("c"))
	control := transport.WriteRequest{Class: transport.FrameControl, Encoded: []byte{protocol.HeaderSize}}
	secondRequest, err := runtime.admit(context.Background(), second, protocol.FlowID{2}, 2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	thirdRequest, err := runtime.admit(context.Background(), third, protocol.FlowID{1}, 3, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	controlRequest, err := runtime.admit(context.Background(), control, protocol.FlowID{}, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	connection.release()
	if err := runtime.wait(context.Background(), firstRequest); err != nil {
		t.Fatal(err)
	}
	if err := runtime.wait(context.Background(), controlRequest); err != nil {
		t.Fatal(err)
	}
	if err := runtime.wait(context.Background(), secondRequest); err != nil {
		t.Fatal(err)
	}
	if err := runtime.wait(context.Background(), thirdRequest); err != nil {
		t.Fatal(err)
	}
	writes := connection.writesSnapshot()
	if len(writes) != 4 || writes[0].Class != transport.FrameData || writes[1].Class != transport.FrameControl || writes[2].Encoded[0] != 2 || writes[3].Encoded[0] != 1 {
		t.Fatalf("write order = %#v", writes)
	}
}

func TestSessionRuntimeCancelAndCloseCompleteRequestsOnce(t *testing.T) {
	connection := newRuntimeTestConnection()
	runtime, err := newSessionRuntime(context.Background(), connection, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := runtime.admit(context.Background(), runtimeRequestData(1, 1, []byte("a")), protocol.FlowID{1}, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	connection.waitEntered(t)
	second, err := runtime.admit(context.Background(), runtimeRequestData(1, 2, []byte("b")), protocol.FlowID{1}, 2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	canceled, err := runtime.admit(ctx, runtimeRequestData(2, 1, []byte("c")), protocol.FlowID{2}, 3, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := runtime.wait(ctx, canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v", err)
	}
	runtime.close(transport.ErrClosed)
	if err := runtime.wait(context.Background(), first); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("selected request error = %v", err)
	}
	if err := runtime.wait(context.Background(), second); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("queued request error = %v", err)
	}
	if snapshot := runtime.snapshot(); snapshot.QueuedFrames != 0 || snapshot.QueuedEncodedBytes != 0 || snapshot.InFlightFrames != 0 || snapshot.ActiveDataFlows != 0 {
		t.Fatalf("closed runtime snapshot = %#v", snapshot)
	}
	runtime.close(errors.New("late close"))
}

func TestSessionRuntimeCallerCancelAfterSelectionReportsRealWriteResult(t *testing.T) {
	connection := newRuntimeTestConnection()
	runtime, err := newSessionRuntime(context.Background(), connection, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	ctx, cancel := context.WithCancel(context.Background())
	request, err := runtime.admit(ctx, runtimeRequestData(1, 1, []byte("a")), protocol.FlowID{1}, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	connection.waitEntered(t)
	cancel()
	connection.release()
	if err := runtime.wait(ctx, request); err != nil {
		t.Fatalf("selected write result = %v, want success", err)
	}
	if snapshot := runtime.snapshot(); snapshot.Closed {
		t.Fatal("caller cancellation closed the shared runtime")
	}
	other, err := runtime.admit(context.Background(), runtimeRequestData(2, 1, []byte("b")), protocol.FlowID{2}, 2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.wait(context.Background(), other); err != nil {
		t.Fatalf("other Flow write result = %v", err)
	}
	if writes := connection.writesSnapshot(); len(writes) != 2 || writes[0].Encoded[0] != 1 || writes[1].Encoded[0] != 2 {
		t.Fatalf("writes after selected cancellation = %#v", writes)
	}
}

func TestSessionRuntimeTracksDataPayloadSeparatelyFromEncodedBytes(t *testing.T) {
	connection := newRuntimeTestConnection()
	runtime, err := newSessionRuntime(context.Background(), connection, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	request, err := runtime.admit(context.Background(), runtimeRequestData(1, 1, []byte("payload")), protocol.FlowID{1}, 7, 3, 7)
	if err != nil {
		t.Fatal(err)
	}
	connection.release()
	if err := runtime.wait(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.snapshot()
	if snapshot.ScheduledData != 7 || snapshot.WrittenData != 7 || snapshot.QueuedDataPayload != 0 || snapshot.InFlightData != 0 {
		t.Fatalf("data payload accounting = %#v", snapshot)
	}
}

func TestSessionRuntimePublishesSharedDataLoadInQualitySnapshot(t *testing.T) {
	connection := newRuntimeTestConnection()
	runtime, err := newSessionRuntime(context.Background(), connection, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	first, err := runtime.admit(context.Background(), runtimeRequestData(1, 1, []byte("first")), protocol.FlowID{1}, 1, 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	connection.waitEntered(t)
	second, err := runtime.admit(context.Background(), runtimeRequestData(2, 1, []byte("second")), protocol.FlowID{2}, 2, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.snapshot()
	if snapshot.Quality.QueuedBytes != 7 || snapshot.Quality.InFlightBytes != 5 {
		t.Fatalf("shared data load = %#v, want queued=7 in_flight=5", snapshot.Quality)
	}
	connection.release()
	if err := runtime.wait(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := runtime.wait(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if snapshot := runtime.snapshot(); snapshot.Quality.QueuedBytes != 0 || snapshot.Quality.InFlightBytes != 0 {
		t.Fatalf("cleared shared data load = %#v", snapshot.Quality)
	}
}

func TestSessionRuntimeAggregatesEligibleDataCreditsAtBoundaries(t *testing.T) {
	connection := newRuntimeTestConnection()
	now := time.Unix(500, 0)
	runtime, err := newSessionRuntimeWithClock(context.Background(), connection, nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	start := now
	runtime.observeDataCredit(32<<10, start, start.Add(100*time.Millisecond))
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 0 || snapshot.EligibleAckedData != 32<<10 {
		t.Fatalf("partial credit window = %#v", snapshot)
	}

	now = start.Add(200 * time.Millisecond)
	runtime.observeDataCredit(32<<10, start.Add(120*time.Millisecond), now)
	snapshot := runtime.snapshot()
	if snapshot.Quality.DataSamples != 1 || snapshot.Quality.SRTT != 0 || snapshot.EligibleAckedData != 64<<10 {
		t.Fatalf("byte threshold snapshot = %#v", snapshot)
	}
	wantCapacity := float64(64<<10) / (200 * time.Millisecond).Seconds()
	if snapshot.Quality.CapacityBytesSec != wantCapacity {
		t.Fatalf("capacity = %v, want %v", snapshot.Quality.CapacityBytesSec, wantCapacity)
	}

	now = start.Add(time.Second)
	runtime.observeDataCredit(1024, now, now.Add(10*time.Millisecond))
	now = start.Add(time.Second + dataCapacityWindowTime)
	runtime.observeDataCredit(1, now, now.Add(time.Millisecond))
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 2 {
		t.Fatalf("time threshold snapshot = %#v", snapshot)
	}
}

func TestSessionRuntimeRejectsOutOfOrderDataCredit(t *testing.T) {
	connection := newRuntimeTestConnection()
	now := time.Unix(700, 0)
	runtime, err := newSessionRuntimeWithClock(context.Background(), connection, nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	runtime.observeProbe(20 * time.Millisecond)
	runtime.observeDataCredit(64<<10, now, now.Add(-time.Millisecond))
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 0 || snapshot.EligibleAckedData != 0 {
		t.Fatalf("out-of-order credit accepted = %#v", snapshot)
	}
}

func TestSessionRuntimeDropsIncompleteDataWindowAcrossIdleGap(t *testing.T) {
	connection := newRuntimeTestConnection()
	now := time.Unix(750, 0)
	runtime, err := newSessionRuntimeWithClock(context.Background(), connection, nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)

	runtime.observeDataCredit(32<<10, now, now.Add(100*time.Millisecond))
	now = now.Add(10 * time.Second)
	runtime.observeDataCredit(32<<10, now, now.Add(100*time.Millisecond))
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 0 {
		t.Fatalf("idle gap completed stale DATA window = %#v", snapshot)
	}
	now = now.Add(100 * time.Millisecond)
	runtime.observeDataCredit(32<<10, now, now.Add(100*time.Millisecond))
	snapshot := runtime.snapshot()
	if snapshot.Quality.DataSamples != 1 {
		t.Fatalf("new continuous DATA window = %#v", snapshot)
	}
	wantCapacity := float64(64<<10) / (200 * time.Millisecond).Seconds()
	if snapshot.Quality.CapacityBytesSec != wantCapacity {
		t.Fatalf("capacity after idle gap = %v, want %v", snapshot.Quality.CapacityBytesSec, wantCapacity)
	}
}

func TestSessionRuntimeThrottlesDiagnosticNotifications(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	connection := newRuntimeTestConnection()
	runtime, err := newSessionRuntimeWithClock(context.Background(), connection, nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	var notifications int32
	runtime.setSnapshotObserver(func(sessionRuntimeSnapshot) { notifications++ })

	first, err := runtime.admit(context.Background(), runtimeRequestData(1, 1, []byte("a")), protocol.FlowID{1}, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	connection.waitEntered(t)
	if got := notifications; got != 2 {
		t.Fatalf("initial notifications = %d, want 2", got)
	}

	now = now.Add(50 * time.Millisecond)
	second, err := runtime.admit(context.Background(), runtimeRequestData(2, 1, []byte("b")), protocol.FlowID{2}, 2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := notifications; got != 2 {
		t.Fatalf("throttled notifications = %d, want 2", got)
	}

	now = now.Add(50 * time.Millisecond)
	third, err := runtime.admit(context.Background(), runtimeRequestData(3, 1, []byte("c")), protocol.FlowID{3}, 3, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := notifications; got != 3 {
		t.Fatalf("interval notifications = %d, want 3", got)
	}

	connection.release()
	for _, request := range []*sessionRuntimeRequest{first, second, third} {
		if err := runtime.wait(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	if got := notifications; got != 3 {
		t.Fatalf("post-write notifications = %d, want 3", got)
	}

	runtime.close(transport.ErrClosed)
	if got := notifications; got != 4 {
		t.Fatalf("terminal notifications = %d, want 4", got)
	}
}

func TestSessionRuntimeTerminalCloseAlwaysNotifiesObserver(t *testing.T) {
	now := time.Unix(2_000, 0).UTC()
	connection := newRuntimeTestConnection()
	runtime, err := newSessionRuntimeWithClock(context.Background(), connection, nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	var closed bool
	runtime.setSnapshotObserver(func(snapshot sessionRuntimeSnapshot) {
		if snapshot.Closed {
			closed = true
		}
	})
	runtime.close(transport.ErrClosed)
	if !closed {
		t.Fatal("terminal close did not notify observer")
	}
}

type runtimeTestConnection struct {
	mu        sync.Mutex
	writes    []transport.WriteRequest
	entered   chan struct{}
	releaseCh chan struct{}
	once      sync.Once
}

func newRuntimeTestConnection() *runtimeTestConnection {
	return &runtimeTestConnection{entered: make(chan struct{}), releaseCh: make(chan struct{})}
}

func (connection *runtimeTestConnection) Capabilities() transport.Capabilities {
	capabilities, _ := transport.NewCapabilities(transport.CapabilitySpec{MaxEncodedFrame: protocol.MaxFrameSize})
	return capabilities
}
func (*runtimeTestConnection) QueueLimits() transport.QueueLimits { return transport.V1QueueLimits() }
func (*runtimeTestConnection) LocalEndpoint() string              { return "127.0.0.1:1" }
func (*runtimeTestConnection) RemoteEndpoint() string             { return "127.0.0.1:2" }
func (*runtimeTestConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, transport.ErrClosed
}
func (connection *runtimeTestConnection) WriteFrame(ctx context.Context, request transport.WriteRequest) error {
	connection.mu.Lock()
	connection.writes = append(connection.writes, transport.WriteRequest{Class: request.Class, Encoded: append([]byte(nil), request.Encoded...)})
	connection.mu.Unlock()
	connection.once.Do(func() { close(connection.entered) })
	select {
	case <-connection.releaseCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*runtimeTestConnection) CloseWrite() error { return nil }
func (*runtimeTestConnection) Close() error      { return nil }
func (connection *runtimeTestConnection) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-connection.entered:
	case <-time.After(time.Second):
		t.Fatal("runtime write did not start")
	}
}
func (connection *runtimeTestConnection) release() { close(connection.releaseCh) }
func (connection *runtimeTestConnection) writesSnapshot() []transport.WriteRequest {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	result := make([]transport.WriteRequest, len(connection.writes))
	copy(result, connection.writes)
	return result
}

func runtimeRequestData(marker byte, _ byte, payload []byte) transport.WriteRequest {
	return transport.WriteRequest{Class: transport.FrameData, Encoded: []byte{marker, byte(len(payload))}}
}
