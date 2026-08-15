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
	if !firstRequest.capacityEligible || !secondRequest.capacityEligible || thirdRequest.capacityEligible {
		t.Fatalf("capacity eligibility = first=%t second=%t third=%t", firstRequest.capacityEligible, secondRequest.capacityEligible, thirdRequest.capacityEligible)
	}
	writes := connection.writesSnapshot()
	if len(writes) != 4 || writes[0].Class != transport.FrameData || writes[1].Class != transport.FrameControl || writes[2].Encoded[0] != 2 || writes[3].Encoded[0] != 1 {
		t.Fatalf("write order = %#v", writes)
	}
}

func TestSessionRuntimeUsesConnectionQueueLimits(t *testing.T) {
	tests := []struct {
		name         string
		limits       transport.QueueLimits
		encodedBytes int
		dataCount    int
		controlFull  bool
	}{
		{
			name: "frame limit",
			limits: transport.QueueLimits{
				MaxFrames: 4, MaxBytes: 1 << 20, ReservedControlFrames: 1, ReservedControlBytes: 16 << 10,
			},
			encodedBytes: 1024, dataCount: 3, controlFull: true,
		},
		{
			name: "byte limit",
			limits: transport.QueueLimits{
				MaxFrames: 8, MaxBytes: 200000, ReservedControlFrames: 2, ReservedControlBytes: 10000,
			},
			encodedBytes: 60000, dataCount: 3,
		},
		{
			name: "expanded configured queue",
			limits: transport.QueueLimits{
				MaxFrames: 1024, MaxBytes: 4 << 20, ReservedControlFrames: 32, ReservedControlBytes: 32 << 10,
			},
			encodedBytes: protocol.MaxFrameSize, dataCount: 63,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := newRuntimeTestConnectionWithLimits(test.limits)
			runtime, err := newSessionRuntime(context.Background(), connection, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.close(transport.ErrClosed)

			requests := make([]*sessionRuntimeRequest, 0, test.dataCount+1)
			first, err := runtime.admit(context.Background(), runtimeRequestDataWithSize(1, test.encodedBytes), protocol.FlowID{1}, 1, 1, uint64(test.encodedBytes))
			if err != nil {
				t.Fatal(err)
			}
			requests = append(requests, first)
			connection.waitEntered(t)
			for index := 1; index < test.dataCount; index++ {
				request, err := runtime.admit(context.Background(), runtimeRequestDataWithSize(byte(index+1), test.encodedBytes), protocol.FlowID{1}, uint64(index+1), 1, uint64(test.encodedBytes))
				if err != nil {
					t.Fatalf("DATA admission %d: %v", index+1, err)
				}
				requests = append(requests, request)
			}
			if _, err := runtime.admit(context.Background(), runtimeRequestDataWithSize(byte(test.dataCount+1), test.encodedBytes), protocol.FlowID{1}, uint64(test.dataCount+1), 1, uint64(test.encodedBytes)); !errors.Is(err, transport.ErrQueueFull) {
				t.Fatalf("overflow DATA error = %v, want %v", err, transport.ErrQueueFull)
			}

			control, err := runtime.admit(context.Background(), transport.WriteRequest{Class: transport.FrameControl, Encoded: []byte{protocol.HeaderSize}}, protocol.FlowID{}, 0, 0, 0)
			if err != nil {
				t.Fatalf("control admission after DATA budget: %v", err)
			}
			if test.controlFull {
				if _, err := runtime.admit(context.Background(), transport.WriteRequest{Class: transport.FrameControl, Encoded: []byte{protocol.HeaderSize}}, protocol.FlowID{}, 0, 0, 0); !errors.Is(err, transport.ErrQueueFull) {
					t.Fatalf("control overflow error = %v, want %v", err, transport.ErrQueueFull)
				}
			}

			connection.release()
			for _, request := range requests {
				if err := runtime.wait(context.Background(), request); err != nil {
					t.Fatal(err)
				}
			}
			if err := runtime.wait(context.Background(), control); err != nil {
				t.Fatal(err)
			}
			if snapshot := runtime.snapshot(); snapshot.QueuedFrames != 0 || snapshot.QueuedEncodedBytes != 0 || snapshot.InFlightFrames != 0 || snapshot.InFlightEncoded != 0 {
				t.Fatalf("drained runtime snapshot = %#v", snapshot)
			}
		})
	}
}

func TestSessionRuntimeSetConnectionUpdatesQueueLimits(t *testing.T) {
	initialLimits := transport.QueueLimits{
		MaxFrames: 4, MaxBytes: 1 << 20, ReservedControlFrames: 1, ReservedControlBytes: 16 << 10,
	}
	replacementLimits := transport.QueueLimits{
		MaxFrames: 6, MaxBytes: 1 << 20, ReservedControlFrames: 1, ReservedControlBytes: 16 << 10,
	}
	initialConnection := newRuntimeTestConnectionWithLimits(initialLimits)
	replacementConnection := newRuntimeTestConnectionWithLimits(replacementLimits)
	runtime, err := newSessionRuntime(context.Background(), initialConnection, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	if err := runtime.setConnection(replacementConnection); err != nil {
		t.Fatal(err)
	}

	requests := make([]*sessionRuntimeRequest, 0, 5)
	for index := 0; index < 5; index++ {
		request, err := runtime.admit(context.Background(), runtimeRequestDataWithSize(byte(index+1), 1024), protocol.FlowID{1}, uint64(index+1), 1, 1024)
		if err != nil {
			t.Fatalf("DATA admission %d: %v", index+1, err)
		}
		requests = append(requests, request)
		if index == 0 {
			replacementConnection.waitEntered(t)
		}
	}
	if _, err := runtime.admit(context.Background(), runtimeRequestDataWithSize(6, 1024), protocol.FlowID{1}, 6, 1, 1024); !errors.Is(err, transport.ErrQueueFull) {
		t.Fatalf("overflow DATA error = %v, want %v", err, transport.ErrQueueFull)
	}

	replacementConnection.release()
	for _, request := range requests {
		if err := runtime.wait(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSessionRuntimeRejectsInvalidConnectionQueueLimits(t *testing.T) {
	connection := newRuntimeTestConnectionWithLimits(transport.QueueLimits{})
	if _, err := newSessionRuntime(context.Background(), connection, nil); !errors.Is(err, ErrWireProtocol) {
		t.Fatalf("invalid queue limits error = %v, want %v", err, ErrWireProtocol)
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
	if snapshot.ScheduledData != 7 || snapshot.WrittenData != 7 || snapshot.QueuedDataPayload != 0 || snapshot.InFlightData != 7 {
		t.Fatalf("data payload accounting = %#v", snapshot)
	}
	runtime.observeFlowDataCredit(protocol.FlowID{1}, 7, time.Now(), time.Now(), false)
	if snapshot := runtime.snapshot(); snapshot.InFlightData != 0 {
		t.Fatalf("acknowledged data payload accounting = %#v", snapshot)
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
	completedAt := runtime.snapshot().Quality
	if completedAt.QueuedBytes != 0 || completedAt.InFlightBytes != 12 {
		t.Fatalf("unacknowledged shared data load = %#v", completedAt)
	}
	runtime.observeFlowDataCredit(protocol.FlowID{1}, 5, time.Now(), time.Now(), false)
	runtime.observeFlowDataCredit(protocol.FlowID{2}, 7, time.Now(), time.Now(), false)
	if snapshot := runtime.snapshot(); snapshot.Quality.QueuedBytes != 0 || snapshot.Quality.InFlightBytes != 0 {
		t.Fatalf("acknowledged shared data load = %#v", snapshot.Quality)
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
	runtime.observeDataCredit(32<<10, start, start.Add(100*time.Millisecond), true)
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 0 || snapshot.EligibleAckedData != 32<<10 {
		t.Fatalf("partial credit window = %#v", snapshot)
	}

	now = start.Add(200 * time.Millisecond)
	runtime.observeDataCredit(16<<10, start.Add(120*time.Millisecond), now, true)
	runtime.observeDataCredit(16<<10, start.Add(130*time.Millisecond), now, true)
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 0 || snapshot.EligibleAckedData != 64<<10 {
		t.Fatalf("credit after baseline = %#v", snapshot)
	}
	now = start.Add(350 * time.Millisecond)
	runtime.observeDataCredit(32<<10, start.Add(220*time.Millisecond), now, true)
	snapshot := runtime.snapshot()
	if snapshot.Quality.DataSamples != 1 || snapshot.Quality.SRTT != 0 || snapshot.EligibleAckedData != 96<<10 {
		t.Fatalf("byte threshold snapshot = %#v", snapshot)
	}
	wantCapacity := float64(64<<10) / dataCapacityWindowTime.Seconds()
	if snapshot.Quality.CapacityBytesSec != wantCapacity {
		t.Fatalf("capacity = %v, want %v", snapshot.Quality.CapacityBytesSec, wantCapacity)
	}

	now = start.Add(time.Second)
	runtime.observeDataCredit(1024, start.Add(500*time.Millisecond), now, true)
	now = start.Add(2 * time.Second)
	runtime.observeDataCredit(1, start.Add(1100*time.Millisecond), now, true)
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 1 || snapshot.EligibleAckedData != 96<<10+1025 {
		t.Fatalf("low-volume credit produced a capacity sample = %#v", snapshot)
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
	runtime.observeDataCredit(64<<10, now, now.Add(-time.Millisecond), true)
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

	runtime.observeDataCredit(32<<10, now, now.Add(100*time.Millisecond), true)
	now = now.Add(10 * time.Second)
	runtime.observeDataCredit(32<<10, now, now.Add(100*time.Millisecond), true)
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 0 {
		t.Fatalf("idle gap completed stale DATA window = %#v", snapshot)
	}
	now = now.Add(100 * time.Millisecond)
	runtime.observeDataCredit(32<<10, now, now.Add(100*time.Millisecond), true)
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 0 {
		t.Fatalf("first credit after new baseline completed DATA window = %#v", snapshot)
	}
	now = now.Add(150 * time.Millisecond)
	runtime.observeDataCredit(32<<10, now, now.Add(100*time.Millisecond), true)
	snapshot := runtime.snapshot()
	if snapshot.Quality.DataSamples != 1 {
		t.Fatalf("new continuous DATA window = %#v", snapshot)
	}
	wantCapacity := float64(64<<10) / dataCapacityWindowTime.Seconds()
	if snapshot.Quality.CapacityBytesSec != wantCapacity {
		t.Fatalf("capacity after idle gap = %v, want %v", snapshot.Quality.CapacityBytesSec, wantCapacity)
	}
}

func TestSessionRuntimeSamplesCompleteDataWindowAtThreshold(t *testing.T) {
	connection := newRuntimeTestConnection()
	start := time.Unix(800, 0)
	runtime, err := newSessionRuntimeWithClock(context.Background(), connection, nil, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)

	runtime.observeDataCredit(1, start, start.Add(100*time.Millisecond), true)
	runtime.observeDataCredit(48<<10, start.Add(110*time.Millisecond), start.Add(350*time.Millisecond), true)
	runtime.observeDataCredit(32<<10, start.Add(120*time.Millisecond), start.Add(350*time.Millisecond), true)
	snapshot := runtime.snapshot()
	if snapshot.Quality.DataSamples != 1 || snapshot.EligibleAckedData != 80<<10+1 {
		t.Fatalf("complete ACK window = %#v", snapshot)
	}
	wantCapacity := float64(80<<10) / dataCapacityWindowTime.Seconds()
	if snapshot.Quality.CapacityBytesSec != wantCapacity {
		t.Fatalf("capacity before idle gap = %v, want %v", snapshot.Quality.CapacityBytesSec, wantCapacity)
	}
}

func TestSessionRuntimeWaitsForMinimumDurationBeforeCapacitySample(t *testing.T) {
	connection := newRuntimeTestConnection()
	start := time.Unix(825, 0)
	runtime, err := newSessionRuntimeWithClock(context.Background(), connection, nil, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)

	runtime.observeDataCredit(1, start, start, true)
	runtime.observeDataCredit(64<<10, start.Add(500*time.Microsecond), start.Add(time.Millisecond), true)
	if snapshot := runtime.snapshot(); snapshot.Quality.DataSamples != 0 {
		t.Fatalf("compressed ACK burst produced a capacity sample = %#v", snapshot)
	}

	runtime.observeDataCredit(64<<10, start.Add(200*time.Millisecond), start.Add(dataCapacityWindowTime), true)
	snapshot := runtime.snapshot()
	if snapshot.Quality.DataSamples != 1 {
		t.Fatalf("complete duration window did not produce a capacity sample = %#v", snapshot)
	}
	wantCapacity := float64(128<<10) / dataCapacityWindowTime.Seconds()
	if snapshot.Quality.CapacityBytesSec != wantCapacity {
		t.Fatalf("capacity after minimum duration = %v, want %v", snapshot.Quality.CapacityBytesSec, wantCapacity)
	}
}

func TestSessionRuntimeRejectsCapacityCreditWithoutSendPressure(t *testing.T) {
	connection := newRuntimeTestConnection()
	start := time.Unix(850, 0)
	runtime, err := newSessionRuntimeWithClock(context.Background(), connection, nil, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)

	runtime.observeDataCredit(32<<10, start, start.Add(100*time.Millisecond), true)
	runtime.observeDataCredit(64<<10, start.Add(110*time.Millisecond), start.Add(200*time.Millisecond), false)
	runtime.observeDataCredit(64<<10, start.Add(210*time.Millisecond), start.Add(300*time.Millisecond), true)
	snapshot := runtime.snapshot()
	if snapshot.Quality.DataSamples != 0 || snapshot.EligibleAckedData != 160<<10 {
		t.Fatalf("credit without send pressure = %#v", snapshot)
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

func TestSessionRuntimeAlwaysPublishesProbeQuality(t *testing.T) {
	now := time.Unix(1_500, 0).UTC()
	runtime, err := newSessionRuntimeWithClock(context.Background(), newRuntimeTestConnection(), nil, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.close(transport.ErrClosed)
	var snapshots []sessionRuntimeSnapshot
	runtime.setSnapshotObserver(func(snapshot sessionRuntimeSnapshot) {
		snapshots = append(snapshots, snapshot)
	})

	runtime.observeProbe(20 * time.Millisecond)
	now = now.Add(snapshotNotifyInterval / 2)
	runtime.observeProbe(30 * time.Millisecond)
	if got := len(snapshots); got != 3 {
		t.Fatalf("probe notifications = %d, want 3", got)
	}
	latest := snapshots[len(snapshots)-1].Quality
	if latest.ProbeSamples != 2 || latest.SRTT == 0 {
		t.Fatalf("latest probe quality = %#v", latest)
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
	limits    transport.QueueLimits
	writes    []transport.WriteRequest
	entered   chan struct{}
	releaseCh chan struct{}
	once      sync.Once
}

func newRuntimeTestConnection() *runtimeTestConnection {
	return newRuntimeTestConnectionWithLimits(transport.V1QueueLimits())
}

func newRuntimeTestConnectionWithLimits(limits transport.QueueLimits) *runtimeTestConnection {
	return &runtimeTestConnection{limits: limits, entered: make(chan struct{}), releaseCh: make(chan struct{})}
}

func (connection *runtimeTestConnection) Capabilities() transport.Capabilities {
	capabilities, _ := transport.NewCapabilities(transport.CapabilitySpec{MaxEncodedFrame: protocol.MaxFrameSize})
	return capabilities
}
func (connection *runtimeTestConnection) QueueLimits() transport.QueueLimits {
	return connection.limits
}
func (*runtimeTestConnection) LocalEndpoint() string  { return "127.0.0.1:1" }
func (*runtimeTestConnection) RemoteEndpoint() string { return "127.0.0.1:2" }
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

func runtimeRequestDataWithSize(marker byte, encodedBytes int) transport.WriteRequest {
	encoded := make([]byte, encodedBytes)
	encoded[0] = marker
	return transport.WriteRequest{Class: transport.FrameData, Encoded: encoded}
}
