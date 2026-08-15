package daemon

import (
	"context"
	"encoding/json"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/auth"
	clientcore "github.com/adrianceding/via/internal/client"
	"github.com/adrianceding/via/internal/flow"
	pathcore "github.com/adrianceding/via/internal/path"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	statusapi "github.com/adrianceding/via/internal/status"
)

func TestRuntimeStatusMergesAuthoritativeSessionObservation(t *testing.T) {
	now := time.Unix(1_000, 0).UTC()
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 1
	observer, err := newRuntimeStatusWithClock(repository, 1, 1, statusKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	connectionID := auth.CorrelationID{1, 2, 3}
	pathGroupID := protocol.PathGroupID{4, 5, 6}
	observer.upsertSessionObservation(runtimeSessionObservation{
		generation: 1, transportName: "tcp", interfaceName: "eth0",
		localAddress: netip.MustParseAddr("192.0.2.1"), localEndpoint: "192.0.2.1:40000",
		remoteEndpoint: "198.51.100.2:9443", connectionID: connectionID.String(), principalID: "edge-1",
		pathGroupID: pathGroupID, lane: 3,
		state: statusapi.SessionReady, reason: statusapi.ReasonPathAdded, reconnects: 2,
	})
	first := observer.sessions[observer.hasher.SessionID(1)]
	if first.ConnectionID != connectionID.String() || first.PrincipalHash == "" ||
		first.PathGroupID != observer.hasher.PathGroupID(pathGroupID) || first.Lane != 3 ||
		first.LocalEndpoint != "192.0.2.1:40000" || first.RemoteEndpoint != "198.51.100.2:9443" ||
		first.StateSince != now || first.Reconnects != 2 {
		t.Fatalf("first session = %#v", first)
	}

	now = now.Add(time.Second)
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	observer.observeSessionProbe(1, 20*time.Millisecond)
	merged := observer.sessions[observer.hasher.SessionID(1)]
	if merged.ConnectionID != first.ConnectionID || merged.PrincipalHash != first.PrincipalHash ||
		merged.PathGroupID != first.PathGroupID || merged.Lane != first.Lane ||
		merged.LocalEndpoint != first.LocalEndpoint || merged.RemoteEndpoint != first.RemoteEndpoint ||
		merged.StateSince != first.StateSince || merged.LastProbeAt != now {
		t.Fatalf("merged session = %#v", merged)
	}

	now = now.Add(time.Second)
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionBackoff, statusapi.ReasonPathRemoved)
	changed := observer.sessions[observer.hasher.SessionID(1)]
	if changed.StateSince != now || changed.ConnectionID != first.ConnectionID {
		t.Fatalf("changed session = %#v", changed)
	}
}

func TestRuntimeStatusPublishesSessionRuntimeQuality(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_500, 0).UTC()
	var statusKey [32]byte
	statusKey[0] = 4
	observer, err := newRuntimeStatusWithClock(repository, 1, 1, statusKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	age := uint64(250)
	observer.observeSessionRuntime(1, sessionRuntimeSnapshot{
		ScheduledData: 1024, WrittenData: 768, EligibleAckedData: 512, DataQueueFrames: 3, ActiveDataFlows: 2,
		Quality: policy.QualitySnapshot{
			ProbeSamples: 2, SRTT: 20 * time.Millisecond, RetryEstimate: 200 * time.Millisecond,
			CapacityBytesSec: 2.5 * 1024 * 1024, QueuedBytes: 4096, InFlightBytes: 2048,
			StallPenalty: 3 * time.Millisecond, DataSamples: 1, DataSampleFresh: true,
			DataSampleAge: time.Duration(age) * time.Millisecond, LastDataCapacity: 3 * 1024 * 1024,
		},
	})
	session := observer.sessions[observer.hasher.SessionID(1)]
	if session.Quality.CapacityBytesSec != uint64(2.5*1024*1024) || session.Quality.QueuedBytes != 4096 ||
		session.Quality.InFlightBytes != 2048 || session.Quality.DataSampleAgeMillis == nil || *session.Quality.DataSampleAgeMillis != age ||
		session.Quality.ScheduledDataPayloadBytes != 1024 || session.Quality.WrittenDataPayloadBytes != 768 ||
		session.Quality.EligibleAckedDataPayloadBytes != 512 || session.Quality.DataQueueFrames != 3 || session.Quality.ActiveDataFlows != 2 {
		t.Fatalf("runtime quality status = %#v", session.Quality)
	}
}

func TestRuntimeStatusAccumulatesReceivedDataPayload(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 6
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	id := observer.hasher.SessionID(1)

	// 两次 DATA 接收累计到同一会话。
	observer.observeSessionDataReceived(1, 100)
	observer.observeSessionDataReceived(1, 250)
	if got := observer.sessions[id].Quality.ReceivedDataPayloadBytes; got != 350 {
		t.Fatalf("received payload = %d, want 350", got)
	}

	// 未注册的代次与零载荷必须安全忽略。
	before := observer.sessions[id].Quality.ReceivedDataPayloadBytes
	observer.observeSessionDataReceived(99, 100)
	observer.observeSessionDataReceived(1, 0)
	if got := observer.sessions[id].Quality.ReceivedDataPayloadBytes; got != before {
		t.Fatalf("ignored observation changed payload from %d to %d", before, got)
	}

	// 会话移除后，迟到接收观察被忽略。
	observer.removeSession(1)
	observer.observeSessionDataReceived(1, 100)
	if _, exists := observer.sessions[id]; exists {
		t.Fatal("removed session was recreated by late receive observation")
	}
}

func TestRuntimeStatusPublishesPeerSendCapacityTimes(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2_000, 0).UTC()
	var statusKey [32]byte
	statusKey[0] = 7
	observer, err := newRuntimeStatusWithClock(repository, 1, 1, statusKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	observer.observeSessionPeerSendCapacity(1, peerSendCapacitySnapshot{
		CapacityBytesSec: 8 << 20, SampleAge: time.Second, SampleFreshness: 3 * time.Second, Measured: true, Fresh: true,
	})
	session := observer.sessions[observer.hasher.SessionID(1)]
	if session.Quality.PeerSendCapacityBytesSec != 8<<20 || session.Quality.PeerSendDataSampleAt == nil ||
		*session.Quality.PeerSendDataSampleAt != now.Add(-time.Second) || session.Quality.PeerSendDataSampleExpiresAt == nil ||
		*session.Quality.PeerSendDataSampleExpiresAt != now.Add(2*time.Second) {
		t.Fatalf("peer send capacity status = %#v", session.Quality)
	}
	observer.observeSessionPeerSendCapacity(1, peerSendCapacitySnapshot{})
	session = observer.sessions[observer.hasher.SessionID(1)]
	if session.Quality.PeerSendCapacityBytesSec != 0 || session.Quality.PeerSendDataSampleAt != nil ||
		session.Quality.PeerSendDataSampleExpiresAt != nil {
		t.Fatalf("cleared peer send capacity status = %#v", session.Quality)
	}
}

func TestRuntimeStatusDropsLateClosedSnapshotAfterSessionRemoval(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 5
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	observer.removeSession(1)
	observer.observeSessionRuntime(1, sessionRuntimeSnapshot{Closed: true})
	if _, exists := observer.runtime[1]; exists {
		t.Fatal("late closed session snapshot was cached after removal")
	}
}

func TestRuntimeStatusAccumulatesFlowRecoveryCountAndDuration(t *testing.T) {
	now := time.Unix(3_000, 0).UTC()
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
	flowID := protocol.FlowID{9}
	correlationID := auth.CorrelationID{9}.String()
	target := protocol.Target{DNSName: "target.example", Port: 443}
	observe := func(state flow.LifecycleState, reason statusapi.TransitionReason) {
		observer.upsertFlowObservation(flowID, target, protocol.DeliveryAdaptive, protocol.PathFastest, runtimeFlowObservation{
			correlationID:     correlationID,
			lifecycleState:    state,
			adaptiveState:     policy.AdaptiveFull,
			txAllocatedOffset: 100, txAcknowledged: 40, rxWrittenOffset: 60,
		}, reason)
	}

	// Relaying 中不产生恢复计数。
	observe(flow.Relaying, statusapi.ReasonStarted)
	if got := observer.flows[flowID].entry.RecoveryCount; got != 0 {
		t.Fatalf("initial recovery count = %d, want 0", got)
	}

	// 进入 Recovering 开始计时；恢复中重复观察不得重置起点。
	now = now.Add(time.Second)
	observe(flow.Recovering, statusapi.ReasonPathRemoved)
	now = now.Add(3 * time.Second)
	observe(flow.Recovering, statusapi.ReasonPathRemoved)

	// 离开 Recovering 完成一次恢复并累计耗时 8s（3001s -> 3009s）。
	now = now.Add(5 * time.Second)
	observe(flow.Relaying, statusapi.ReasonPathAdded)
	entry := observer.flows[flowID].entry
	if entry.RecoveryCount != 1 || entry.RecoveryMicros != 8_000_000 {
		t.Fatalf("recovery after first cycle = count %d, micros %d", entry.RecoveryCount, entry.RecoveryMicros)
	}

	// 第二次进入/离开再累计一次（3011s -> 3011.5s = 500ms）。
	now = now.Add(2 * time.Second)
	observe(flow.Recovering, statusapi.ReasonPathRemoved)
	now = now.Add(500 * time.Millisecond)
	observe(flow.Relaying, statusapi.ReasonPathAdded)
	entry = observer.flows[flowID].entry
	if entry.RecoveryCount != 2 || entry.RecoveryMicros != 8_500_000 {
		t.Fatalf("recovery after second cycle = count %d, micros %d", entry.RecoveryCount, entry.RecoveryMicros)
	}

	// 终态记录携带累计恢复统计。
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- repository.Run(ctx, nil) }()
	now = now.Add(time.Second)
	observer.terminalFlow(flowID, statusapi.FlowClosed, statusapi.ReasonCompleted)
	waitFor(t, time.Second, func() bool { return len(repository.Snapshot().Terminals) == 1 }, "flow terminal")
	terminal := repository.Snapshot().Terminals[0]
	if terminal.RecoveryCount != 2 || terminal.RecoveryMicros != 8_500_000 {
		t.Fatalf("terminal recovery = count %d, micros %d", terminal.RecoveryCount, terminal.RecoveryMicros)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStatusPublishesResourceLimitsAndRejections(t *testing.T) {
	now := time.Unix(4_000, 0).UTC()
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 4
	observer, err := newRuntimeStatusWithClock(repository, 1, 1, statusKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- repository.Run(ctx, nil) }()

	observer.setResourceLimits(64, 128, 16, 8, 512)
	observer.rejectSession()
	observer.rejectSession()
	observer.rejectFlow(flowRejectionRateLimited)
	observer.rejectFlow(flowRejectionOpeningCapacity)
	observer.rejectFlow(flowRejectionTargetDialCapacity)
	observer.rejectSOCKS()
	observer.rejectSOCKS()
	observer.rejectSOCKS()

	waitFor(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return snapshot.Resources.MaxSessions == 64 && snapshot.Rejected.Sessions == 2 &&
			snapshot.Rejected.Flows == 3 && snapshot.Rejected.FlowRateLimited == 1 &&
			snapshot.Rejected.FlowOpeningCapacity == 1 && snapshot.Rejected.FlowTargetDialCapacity == 1 &&
			snapshot.Rejected.SOCKSConnections == 3
	}, "resource limits and rejections")
	snapshot := repository.Snapshot()
	if snapshot.Resources.MaxFlows != 128 || snapshot.Resources.MaxSOCKSConnections != 16 ||
		snapshot.Resources.MaxPendingTargetDials != 8 || snapshot.Resources.MaxTombstones != 512 {
		t.Fatalf("max resources = %#v", snapshot.Resources)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStatusPublishesAuthoritativeFlowObservationAndTerminal(t *testing.T) {
	now := time.Unix(2_000, 0).UTC()
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var statusKey [32]byte
	statusKey[0] = 2
	observer, err := newRuntimeStatusWithClock(repository, 1, 1, statusKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	connectionID := auth.CorrelationID{1}.String()
	observer.upsertSessionObservation(runtimeSessionObservation{
		generation: 7, transportName: "tcp", interfaceName: "eth0", localAddress: netip.MustParseAddr("192.0.2.1"),
		connectionID: connectionID, principalID: "edge-1", state: statusapi.SessionReady, reason: statusapi.ReasonPathAdded,
	})
	flowID := protocol.FlowID{1}
	correlationID := auth.CorrelationID{2}.String()
	target := protocol.Target{DNSName: "target.example", Port: 443}
	observer.upsertFlowObservation(flowID, target, protocol.DeliveryAdaptive, protocol.PathFastest, runtimeFlowObservation{
		correlationID: correlationID, lifecycleState: flow.Relaying,
		adaptiveState: policy.AdaptiveTargeted, adaptiveTransition: policy.TransitionAcknowledgementGap,
		publishedAttachments: 2, policyAttachments: 1,
		preferredAttachment: flow.AttachmentKey{SessionGeneration: 7, AttachmentGeneration: 1}, hasPreferred: true,
		txAllocatedOffset: 100, txAcknowledged: 40, rxWrittenOffset: 80,
	}, statusapi.ReasonStarted)
	entry := observer.flows[flowID].entry
	if entry.FlowID != correlationID || entry.PublishedAttachments != 2 || entry.PolicyAttachments != 1 ||
		entry.PreferredConnectionID != connectionID || entry.UnacknowledgedBytes != 60 ||
		entry.AdaptiveTransition != statusapi.AdaptiveTransitionAcknowledgementGap || entry.StartedAt != now || entry.StateSince != now {
		t.Fatalf("flow entry = %#v", entry)
	}

	now = now.Add(time.Second)
	observer.upsertFlowObservation(flowID, target, protocol.DeliveryAdaptive, protocol.PathFastest, runtimeFlowObservation{
		correlationID: correlationID, lifecycleState: flow.Relaying,
		adaptiveState: policy.AdaptiveTargeted, adaptiveTransition: policy.TransitionAcknowledgementGap,
		publishedAttachments: 2, policyAttachments: 1,
		preferredAttachment: flow.AttachmentKey{SessionGeneration: 7, AttachmentGeneration: 1}, hasPreferred: true,
		txAllocatedOffset: 120, txAcknowledged: 60, rxWrittenOffset: 80,
	}, statusapi.ReasonStarted)
	if got := observer.flows[flowID].entry.StateSince; got != entry.StateSince {
		t.Fatalf("unchanged state since = %v, want %v", got, entry.StateSince)
	}

	now = now.Add(time.Second)
	observer.terminalFlow(flowID, statusapi.FlowClosed, statusapi.ReasonCompleted)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- repository.Run(ctx, nil) }()
	waitFor(t, time.Second, func() bool { return len(repository.Snapshot().Terminals) == 1 }, "flow terminal")
	terminal := repository.Snapshot().Terminals[0]
	if terminal.FlowID != correlationID || terminal.StartedAt != entry.StartedAt ||
		terminal.FinishedAt != now || terminal.DeliveryMode != protocol.DeliveryAdaptive {
		t.Fatalf("terminal = %#v", terminal)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStatusPublishesHashedBoundedLifecycle(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 2, Sessions: 2, Flows: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- repository.Run(ctx, nil) }()

	var key [32]byte
	key[0] = 1
	observer, err := newRuntimeStatusWithKey(repository, 2, 12345, key)
	if err != nil {
		t.Fatal(err)
	}
	observer.syncInterfaces(pathcore.Snapshot{Decisions: []pathcore.Decision{{
		InterfaceIndex: 7, InterfaceName: "wan-a", Reason: pathcore.ReasonEligible,
		Candidates: []pathcore.Candidate{{InterfaceIndex: 7, InterfaceName: "wan-a", LocalAddress: netip.MustParseAddr("192.0.2.7")}},
	}}})
	observer.upsertSession(9, "http2", "wan-a", netip.MustParseAddr("192.0.2.7"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	observer.observeSessionProbe(9, 50*time.Millisecond)
	flowID := protocol.FlowID{0xde, 0xad, 0xbe, 0xef}
	target := protocol.Target{DNSName: "secret-target.example", Port: 443}
	observer.upsertFlow(flowID, target, protocol.DeliveryAdaptive, protocol.PathFastest, flow.FlowSnapshot{
		Lifecycle:         flow.LifecycleSnapshot{State: flow.Recovering},
		TxAllocatedOffset: 100, TxAcknowledgedOffset: 40,
		Rx: flow.RxSnapshot{WrittenOffset: 80},
	}, policy.Snapshot{State: policy.AdaptiveFull}, statusapi.ReasonPathRemoved)
	observer.frameSent(101)
	observer.frameReceived(102)
	observer.recordFlowTraffic(flowID, 11, 12)
	observer.upsertFlow(flowID, target, protocol.DeliveryAdaptive, protocol.PathFastest, flow.FlowSnapshot{
		Lifecycle:         flow.LifecycleSnapshot{State: flow.Recovering},
		TxAllocatedOffset: 100, TxAcknowledgedOffset: 40,
		Rx: flow.RxSnapshot{WrittenOffset: 80},
	}, policy.Snapshot{State: policy.AdaptiveFull}, statusapi.ReasonNone)

	waitFor(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Interfaces) == 1 && len(snapshot.Sessions) == 1 && len(snapshot.Flows) == 1 &&
			snapshot.Resources.Flows == 1 && snapshot.Resources.Sessions == 1 && snapshot.Resources.RecoveringFlows == 1 &&
			snapshot.Counters.FramesSent == 1 && snapshot.Counters.FramesReceived == 1 &&
			snapshot.Flows[0].RetransmittedBytes == 11 && snapshot.Flows[0].RedundantBytes == 12 &&
			snapshot.Sessions[0].Quality.SmoothedRTTMicros == 50_000 && snapshot.Sessions[0].Fastest
	}, "runtime status publication")
	snapshot := repository.Snapshot()
	if snapshot.Sessions[0].Transport != "http2" {
		t.Fatalf("session transport = %q, want http2", snapshot.Sessions[0].Transport)
	}
	if snapshot.Resources.ReservedBytes != 12345 || snapshot.Flows[0].TxAllocatedOffset != 100 ||
		snapshot.Flows[0].TxAcknowledged != 40 || snapshot.Flows[0].RxWrittenOffset != 80 ||
		snapshot.Flows[0].RetransmittedBytes != 11 || snapshot.Flows[0].RedundantBytes != 12 ||
		snapshot.Counters.RetransmittedBytes != 11 || snapshot.Counters.RedundantBytes != 12 {
		t.Fatalf("status snapshot = %#v", snapshot)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-target.example", "deadbeef"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("status leaked %q: %s", forbidden, encoded)
		}
	}

	observer.terminalFlow(flowID, statusapi.FlowReset, statusapi.ReasonDeadlineExceeded)
	observer.removeSession(9)
	observer.syncInterfaces(pathcore.Snapshot{})
	waitFor(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Interfaces) == 0 && len(snapshot.Sessions) == 0 && len(snapshot.Flows) == 0 &&
			len(snapshot.Terminals) == 1 && snapshot.Resources.Flows == 0 && snapshot.Resources.Sessions == 0 &&
			snapshot.Resources.RecoveringFlows == 0
	}, "runtime status cleanup")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStatusMovesFastestSessionMarker(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 3, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- repository.Run(ctx, nil) }()
	var key [32]byte
	key[0] = 2
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, key)
	if err != nil {
		t.Fatal(err)
	}
	observer.upsertSession(1, "tcp", "wan-slow", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	observer.observeSessionProbe(1, 50*time.Millisecond)
	observer.upsertSession(2, "tcp", "wan-fast", netip.MustParseAddr("192.0.2.2"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	observer.observeSessionProbe(2, 20*time.Millisecond)
	waitFor(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		if len(snapshot.Sessions) != 2 {
			return false
		}
		for _, session := range snapshot.Sessions {
			if session.Interface == "wan-fast" {
				return session.Fastest && session.Quality.SmoothedRTTMicros == 20_000
			}
		}
		return false
	}, "single fastest marker")
	for _, session := range repository.Snapshot().Sessions {
		if session.Fastest != (session.Interface == "wan-fast") {
			t.Fatalf("initial fastest marker = %#v", repository.Snapshot().Sessions)
		}
	}
	observer.setSessionStallPenalty(2, probeTimeout)
	waitFor(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		for _, session := range snapshot.Sessions {
			if session.Interface == "wan-slow" && session.Fastest {
				return true
			}
			if session.Interface == "wan-fast" && session.Quality.StallPenaltyMicros != uint64(probeTimeout/time.Microsecond) {
				return false
			}
		}
		return false
	}, "fastest marker avoids stalled session")
	observer.observeSessionProbe(2, 20*time.Millisecond)
	waitFor(t, time.Second, func() bool {
		for _, session := range repository.Snapshot().Sessions {
			if session.Interface == "wan-fast" {
				return session.Fastest && session.Quality.StallPenaltyMicros == 0
			}
		}
		return false
	}, "recovered session regains fastest marker")
	observer.removeSession(2)
	waitFor(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Sessions) == 1 && snapshot.Sessions[0].Interface == "wan-slow" && snapshot.Sessions[0].Fastest
	}, "fastest marker after removal")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStatusBatchClientSyncSelectsFastestSession(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 3, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- repository.Run(ctx, nil) }()
	var key [32]byte
	key[0] = 7
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, key)
	if err != nil {
		t.Fatal(err)
	}
	for generation, rtt := range map[uint64]time.Duration{
		1: 50 * time.Millisecond,
		2: 20 * time.Millisecond,
		3: 80 * time.Millisecond,
	} {
		observer.runtime[generation] = sessionRuntimeSnapshot{
			Quality: policy.QualitySnapshot{ProbeSamples: 1, SRTT: rtt},
		}
	}
	observer.syncClientSessions("tcp", "edge-1", clientcore.SessionManagerSnapshot{
		Sessions: []clientcore.ManagedSessionSnapshot{
			{Generation: 1, Candidate: pathcore.Candidate{InterfaceIndex: 1, InterfaceName: "wan-slow", LocalAddress: netip.MustParseAddr("192.0.2.1")}, PathGroupID: protocol.PathGroupID{1}, State: clientcore.ManagedSessionReady},
			{Generation: 2, Candidate: pathcore.Candidate{InterfaceIndex: 2, InterfaceName: "wan-fast", LocalAddress: netip.MustParseAddr("192.0.2.2")}, PathGroupID: protocol.PathGroupID{1}, State: clientcore.ManagedSessionReady},
			{Generation: 3, Candidate: pathcore.Candidate{InterfaceIndex: 3, InterfaceName: "wan-slower", LocalAddress: netip.MustParseAddr("192.0.2.3")}, PathGroupID: protocol.PathGroupID{1}, State: clientcore.ManagedSessionReady},
		},
	}, nil)
	waitFor(t, time.Second, func() bool {
		return len(repository.Snapshot().Sessions) == 3
	}, "batched client sessions")
	fastest := ""
	for _, session := range repository.Snapshot().Sessions {
		if session.Fastest {
			if fastest != "" {
				t.Fatalf("multiple fastest sessions = %q and %q", fastest, session.Interface)
			}
			fastest = session.Interface
		}
	}
	if fastest != "wan-fast" {
		t.Fatalf("fastest session = %q, want wan-fast", fastest)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStatusSuppressesIdenticalFlowUpdates(t *testing.T) {
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	var key [32]byte
	key[0] = 3
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, key)
	if err != nil {
		t.Fatal(err)
	}
	flowID := protocol.FlowID{1}
	target := protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 443}
	snapshot := flow.FlowSnapshot{
		Lifecycle: flow.LifecycleSnapshot{State: flow.Relaying},
		Rx:        flow.RxSnapshot{},
	}
	policySnapshot := policy.Snapshot{State: policy.AdaptiveSingle}
	for range 2_000 {
		observer.upsertFlow(
			flowID, target, protocol.DeliveryAdaptive, protocol.PathFastest,
			snapshot, policySnapshot, statusapi.ReasonStarted,
		)
	}
	if dropped := repository.Snapshot().Counters.DroppedStatusEvents; dropped != 0 {
		t.Fatalf("identical flow updates dropped events = %d", dropped)
	}
}

func TestLimitedListenerCapsConnectionsAndCloseUnblocksAccept(t *testing.T) {
	inner := newQueuedListener()
	wrappedValue, err := newLimitedListener(inner, 2)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := wrappedValue.(*limitedListener)
	peers := make([]net.Conn, 0, 3)
	for range 3 {
		server, peer := net.Pipe()
		inner.connections <- server
		peers = append(peers, peer)
	}
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()
	first, err := wrapped.Accept()
	if err != nil {
		t.Fatal(err)
	}
	second, err := wrapped.Accept()
	if err != nil {
		t.Fatal(err)
	}
	thirdResult := make(chan net.Conn, 1)
	thirdError := make(chan error, 1)
	go func() {
		connection, acceptErr := wrapped.Accept()
		thirdResult <- connection
		thirdError <- acceptErr
	}()
	select {
	case <-thirdResult:
		t.Fatal("third connection bypassed listener limit")
	case <-time.After(20 * time.Millisecond):
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	var third net.Conn
	select {
	case third = <-thirdResult:
		if err := <-thirdError; err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("released slot did not resume Accept")
	}
	_ = second.Close()
	_ = third.Close()

	server, peer := net.Pipe()
	inner.connections <- server
	peers = append(peers, peer)
	holdA, _ := wrapped.Accept()
	server, peer = net.Pipe()
	inner.connections <- server
	peers = append(peers, peer)
	holdB, _ := wrapped.Accept()
	blocked := make(chan error, 1)
	go func() {
		_, acceptErr := wrapped.Accept()
		blocked <- acceptErr
	}()
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("closed limited listener returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("listener close did not unblock capacity wait")
	}
	_ = holdA.Close()
	_ = holdB.Close()
}

type queuedListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

func newQueuedListener() *queuedListener {
	return &queuedListener{connections: make(chan net.Conn, 8), closed: make(chan struct{})}
}

func (listener *queuedListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.connections:
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *queuedListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (*queuedListener) Addr() net.Addr { return statusTestAddress("status") }

type statusTestAddress string

func (address statusTestAddress) Network() string { return "test" }
func (address statusTestAddress) String() string  { return string(address) }
