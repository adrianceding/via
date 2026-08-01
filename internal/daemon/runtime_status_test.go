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
	observer.upsertSessionObservation(runtimeSessionObservation{
		generation: 1, transportName: "tcp", interfaceName: "eth0",
		localAddress: netip.MustParseAddr("192.0.2.1"), localEndpoint: "192.0.2.1:40000",
		remoteEndpoint: "198.51.100.2:9443", connectionID: connectionID.String(), principalID: "edge-1",
		state: statusapi.SessionReady, reason: statusapi.ReasonPathAdded, reconnects: 2,
	})
	first := observer.sessions[observer.hasher.SessionID(1)]
	if first.ConnectionID != connectionID.String() || first.PrincipalHash == "" ||
		first.LocalEndpoint != "192.0.2.1:40000" || first.RemoteEndpoint != "198.51.100.2:9443" ||
		first.StateSince != now || first.Reconnects != 2 {
		t.Fatalf("first session = %#v", first)
	}

	now = now.Add(time.Second)
	observer.upsertSession(1, "tcp", "eth0", netip.MustParseAddr("192.0.2.1"), statusapi.SessionReady, statusapi.ReasonPathAdded)
	observer.observeSessionProbe(1, 20*time.Millisecond)
	merged := observer.sessions[observer.hasher.SessionID(1)]
	if merged.ConnectionID != first.ConnectionID || merged.PrincipalHash != first.PrincipalHash ||
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
