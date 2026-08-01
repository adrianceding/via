package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	clientcore "github.com/adrianceding/via/internal/client"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
	servercore "github.com/adrianceding/via/internal/server"
	statusapi "github.com/adrianceding/via/internal/status"
)

func waitSimulationResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("simulated transfer did not complete")
	}
}

func simulationClientFlow(t *testing.T, harness *simulationHarness) *clientFlow {
	t.Helper()
	harness.client.flowsMu.RLock()
	defer harness.client.flowsMu.RUnlock()
	if len(harness.client.flows) != 1 {
		t.Fatalf("client flow count = %d, want 1", len(harness.client.flows))
	}
	for _, instance := range harness.client.flows {
		return instance
	}
	t.Fatal("missing client flow")
	return nil
}

func forceSimulationClientRetry(t *testing.T, harness *simulationHarness) {
	t.Helper()
	instance := simulationClientFlow(t, harness)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snapshot, ok := instance.snapshot(ctx)
	if !ok {
		t.Fatal("client retry snapshot unavailable")
	}
	if snapshot.Flow.TxReplayBytes == 0 {
		if snapshot.Flow.TxAcknowledgedOffset != snapshot.Flow.TxAllocatedOffset {
			t.Fatalf("client retry converged without cumulative acknowledgement: %#v", snapshot.Flow)
		}
		return
	}
	if snapshot.Flow.RetryGeneration == 0 {
		t.Fatalf("client retry is not actionable: %#v", snapshot.Flow)
	}
	select {
	case instance.events <- clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelayRetryDeadline, Generation: snapshot.Flow.RetryGeneration,
	}:
	case <-ctx.Done():
		t.Fatal("client retry event blocked")
	}
}

func setSimulationPathQualities(t *testing.T, harness *simulationHarness) {
	t.Helper()
	for _, session := range harness.client.readySessions() {
		host, _, err := net.SplitHostPort(session.connection.LocalEndpoint())
		if err != nil {
			t.Fatal(err)
		}
		rtt := 10 * time.Millisecond
		if host == simulationAddressB.String() {
			rtt = 500 * time.Millisecond
		}
		harness.client.notifyProbeQuality(session, rtt)
	}
	instance := simulationClientFlow(t, harness)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snapshot, ok := instance.snapshot(ctx)
	if !ok || len(snapshot.Policy.Attachments) != 2 {
		t.Fatalf("client quality snapshot = %#v, available %v", snapshot.Policy, ok)
	}

	harness.server.flowsMu.RLock()
	var serverInstance *serverFlow
	for _, candidate := range harness.server.flows {
		serverInstance = candidate
	}
	harness.server.flowsMu.RUnlock()
	if serverInstance == nil {
		t.Fatal("missing server flow for path quality")
	}
	for _, attachment := range serverInstance.snapshot().Flow.Lifecycle.Published {
		session := harness.server.session(attachment.SessionGeneration)
		if session == nil {
			t.Fatal("missing server session for path quality")
		}
		host, _, err := net.SplitHostPort(session.connection.RemoteEndpoint())
		if err != nil {
			t.Fatal(err)
		}
		rtt := 10 * time.Millisecond
		if host == simulationAddressB.String() {
			rtt = 500 * time.Millisecond
		}
		serverInstance.observeProbe(attachment, rtt)
	}
}

func setSimulationSessionQuality(t *testing.T, harness *simulationHarness, address netip.Addr, rtt time.Duration) {
	t.Helper()
	for _, session := range harness.client.readySessions() {
		host, _, err := net.SplitHostPort(session.connection.LocalEndpoint())
		if err != nil {
			t.Fatal(err)
		}
		if host != address.String() {
			continue
		}
		session.probeMu.Lock()
		session.probeSRTT = rtt
		session.probeMu.Unlock()
		break
	}
	harness.server.sessionsMu.RLock()
	defer harness.server.sessionsMu.RUnlock()
	for _, session := range harness.server.sessions {
		host, _, err := net.SplitHostPort(session.connection.RemoteEndpoint())
		if err != nil {
			t.Fatal(err)
		}
		if host != address.String() {
			continue
		}
		session.probeMu.Lock()
		session.probeSRTT = rtt
		session.probeMu.Unlock()
		return
	}
	t.Fatalf("missing simulated server session for %s", address)
}

func forceSimulationServerRetry(t *testing.T, harness *simulationHarness) {
	t.Helper()
	harness.server.flowsMu.RLock()
	if len(harness.server.flows) != 1 {
		count := len(harness.server.flows)
		harness.server.flowsMu.RUnlock()
		t.Fatalf("server flow count = %d, want 1", count)
	}
	var instance *serverFlow
	for _, candidate := range harness.server.flows {
		instance = candidate
	}
	harness.server.flowsMu.RUnlock()
	snapshot := instance.snapshot().Flow
	if snapshot.TxReplayBytes == 0 {
		if snapshot.TxAcknowledgedOffset != snapshot.TxAllocatedOffset {
			t.Fatalf("server retry converged without cumulative acknowledgement: %#v", snapshot)
		}
		return
	}
	if snapshot.RetryGeneration == 0 {
		t.Fatalf("server retry is not actionable: %#v", snapshot)
	}
	if err := instance.handle(servercore.RelayEvent{
		Kind: servercore.RelayRetryDeadline, Generation: snapshot.RetryGeneration,
	}); err != nil {
		t.Fatal(err)
	}
}

func completeSimulationClosing(t *testing.T, harness *simulationHarness) {
	t.Helper()
	clientInstance := simulationClientFlow(t, harness)
	harness.server.flowsMu.RLock()
	var serverInstance *serverFlow
	for _, candidate := range harness.server.flows {
		serverInstance = candidate
	}
	harness.server.flowsMu.RUnlock()
	if serverInstance == nil {
		t.Fatal("missing server flow for closing completion")
	}

	var clientGeneration, serverGeneration uint64
	waitFor(t, 5*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		clientSnapshot, ok := clientInstance.snapshot(ctx)
		cancel()
		serverSnapshot := serverInstance.snapshot().Flow
		if !ok || clientSnapshot.Flow.Lifecycle.State != flow.Closing || serverSnapshot.Lifecycle.State != flow.Closing {
			return false
		}
		clientGeneration = clientSnapshot.Flow.Lifecycle.ClosingDeadlineGeneration
		serverGeneration = serverSnapshot.Lifecycle.ClosingDeadlineGeneration
		return clientGeneration != 0 && serverGeneration != 0
	}, "both flows entering closing linger")

	clientInstance.emit(clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelayClosingDeadline, Generation: clientGeneration,
	})
	if err := serverInstance.handle(servercore.RelayEvent{
		Kind: servercore.RelayClosingDeadline, Generation: serverGeneration,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSimulatedFullChainFrameFaults(t *testing.T) {
	payload := bytes.Repeat([]byte("frame-fault|"), 16<<10)
	tests := []struct {
		name       string
		configure  func(*simulationHarness)
		beforeWait func(*testing.T, *simulationHarness)
		cleanup    func(*testing.T, *simulationHarness)
	}{
		{
			name: "duplicate",
			configure: func(harness *simulationHarness) {
				harness.network.controller(simulationAddressA, simulatedUplink).set(simulatedFault{kind: simulatedFaultDuplicate, frameType: protocol.TypeData, remaining: -1})
				harness.network.controller(simulationAddressA, simulatedDownlink).set(simulatedFault{kind: simulatedFaultDuplicate, frameType: protocol.TypeData, remaining: -1})
			},
		},
		{
			name: "reorder",
			configure: func(harness *simulationHarness) {
				for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
					harness.network.controller(address, simulatedUplink).set(simulatedFault{kind: simulatedFaultReorder, frameType: protocol.TypeData, remaining: 2})
					harness.network.controller(address, simulatedDownlink).set(simulatedFault{kind: simulatedFaultReorder, frameType: protocol.TypeData, remaining: 2})
				}
			},
		},
		{
			name: "delay",
			configure: func(harness *simulationHarness) {
				for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
					harness.network.controller(address, simulatedUplink).set(simulatedFault{kind: simulatedFaultHold, frameType: protocol.TypeData, remaining: 1})
				}
			},
			beforeWait: func(t *testing.T, harness *simulationHarness) {
				for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
					controller := harness.network.controller(address, simulatedUplink)
					waitFor(t, 5*time.Second, func() bool { return controller.heldCount() == 1 }, "held delayed frame")
				}
				if err := harness.network.controller(simulationAddressA, simulatedUplink).releaseOne(context.Background()); err != nil {
					t.Fatal(err)
				}
			},
			cleanup: func(t *testing.T, harness *simulationHarness) {
				if err := harness.network.controller(simulationAddressB, simulatedUplink).releaseAll(context.Background()); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "rate",
			configure: func(harness *simulationHarness) {
				for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
					harness.network.controller(address, simulatedUplink).set(simulatedFault{kind: simulatedFaultHold, frameType: protocol.TypeData, remaining: 2})
				}
			},
			beforeWait: func(t *testing.T, harness *simulationHarness) {
				for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
					controller := harness.network.controller(address, simulatedUplink)
					waitFor(t, 5*time.Second, func() bool { return controller.heldCount() == 2 }, "rate-limited frames")
				}
				controller := harness.network.controller(simulationAddressA, simulatedUplink)
				if err := controller.releaseOne(context.Background()); err != nil {
					t.Fatal(err)
				}
				waitFor(t, 5*time.Second, func() bool {
					harness.server.flowsMu.RLock()
					defer harness.server.flowsMu.RUnlock()
					for _, instance := range harness.server.flows {
						return instance.snapshot().Flow.Rx.WrittenOffset != 0
					}
					return false
				}, "first rate budget delivery")
				if err := controller.releaseOne(context.Background()); err != nil {
					t.Fatal(err)
				}
			},
			cleanup: func(t *testing.T, harness *simulationHarness) {
				if err := harness.network.controller(simulationAddressB, simulatedUplink).releaseAll(context.Background()); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "stall",
			configure: func(harness *simulationHarness) {
				for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
					harness.network.controller(address, simulatedUplink).set(simulatedFault{kind: simulatedFaultStall, frameType: protocol.TypeData, remaining: 1})
				}
			},
			beforeWait: func(t *testing.T, harness *simulationHarness) {
				for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
					controller := harness.network.controller(address, simulatedUplink)
					select {
					case <-controller.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("simulated DATA write did not stall")
					}
				}
				for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
					harness.network.controller(address, simulatedUplink).resume()
				}
			},
		},
		{
			name: "one-way-blackhole",
			configure: func(harness *simulationHarness) {
				harness.network.controller(simulationAddressA, simulatedUplink).set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeData, remaining: -1})
				harness.network.controller(simulationAddressB, simulatedDownlink).set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeData, remaining: -1})
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newSimulationHarness(t, protocol.DeliveryRedundant, protocol.PathNone)
			defer harness.close()
			application := harness.openApplication()
			defer application.Close()
			harness.waitAttachments(2)
			testCase.configure(harness)
			result := startSimulationRoundTrip(application, payload)
			if testCase.beforeWait != nil {
				testCase.beforeWait(t, harness)
			}
			waitSimulationResult(t, result)
			if testCase.cleanup != nil {
				testCase.cleanup(t, harness)
			}
			if harness.targetTotal.Load() != 1 {
				t.Fatalf("target connections = %d", harness.targetTotal.Load())
			}
		})
	}
}

func TestSimulatedFullChainAllCopiesLostRetransmits(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryRedundant, protocol.PathNone)
	defer harness.close()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)
	for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
		harness.network.controller(address, simulatedUplink).set(simulatedFault{
			kind: simulatedFaultDrop, frameType: protocol.TypeData, matchDataOffset: true, dataOffset: 0, remaining: 1,
		})
	}
	payload := bytes.Repeat([]byte("all-copies-lost|"), 8<<10)
	result := startSimulationRoundTrip(application, payload)
	for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
		controller := harness.network.controller(address, simulatedUplink)
		waitFor(t, 5*time.Second, func() bool { return controller.matchCount() == 1 }, "dropped DATA copy")
	}
	forceSimulationClientRetry(t, harness)
	waitSimulationResult(t, result)
	waitFor(t, 5*time.Second, func() bool {
		return harness.client.statusRepository.Snapshot().Counters.RetransmittedBytes != 0
	}, "retransmission status counter")
	if harness.targetTotal.Load() != 1 {
		t.Fatalf("target connections = %d", harness.targetTotal.Load())
	}
}

func TestSimulatedFullChainAdaptiveFastestRecoversFromSustainedOneWayBlackhole(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryAdaptive, protocol.PathFastest)
	defer harness.close()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)
	setSimulationPathQualities(t, harness)

	primary := harness.network.controller(simulationAddressA, simulatedDownlink)
	backup := harness.network.controller(simulationAddressB, simulatedDownlink)
	primary.set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeData, remaining: -1})
	payload := bytes.Repeat([]byte("adaptive-one-way-blackhole|"), 1024)
	result := startSimulationRoundTrip(application, payload)
	waitFor(t, 5*time.Second, func() bool { return primary.matchCount() != 0 }, "blackholed fastest-path DATA")
	forceSimulationServerRetry(t, harness)
	waitSimulationResult(t, result)
	if backup.count(protocol.TypeData) == 0 {
		t.Fatal("backup path did not carry the retransmitted DATA")
	}
	if harness.targetTotal.Load() != 1 {
		t.Fatalf("target connections = %d", harness.targetTotal.Load())
	}
	waitFor(t, 5*time.Second, func() bool {
		return harness.server.statusRepository.Snapshot().Counters.RetransmittedBytes != 0
	}, "server retransmission status counter")
}

func TestSimulatedFullChainNewAdaptiveFlowUsesExistingFastestSessionQuality(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryAdaptive, protocol.PathFastest)
	defer harness.close()
	setSimulationSessionQuality(t, harness, simulationAddressA, 350*time.Millisecond)
	setSimulationSessionQuality(t, harness, simulationAddressB, 170*time.Millisecond)

	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)
	payload := bytes.Repeat([]byte("existing-session-quality|"), 1024)
	waitSimulationResult(t, startSimulationRoundTrip(application, payload))

	uplinkA := harness.network.controller(simulationAddressA, simulatedUplink)
	uplinkB := harness.network.controller(simulationAddressB, simulatedUplink)
	downlinkA := harness.network.controller(simulationAddressA, simulatedDownlink)
	downlinkB := harness.network.controller(simulationAddressB, simulatedDownlink)
	if uplinkA.count(protocol.TypeOpen) == 0 || uplinkB.count(protocol.TypeOpen) == 0 {
		t.Fatalf("parallel OPEN counts = A:%d B:%d", uplinkA.count(protocol.TypeOpen), uplinkB.count(protocol.TypeOpen))
	}
	if uplinkA.count(protocol.TypeData) != 0 || uplinkB.count(protocol.TypeData) == 0 ||
		downlinkA.count(protocol.TypeData) != 0 || downlinkB.count(protocol.TypeData) == 0 {
		t.Fatalf("DATA counts = up A:%d B:%d down A:%d B:%d",
			uplinkA.count(protocol.TypeData), uplinkB.count(protocol.TypeData),
			downlinkA.count(protocol.TypeData), downlinkB.count(protocol.TypeData))
	}
}

func TestSimulatedFullChainAllInterfacesRecoverWithoutRedialingTarget(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryRedundant, protocol.PathNone)
	defer harness.close()
	defer func() {
		if t.Failed() {
			t.Logf("client status: %#v", harness.client.statusRepository.Snapshot())
			t.Logf("server status: %#v", harness.server.statusRepository.Snapshot())
		}
	}()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)
	for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
		harness.network.controller(address, simulatedUplink).set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeData, remaining: -1})
	}
	result := startSimulationRoundTrip(application, []byte("retained-across-all-path-loss"))
	for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
		controller := harness.network.controller(address, simulatedUplink)
		waitFor(t, 5*time.Second, func() bool { return controller.matchCount() != 0 }, "initial blackholed DATA")
	}
	harness.enumerator.set()
	harness.tickPaths()
	harness.waitSessions(0)
	defer func() {
		if t.Failed() {
			t.Logf("client status after all paths lost: %#v", harness.client.statusRepository.Snapshot())
			t.Logf("server status after all paths lost: %#v", harness.server.statusRepository.Snapshot())
		}
	}()
	waitFor(t, 5*time.Second, func() bool {
		snapshot := harness.client.statusRepository.Snapshot()
		return snapshot.Resources.RecoveringFlows == 1 && len(snapshot.Flows) == 1 &&
			snapshot.Flows[0].State == statusapi.FlowRecovering
	}, "client flow recovering")
	for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
		harness.network.controller(address, simulatedUplink).set(simulatedFault{kind: simulatedFaultNormal})
	}
	harness.enumerator.set(
		simulatedInterface(1, "sim-a", simulationAddressA),
		simulatedInterface(2, "sim-b", simulationAddressB),
	)
	harness.tickPaths()
	harness.waitSessions(2)
	harness.waitAttachments(2)
	waitSimulationResult(t, result)
	if harness.targetTotal.Load() != 1 {
		t.Fatalf("target connections after recovery = %d", harness.targetTotal.Load())
	}
}

func TestSimulatedFullChainPayloadBoundariesAndHalfClose(t *testing.T) {
	sizes := []int{1, 64 << 10, 3 << 20}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("bytes-%d", size), func(t *testing.T) {
			harness := newSimulationHarness(t, protocol.DeliveryAdaptive, protocol.PathDistributed)
			defer harness.close()
			application := harness.openApplication()
			defer application.Close()
			harness.waitAttachments(2)
			payload := bytes.Repeat([]byte{byte(size)}, size)
			if err := simulationRoundTrip(application, payload); err != nil {
				t.Fatal(err)
			}
			if harness.targetTotal.Load() != 1 {
				t.Fatalf("target connections = %d", harness.targetTotal.Load())
			}
		})
	}

	t.Run("half-close", func(t *testing.T) {
		harness := newSimulationHarness(t, protocol.DeliveryRedundant, protocol.PathNone)
		defer harness.close()
		application := harness.openApplication()
		harness.waitAttachments(2)
		payload := bytes.Repeat([]byte("half-close|"), 8<<10)
		if err := writeAll(application, payload); err != nil {
			t.Fatal(err)
		}
		tcp, ok := application.(*net.TCPConn)
		if !ok {
			t.Fatal("SOCKS application connection is not TCP")
		}
		if err := tcp.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		_ = application.SetReadDeadline(time.Now().Add(15 * time.Second))
		received, err := io.ReadAll(application)
		if err != nil || !bytes.Equal(received, payload) {
			t.Fatalf("half-close response bytes=%d/%d err=%v", len(received), len(payload), err)
		}
		_ = application.Close()
		completeSimulationClosing(t, harness)
		waitFor(t, 5*time.Second, func() bool {
			harness.client.flowsMu.RLock()
			clientFlows := len(harness.client.flows)
			harness.client.flowsMu.RUnlock()
			harness.server.flowsMu.RLock()
			serverFlows := len(harness.server.flows)
			harness.server.flowsMu.RUnlock()
			return clientFlows == 0 && serverFlows == 0
		}, "half-close flow convergence")
	})
}

func TestSimulatedFullChainConcurrentFlows(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryAdaptive, protocol.PathDistributed)
	defer harness.close()
	const count = 8
	applications := make([]net.Conn, 0, count)
	for range count {
		applications = append(applications, harness.openApplication())
	}
	defer func() {
		for _, application := range applications {
			_ = application.Close()
		}
	}()
	harness.waitAttachments(count * 2)

	var group sync.WaitGroup
	errorsByFlow := make(chan error, count)
	for index, application := range applications {
		group.Add(1)
		go func() {
			defer group.Done()
			payload := bytes.Repeat([]byte(fmt.Sprintf("flow-%d|", index)), 16<<10)
			errorsByFlow <- simulationRoundTrip(application, payload)
		}()
	}
	group.Wait()
	close(errorsByFlow)
	for err := range errorsByFlow {
		if err != nil {
			t.Fatal(err)
		}
	}
	if harness.targetTotal.Load() != count {
		t.Fatalf("target connections = %d, want %d", harness.targetTotal.Load(), count)
	}
}
