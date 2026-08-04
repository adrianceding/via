package daemon

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/config"
	pathcore "github.com/adrianceding/via/internal/path"
	"github.com/adrianceding/via/internal/protocol"
	statusapi "github.com/adrianceding/via/internal/status"
)

var (
	simulationAddressA = netip.MustParseAddr("192.0.2.10")
	simulationAddressB = netip.MustParseAddr("198.51.100.20")
	simulationAddressC = netip.MustParseAddr("203.0.113.30")
)

type simulatedEnumerator struct {
	mu         sync.Mutex
	interfaces []pathcore.Interface
}

func (enumerator *simulatedEnumerator) Interfaces() ([]pathcore.Interface, error) {
	enumerator.mu.Lock()
	defer enumerator.mu.Unlock()
	result := make([]pathcore.Interface, len(enumerator.interfaces))
	for index, networkInterface := range enumerator.interfaces {
		result[index] = networkInterface
		result[index].Addresses = append([]netip.Addr(nil), networkInterface.Addresses...)
	}
	return result, nil
}

func (enumerator *simulatedEnumerator) set(interfaces ...pathcore.Interface) {
	enumerator.mu.Lock()
	enumerator.interfaces = append([]pathcore.Interface(nil), interfaces...)
	enumerator.mu.Unlock()
}

func simulatedInterface(index int, name string, address netip.Addr) pathcore.Interface {
	return pathcore.Interface{Index: index, Name: name, Up: true, Addresses: []netip.Addr{address}}
}

type simulationHarness struct {
	t            *testing.T
	network      *simulatedNetwork
	enumerator   *simulatedEnumerator
	pathTicks    chan time.Time
	server       *serverDaemon
	client       *clientDaemon
	cancelServer context.CancelFunc
	cancelClient context.CancelFunc
	serverResult chan error
	clientResult chan error
	target       net.Listener
	targetDone   chan struct{}
	targetTotal  atomic.Int32
	targetActive atomic.Int32
	targetBytes  atomic.Uint64
	targetHandle func(*simulationHarness, net.Conn)
	closed       atomic.Bool
}

func newSimulationHarness(t *testing.T, mode protocol.DeliveryMode, selection protocol.PathSelection) *simulationHarness {
	return newSimulationHarnessConfigured(t, mode, selection, nil, true)
}

func newSimulationHarnessWithTarget(t *testing.T, mode protocol.DeliveryMode, selection protocol.PathSelection, targetHandle func(*simulationHarness, net.Conn)) *simulationHarness {
	return newSimulationHarnessConfigured(t, mode, selection, targetHandle, true)
}

func newSimulationHarnessConfigured(t *testing.T, mode protocol.DeliveryMode, selection protocol.PathSelection, targetHandle func(*simulationHarness, net.Conn), authentication bool) *simulationHarness {
	t.Helper()
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	relayAddress := reserveAddress(t)
	socksAddress := reserveAddress(t)
	serverConfiguration := decodeSimulationServer(t, relayAddress)
	clientConfiguration := decodeSimulationClient(t, relayAddress, socksAddress, mode, selection)
	if !authentication {
		clientConfiguration.SOCKSAuth = nil
	}
	network := newSimulatedNetwork(t, simulationAddressA, simulationAddressB, simulationAddressC)
	server, err := newServerDaemon(serverConfiguration)
	if err != nil {
		_ = target.Close()
		t.Fatal(err)
	}
	_ = server.listener.Close()
	server.listener = network.listener
	client, err := newClientDaemon(clientConfiguration)
	if err != nil {
		server.cancelDials()
		server.cancelRuntime()
		server.closeAll()
		_ = target.Close()
		t.Fatal(err)
	}
	enumerator := &simulatedEnumerator{}
	enumerator.set(
		simulatedInterface(1, "sim-a", simulationAddressA),
		simulatedInterface(2, "sim-b", simulationAddressB),
	)
	filter, err := pathcore.NewFilter(clientConfiguration.Interfaces.Include, clientConfiguration.Interfaces.Exclude)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := pathcore.NewManager(enumerator, filter, netip.MustParseAddr("127.0.0.1"))
	if err != nil {
		t.Fatal(err)
	}
	pathTicks := make(chan time.Time, 16)
	client.factory = network
	client.pathManager = manager
	client.pathTicks = pathTicks
	harness := &simulationHarness{
		t: t, network: network, enumerator: enumerator, pathTicks: pathTicks,
		server: server, client: client, target: target, targetDone: make(chan struct{}),
		serverResult: make(chan error, 1), clientResult: make(chan error, 1), targetHandle: targetHandle,
	}
	if harness.targetHandle == nil {
		harness.targetHandle = echoSimulationTarget
	}
	go harness.runTarget()
	serverCtx, cancelServer := context.WithCancel(context.Background())
	clientCtx, cancelClient := context.WithCancel(context.Background())
	harness.cancelServer = cancelServer
	harness.cancelClient = cancelClient
	go func() { harness.serverResult <- server.run(serverCtx) }()
	go func() { harness.clientResult <- client.run(clientCtx) }()
	harness.waitSessions(2)
	return harness
}

func (harness *simulationHarness) runTarget() {
	defer close(harness.targetDone)
	for {
		connection, err := harness.target.Accept()
		if err != nil {
			return
		}
		harness.targetTotal.Add(1)
		harness.targetActive.Add(1)
		go func() {
			defer connection.Close()
			defer harness.targetActive.Add(-1)
			harness.targetHandle(harness, connection)
		}()
	}
}

func echoSimulationTarget(harness *simulationHarness, connection net.Conn) {
	buffer := make([]byte, protocol.MaxDataLength)
	for {
		read, readErr := connection.Read(buffer)
		if read > 0 {
			harness.targetBytes.Add(uint64(read))
			if writeAll(connection, buffer[:read]) != nil {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}

func (harness *simulationHarness) close() {
	if harness == nil || harness.closed.Swap(true) {
		return
	}
	harness.cancelClient()
	harness.cancelServer()
	for name, result := range map[string]<-chan error{"client": harness.clientResult, "server": harness.serverResult} {
		select {
		case err := <-result:
			if err != nil {
				harness.t.Errorf("%s simulation shutdown: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			harness.t.Errorf("%s simulation did not shut down", name)
		}
	}
	_ = harness.target.Close()
	<-harness.targetDone
	waitFor(harness.t, time.Second, func() bool { return harness.targetActive.Load() == 0 }, "simulation target cleanup")
	waitFor(harness.t, time.Second, func() bool { return harness.network.activePairs() == 0 }, "simulation connection cleanup")
	if held := harness.network.heldFrames(); held != 0 {
		harness.t.Errorf("held simulated frames after shutdown = %d", held)
	}
	if sessions := len(harness.client.readySessions()); sessions != 0 {
		harness.t.Errorf("client sessions after shutdown = %d", sessions)
	}
	harness.client.flowsMu.RLock()
	clientFlows := len(harness.client.flows)
	clientActors := len(harness.client.actors)
	harness.client.flowsMu.RUnlock()
	harness.server.flowsMu.RLock()
	serverFlows := len(harness.server.flows)
	harness.server.flowsMu.RUnlock()
	if clientFlows != 0 || clientActors != 0 || serverFlows != 0 {
		harness.t.Errorf("flows after shutdown = client %d actors %d server %d", clientFlows, clientActors, serverFlows)
	}
}

func (harness *simulationHarness) assertTransfer(application net.Conn, payload []byte, total uint64) {
	harness.t.Helper()
	if err := simulationRoundTrip(application, payload); err != nil {
		harness.t.Fatalf("%v\nclient status: %#v\nserver status: %#v", err, harness.client.statusRepository.Snapshot(), harness.server.statusRepository.Snapshot())
	}
	harness.assertTransferState(total)
}

func (harness *simulationHarness) assertTransferState(total uint64) {
	harness.t.Helper()
	waitFor(harness.t, 5*time.Second, func() bool {
		client := harness.client.statusRepository.Snapshot()
		server := harness.server.statusRepository.Snapshot()
		if len(client.Flows) != 1 || len(server.Flows) != 1 {
			return false
		}
		clientFlow, serverFlow := client.Flows[0], server.Flows[0]
		clientConnections := make(map[string]struct{}, len(client.Sessions))
		for _, session := range client.Sessions {
			if session.ConnectionID != "" {
				clientConnections[session.ConnectionID] = struct{}{}
			}
		}
		sharedConnection := false
		for _, session := range server.Sessions {
			if _, ok := clientConnections[session.ConnectionID]; ok && session.ConnectionID != "" {
				sharedConnection = true
				break
			}
		}
		_, clientPreferredKnown := clientConnections[clientFlow.PreferredConnectionID]
		return harness.targetBytes.Load() == total &&
			sharedConnection && clientFlow.FlowID != "" && clientFlow.FlowID == serverFlow.FlowID &&
			(clientFlow.PreferredConnectionID == "" || clientPreferredKnown) &&
			clientFlow.State == statusapi.FlowRelaying && serverFlow.State == statusapi.FlowRelaying &&
			clientFlow.TxAllocatedOffset == total && clientFlow.TxAcknowledged == total && clientFlow.RxWrittenOffset == total &&
			serverFlow.TxAllocatedOffset == total && serverFlow.TxAcknowledged == total && serverFlow.RxWrittenOffset == total
	}, fmt.Sprintf("exact bidirectional transfer at offset %d", total))
	if got := harness.targetBytes.Load(); got != total {
		harness.t.Fatalf("target bytes = %d, want %d", got, total)
	}
	if got := harness.targetTotal.Load(); got != 1 {
		harness.t.Fatalf("target connections = %d, want 1", got)
	}
}

func (harness *simulationHarness) waitSessionAddresses(addresses ...netip.Addr) {
	harness.t.Helper()
	want := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		want[address.Unmap().String()] = struct{}{}
	}
	waitFor(harness.t, 5*time.Second, func() bool {
		snapshot := harness.client.statusRepository.Snapshot()
		if len(snapshot.Sessions) != len(want) {
			return false
		}
		for _, session := range snapshot.Sessions {
			if session.State != statusapi.SessionReady {
				return false
			}
			if _, ok := want[session.LocalAddress]; !ok {
				return false
			}
		}
		return true
	}, "simulated session addresses")
}

func (harness *simulationHarness) waitSessionReason(address netip.Addr, reason statusapi.TransitionReason) {
	harness.t.Helper()
	want := address.Unmap().String()
	waitFor(harness.t, 5*time.Second, func() bool {
		for _, session := range harness.client.statusRepository.Snapshot().Sessions {
			if session.LocalAddress == want {
				return session.State == statusapi.SessionReady && session.Reason == reason
			}
		}
		return false
	}, fmt.Sprintf("session %s reason %d", want, reason))
}

func (harness *simulationHarness) waitSessions(want int) {
	harness.t.Helper()
	waitFor(harness.t, 5*time.Second, func() bool { return len(harness.client.readySessions()) == want }, fmt.Sprintf("%d simulated sessions", want))
}

func (harness *simulationHarness) tickPaths() {
	harness.t.Helper()
	select {
	case harness.pathTicks <- time.Unix(1, 0):
	case <-time.After(time.Second):
		harness.t.Fatal("simulated path tick blocked")
	}
}

func (harness *simulationHarness) openApplication() net.Conn {
	harness.t.Helper()
	connection, err := net.DialTimeout("tcp", harness.client.configuration.SOCKSListen, time.Second)
	if err != nil {
		harness.t.Fatal(err)
	}
	_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
	if harness.client.configuration.SOCKSAuth == nil {
		if _, err := connection.Write([]byte{5, 1, 0}); err != nil {
			harness.t.Fatal(err)
		}
		method := make([]byte, 2)
		if _, err := io.ReadFull(connection, method); err != nil || method[0] != 5 || method[1] != 0 {
			harness.t.Fatalf("simulation no-auth method = %v, %v", method, err)
		}
	} else {
		writeSOCKSAuthentication(harness.t, connection, daemonSOCKSUsername, daemonSOCKSPassword, true)
	}
	host, portText, _ := net.SplitHostPort(harness.target.Addr().String())
	address := net.ParseIP(host).To4()
	port, _ := strconv.ParseUint(portText, 10, 16)
	request := make([]byte, 10)
	request[0], request[1], request[3] = 5, 1, 1
	copy(request[4:8], address)
	binary.BigEndian.PutUint16(request[8:], uint16(port))
	if _, err := connection.Write(request); err != nil {
		harness.t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(connection, reply); err != nil || reply[1] != 0 {
		harness.t.Fatalf("simulation SOCKS reply = %v, %v", reply, err)
	}
	_ = connection.SetDeadline(time.Time{})
	return connection
}

func (harness *simulationHarness) waitAttachments(want int) {
	harness.t.Helper()
	waitFor(harness.t, 5*time.Second, func() bool {
		count := 0
		for _, session := range harness.client.readySessions() {
			count += len(session.allAttachments())
		}
		return count == want
	}, fmt.Sprintf("%d simulated attachments", want))
}

func simulationRoundTrip(connection net.Conn, payload []byte) error {
	_ = connection.SetDeadline(time.Now().Add(15 * time.Second))
	writeResult := make(chan error, 1)
	go func() { writeResult <- writeAll(connection, payload) }()
	received := make([]byte, len(payload))
	readBytes, readErr := io.ReadFull(connection, received)
	writeErr := <-writeResult
	_ = connection.SetDeadline(time.Time{})
	if writeErr != nil || readErr != nil || !bytes.Equal(received, payload) {
		return fmt.Errorf("simulation round trip: write=%v read=%v bytes=%d/%d", writeErr, readErr, readBytes, len(payload))
	}
	return nil
}

func startSimulationRoundTrip(connection net.Conn, payload []byte) <-chan error {
	result := make(chan error, 1)
	go func() { result <- simulationRoundTrip(connection, payload) }()
	return result
}

func awaitSimulationRoundTrip(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("simulated round trip did not complete")
	}
}

func waitSimulatedHeld(t *testing.T, controller *simulatedFaultController) {
	t.Helper()
	watchdog := time.NewTimer(5 * time.Second)
	defer watchdog.Stop()
	for controller.heldCount() == 0 {
		select {
		case <-controller.heldEvents():
		case <-watchdog.C:
			t.Fatal("simulated fault did not hold a frame")
		}
	}
}

func releaseRateLimitedRoundTrip(t *testing.T, result <-chan error, controllers ...*simulatedFaultController) {
	t.Helper()
	if len(controllers) != 4 {
		t.Fatalf("rate controllers = %d, want 4", len(controllers))
	}
	cleanup := func() {
		for _, controller := range controllers {
			controller.set(simulatedFault{kind: simulatedFaultNormal})
		}
		for _, controller := range controllers {
			if err := controller.releaseAll(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	watchdog := time.NewTimer(20 * time.Second)
	defer watchdog.Stop()
	for {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
			cleanup()
			return
		default:
		}
		released := false
		for _, controller := range controllers {
			if controller.heldCount() == 0 {
				continue
			}
			if err := controller.releaseOne(context.Background()); err != nil {
				t.Fatal(err)
			}
			released = true
		}
		if released {
			continue
		}
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
			cleanup()
			return
		case <-controllers[0].heldEvents():
		case <-controllers[1].heldEvents():
		case <-controllers[2].heldEvents():
		case <-controllers[3].heldEvents():
		case <-watchdog.C:
			t.Fatal("rate-limited simulated round trip did not complete")
		}
	}
}

func decodeSimulationServer(t *testing.T, relayAddress string) config.Server {
	t.Helper()
	value, err := config.DecodeServer([]byte(fmt.Sprintf(`transport: {type: tcp, listen: %q}
principals:
  - id: client-01
    psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
limits: {sessions: 8, sessions_per_principal: 8, auth_in_progress: 8}
deadlines: {drain_cleanup: "1s"}
`, relayAddress)))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func decodeSimulationClient(t *testing.T, relayAddress, socksAddress string, mode protocol.DeliveryMode, selection protocol.PathSelection) config.Client {
	t.Helper()
	delivery := "delivery: {mode: redundant}"
	if mode == protocol.DeliveryAdaptive {
		if selection == protocol.PathDistributed {
			delivery = `delivery:
  mode: adaptive
  path_selection: distributed
  constraints:
    max_delivery_delay: "10s"
    max_delay_gap: "10s"
    constraint_fallback: pause`
		} else {
			delivery = "delivery: {mode: adaptive, path_selection: fastest}"
		}
	}
	value, err := config.DecodeClient([]byte(fmt.Sprintf(`socks_listen: %q
socks_auth: {username: %q, password: %q}
transport: {type: tcp, address: %q}
%s
interfaces: {include: ["sim-*"]}
principal_id: client-01
psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
limits: {sessions: 2, auth_in_progress: 2}
deadlines: {drain_cleanup: "1s"}
`, socksAddress, daemonSOCKSUsername, daemonSOCKSPassword, relayAddress, delivery)))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestSimulatedFullChainDeliveryModes(t *testing.T) {
	cases := []struct {
		name      string
		mode      protocol.DeliveryMode
		selection protocol.PathSelection
	}{
		{name: "redundant", mode: protocol.DeliveryRedundant, selection: protocol.PathNone},
		{name: "adaptive-fastest", mode: protocol.DeliveryAdaptive, selection: protocol.PathFastest},
		{name: "adaptive-distributed", mode: protocol.DeliveryAdaptive, selection: protocol.PathDistributed},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newSimulationHarness(t, testCase.mode, testCase.selection)
			defer harness.close()
			application := harness.openApplication()
			defer application.Close()
			harness.waitAttachments(2)
			if testCase.selection == protocol.PathDistributed {
				want := protocol.DeliveryConstraints{
					MaxDeliveryDelay: 10 * time.Second,
					MaxDelayGap:      10 * time.Second,
					Fallback:         protocol.DeliveryFallbackPause,
				}
				if got := harness.client.configuration.Delivery.Constraints; got != want {
					t.Fatalf("client distributed constraints = %#v, want %#v", got, want)
				}
				harness.assertServerDeliveryConstraints(want)
			}
			if testCase.selection == protocol.PathFastest {
				setSimulationPathQualities(t, harness)
			}
			beforeTargets := harness.targetTotal.Load()
			payload := bytes.Repeat([]byte(testCase.name+"|"), 8<<10)
			if err := simulationRoundTrip(application, payload); err != nil {
				t.Fatalf("%v\nclient status: %#v\nserver status: %#v", err, harness.client.statusRepository.Snapshot(), harness.server.statusRepository.Snapshot())
			}
			if got := harness.targetTotal.Load() - beforeTargets; got != 0 {
				// The target was created by openApplication, before the baseline above.
				t.Fatalf("extra target connections = %d", got)
			}
			uplinkA := harness.network.controller(simulationAddressA, simulatedUplink).count(protocol.TypeData)
			uplinkB := harness.network.controller(simulationAddressB, simulatedUplink).count(protocol.TypeData)
			downlinkA := harness.network.controller(simulationAddressA, simulatedDownlink).count(protocol.TypeData)
			downlinkB := harness.network.controller(simulationAddressB, simulatedDownlink).count(protocol.TypeData)
			switch testCase.mode {
			case protocol.DeliveryRedundant:
				if uplinkA == 0 || uplinkB == 0 || downlinkA == 0 || downlinkB == 0 {
					t.Fatalf("redundant DATA counts = up %d/%d down %d/%d", uplinkA, uplinkB, downlinkA, downlinkB)
				}
			case protocol.DeliveryAdaptive:
				if testCase.selection == protocol.PathFastest && (uplinkA == 0 || uplinkB != 0 || downlinkA == 0 || downlinkB != 0) {
					t.Fatalf("fastest DATA counts = up %d/%d down %d/%d", uplinkA, uplinkB, downlinkA, downlinkB)
				}
			}
			if harness.targetTotal.Load() != 1 {
				t.Fatalf("target connections = %d", harness.targetTotal.Load())
			}
		})
	}
}

func TestSimulatedFullChainDistributedUsesBothLoadedSessions(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryAdaptive, protocol.PathDistributed)
	defer harness.close()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)

	uplinkA := harness.network.controller(simulationAddressA, simulatedUplink)
	uplinkB := harness.network.controller(simulationAddressB, simulatedUplink)
	downlinkA := harness.network.controller(simulationAddressA, simulatedDownlink)
	downlinkB := harness.network.controller(simulationAddressB, simulatedDownlink)
	controllers := []*simulatedFaultController{uplinkA, uplinkB, downlinkA, downlinkB}
	for _, controller := range controllers {
		controller.set(simulatedFault{kind: simulatedFaultStall, frameType: protocol.TypeData, remaining: 1})
	}

	payload := bytes.Repeat([]byte("distributed-load|"), 64<<10)
	result := startSimulationRoundTrip(application, payload)
	for index, controller := range controllers[:2] {
		select {
		case <-controller.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("uplink %d did not receive DATA while the peer was loaded", index)
		}
	}
	uplinkA.resume()
	uplinkB.resume()
	for index, controller := range controllers[2:] {
		select {
		case <-controller.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("downlink %d did not receive DATA while the peer was loaded", index)
		}
	}
	downlinkA.resume()
	downlinkB.resume()
	waitSimulationResult(t, result)
	if harness.targetTotal.Load() != 1 {
		t.Fatalf("target connections = %d", harness.targetTotal.Load())
	}
}

func TestSimulatedFullChainWithoutSOCKSAuthentication(t *testing.T) {
	harness := newSimulationHarnessConfigured(t, protocol.DeliveryAdaptive, protocol.PathFastest, nil, false)
	defer harness.close()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)
	payload := bytes.Repeat([]byte("no-auth|"), 8<<10)
	harness.assertTransfer(application, payload, uint64(len(payload)))
}

func (harness *simulationHarness) assertServerDeliveryConstraints(want protocol.DeliveryConstraints) {
	harness.t.Helper()
	var instance *serverFlow
	harness.server.flowsMu.RLock()
	for _, candidate := range harness.server.flows {
		instance = candidate
		break
	}
	harness.server.flowsMu.RUnlock()
	if instance == nil {
		harness.t.Fatal("server flow missing after SOCKS success")
	}
	instance.mu.Lock()
	got := instance.relay.Snapshot().Policy.Config.Constraints
	instance.mu.Unlock()
	if got != want {
		harness.t.Fatalf("server downlink constraints = %#v, want %#v", got, want)
	}
}

func TestSimulatedFullChainRecoversWhenAllSessionsLoseSameFrame(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryRedundant, protocol.PathNone)
	defer harness.close()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)

	controllers := []*simulatedFaultController{
		harness.network.controller(simulationAddressA, simulatedUplink),
		harness.network.controller(simulationAddressB, simulatedUplink),
		harness.network.controller(simulationAddressA, simulatedDownlink),
		harness.network.controller(simulationAddressB, simulatedDownlink),
	}
	for _, controller := range controllers {
		controller.set(simulatedFault{
			kind: simulatedFaultDrop, frameType: protocol.TypeData, matchDataOffset: true, dataOffset: 0, remaining: 1,
		})
	}
	first := bytes.Repeat([]byte("all-data-copies-lost|"), 8<<10)
	total := uint64(len(first))
	result := startSimulationRoundTrip(application, first)
	for index, controller := range controllers[:2] {
		waitFor(t, 5*time.Second, func() bool { return controller.matchCount() == 1 }, fmt.Sprintf("uplink DATA drop %d", index))
	}
	forceSimulationClientRetry(t, harness)
	for index, controller := range controllers[2:] {
		waitFor(t, 5*time.Second, func() bool { return controller.matchCount() == 1 }, fmt.Sprintf("downlink DATA drop %d", index))
	}
	forceSimulationServerRetry(t, harness)
	awaitSimulationRoundTrip(t, result)
	harness.assertTransferState(total)
	for index, controller := range controllers {
		if matches := controller.matchCount(); matches != 1 {
			t.Fatalf("DATA drop controller %d matches = %d, want 1", index, matches)
		}
	}

	for _, controller := range controllers {
		controller.set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeACK, remaining: -1})
	}
	second := bytes.Repeat([]byte("all-ack-copies-lost|"), 8<<10)
	total += uint64(len(second))
	result = startSimulationRoundTrip(application, second)
	awaitSimulationRoundTrip(t, result)
	for index, controller := range controllers {
		if matches := controller.matchCount(); matches == 0 {
			t.Fatalf("ACK drop controller %d did not match", index)
		}
		controller.set(simulatedFault{kind: simulatedFaultNormal})
	}
	forceSimulationClientRetry(t, harness)
	forceSimulationServerRetry(t, harness)
	harness.assertTransferState(total)
	waitFor(t, 5*time.Second, func() bool {
		client := harness.client.statusRepository.Snapshot()
		server := harness.server.statusRepository.Snapshot()
		return len(client.Flows) == 1 && client.Flows[0].RetransmittedBytes != 0 &&
			len(server.Flows) == 1 && server.Flows[0].RetransmittedBytes != 0
	}, "bidirectional retransmission counters")
}

func TestSimulatedFullChainFrameFaultMatrix(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryRedundant, protocol.PathNone)
	defer harness.close()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)

	uplinkA := harness.network.controller(simulationAddressA, simulatedUplink)
	uplinkB := harness.network.controller(simulationAddressB, simulatedUplink)
	downlinkA := harness.network.controller(simulationAddressA, simulatedDownlink)
	downlinkB := harness.network.controller(simulationAddressB, simulatedDownlink)
	controllers := []*simulatedFaultController{uplinkA, uplinkB, downlinkA, downlinkB}
	var total uint64

	for _, controller := range controllers {
		controller.set(simulatedFault{kind: simulatedFaultDuplicate, frameType: protocol.TypeData, remaining: 1})
	}
	duplicate := bytes.Repeat([]byte("duplicate|"), 16<<10)
	total += uint64(len(duplicate))
	harness.assertTransfer(application, duplicate, total)
	for index, controller := range controllers {
		if matches := controller.matchCount(); matches != 1 {
			t.Fatalf("duplicate controller %d matches = %d, want 1", index, matches)
		}
	}

	for _, controller := range controllers {
		controller.set(simulatedFault{kind: simulatedFaultReorder, frameType: protocol.TypeData, remaining: 2})
	}
	reordered := bytes.Repeat([]byte("reordered|"), 64<<10)
	total += uint64(len(reordered))
	harness.assertTransfer(application, reordered, total)
	for index, controller := range controllers {
		if matches := controller.matchCount(); matches != 2 {
			t.Fatalf("reorder controller %d matches = %d, want 2", index, matches)
		}
	}

	for _, controller := range controllers {
		controller.set(simulatedFault{kind: simulatedFaultDelay, frameType: protocol.TypeData, remaining: 1})
	}
	delayed := bytes.Repeat([]byte("delayed|"), 16<<10)
	result := startSimulationRoundTrip(application, delayed)
	waitSimulatedHeld(t, uplinkA)
	waitSimulatedHeld(t, uplinkB)
	if err := uplinkA.releaseOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := uplinkB.releaseOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitSimulatedHeld(t, downlinkA)
	waitSimulatedHeld(t, downlinkB)
	if err := downlinkA.releaseOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := downlinkB.releaseOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitSimulationRoundTrip(t, result)
	total += uint64(len(delayed))
	harness.assertTransferState(total)

	for _, controller := range controllers {
		controller.set(simulatedFault{kind: simulatedFaultRateLimit, frameType: protocol.TypeData, remaining: -1})
	}
	rateLimited := bytes.Repeat([]byte("rate-limited|"), 32<<10)
	result = startSimulationRoundTrip(application, rateLimited)
	releaseRateLimitedRoundTrip(t, result, controllers...)
	total += uint64(len(rateLimited))
	harness.assertTransferState(total)
	for _, controller := range controllers {
		controller.set(simulatedFault{kind: simulatedFaultNormal})
	}

	uplinkA.set(simulatedFault{kind: simulatedFaultStall, frameType: protocol.TypeData, remaining: 1})
	downlinkA.set(simulatedFault{kind: simulatedFaultStall, frameType: protocol.TypeData, remaining: 1})
	stalled := bytes.Repeat([]byte("single-path-stall|"), 16<<10)
	result = startSimulationRoundTrip(application, stalled)
	select {
	case <-uplinkA.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("uplink write did not enter stall")
	}
	select {
	case <-downlinkA.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("downlink write did not enter stall")
	}
	uplinkA.resume()
	downlinkA.resume()
	awaitSimulationRoundTrip(t, result)
	total += uint64(len(stalled))
	harness.assertTransferState(total)
}

func TestSimulatedFullChainBlackholeCloseAndRecovery(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryRedundant, protocol.PathNone)
	defer harness.close()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)

	uplinkA := harness.network.controller(simulationAddressA, simulatedUplink)
	downlinkB := harness.network.controller(simulationAddressB, simulatedDownlink)
	uplinkA.set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeData, remaining: -1})
	downlinkB.set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeData, remaining: -1})
	first := bytes.Repeat([]byte("cross-direction-blackhole|"), 8<<10)
	total := uint64(len(first))
	harness.assertTransfer(application, first, total)
	if uplinkA.matchCount() == 0 || downlinkB.matchCount() == 0 {
		t.Fatalf("blackhole matches = uplink %d downlink %d", uplinkA.matchCount(), downlinkB.matchCount())
	}
	uplinkA.set(simulatedFault{kind: simulatedFaultNormal})
	downlinkB.set(simulatedFault{kind: simulatedFaultNormal})

	beforeDials := harness.network.nextPort.Load()
	uplinkA.set(simulatedFault{kind: simulatedFaultClose, frameType: protocol.TypeData, remaining: 1})
	second := bytes.Repeat([]byte("connection-close|"), 8<<10)
	total += uint64(len(second))
	result := startSimulationRoundTrip(application, second)
	waitFor(t, 5*time.Second, func() bool { return uplinkA.matchCount() == 1 }, "closed simulated connection")
	forceSimulationClientRetry(t, harness)
	awaitSimulationRoundTrip(t, result)
	harness.assertTransferState(total)
	harness.enumerator.set(simulatedInterface(2, "sim-b", simulationAddressB))
	harness.tickPaths()
	harness.waitSessions(1)
	harness.waitAttachments(1)
	waitFor(t, 5*time.Second, func() bool {
		interfaces := harness.client.statusRepository.Snapshot().Interfaces
		return len(interfaces) == 1 && interfaces[0].Name == "sim-b"
	}, "closed path removal refresh")
	harness.enumerator.set(
		simulatedInterface(1, "sim-a", simulationAddressA),
		simulatedInterface(2, "sim-b", simulationAddressB),
	)
	harness.tickPaths()
	waitFor(t, 5*time.Second, func() bool { return harness.network.nextPort.Load() > beforeDials }, "replacement simulated connection")
	harness.waitSessions(2)
	harness.waitAttachments(2)
	harness.waitSessionReason(simulationAddressA, statusapi.ReasonPathAdded)

	third := bytes.Repeat([]byte("after-recovery|"), 8<<10)
	total += uint64(len(third))
	harness.assertTransfer(application, third, total)
}

func TestSimulatedFullChainDynamicInterfaces(t *testing.T) {
	harness := newSimulationHarness(t, protocol.DeliveryRedundant, protocol.PathNone)
	defer harness.close()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)
	harness.waitSessionAddresses(simulationAddressA, simulationAddressB)
	var total uint64
	transfer := func(label string) {
		payload := bytes.Repeat([]byte(label+"|"), 4<<10)
		total += uint64(len(payload))
		harness.assertTransfer(application, payload, total)
	}
	transfer("initial-paths")

	harness.enumerator.set(simulatedInterface(1, "sim-a", simulationAddressA))
	harness.tickPaths()
	harness.waitSessions(1)
	harness.waitAttachments(1)
	harness.waitSessionAddresses(simulationAddressA)
	transfer("path-deleted")

	harness.enumerator.set(
		simulatedInterface(1, "sim-a", simulationAddressA),
		simulatedInterface(2, "sim-b", simulationAddressC),
	)
	harness.tickPaths()
	harness.waitSessions(2)
	harness.waitAttachments(2)
	harness.waitSessionAddresses(simulationAddressA, simulationAddressC)
	harness.waitSessionReason(simulationAddressC, statusapi.ReasonPathAdded)
	transfer("path-added-with-new-address")

	harness.enumerator.set(
		simulatedInterface(1, "sim-a", simulationAddressA),
		simulatedInterface(2, "ignored-b", simulationAddressC),
	)
	harness.tickPaths()
	harness.waitSessions(1)
	harness.waitAttachments(1)
	harness.waitSessionAddresses(simulationAddressA)
	waitFor(t, 5*time.Second, func() bool {
		for _, networkInterface := range harness.client.statusRepository.Snapshot().Interfaces {
			if networkInterface.Index == 2 {
				return networkInterface.Name == "ignored-b" && networkInterface.Reason == statusapi.InterfaceNotIncluded
			}
		}
		return false
	}, "renamed interface rejection")
	transfer("path-renamed-out-of-filter")

	down := simulatedInterface(2, "sim-b", simulationAddressC)
	down.Up = false
	harness.enumerator.set(simulatedInterface(1, "sim-a", simulationAddressA), down)
	harness.tickPaths()
	waitFor(t, 5*time.Second, func() bool {
		for _, networkInterface := range harness.client.statusRepository.Snapshot().Interfaces {
			if networkInterface.Index == 2 {
				return networkInterface.Name == "sim-b" && networkInterface.Reason == statusapi.InterfaceDown
			}
		}
		return false
	}, "interface-down reason")
	transfer("path-down")

	noAddress := pathcore.Interface{Index: 2, Name: "sim-b", Up: true}
	harness.enumerator.set(simulatedInterface(1, "sim-a", simulationAddressA), noAddress)
	harness.tickPaths()
	waitFor(t, 5*time.Second, func() bool {
		for _, networkInterface := range harness.client.statusRepository.Snapshot().Interfaces {
			if networkInterface.Index == 2 {
				return networkInterface.Reason == statusapi.InterfaceNoAddress
			}
		}
		return false
	}, "address-loss reason")
	transfer("path-address-lost")

	harness.enumerator.set(
		simulatedInterface(1, "sim-a", simulationAddressA),
		simulatedInterface(2, "sim-b", simulationAddressB),
	)
	harness.tickPaths()
	harness.waitSessions(2)
	harness.waitAttachments(2)
	harness.waitSessionAddresses(simulationAddressA, simulationAddressB)
	harness.waitSessionReason(simulationAddressB, statusapi.ReasonPathAdded)
	transfer("path-address-restored")
}
