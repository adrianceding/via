package daemon

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

const maxSimulatedHeldFrames = transport.V1OutputQueueFrameLimit

type simulatedDirection uint8

const (
	simulatedUplink simulatedDirection = iota + 1
	simulatedDownlink
)

type simulatedFaultKind uint8

const (
	simulatedFaultNormal simulatedFaultKind = iota
	simulatedFaultDrop
	simulatedFaultDuplicate
	simulatedFaultHold
	simulatedFaultDelay
	simulatedFaultRateLimit
	simulatedFaultReorder
	simulatedFaultStall
	simulatedFaultClose
)

type simulatedFault struct {
	kind            simulatedFaultKind
	frameType       protocol.Type
	matchDataOffset bool
	dataOffset      uint64
	remaining       int
}

type simulatedNetwork struct {
	capabilities transport.Capabilities
	listener     *simulatedListener

	mu       sync.Mutex
	paths    map[netip.Addr]*simulatedPath
	nextPort atomic.Uint32
}

type simulatedPath struct {
	uplink   *simulatedFaultController
	downlink *simulatedFaultController
	mu       sync.Mutex
	pairs    []*simulatedConnectionPair
}

func newSimulatedNetwork(t *testing.T, addresses ...netip.Addr) *simulatedNetwork {
	t.Helper()
	capabilities, err := transport.NewCapabilities(transport.CapabilitySpec{
		MaxEncodedFrame: protocol.MaxFrameSize, Reliable: true, Ordered: true, HalfClose: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	network := &simulatedNetwork{
		capabilities: capabilities,
		listener:     newSimulatedListener(),
		paths:        make(map[netip.Addr]*simulatedPath, len(addresses)),
	}
	for _, address := range addresses {
		network.paths[address.Unmap()] = &simulatedPath{
			uplink: newSimulatedFaultController(), downlink: newSimulatedFaultController(),
		}
	}
	return network
}

func (network *simulatedNetwork) Capabilities() transport.Capabilities { return network.capabilities }

func (network *simulatedNetwork) NewDialer(options transport.DialOptions) (transport.Dialer, error) {
	if options.QueueLimits != transport.V1QueueLimits() {
		return nil, transport.ErrInvalidQueueLimits
	}
	host, _, err := net.SplitHostPort(options.LocalEndpoint)
	if err != nil {
		return nil, transport.ErrInvalidTCPConfig
	}
	address, err := netip.ParseAddr(host)
	if err != nil {
		return nil, transport.ErrInvalidTCPConfig
	}
	address = address.Unmap()
	network.mu.Lock()
	path := network.paths[address]
	network.mu.Unlock()
	if path == nil {
		return nil, transport.ErrInvalidTCPConfig
	}
	return simulatedDialer{network: network, path: path, localAddress: address, remoteEndpoint: options.RemoteEndpoint}, nil
}

func (network *simulatedNetwork) NewListener(options transport.ListenOptions) (transport.Listener, error) {
	if options.QueueLimits != transport.V1QueueLimits() {
		return nil, transport.ErrInvalidQueueLimits
	}
	return network.listener, nil
}

func (network *simulatedNetwork) controller(address netip.Addr, direction simulatedDirection) *simulatedFaultController {
	network.mu.Lock()
	path := network.paths[address.Unmap()]
	network.mu.Unlock()
	if path == nil {
		return nil
	}
	if direction == simulatedUplink {
		return path.uplink
	}
	return path.downlink
}

func (network *simulatedNetwork) closePath(address netip.Addr) {
	network.mu.Lock()
	path := network.paths[address.Unmap()]
	network.mu.Unlock()
	if path == nil {
		return
	}
	path.mu.Lock()
	pairs := append([]*simulatedConnectionPair(nil), path.pairs...)
	path.mu.Unlock()
	for _, pair := range pairs {
		pair.close()
	}
}

func (network *simulatedNetwork) activePairs() int {
	network.mu.Lock()
	paths := make([]*simulatedPath, 0, len(network.paths))
	for _, path := range network.paths {
		paths = append(paths, path)
	}
	network.mu.Unlock()
	active := 0
	for _, path := range paths {
		path.mu.Lock()
		for _, pair := range path.pairs {
			if !pair.closed.Load() {
				active++
			}
		}
		path.mu.Unlock()
	}
	return active
}

func (network *simulatedNetwork) heldFrames() int {
	network.mu.Lock()
	paths := make([]*simulatedPath, 0, len(network.paths))
	for _, path := range network.paths {
		paths = append(paths, path)
	}
	network.mu.Unlock()
	held := 0
	for _, path := range paths {
		held += path.uplink.heldCount()
		held += path.downlink.heldCount()
	}
	return held
}

type simulatedDialer struct {
	network        *simulatedNetwork
	path           *simulatedPath
	localAddress   netip.Addr
	remoteEndpoint string
}

func (dialer simulatedDialer) Dial(ctx context.Context) (transport.Connection, error) {
	port := dialer.network.nextPort.Add(1)
	if port == 0 || port > 65535 {
		return nil, ErrWireCapacity
	}
	pair := &simulatedConnectionPair{done: make(chan struct{})}
	client := &simulatedConnection{
		capabilities: dialer.network.capabilities, path: dialer.path, direction: simulatedUplink,
		localEndpoint: netip.AddrPortFrom(dialer.localAddress, uint16(port)).String(), remoteEndpoint: dialer.remoteEndpoint,
		inbound: make(chan []byte, transport.V1OutputQueueFrameLimit), pair: pair,
	}
	server := &simulatedConnection{
		capabilities: dialer.network.capabilities, path: dialer.path, direction: simulatedDownlink,
		localEndpoint: dialer.remoteEndpoint, remoteEndpoint: client.localEndpoint,
		inbound: make(chan []byte, transport.V1OutputQueueFrameLimit), pair: pair,
	}
	client.peer, server.peer = server, client
	dialer.path.mu.Lock()
	active := dialer.path.pairs[:0]
	for _, existing := range dialer.path.pairs {
		if !existing.closed.Load() {
			active = append(active, existing)
		}
	}
	if len(active) >= int(transport.V1OutputQueueFrameLimit) {
		dialer.path.pairs = active
		dialer.path.mu.Unlock()
		pair.close()
		return nil, ErrWireCapacity
	}
	dialer.path.pairs = append(active, pair)
	dialer.path.mu.Unlock()
	select {
	case dialer.network.listener.connections <- server:
		return client, nil
	case <-ctx.Done():
		pair.close()
		return nil, ctx.Err()
	case <-dialer.network.listener.done:
		pair.close()
		return nil, transport.ErrClosed
	}
}

type simulatedListener struct {
	connections chan transport.Connection
	done        chan struct{}
	closeOnce   sync.Once
}

func newSimulatedListener() *simulatedListener {
	return &simulatedListener{
		connections: make(chan transport.Connection, transport.V1OutputQueueFrameLimit),
		done:        make(chan struct{}),
	}
}

func (listener *simulatedListener) Accept(ctx context.Context) (transport.Connection, error) {
	select {
	case connection := <-listener.connections:
		return connection, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-listener.done:
		return nil, transport.ErrClosed
	}
}

func (listener *simulatedListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.done) })
	return nil
}

type simulatedConnectionPair struct {
	done      chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
}

func (pair *simulatedConnectionPair) close() {
	pair.closeOnce.Do(func() {
		pair.closed.Store(true)
		close(pair.done)
	})
}

type simulatedConnection struct {
	capabilities   transport.Capabilities
	path           *simulatedPath
	direction      simulatedDirection
	localEndpoint  string
	remoteEndpoint string
	inbound        chan []byte
	pair           *simulatedConnectionPair
	peer           *simulatedConnection
	writeMu        sync.Mutex
}

func (connection *simulatedConnection) Capabilities() transport.Capabilities {
	return connection.capabilities
}
func (*simulatedConnection) QueueLimits() transport.QueueLimits { return transport.V1QueueLimits() }
func (connection *simulatedConnection) LocalEndpoint() string   { return connection.localEndpoint }
func (connection *simulatedConnection) RemoteEndpoint() string  { return connection.remoteEndpoint }

func (connection *simulatedConnection) ReadFrame(ctx context.Context) ([]byte, error) {
	select {
	case encoded := <-connection.inbound:
		return append([]byte(nil), encoded...), nil
	case <-connection.pair.done:
		return nil, transport.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (connection *simulatedConnection) WriteFrame(ctx context.Context, request transport.WriteRequest) error {
	if err := request.Validate(connection.capabilities); err != nil {
		return err
	}
	connection.writeMu.Lock()
	defer connection.writeMu.Unlock()
	select {
	case <-connection.pair.done:
		return transport.ErrClosed
	default:
	}
	controller := connection.path.uplink
	if connection.direction == simulatedDownlink {
		controller = connection.path.downlink
	}
	return controller.transmit(ctx, append([]byte(nil), request.Encoded...), connection.peer)
}

func TestSimulatedTransportSerializesWritesPerConnection(t *testing.T) {
	address := netip.MustParseAddr("192.0.2.12")
	network := newSimulatedNetwork(t, address)
	dialer, err := network.NewDialer(transport.DialOptions{
		RemoteEndpoint: "127.0.0.1:9443", LocalEndpoint: "192.0.2.12:0", QueueLimits: transport.V1QueueLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstClient, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstServer, err := network.listener.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondClient, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondServer, err := network.listener.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer firstClient.Close()
	defer secondClient.Close()

	controller := network.controller(address, simulatedUplink)
	dataA := encodeSimulationMessage(t, protocol.Data{FlowID: protocol.FlowID{1}, Bytes: []byte("a")})
	dataB := encodeSimulationMessage(t, protocol.Data{FlowID: protocol.FlowID{1}, Offset: 1, Bytes: []byte("b")})
	dataC := encodeSimulationMessage(t, protocol.Data{FlowID: protocol.FlowID{2}, Bytes: []byte("c")})
	controller.set(simulatedFault{kind: simulatedFaultStall, frameType: protocol.TypeData, remaining: 1})

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- firstClient.WriteFrame(context.Background(), transport.WriteRequest{Class: transport.FrameData, Encoded: dataA})
	}()
	select {
	case <-controller.entered:
	case <-time.After(time.Second):
		t.Fatal("first write did not enter simulated stall")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- firstClient.WriteFrame(context.Background(), transport.WriteRequest{Class: transport.FrameData, Encoded: dataB})
	}()

	if err := secondClient.WriteFrame(context.Background(), transport.WriteRequest{Class: transport.FrameData, Encoded: dataC}); err != nil {
		t.Fatalf("independent connection write blocked: %v", err)
	}
	if got := readSimulationFrame(t, secondServer); !bytes.Equal(got, dataC) {
		t.Fatal("independent connection frame changed")
	}

	controller.resume()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got := readSimulationFrame(t, firstServer); !bytes.Equal(got, dataA) {
		t.Fatal("first serialized frame changed")
	}
	if got := readSimulationFrame(t, firstServer); !bytes.Equal(got, dataB) {
		t.Fatal("second serialized frame changed")
	}
}

func (*simulatedConnection) CloseWrite() error { return nil }
func (connection *simulatedConnection) Close() error {
	connection.pair.close()
	return nil
}

type simulatedHeldFrame struct {
	encoded     []byte
	destination *simulatedConnection
}

type simulatedFaultController struct {
	mu        sync.Mutex
	fault     simulatedFault
	held      []simulatedHeldFrame
	heldReady chan struct{}
	stall     chan struct{}
	entered   chan struct{}
	counts    map[protocol.Type]uint64
	matches   uint64
}

func newSimulatedFaultController() *simulatedFaultController {
	return &simulatedFaultController{
		fault: simulatedFault{kind: simulatedFaultNormal}, counts: make(map[protocol.Type]uint64, 16),
		heldReady: make(chan struct{}, 1),
	}
}

func (controller *simulatedFaultController) count(frameType protocol.Type) uint64 {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.counts[frameType]
}

func (controller *simulatedFaultController) matchCount() uint64 {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.matches
}

func (controller *simulatedFaultController) heldCount() int {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return len(controller.held)
}

func (controller *simulatedFaultController) heldEvents() <-chan struct{} {
	return controller.heldReady
}

func (controller *simulatedFaultController) set(fault simulatedFault) {
	controller.mu.Lock()
	if controller.stall != nil {
		close(controller.stall)
	}
	controller.fault = fault
	controller.matches = 0
	controller.stall = nil
	controller.entered = nil
	if fault.kind == simulatedFaultStall {
		controller.stall = make(chan struct{})
		controller.entered = make(chan struct{}, 1)
	}
	controller.mu.Unlock()
}

func (controller *simulatedFaultController) resume() {
	controller.mu.Lock()
	if controller.stall != nil {
		close(controller.stall)
		controller.stall = nil
	}
	controller.fault = simulatedFault{kind: simulatedFaultNormal}
	controller.mu.Unlock()
}

func (controller *simulatedFaultController) releaseOne(ctx context.Context) error {
	controller.mu.Lock()
	if len(controller.held) == 0 {
		controller.mu.Unlock()
		return nil
	}
	frame := controller.held[0]
	copy(controller.held, controller.held[1:])
	controller.held = controller.held[:len(controller.held)-1]
	controller.mu.Unlock()
	return deliverSimulated(ctx, frame.encoded, frame.destination)
}

func (controller *simulatedFaultController) releaseAll(ctx context.Context) error {
	for {
		controller.mu.Lock()
		empty := len(controller.held) == 0
		controller.mu.Unlock()
		if empty {
			return nil
		}
		if err := controller.releaseOne(ctx); err != nil {
			return err
		}
	}
}

func (controller *simulatedFaultController) transmit(ctx context.Context, encoded []byte, destination *simulatedConnection) error {
	frame, message, err := protocol.DecodeEncodedFrame(encoded)
	if err != nil {
		return err
	}
	controller.mu.Lock()
	controller.counts[frame.Type]++
	fault := controller.fault
	matched := fault.frameType == 0 || fault.frameType == frame.Type
	if matched && fault.matchDataOffset {
		data, ok := message.(protocol.Data)
		matched = ok && data.Offset == fault.dataOffset
	}
	if !matched || fault.remaining == 0 && fault.kind != simulatedFaultNormal {
		controller.mu.Unlock()
		return deliverSimulated(ctx, encoded, destination)
	}
	if fault.kind != simulatedFaultNormal {
		controller.matches++
	}
	if fault.remaining > 0 {
		controller.fault.remaining--
	}
	switch fault.kind {
	case simulatedFaultNormal:
		controller.mu.Unlock()
		return deliverSimulated(ctx, encoded, destination)
	case simulatedFaultDrop:
		controller.mu.Unlock()
		return nil
	case simulatedFaultDuplicate:
		controller.mu.Unlock()
		if err := deliverSimulated(ctx, encoded, destination); err != nil {
			return err
		}
		return deliverSimulated(ctx, encoded, destination)
	case simulatedFaultHold, simulatedFaultDelay, simulatedFaultRateLimit:
		if len(controller.held) >= int(maxSimulatedHeldFrames) {
			controller.mu.Unlock()
			return ErrWireCapacity
		}
		controller.held = append(controller.held, simulatedHeldFrame{encoded: encoded, destination: destination})
		select {
		case controller.heldReady <- struct{}{}:
		default:
		}
		controller.mu.Unlock()
		return nil
	case simulatedFaultReorder:
		if len(controller.held) == 0 {
			controller.held = append(controller.held, simulatedHeldFrame{encoded: encoded, destination: destination})
			controller.mu.Unlock()
			return nil
		}
		first := controller.held[0]
		controller.held = controller.held[:0]
		controller.mu.Unlock()
		if err := deliverSimulated(ctx, encoded, destination); err != nil {
			return err
		}
		return deliverSimulated(ctx, first.encoded, first.destination)
	case simulatedFaultStall:
		stall := controller.stall
		entered := controller.entered
		controller.mu.Unlock()
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-stall:
			return deliverSimulated(ctx, encoded, destination)
		case <-ctx.Done():
			return ctx.Err()
		case <-destination.pair.done:
			return transport.ErrClosed
		}
	case simulatedFaultClose:
		controller.mu.Unlock()
		destination.pair.close()
		return transport.ErrClosed
	default:
		controller.mu.Unlock()
		return ErrWireProtocol
	}
}

func deliverSimulated(ctx context.Context, encoded []byte, destination *simulatedConnection) error {
	select {
	case destination.inbound <- append([]byte(nil), encoded...):
		return nil
	case <-destination.pair.done:
		return transport.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSimulatedTransportFrameFaultsAndBounds(t *testing.T) {
	address := netip.MustParseAddr("192.0.2.10")
	network := newSimulatedNetwork(t, address)
	dialer, err := network.NewDialer(transport.DialOptions{
		RemoteEndpoint: "127.0.0.1:9443", LocalEndpoint: "192.0.2.10:0", QueueLimits: transport.V1QueueLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server, err := network.listener.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	controller := network.controller(address, simulatedUplink)
	dataA := encodeSimulationMessage(t, protocol.Data{FlowID: protocol.FlowID{1}, Bytes: []byte("a")})
	dataB := encodeSimulationMessage(t, protocol.Data{FlowID: protocol.FlowID{1}, Offset: 1, Bytes: []byte("b")})

	controller.set(simulatedFault{kind: simulatedFaultDuplicate, frameType: protocol.TypeData, remaining: 1})
	writeSimulationFrame(t, client, dataA)
	if got := readSimulationFrame(t, server); string(got) != string(dataA) {
		t.Fatal("first duplicate changed")
	}
	if got := readSimulationFrame(t, server); string(got) != string(dataA) {
		t.Fatal("second duplicate changed")
	}

	controller.set(simulatedFault{kind: simulatedFaultDelay, frameType: protocol.TypeData, remaining: 1})
	writeSimulationFrame(t, client, dataA)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := server.ReadFrame(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("held frame read = %v", err)
	}
	if err := controller.releaseOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	readSimulationFrame(t, server)

	controller.set(simulatedFault{kind: simulatedFaultReorder, frameType: protocol.TypeData, remaining: 2})
	writeSimulationFrame(t, client, dataA)
	writeSimulationFrame(t, client, dataB)
	if got := readSimulationFrame(t, server); string(got) != string(dataB) {
		t.Fatal("reorder did not deliver second frame first")
	}
	if got := readSimulationFrame(t, server); string(got) != string(dataA) {
		t.Fatal("reorder did not release first frame")
	}

	controller.set(simulatedFault{kind: simulatedFaultStall, frameType: protocol.TypeData, remaining: 1})
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- client.WriteFrame(context.Background(), transport.WriteRequest{Class: transport.FrameData, Encoded: dataA})
	}()
	<-controller.entered
	controller.resume()
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	readSimulationFrame(t, server)

	// Explicit release models a deterministic byte-rate budget without using
	// wall-clock sleeps to decide delivery order.
	controller.set(simulatedFault{kind: simulatedFaultRateLimit, frameType: protocol.TypeData, remaining: 2})
	writeSimulationFrame(t, client, dataA)
	writeSimulationFrame(t, client, dataB)
	if err := controller.releaseOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := readSimulationFrame(t, server); string(got) != string(dataA) {
		t.Fatal("rate release changed first frame")
	}
	if err := controller.releaseOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := readSimulationFrame(t, server); string(got) != string(dataB) {
		t.Fatal("rate release changed second frame")
	}

	controller.set(simulatedFault{kind: simulatedFaultHold, frameType: protocol.TypeData, remaining: -1})
	for range maxSimulatedHeldFrames {
		writeSimulationFrame(t, client, dataA)
	}
	if err := client.WriteFrame(context.Background(), transport.WriteRequest{Class: transport.FrameData, Encoded: dataA}); !errors.Is(err, ErrWireCapacity) {
		t.Fatalf("held-frame overflow = %v", err)
	}
}

func TestSimulatedTransportDropBlackholeAndClose(t *testing.T) {
	address := netip.MustParseAddr("192.0.2.11")
	network := newSimulatedNetwork(t, address)
	dialer, err := network.NewDialer(transport.DialOptions{
		RemoteEndpoint: "127.0.0.1:9443", LocalEndpoint: "192.0.2.11:0", QueueLimits: transport.V1QueueLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server, err := network.listener.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	controller := network.controller(address, simulatedUplink)
	encoded := encodeSimulationMessage(t, protocol.Data{FlowID: protocol.FlowID{1}, Bytes: []byte("drop")})
	controller.set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeData, remaining: 1})
	writeSimulationFrame(t, client, encoded)
	assertNoSimulatedFrame(t, server)
	writeSimulationFrame(t, client, encoded)
	readSimulationFrame(t, server)

	controller.set(simulatedFault{kind: simulatedFaultDrop, frameType: protocol.TypeData, remaining: -1})
	writeSimulationFrame(t, client, encoded)
	writeSimulationFrame(t, client, encoded)
	assertNoSimulatedFrame(t, server)
	controller.set(simulatedFault{kind: simulatedFaultNormal})
	writeSimulationFrame(t, client, encoded)
	readSimulationFrame(t, server)

	controller.set(simulatedFault{kind: simulatedFaultClose, frameType: protocol.TypeData, remaining: 1})
	if err := client.WriteFrame(context.Background(), transport.WriteRequest{Class: transport.FrameData, Encoded: encoded}); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("close fault = %v", err)
	}
	if _, err := server.ReadFrame(context.Background()); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("peer close read = %v", err)
	}
}

func assertNoSimulatedFrame(t *testing.T, connection transport.Connection) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := connection.ReadFrame(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected simulated frame: %v", err)
	}
}

func encodeSimulationMessage(t *testing.T, message protocol.Message) []byte {
	t.Helper()
	encoded, err := protocol.EncodeMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func writeSimulationFrame(t *testing.T, connection transport.Connection, encoded []byte) {
	t.Helper()
	if err := connection.WriteFrame(context.Background(), transport.WriteRequest{Class: transport.FrameData, Encoded: encoded}); err != nil {
		t.Fatal(err)
	}
}

func readSimulationFrame(t *testing.T, connection transport.Connection) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	encoded, err := connection.ReadFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
