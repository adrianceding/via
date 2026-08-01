package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
)

func TestTCPFactoryCapabilitiesAndConfiguration(t *testing.T) {
	if _, err := NewTCPFactory(TCPConfig{}); !errors.Is(err, ErrInvalidTCPConfig) {
		t.Fatalf("zero config error = %v", err)
	}
	if _, err := NewTCPFactory(TCPConfig{FrameTotal: time.Second, FrameNoProgress: 2 * time.Second}); !errors.Is(err, ErrInvalidTCPConfig) {
		t.Fatalf("inverted deadlines error = %v", err)
	}
	if _, err := NewTCPFactory(TCPConfig{FrameTotal: 31 * time.Second, FrameNoProgress: time.Second}); !errors.Is(err, ErrInvalidTCPConfig) {
		t.Fatalf("oversized total deadline error = %v", err)
	}
	if _, err := NewTCPFactory(TCPConfig{FrameTotal: 5 * time.Second, FrameNoProgress: time.Second - 1}); !errors.Is(err, ErrInvalidTCPConfig) {
		t.Fatalf("undersized progress deadline error = %v", err)
	}
	factory, err := NewTCPFactory(DefaultTCPConfig())
	if err != nil {
		t.Fatal(err)
	}
	capabilities := factory.Capabilities()
	if capabilities.MaxEncodedFrame() != protocol.MaxFrameSize || !capabilities.Reliable() || !capabilities.Ordered() ||
		capabilities.Encrypted() || capabilities.Multiplexed() || !capabilities.HalfClose() {
		t.Fatalf("TCP capabilities = %#v", capabilities)
	}
	if _, err := factory.NewDialer(DialOptions{RemoteEndpoint: "missing-port", QueueLimits: V1QueueLimits()}); !errors.Is(err, ErrInvalidTCPConfig) {
		t.Fatalf("invalid endpoint error = %v", err)
	}
	if _, err := factory.NewDialer(DialOptions{RemoteEndpoint: "unresolved.invalid:9443", QueueLimits: V1QueueLimits()}); err != nil {
		t.Fatalf("dialer construction unexpectedly resolved DNS: %v", err)
	}
	wrongLimits := V1QueueLimits()
	wrongLimits.MaxFrames--
	if _, err := factory.NewDialer(DialOptions{RemoteEndpoint: "127.0.0.1:9443", QueueLimits: wrongLimits}); !errors.Is(err, ErrInvalidQueueLimits) {
		t.Fatalf("non-v1 queue limits error = %v", err)
	}
	if _, err := factory.NewDialer(DialOptions{
		RemoteEndpoint: "127.0.0.1:9443",
		LocalEndpoint:  "localhost:0",
		QueueLimits:    V1QueueLimits(),
	}); !errors.Is(err, ErrInvalidTCPConfig) {
		t.Fatalf("non-literal local endpoint error = %v", err)
	}
	if _, err := factory.NewDialer(DialOptions{
		RemoteEndpoint: "127.0.0.1:9443", InterfaceName: "bad\x00name", QueueLimits: V1QueueLimits(),
	}); !errors.Is(err, ErrInvalidTCPConfig) {
		t.Fatalf("invalid interface name error = %v", err)
	}
}

func TestTCPConnectionTransfersOneCompleteEncodedFrame(t *testing.T) {
	left, right := net.Pipe()
	client := newTestTCPConnection(t, left)
	server := newTestTCPConnection(t, right)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	encoded, err := protocol.EncodeMessage(protocol.Probe{Token: 42})
	if err != nil {
		t.Fatal(err)
	}
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- client.WriteFrame(context.Background(), WriteRequest{Class: FrameControl, Encoded: encoded})
	}()
	received, err := server.ReadFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, encoded) {
		t.Fatalf("received %x, want %x", received, encoded)
	}
	if err := waitResult(t, writeResult); err != nil {
		t.Fatalf("write result = %v", err)
	}
}

func TestTCPConnectionAcceptsMaximumEncodedFrame(t *testing.T) {
	left, right := net.Pipe()
	client := newTestTCPConnection(t, left)
	server := newTestTCPConnection(t, right)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	encoded, err := protocol.EncodeMessage(protocol.Data{Bytes: make([]byte, protocol.MaxDataLength)})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != protocol.MaxFrameSize {
		t.Fatalf("encoded frame length = %d", len(encoded))
	}
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- client.WriteFrame(context.Background(), WriteRequest{Class: FrameData, Encoded: encoded})
	}()
	received, err := server.ReadFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, encoded) {
		t.Fatal("maximum frame changed during transport")
	}
	if err := waitResult(t, writeResult); err != nil {
		t.Fatal(err)
	}
}

func TestTCPFactoryDialAcceptAndHalfClose(t *testing.T) {
	address := reserveLoopbackAddress(t)
	factory, err := NewTCPFactory(DefaultTCPConfig())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := factory.NewListener(ListenOptions{LocalEndpoint: address, QueueLimits: V1QueueLimits()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	dialer, err := factory.NewDialer(DialOptions{RemoteEndpoint: address, QueueLimits: V1QueueLimits()})
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan Connection, 1)
	acceptErr := make(chan error, 1)
	go func() {
		connection, err := listener.Accept(context.Background())
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- connection
	}()
	client, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	var server Connection
	select {
	case server = <-accepted:
	case err := <-acceptErr:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("TCP accept did not terminate")
	}
	t.Cleanup(func() { _ = server.Close() })
	if client.Capabilities() != factory.Capabilities() || client.QueueLimits() != V1QueueLimits() {
		t.Fatalf("connection contract = %#v / %#v", client.Capabilities(), client.QueueLimits())
	}
	if client.LocalEndpoint() == "" || client.RemoteEndpoint() == "" ||
		client.LocalEndpoint() != server.RemoteEndpoint() || client.RemoteEndpoint() != server.LocalEndpoint() {
		t.Fatalf("connection endpoints client=%q/%q server=%q/%q",
			client.LocalEndpoint(), client.RemoteEndpoint(), server.LocalEndpoint(), server.RemoteEndpoint())
	}

	encoded, err := protocol.EncodeMessage(protocol.Probe{Token: 77})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.WriteFrame(context.Background(), WriteRequest{Class: FrameControl, Encoded: encoded}); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	received, err := server.ReadFrame(context.Background())
	if err != nil || !bytes.Equal(received, encoded) {
		t.Fatalf("server frame = %x, %v", received, err)
	}
	if _, err := server.ReadFrame(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("read after peer CloseWrite = %v", err)
	}
	if err := client.WriteFrame(context.Background(), WriteRequest{Class: FrameControl, Encoded: encoded}); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after CloseWrite = %v", err)
	}
}

func TestTCPConnectionRejectsInvalidHeaderBeforePayloadRead(t *testing.T) {
	left, right := net.Pipe()
	receiver := newTestTCPConnection(t, right)
	t.Cleanup(func() {
		_ = left.Close()
		_ = receiver.Close()
	})

	header := []byte{'V', 'I', protocol.Version, byte(protocol.TypeData), 0, 0, 0, 24}
	writeResult := make(chan error, 1)
	go func() {
		_, err := left.Write(header)
		writeResult <- err
	}()
	if _, err := receiver.ReadFrame(context.Background()); !errors.Is(err, protocol.ErrInvalidLength) {
		t.Fatalf("invalid header error = %v", err)
	}
	if err := waitResult(t, writeResult); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed() {
		t.Fatal("invalid frame did not close TCP connection")
	}
}

func TestTCPFrameDeadlinesStartAfterFirstByteAndRefreshOnProgress(t *testing.T) {
	encoded, err := protocol.EncodeMessage(protocol.Probe{Token: 7})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_000, 0)
	underlying := &scriptedConn{readData: encoded, now: base}
	capabilities := testCapabilities(t)
	connection := newTCPConnection(underlying, DefaultTCPConfig(), capabilities, V1QueueLimits(), func() time.Time {
		return underlying.currentTime()
	})
	t.Cleanup(func() { _ = connection.Close() })

	received, err := connection.ReadFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received, encoded) {
		t.Fatalf("received %x, want %x", received, encoded)
	}
	deadlines := underlying.readDeadlineSnapshot()
	if len(deadlines) < 4 || !deadlines[0].IsZero() {
		t.Fatalf("read deadlines = %v", deadlines)
	}
	if want := base.Add(6 * time.Second); !deadlines[1].Equal(want) {
		t.Fatalf("header deadline = %v, want %v", deadlines[1], want)
	}
	if want := base.Add(7 * time.Second); !deadlines[2].Equal(want) {
		t.Fatalf("payload deadline = %v, want %v", deadlines[2], want)
	}
	if !deadlines[len(deadlines)-1].IsZero() {
		t.Fatalf("successful frame left read deadline %v", deadlines[len(deadlines)-1])
	}
}

func TestTCPPartialFrameTimeoutMapsToFrameDeadline(t *testing.T) {
	underlying := &timeoutReadConn{firstByte: 'V'}
	connection := newTCPConnection(underlying, DefaultTCPConfig(), testCapabilities(t), V1QueueLimits(), time.Now)
	t.Cleanup(func() { _ = connection.Close() })
	if _, err := connection.ReadFrame(context.Background()); !errors.Is(err, ErrFrameDeadline) {
		t.Fatalf("partial frame timeout error = %v", err)
	}
	if !connection.closed() {
		t.Fatal("partial frame timeout did not close connection")
	}
}

func TestTCPIdleReadUsesCallerCancellationNotFrameDeadline(t *testing.T) {
	left, right := net.Pipe()
	connection := newTestTCPConnection(t, left)
	t.Cleanup(func() {
		_ = connection.Close()
		_ = right.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := connection.ReadFrame(ctx)
		result <- err
	}()
	cancel()
	if err := waitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("idle cancellation error = %v", err)
	}
}

func TestTCPOutputQueuePreservesControlReserve(t *testing.T) {
	underlying := newBlockingConn()
	capabilities, err := NewCapabilities(CapabilitySpec{MaxEncodedFrame: 1, Reliable: true, Ordered: true})
	if err != nil {
		t.Fatal(err)
	}
	limits := QueueLimits{MaxFrames: 3, MaxBytes: 3, ReservedControlFrames: 1, ReservedControlBytes: 1}
	connection := newTCPConnection(underlying, DefaultTCPConfig(), capabilities, limits, time.Now)
	t.Cleanup(func() { _ = connection.Close() })

	first := asyncWrite(connection, FrameData)
	waitSignal(t, underlying.writeStarted)
	second := asyncWrite(connection, FrameData)
	waitQueueFrames(t, connection, 2)
	if err := connection.WriteFrame(context.Background(), WriteRequest{Class: FrameData, Encoded: []byte{3}}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("data overflow error = %v", err)
	}
	control := asyncWrite(connection, FrameControl)
	waitQueueFrames(t, connection, 3)
	close(underlying.releaseWrites)
	for name, result := range map[string]<-chan error{"first": first, "second": second, "control": control} {
		if err := waitResult(t, result); err != nil {
			t.Fatalf("%s write = %v", name, err)
		}
	}
}

func TestTCPCloseUnblocksReadAndWrite(t *testing.T) {
	left, right := net.Pipe()
	connection := newTestTCPConnection(t, left)
	readResult := make(chan error, 1)
	writeResult := make(chan error, 1)
	go func() {
		_, err := connection.ReadFrame(context.Background())
		readResult <- err
	}()
	encoded, err := protocol.EncodeMessage(protocol.Probe{Token: 1})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		writeResult <- connection.WriteFrame(context.Background(), WriteRequest{Class: FrameControl, Encoded: encoded})
	}()
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	_ = right.Close()
	if err := waitResult(t, readResult); err == nil {
		t.Fatal("blocked read returned nil after close")
	}
	if err := waitResult(t, writeResult); err == nil {
		t.Fatal("blocked write returned nil after close")
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
}

func newTestTCPConnection(t *testing.T, conn net.Conn) *tcpConnection {
	t.Helper()
	return newTCPConnection(conn, DefaultTCPConfig(), testCapabilities(t), V1QueueLimits(), time.Now)
}

func testCapabilities(t *testing.T) Capabilities {
	t.Helper()
	capabilities, err := NewCapabilities(CapabilitySpec{
		MaxEncodedFrame: protocol.MaxFrameSize,
		Reliable:        true,
		Ordered:         true,
		HalfClose:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return capabilities
}

func asyncWrite(connection *tcpConnection, class FrameClass) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- connection.WriteFrame(context.Background(), WriteRequest{Class: class, Encoded: []byte{1}})
	}()
	return result
}

func waitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not terminate")
		return nil
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not start")
	}
}

func waitQueueFrames(t *testing.T, connection *tcpConnection, want uint32) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		connection.queueMu.Lock()
		got := connection.queueFrames
		connection.queueMu.Unlock()
		if got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("queue frames = %d, want %d", got, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

type scriptedConn struct {
	mu            sync.Mutex
	readData      []byte
	readOffset    int
	now           time.Time
	readDeadlines []time.Time
	closed        bool
}

func (connection *scriptedConn) Read(target []byte) (int, error) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.closed {
		return 0, net.ErrClosed
	}
	if connection.readOffset == len(connection.readData) {
		return 0, io.EOF
	}
	n := copy(target, connection.readData[connection.readOffset:])
	connection.readOffset += n
	connection.now = connection.now.Add(time.Second)
	return n, nil
}

func (connection *scriptedConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *scriptedConn) Close() error {
	connection.mu.Lock()
	connection.closed = true
	connection.mu.Unlock()
	return nil
}
func (connection *scriptedConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (connection *scriptedConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (connection *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (connection *scriptedConn) SetWriteDeadline(time.Time) error { return nil }
func (connection *scriptedConn) SetReadDeadline(deadline time.Time) error {
	connection.mu.Lock()
	connection.readDeadlines = append(connection.readDeadlines, deadline)
	connection.mu.Unlock()
	return nil
}
func (connection *scriptedConn) currentTime() time.Time {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.now
}
func (connection *scriptedConn) readDeadlineSnapshot() []time.Time {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return append([]time.Time(nil), connection.readDeadlines...)
}

type blockingConn struct {
	writeStarted  chan struct{}
	releaseWrites chan struct{}
	closeOnce     sync.Once
	closed        chan struct{}
}

func newBlockingConn() *blockingConn {
	return &blockingConn{
		writeStarted:  make(chan struct{}, 8),
		releaseWrites: make(chan struct{}),
		closed:        make(chan struct{}),
	}
}

func (connection *blockingConn) Read([]byte) (int, error) {
	<-connection.closed
	return 0, net.ErrClosed
}
func (connection *blockingConn) Write(data []byte) (int, error) {
	connection.writeStarted <- struct{}{}
	select {
	case <-connection.releaseWrites:
		return len(data), nil
	case <-connection.closed:
		return 0, net.ErrClosed
	}
}
func (connection *blockingConn) Close() error {
	connection.closeOnce.Do(func() { close(connection.closed) })
	return nil
}
func (connection *blockingConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (connection *blockingConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (connection *blockingConn) SetDeadline(time.Time) error      { return nil }
func (connection *blockingConn) SetReadDeadline(time.Time) error  { return nil }
func (connection *blockingConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (address dummyAddr) Network() string { return "test" }
func (address dummyAddr) String() string  { return string(address) }

type timeoutReadConn struct {
	firstByte byte
	readCount int
	closed    bool
}

func (connection *timeoutReadConn) Read(target []byte) (int, error) {
	connection.readCount++
	if connection.readCount == 1 {
		target[0] = connection.firstByte
		return 1, nil
	}
	return 0, testTimeoutError{}
}
func (connection *timeoutReadConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *timeoutReadConn) Close() error                   { connection.closed = true; return nil }
func (connection *timeoutReadConn) LocalAddr() net.Addr            { return dummyAddr("local") }
func (connection *timeoutReadConn) RemoteAddr() net.Addr           { return dummyAddr("remote") }
func (connection *timeoutReadConn) SetDeadline(time.Time) error    { return nil }
func (connection *timeoutReadConn) SetReadDeadline(time.Time) error {
	return nil
}
func (connection *timeoutReadConn) SetWriteDeadline(time.Time) error { return nil }

type testTimeoutError struct{}

func (testTimeoutError) Error() string   { return "timeout" }
func (testTimeoutError) Timeout() bool   { return true }
func (testTimeoutError) Temporary() bool { return true }
