package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adrianceding/via/internal/protocol"
)

var (
	ErrInvalidTCPConfig = errors.New("transport: invalid tcp configuration")
	ErrFrameDeadline    = errors.New("transport: frame read deadline exceeded")
)

const tcpWriteBufferBytes = 128 << 10

type tcpWriteBufferConnection interface {
	SetWriteBuffer(int) error
	Close() error
}

type TCPConfig struct {
	FrameTotal       time.Duration
	FrameNoProgress  time.Duration
	WriteBufferBytes int
}

func DefaultTCPConfig() TCPConfig {
	return TCPConfig{FrameTotal: 30 * time.Second, FrameNoProgress: 5 * time.Second, WriteBufferBytes: tcpWriteBufferBytes}
}

func (config TCPConfig) Validate() error {
	if config.WriteBufferBytes == 0 {
		config.WriteBufferBytes = tcpWriteBufferBytes
	}
	if config.WriteBufferBytes < protocol.MaxFrameSize || config.WriteBufferBytes > 16<<20 ||
		config.FrameTotal < 5*time.Second || config.FrameTotal > 30*time.Second ||
		config.FrameNoProgress < time.Second || config.FrameNoProgress > 30*time.Second ||
		config.FrameNoProgress > config.FrameTotal {
		return ErrInvalidTCPConfig
	}
	return nil
}

type TCPFactory struct {
	config       TCPConfig
	capabilities Capabilities
}

func NewTCPFactory(config TCPConfig) (*TCPFactory, error) {
	if config.WriteBufferBytes == 0 {
		config.WriteBufferBytes = tcpWriteBufferBytes
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	capabilities, err := NewCapabilities(CapabilitySpec{
		MaxEncodedFrame: protocol.MaxFrameSize,
		Reliable:        true,
		Ordered:         true,
		HalfClose:       true,
	})
	if err != nil {
		return nil, err
	}
	return &TCPFactory{config: config, capabilities: capabilities}, nil
}

func (factory *TCPFactory) Capabilities() Capabilities {
	if factory == nil {
		return Capabilities{}
	}
	return factory.capabilities
}

func (factory *TCPFactory) NewDialer(options DialOptions) (Dialer, error) {
	if factory == nil {
		return nil, ErrInvalidTCPConfig
	}
	if err := options.QueueLimits.Validate(factory.capabilities); err != nil {
		return nil, err
	}
	if _, _, err := splitEndpoint(options.RemoteEndpoint, false); err != nil {
		return nil, fmt.Errorf("%w: invalid remote endpoint", ErrInvalidTCPConfig)
	}
	var local *net.TCPAddr
	if options.LocalEndpoint != "" {
		resolved, err := literalTCPAddr(options.LocalEndpoint, true)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid local endpoint", ErrInvalidTCPConfig)
		}
		local = resolved
	}
	if len(options.InterfaceName) > 64 || strings.IndexByte(options.InterfaceName, 0) >= 0 {
		return nil, fmt.Errorf("%w: invalid interface name", ErrInvalidTCPConfig)
	}
	return &tcpDialer{
		factory: factory, remote: options.RemoteEndpoint, local: local,
		interfaceName: options.InterfaceName, limits: options.QueueLimits,
	}, nil
}

func (factory *TCPFactory) NewListener(options ListenOptions) (Listener, error) {
	if factory == nil {
		return nil, ErrInvalidTCPConfig
	}
	if err := options.QueueLimits.Validate(factory.capabilities); err != nil {
		return nil, err
	}
	local, err := literalTCPAddr(options.LocalEndpoint, false)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid listen endpoint", ErrInvalidTCPConfig)
	}
	listener, err := net.ListenTCP("tcp", local)
	if err != nil {
		return nil, err
	}
	return &tcpListener{factory: factory, listener: listener, limits: options.QueueLimits}, nil
}

type tcpDialer struct {
	factory       *TCPFactory
	remote        string
	local         *net.TCPAddr
	interfaceName string
	limits        QueueLimits
}

func (dialer *tcpDialer) Dial(ctx context.Context) (Connection, error) {
	netDialer := net.Dialer{LocalAddr: dialer.local}
	setDialerInterface(&netDialer, dialer.interfaceName)
	connection, err := netDialer.DialContext(ctx, "tcp", dialer.remote)
	if err != nil {
		return nil, err
	}
	tcpConnection, ok := connection.(*net.TCPConn)
	if !ok {
		_ = connection.Close()
		return nil, ErrInvalidTCPConfig
	}
	if err := configureTCPWriteBuffer(tcpConnection, dialer.factory.config.WriteBufferBytes); err != nil {
		return nil, err
	}
	return newTCPConnection(connection, dialer.factory.config, dialer.factory.capabilities, dialer.limits, time.Now), nil
}

type tcpListener struct {
	factory  *TCPFactory
	listener *net.TCPListener
	limits   QueueLimits
}

func (listener *tcpListener) Accept(ctx context.Context) (Connection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := listener.listener.SetDeadline(deadline); err != nil {
			return nil, err
		}
	} else if err := listener.listener.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	stop := interruptOnCancel(ctx, func() { _ = listener.listener.SetDeadline(time.Now()) })
	connection, err := listener.listener.AcceptTCP()
	stop()
	_ = listener.listener.SetDeadline(time.Time{})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if err := configureTCPWriteBuffer(connection, listener.factory.config.WriteBufferBytes); err != nil {
		return nil, err
	}
	return newTCPConnection(connection, listener.factory.config, listener.factory.capabilities, listener.limits, time.Now), nil
}

func configureTCPWriteBuffer(connection tcpWriteBufferConnection, sizes ...int) error {
	size := tcpWriteBufferBytes
	if len(sizes) != 0 {
		size = sizes[0]
	}
	if err := connection.SetWriteBuffer(size); err != nil {
		_ = connection.Close()
		return err
	}
	return nil
}

func (listener *tcpListener) Close() error { return listener.listener.Close() }

type tcpState uint8

const (
	tcpOpen tcpState = iota + 1
	tcpDraining
	tcpClosed
)

type writeEntry struct {
	request    WriteRequest
	ctx        context.Context
	closeWrite bool
	result     chan error
	bytes      uint64
	started    bool
}

var tcpWriteEntryPool = sync.Pool{New: func() any {
	return &writeEntry{result: make(chan error, 1)}
}}

func acquireWriteEntry() *writeEntry {
	return tcpWriteEntryPool.Get().(*writeEntry)
}

func releaseWriteEntry(entry *writeEntry) {
	if entry == nil {
		return
	}
	result := entry.result
	*entry = writeEntry{result: result}
	tcpWriteEntryPool.Put(entry)
}

type tcpConnection struct {
	conn         net.Conn
	config       TCPConfig
	capabilities Capabilities
	limits       QueueLimits
	now          func() time.Time

	readMu sync.Mutex

	queueMu     sync.Mutex
	queue       []*writeEntry
	queueFrames uint32
	queueBytes  uint64
	state       tcpState
	wake        chan struct{}
	done        chan struct{}
	writerDone  chan struct{}
	closeOnce   sync.Once
}

func newTCPConnection(conn net.Conn, config TCPConfig, capabilities Capabilities, limits QueueLimits, now func() time.Time) *tcpConnection {
	connection := &tcpConnection{
		conn:         conn,
		config:       config,
		capabilities: capabilities,
		limits:       limits,
		now:          now,
		state:        tcpOpen,
		wake:         make(chan struct{}, 1),
		done:         make(chan struct{}),
		writerDone:   make(chan struct{}),
	}
	go connection.runWriter()
	return connection
}

func (connection *tcpConnection) Capabilities() Capabilities { return connection.capabilities }
func (connection *tcpConnection) QueueLimits() QueueLimits   { return connection.limits }
func (connection *tcpConnection) LocalEndpoint() string {
	if connection == nil || connection.conn == nil || connection.conn.LocalAddr() == nil {
		return ""
	}
	return connection.conn.LocalAddr().String()
}
func (connection *tcpConnection) RemoteEndpoint() string {
	if connection == nil || connection.conn == nil || connection.conn.RemoteAddr() == nil {
		return ""
	}
	return connection.conn.RemoteAddr().String()
}

func (connection *tcpConnection) ReadFrame(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	connection.readMu.Lock()
	defer connection.readMu.Unlock()
	if connection.closed() {
		return nil, ErrClosed
	}

	header := make([]byte, protocol.HeaderSize)
	if err := connection.readIdleFirstByte(ctx, header[:1]); err != nil {
		connection.closeOnReadError()
		return nil, err
	}
	started := connection.now()
	if err := connection.readWithFrameDeadlines(ctx, header[1:], started); err != nil {
		connection.closeOnReadError()
		return nil, err
	}
	frameType, payloadLength, err := protocol.ParseHeader(header)
	if err != nil {
		connection.closeOnReadError()
		return nil, err
	}
	encoded := make([]byte, protocol.HeaderSize+int(payloadLength))
	copy(encoded, header)
	if err := connection.readWithFrameDeadlines(ctx, encoded[protocol.HeaderSize:], started); err != nil {
		connection.closeOnReadError()
		return nil, err
	}
	if _, err := protocol.DecodeMessage(protocol.Frame{Type: frameType, Payload: encoded[protocol.HeaderSize:]}); err != nil {
		connection.closeOnReadError()
		return nil, err
	}
	_ = connection.conn.SetReadDeadline(time.Time{})
	return encoded, nil
}

func (connection *tcpConnection) readIdleFirstByte(ctx context.Context, target []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.conn.SetReadDeadline(deadline); err != nil {
			return err
		}
	} else if err := connection.conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	stop := interruptOnCancel(ctx, func() { _ = connection.conn.SetReadDeadline(connection.now()) })
	err := readExactly(connection.conn, target)
	stop()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (connection *tcpConnection) readWithFrameDeadlines(ctx context.Context, target []byte, started time.Time) error {
	totalDeadline := started.Add(connection.config.FrameTotal)
	for len(target) > 0 {
		deadline := minTime(totalDeadline, connection.now().Add(connection.config.FrameNoProgress))
		if contextDeadline, ok := ctx.Deadline(); ok {
			deadline = minTime(deadline, contextDeadline)
		}
		if err := connection.conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		stop := interruptOnCancel(ctx, func() { _ = connection.conn.SetReadDeadline(connection.now()) })
		n, err := connection.conn.Read(target)
		stop()
		if n < 0 || n > len(target) {
			return io.ErrNoProgress
		}
		if n > 0 {
			target = target[n:]
			if len(target) == 0 {
				return nil
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return ErrFrameDeadline
			}
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func (connection *tcpConnection) WriteFrame(ctx context.Context, request WriteRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := request.Validate(connection.capabilities); err != nil {
		return err
	}
	entry := acquireWriteEntry()
	entry.request = WriteRequest{Class: request.Class, Encoded: request.Encoded}
	entry.ctx = ctx
	entry.bytes = uint64(len(request.Encoded))
	if err := connection.enqueue(entry); err != nil {
		releaseWriteEntry(entry)
		return err
	}
	defer releaseWriteEntry(entry)
	select {
	case err := <-entry.result:
		return err
	case <-ctx.Done():
		if connection.cancelQueuedWrite(entry) {
			return ctx.Err()
		}
		return <-entry.result
	case <-connection.done:
		return <-entry.result
	}
}

func (connection *tcpConnection) CloseWrite() error {
	entry := acquireWriteEntry()
	entry.ctx = context.Background()
	entry.closeWrite = true
	connection.queueMu.Lock()
	if connection.state != tcpOpen {
		connection.queueMu.Unlock()
		releaseWriteEntry(entry)
		return ErrClosed
	}
	if !connection.fitsLocked(FrameControl, 0) {
		connection.queueMu.Unlock()
		releaseWriteEntry(entry)
		return ErrQueueFull
	}
	connection.state = tcpDraining
	connection.addLocked(entry)
	connection.queueMu.Unlock()
	connection.signalWriter()
	defer releaseWriteEntry(entry)
	select {
	case err := <-entry.result:
		return err
	case <-connection.done:
		return ErrClosed
	}
}

func (connection *tcpConnection) Close() error {
	closeErr := connection.closeConnection()
	<-connection.writerDone
	return closeErr
}

func (connection *tcpConnection) closeConnection() error {
	var closeErr error
	connection.closeOnce.Do(func() {
		connection.queueMu.Lock()
		connection.state = tcpClosed
		connection.queueMu.Unlock()
		close(connection.done)
		closeErr = connection.conn.Close()
		connection.signalWriter()
	})
	return closeErr
}

func (connection *tcpConnection) enqueue(entry *writeEntry) error {
	connection.queueMu.Lock()
	defer connection.queueMu.Unlock()
	if connection.state != tcpOpen {
		return ErrClosed
	}
	if !connection.fitsLocked(entry.request.Class, entry.bytes) {
		return ErrQueueFull
	}
	// The WriteRequest contract already forbids the caller from mutating
	// Encoded before WriteFrame returns; the writer goroutine is the only
	// reader between enqueue and completion, so no defensive copy is needed.
	connection.addLocked(entry)
	connection.signalWriter()
	return nil
}

func (connection *tcpConnection) cancelQueuedWrite(entry *writeEntry) bool {
	connection.queueMu.Lock()
	defer connection.queueMu.Unlock()
	for index, queued := range connection.queue {
		if queued != entry {
			continue
		}
		copy(connection.queue[index:], connection.queue[index+1:])
		connection.queue[len(connection.queue)-1] = nil
		connection.queue = connection.queue[:len(connection.queue)-1]
		connection.queueFrames--
		connection.queueBytes -= entry.bytes
		return true
	}
	return false
}

func (connection *tcpConnection) fitsLocked(class FrameClass, size uint64) bool {
	if connection.queueFrames >= connection.limits.MaxFrames || size > connection.limits.MaxBytes-connection.queueBytes {
		return false
	}
	if class == FrameData {
		dataByteLimit := connection.limits.MaxBytes - connection.limits.ReservedControlBytes
		if connection.queueBytes > dataByteLimit {
			return false
		}
		return connection.queueFrames+1 <= connection.limits.MaxFrames-connection.limits.ReservedControlFrames &&
			size <= dataByteLimit-connection.queueBytes
	}
	return true
}

func (connection *tcpConnection) addLocked(entry *writeEntry) {
	connection.queue = append(connection.queue, entry)
	connection.queueFrames++
	connection.queueBytes += entry.bytes
}

func (connection *tcpConnection) runWriter() {
	defer close(connection.writerDone)
	for {
		entry := connection.nextWrite()
		if entry == nil {
			connection.failQueued(ErrClosed)
			return
		}
		err := connection.executeWrite(entry)
		connection.finishWrite(entry, err)
		fatal := err != nil && (entry.started || (!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)))
		if fatal {
			_ = connection.closeConnection()
			connection.failQueued(err)
		}
		entry.result <- err
		if fatal {
			return
		}
	}
}

func (connection *tcpConnection) nextWrite() *writeEntry {
	for {
		connection.queueMu.Lock()
		if len(connection.queue) > 0 {
			entry := connection.queue[0]
			copy(connection.queue, connection.queue[1:])
			connection.queue[len(connection.queue)-1] = nil
			connection.queue = connection.queue[:len(connection.queue)-1]
			connection.queueMu.Unlock()
			return entry
		}
		closed := connection.state == tcpClosed
		connection.queueMu.Unlock()
		if closed {
			return nil
		}
		select {
		case <-connection.wake:
		case <-connection.done:
			return nil
		}
	}
}

func (connection *tcpConnection) executeWrite(entry *writeEntry) error {
	if err := entry.ctx.Err(); err != nil {
		return err
	}
	if entry.closeWrite {
		entry.started = true
		closeWriter, ok := connection.conn.(interface{ CloseWrite() error })
		if !ok {
			return ErrHalfCloseUnsupported
		}
		return closeWriter.CloseWrite()
	}
	if deadline, ok := entry.ctx.Deadline(); ok {
		if err := connection.conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
	} else if err := connection.conn.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}
	entry.started = true
	stop := interruptOnCancel(entry.ctx, func() { _ = connection.conn.SetWriteDeadline(connection.now()) })
	err := writeExactly(connection.conn, entry.request.Encoded)
	stop()
	_ = connection.conn.SetWriteDeadline(time.Time{})
	if entry.ctx.Err() != nil {
		return entry.ctx.Err()
	}
	return err
}

func (connection *tcpConnection) finishWrite(entry *writeEntry, _ error) {
	connection.queueMu.Lock()
	connection.queueFrames--
	connection.queueBytes -= entry.bytes
	connection.queueMu.Unlock()
}

func (connection *tcpConnection) failQueued(err error) {
	connection.queueMu.Lock()
	queued := connection.queue
	connection.queue = nil
	for _, entry := range queued {
		connection.queueFrames--
		connection.queueBytes -= entry.bytes
	}
	connection.queueMu.Unlock()
	for _, entry := range queued {
		entry.result <- err
	}
}

func (connection *tcpConnection) signalWriter() {
	select {
	case connection.wake <- struct{}{}:
	default:
	}
}

func (connection *tcpConnection) closed() bool {
	connection.queueMu.Lock()
	defer connection.queueMu.Unlock()
	return connection.state == tcpClosed
}

func (connection *tcpConnection) closeOnReadError() { _ = connection.Close() }

func readExactly(reader io.Reader, target []byte) error {
	for len(target) > 0 {
		n, err := reader.Read(target)
		if n < 0 || n > len(target) {
			return io.ErrNoProgress
		}
		if n > 0 {
			target = target[n:]
			if len(target) == 0 {
				return nil
			}
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func writeExactly(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func splitEndpoint(endpoint string, allowZeroPort bool) (string, uint16, error) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return "", 0, ErrInvalidTCPConfig
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || (!allowZeroPort && port == 0) {
		return "", 0, ErrInvalidTCPConfig
	}
	return host, uint16(port), nil
}

func literalTCPAddr(endpoint string, allowZeroPort bool) (*net.TCPAddr, error) {
	host, port, err := splitEndpoint(endpoint, allowZeroPort)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, ErrInvalidTCPConfig
	}
	return &net.TCPAddr{IP: ip, Port: int(port)}, nil
}

func interruptOnCancel(ctx context.Context, interrupt func()) func() {
	// Fast path: a context that can never be cancelled (Done() == nil, e.g.
	// context.Background) never fires the interrupt, so skip the AfterFunc
	// registration and the per-call channel allocation entirely.
	if ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		interrupt()
		close(done)
	})
	return func() {
		if !stop() {
			<-done
		}
	}
}
