package client

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestApplicationIOExecutorMapsReadWriteAndCloseWriteResults(t *testing.T) {
	connection := &scriptedApplicationConnection{
		readData:      []byte("reply"),
		readErr:       io.EOF,
		writeN:        2,
		writeErr:      io.ErrClosedPipe,
		closeWriteErr: io.ErrUnexpectedEOF,
	}
	executor, err := NewApplicationIOExecutor(connection)
	if err != nil {
		t.Fatal(err)
	}

	readEvent, hasResult, err := executor.Execute(ApplicationRelayAction{
		Kind: ApplicationRelayActionRead, Generation: 1, MaxBytes: 8,
	})
	if err != nil || !hasResult || readEvent.Kind != ApplicationRelayReadResult ||
		readEvent.Generation != 1 || string(readEvent.Data) != "reply" || !errors.Is(readEvent.Err, io.EOF) {
		t.Fatalf("read result = %#v, %v, %v", readEvent, hasResult, err)
	}

	writeEvent, hasResult, err := executor.Execute(ApplicationRelayAction{
		Kind: ApplicationRelayActionWrite, Generation: 2, data: []byte("data"),
	})
	if err != nil || !hasResult || writeEvent.Kind != ApplicationRelayWriteResult ||
		writeEvent.Generation != 2 || writeEvent.N != 2 || !errors.Is(writeEvent.Err, io.ErrClosedPipe) {
		t.Fatalf("write result = %#v, %v, %v", writeEvent, hasResult, err)
	}
	if got := connection.writtenBytes(); string(got) != "data" {
		t.Fatalf("written bytes = %q", got)
	}

	closeWriteEvent, hasResult, err := executor.Execute(ApplicationRelayAction{
		Kind: ApplicationRelayActionCloseWrite, Generation: 3,
	})
	if err != nil || !hasResult || closeWriteEvent.Kind != ApplicationRelayCloseWriteResult ||
		closeWriteEvent.Generation != 3 || !errors.Is(closeWriteEvent.Err, io.ErrUnexpectedEOF) {
		t.Fatalf("close-write result = %#v, %v, %v", closeWriteEvent, hasResult, err)
	}
	if connection.closeWriteCount() != 1 {
		t.Fatalf("CloseWrite calls = %d", connection.closeWriteCount())
	}
}

func TestApplicationIOExecutorRejectsUnsupportedHalfClose(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	executor, err := NewApplicationIOExecutor(local)
	if err != nil {
		t.Fatal(err)
	}
	event, hasResult, err := executor.Execute(ApplicationRelayAction{
		Kind: ApplicationRelayActionCloseWrite, Generation: 7,
	})
	if err != nil || !hasResult || event.Kind != ApplicationRelayCloseWriteResult || !errors.Is(event.Err, ErrApplicationHalfClose) {
		t.Fatalf("unsupported close-write = %#v, %v, %v", event, hasResult, err)
	}
	_, _, _ = executor.Execute(ApplicationRelayAction{Kind: ApplicationRelayActionClose, Generation: 8})
}

func TestApplicationIOExecutorCloseUnblocksReadAndIsIdempotent(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	executor, err := NewApplicationIOExecutor(local)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan ApplicationRelayEvent, 1)
	go func() {
		event, _, _ := executor.Execute(ApplicationRelayAction{
			Kind: ApplicationRelayActionRead, Generation: 11, MaxBytes: 32,
		})
		result <- event
	}()

	if event, hasResult, err := executor.Execute(ApplicationRelayAction{
		Kind: ApplicationRelayActionClose, Generation: 12,
	}); err != nil || hasResult || event.Kind != 0 {
		t.Fatalf("close result = %#v, %v, %v", event, hasResult, err)
	}
	if _, hasResult, err := executor.Execute(ApplicationRelayAction{
		Kind: ApplicationRelayActionClose, Generation: 13,
	}); err != nil || hasResult {
		t.Fatalf("second close = hasResult %v, err %v", hasResult, err)
	}

	select {
	case event := <-result:
		if event.Kind != ApplicationRelayReadResult || event.Generation != 11 || event.Err == nil {
			t.Fatalf("unblocked read event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("application Close did not unblock Read")
	}
}

func TestApplicationIOExecutorGuardsActionAndConnectionContracts(t *testing.T) {
	if _, err := NewApplicationIOExecutor(nil); !errors.Is(err, ErrInvalidApplicationIOAction) {
		t.Fatalf("nil connection error = %v", err)
	}
	connection := &scriptedApplicationConnection{writeN: 5}
	executor, _ := NewApplicationIOExecutor(connection)
	invalid := []ApplicationRelayAction{
		{},
		{Kind: ApplicationRelayActionRead, Generation: 1},
		{Kind: ApplicationRelayActionRead, Generation: 1, MaxBytes: MaxApplicationReadBytes + 1},
		{Kind: ApplicationRelayActionWrite, Generation: 1},
		{Kind: ApplicationRelayActionWrite, Generation: 1, data: make([]byte, MaxApplicationWriteBytes+1)},
		{Kind: ApplicationRelayActionSendMessage, Generation: 1},
	}
	for _, action := range invalid {
		if _, _, err := executor.Execute(action); !errors.Is(err, ErrInvalidApplicationIOAction) {
			t.Fatalf("action %#v error = %v", action, err)
		}
	}
	event, hasResult, err := executor.Execute(ApplicationRelayAction{
		Kind: ApplicationRelayActionWrite, Generation: 2, data: []byte("four"),
	})
	if err != nil || !hasResult || !errors.Is(event.Err, ErrApplicationIOContract) || event.N != 5 {
		t.Fatalf("contract violation = %#v, %v, %v", event, hasResult, err)
	}
}

func TestApplicationIOExecutorAcceptsFullReceiveWindowWrite(t *testing.T) {
	connection := &scriptedApplicationConnection{writeN: MaxApplicationWriteBytes}
	executor, _ := NewApplicationIOExecutor(connection)
	payload := make([]byte, MaxApplicationWriteBytes)
	event, hasResult, err := executor.Execute(ApplicationRelayAction{
		Kind: ApplicationRelayActionWrite, Generation: 1, data: payload,
	})
	if err != nil || !hasResult || event.Err != nil || event.N != len(payload) {
		t.Fatalf("full-window write = %#v, %v, %v", event, hasResult, err)
	}
}

func TestApplicationIOExecutorWriteDoesNotAllocate(t *testing.T) {
	connection := &discardApplicationConnection{}
	executor, err := NewApplicationIOExecutor(connection)
	if err != nil {
		t.Fatal(err)
	}
	action := ApplicationRelayAction{
		Kind: ApplicationRelayActionWrite, Generation: 1, data: make([]byte, MaxApplicationWriteBytes),
	}
	allocations := testing.AllocsPerRun(100, func() {
		event, hasResult, executeErr := executor.Execute(action)
		if executeErr != nil || !hasResult || event.N != MaxApplicationWriteBytes {
			t.Fatalf("write result = %#v, %v, %v", event, hasResult, executeErr)
		}
	})
	if allocations != 0 {
		t.Fatalf("application write allocations = %.0f, want 0", allocations)
	}
}

func TestApplicationIOExecutorReusesReadBuffer(t *testing.T) {
	connection := &fixedApplicationConnection{data: []byte("read")}
	executor, err := NewApplicationIOExecutor(connection)
	if err != nil {
		t.Fatal(err)
	}
	action := ApplicationRelayAction{Kind: ApplicationRelayActionRead, Generation: 1, MaxBytes: MaxApplicationReadBytes}
	if _, _, err := executor.Execute(action); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		event, hasResult, executeErr := executor.Execute(action)
		if executeErr != nil || !hasResult || string(event.Data) != "read" {
			t.Fatalf("read result = %#v, %v, %v", event, hasResult, executeErr)
		}
	})
	if allocations != 0 {
		t.Fatalf("reused application read allocations = %.0f, want 0", allocations)
	}
}

type fixedApplicationConnection struct {
	data []byte
}

func (connection *fixedApplicationConnection) Read(buffer []byte) (int, error) {
	return copy(buffer, connection.data), nil
}
func (*fixedApplicationConnection) Write(buffer []byte) (int, error) { return len(buffer), nil }
func (*fixedApplicationConnection) Close() error                     { return nil }
func (*fixedApplicationConnection) LocalAddr() net.Addr              { return nil }
func (*fixedApplicationConnection) RemoteAddr() net.Addr             { return nil }
func (*fixedApplicationConnection) SetDeadline(time.Time) error      { return nil }
func (*fixedApplicationConnection) SetReadDeadline(time.Time) error  { return nil }
func (*fixedApplicationConnection) SetWriteDeadline(time.Time) error { return nil }

type discardApplicationConnection struct{}

func (*discardApplicationConnection) Read([]byte) (int, error)         { return 0, io.EOF }
func (*discardApplicationConnection) Write(buffer []byte) (int, error) { return len(buffer), nil }
func (*discardApplicationConnection) Close() error                     { return nil }
func (*discardApplicationConnection) LocalAddr() net.Addr              { return nil }
func (*discardApplicationConnection) RemoteAddr() net.Addr             { return nil }
func (*discardApplicationConnection) SetDeadline(time.Time) error      { return nil }
func (*discardApplicationConnection) SetReadDeadline(time.Time) error  { return nil }
func (*discardApplicationConnection) SetWriteDeadline(time.Time) error { return nil }

type scriptedApplicationConnection struct {
	mu            sync.Mutex
	readData      []byte
	readErr       error
	written       []byte
	writeN        int
	writeErr      error
	closeWriteErr error
	closeWrites   int
	closes        int
}

func (connection *scriptedApplicationConnection) Read(buffer []byte) (int, error) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	n := copy(buffer, connection.readData)
	return n, connection.readErr
}

func (connection *scriptedApplicationConnection) Write(buffer []byte) (int, error) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.written = append(connection.written, buffer...)
	return connection.writeN, connection.writeErr
}

func (connection *scriptedApplicationConnection) CloseWrite() error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.closeWrites++
	return connection.closeWriteErr
}

func (connection *scriptedApplicationConnection) Close() error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.closes++
	return nil
}

func (connection *scriptedApplicationConnection) LocalAddr() net.Addr              { return nil }
func (connection *scriptedApplicationConnection) RemoteAddr() net.Addr             { return nil }
func (connection *scriptedApplicationConnection) SetDeadline(time.Time) error      { return nil }
func (connection *scriptedApplicationConnection) SetReadDeadline(time.Time) error  { return nil }
func (connection *scriptedApplicationConnection) SetWriteDeadline(time.Time) error { return nil }

func (connection *scriptedApplicationConnection) writtenBytes() []byte {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return append([]byte(nil), connection.written...)
}

func (connection *scriptedApplicationConnection) closeWriteCount() int {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.closeWrites
}
