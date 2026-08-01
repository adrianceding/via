package server

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestTargetIOExecutorMapsReadWriteAndCloseWriteResults(t *testing.T) {
	connection := &scriptedTargetConnection{
		readData:      []byte("reply"),
		readErr:       io.EOF,
		writeN:        2,
		writeErr:      io.ErrClosedPipe,
		closeWriteErr: io.ErrUnexpectedEOF,
	}
	executor, err := NewTargetIOExecutor(connection)
	if err != nil {
		t.Fatal(err)
	}

	readEvent, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionReadTarget, Generation: 1, MaxBytes: 8,
	})
	if err != nil || !hasResult || readEvent.Kind != RelayTargetReadResult ||
		readEvent.Generation != 1 || string(readEvent.Data) != "reply" || !errors.Is(readEvent.Err, io.EOF) {
		t.Fatalf("read result = %#v, %v, %v", readEvent, hasResult, err)
	}

	writeEvent, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionWriteTarget, Generation: 2, data: []byte("data"),
	})
	if err != nil || !hasResult || writeEvent.Kind != RelayTargetWriteResult ||
		writeEvent.Generation != 2 || writeEvent.N != 2 || !errors.Is(writeEvent.Err, io.ErrClosedPipe) {
		t.Fatalf("write result = %#v, %v, %v", writeEvent, hasResult, err)
	}
	if got := connection.writtenBytes(); string(got) != "data" {
		t.Fatalf("written bytes = %q", got)
	}

	closeWriteEvent, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionCloseWriteTarget, Generation: 3,
	})
	if err != nil || !hasResult || closeWriteEvent.Kind != RelayTargetCloseWriteResult ||
		closeWriteEvent.Generation != 3 || !errors.Is(closeWriteEvent.Err, io.ErrUnexpectedEOF) {
		t.Fatalf("close-write result = %#v, %v, %v", closeWriteEvent, hasResult, err)
	}
	if connection.closeWriteCount() != 1 {
		t.Fatalf("CloseWrite calls = %d", connection.closeWriteCount())
	}
}

func TestTargetIOExecutorRejectsUnsupportedHalfClose(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	executor, err := NewTargetIOExecutor(local)
	if err != nil {
		t.Fatal(err)
	}
	event, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionCloseWriteTarget, Generation: 7,
	})
	if err != nil || !hasResult || event.Kind != RelayTargetCloseWriteResult || !errors.Is(event.Err, ErrTargetHalfClose) {
		t.Fatalf("unsupported close-write = %#v, %v, %v", event, hasResult, err)
	}
	_, _, _ = executor.Execute(RelayAction{Kind: RelayActionCloseTarget, Generation: 8})
}

func TestTargetIOExecutorCloseUnblocksReadAndIsIdempotent(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	executor, err := NewTargetIOExecutor(local)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan RelayEvent, 1)
	go func() {
		event, _, _ := executor.Execute(RelayAction{
			Kind: RelayActionReadTarget, Generation: 11, MaxBytes: 32,
		})
		result <- event
	}()

	if event, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionCloseTarget, Generation: 12,
	}); err != nil || hasResult || event.Kind != 0 {
		t.Fatalf("close result = %#v, %v, %v", event, hasResult, err)
	}
	if _, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionCloseTarget, Generation: 13,
	}); err != nil || hasResult {
		t.Fatalf("second close = hasResult %v, err %v", hasResult, err)
	}

	select {
	case event := <-result:
		if event.Kind != RelayTargetReadResult || event.Generation != 11 || event.Err == nil {
			t.Fatalf("unblocked read event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("target Close did not unblock Read")
	}
}

func TestTargetIOExecutorGuardsActionAndConnectionContracts(t *testing.T) {
	if _, err := NewTargetIOExecutor(nil); !errors.Is(err, ErrInvalidTargetIOAction) {
		t.Fatalf("nil connection error = %v", err)
	}
	connection := &scriptedTargetConnection{writeN: 5}
	executor, _ := NewTargetIOExecutor(connection)
	invalid := []RelayAction{
		{},
		{Kind: RelayActionReadTarget, Generation: 1},
		{Kind: RelayActionReadTarget, Generation: 1, MaxBytes: MaxTargetReadBytes + 1},
		{Kind: RelayActionWriteTarget, Generation: 1},
		{Kind: RelayActionWriteTarget, Generation: 1, data: make([]byte, MaxTargetWriteBytes+1)},
		{Kind: RelayActionSendMessage, Generation: 1},
	}
	for _, action := range invalid {
		if _, _, err := executor.Execute(action); !errors.Is(err, ErrInvalidTargetIOAction) {
			t.Fatalf("action %#v error = %v", action, err)
		}
	}
	event, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionWriteTarget, Generation: 2, data: []byte("four"),
	})
	if err != nil || !hasResult || !errors.Is(event.Err, ErrTargetIOContract) || event.N != 5 {
		t.Fatalf("contract violation = %#v, %v, %v", event, hasResult, err)
	}
}

func TestTargetIOExecutorAcceptsFullReceiveWindowWrite(t *testing.T) {
	connection := &scriptedTargetConnection{writeN: MaxTargetWriteBytes}
	executor, _ := NewTargetIOExecutor(connection)
	payload := make([]byte, MaxTargetWriteBytes)
	event, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionWriteTarget, Generation: 1, data: payload,
	})
	if err != nil || !hasResult || event.Err != nil || event.N != len(payload) {
		t.Fatalf("full-window write = %#v, %v, %v", event, hasResult, err)
	}
}

type scriptedTargetConnection struct {
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

func (connection *scriptedTargetConnection) Read(buffer []byte) (int, error) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	n := copy(buffer, connection.readData)
	return n, connection.readErr
}

func (connection *scriptedTargetConnection) Write(buffer []byte) (int, error) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.written = append(connection.written, buffer...)
	return connection.writeN, connection.writeErr
}

func (connection *scriptedTargetConnection) CloseWrite() error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.closeWrites++
	return connection.closeWriteErr
}

func (connection *scriptedTargetConnection) Close() error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.closes++
	return nil
}

func (connection *scriptedTargetConnection) LocalAddr() net.Addr              { return testAddress("local") }
func (connection *scriptedTargetConnection) RemoteAddr() net.Addr             { return testAddress("remote") }
func (connection *scriptedTargetConnection) SetDeadline(time.Time) error      { return nil }
func (connection *scriptedTargetConnection) SetReadDeadline(time.Time) error  { return nil }
func (connection *scriptedTargetConnection) SetWriteDeadline(time.Time) error { return nil }

func (connection *scriptedTargetConnection) writtenBytes() []byte {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return append([]byte(nil), connection.written...)
}

func (connection *scriptedTargetConnection) closeWriteCount() int {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.closeWrites
}
