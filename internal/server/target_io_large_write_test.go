package server

import (
	"bytes"
	"io"
	"net"
	"testing"
)

func TestTargetIOExecutorAcceptsReassembledReceiveWindowWrite(t *testing.T) {
	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()

	executor, err := NewTargetIOExecutor(local)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x5a}, MaxTargetWriteBytes)
	readDone := make(chan error, 1)
	go func() {
		received := make([]byte, len(payload))
		_, readErr := io.ReadFull(peer, received)
		if readErr == nil && !bytes.Equal(received, payload) {
			readErr = ErrTargetIOContract
		}
		readDone <- readErr
	}()

	event, hasResult, err := executor.Execute(RelayAction{
		Kind: RelayActionWriteTarget, Generation: 1, data: payload,
	})
	if err != nil || !hasResult || event.Err != nil || event.N != len(payload) {
		t.Fatalf("receive-window write result = %#v, %v, %v", event, hasResult, err)
	}
	if readErr := <-readDone; readErr != nil {
		t.Fatal(readErr)
	}
}

func TestTargetIOExecutorRejectsWriteBeyondReceiveWindow(t *testing.T) {
	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()

	executor, err := NewTargetIOExecutor(local)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = executor.Execute(RelayAction{
		Kind: RelayActionWriteTarget, Generation: 1, data: make([]byte, MaxTargetWriteBytes+1),
	})
	if err != ErrInvalidTargetIOAction {
		t.Fatalf("oversized write error = %v", err)
	}
}
