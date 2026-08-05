package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
)

// benchCapabilities mirrors testCapabilities but accepts testing.B.
func benchCapabilities() Capabilities {
	capabilities, err := NewCapabilities(CapabilitySpec{
		MaxEncodedFrame: protocol.MaxFrameSize,
		Reliable:        true,
		Ordered:         true,
		HalfClose:       true,
	})
	if err != nil {
		panic(err)
	}
	return capabilities
}

// benchEncodedDataFrame returns one complete maximum-size DATA frame.
func benchEncodedDataFrame(b *testing.B) []byte {
	b.Helper()
	encoded, err := protocol.EncodeMessage(protocol.Data{Bytes: make([]byte, protocol.MaxDataLength)})
	if err != nil {
		b.Fatal(err)
	}
	return encoded
}

// drainPipe consumes bytes from the pipe until the peer closes, so the write
// side never blocks on the peer.
func drainPipe(connection net.Conn, done chan<- struct{}) {
	defer close(done)
	buffer := make([]byte, 1<<20)
	for {
		n, err := connection.Read(buffer)
		_ = n
		if err != nil {
			return
		}
	}
}

// BenchmarkTCPConnectionWriteFrame measures the full write-side path of one
// maximum DATA frame: validation, queue bookkeeping, writer goroutine handoff,
// deadline syscalls and the result channel round trip. context.Background hits
// the non-cancellable fast path. The peer is a bare pipe reader so only the
// transport write stack is measured.
func BenchmarkTCPConnectionWriteFrame(b *testing.B) {
	left, right := net.Pipe()
	connection := newTCPConnection(left, DefaultTCPConfig(), benchCapabilities(), V1QueueLimits(), time.Now)
	encoded := benchEncodedDataFrame(b)
	done := make(chan struct{})
	go drainPipe(right, done)
	b.Cleanup(func() {
		_ = connection.Close()
		<-done
	})

	request := WriteRequest{Class: FrameData, Encoded: encoded}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := connection.WriteFrame(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRawPipeWrite is the baseline: the same frame written directly to
// the pipe with the same write loop, no transport stack. The delta to
// BenchmarkTCPConnectionWriteFrame isolates queue bookkeeping, allocations,
// deadline syscalls and goroutine handoff.
func BenchmarkRawPipeWrite(b *testing.B) {
	left, right := net.Pipe()
	encoded := benchEncodedDataFrame(b)
	done := make(chan struct{})
	go drainPipe(right, done)
	b.Cleanup(func() {
		_ = left.Close()
		<-done
	})

	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := writeExactly(left, encoded); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTCPConnectionWriteFrameControl measures the same path for a small
// control frame, where fixed queue and handoff overhead dominates.
func BenchmarkTCPConnectionWriteFrameControl(b *testing.B) {
	left, right := net.Pipe()
	connection := newTCPConnection(left, DefaultTCPConfig(), benchCapabilities(), V1QueueLimits(), time.Now)
	encoded, err := protocol.EncodeMessage(protocol.Probe{Token: 42})
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan struct{})
	go drainPipe(right, done)
	b.Cleanup(func() {
		_ = connection.Close()
		<-done
	})

	request := WriteRequest{Class: FrameControl, Encoded: encoded}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := connection.WriteFrame(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}
