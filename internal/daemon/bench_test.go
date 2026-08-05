package daemon

import (
	"context"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

// benchConnection succeeds immediately on every WriteFrame without copying,
// isolating scheduler enqueue, worker handoff and result channel round trip
// from carrier write cost.
type benchConnection struct{}

func (benchConnection) Capabilities() transport.Capabilities {
	capabilities, err := transport.NewCapabilities(transport.CapabilitySpec{MaxEncodedFrame: protocol.MaxFrameSize})
	if err != nil {
		panic(err)
	}
	return capabilities
}

func (benchConnection) QueueLimits() transport.QueueLimits { return transport.V1QueueLimits() }
func (benchConnection) LocalEndpoint() string              { return "bench:1" }
func (benchConnection) RemoteEndpoint() string             { return "bench:2" }
func (benchConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, transport.ErrClosed
}
func (benchConnection) WriteFrame(context.Context, transport.WriteRequest) error { return nil }
func (benchConnection) CloseWrite() error                                        { return nil }
func (benchConnection) Close() error                                             { return nil }

// BenchmarkSessionRuntimeSubmitData measures one synchronous maximum DATA
// frame through admit -> scheduler -> worker -> completeWrite -> result. This
// includes request allocation, per-frame context.WithTimeout and worker
// handoff, but no encoded-frame clone.
func BenchmarkSessionRuntimeSubmitData(b *testing.B) {
	runtime, err := newSessionRuntime(context.Background(), benchConnection{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { runtime.close(transport.ErrClosed) })

	encoded, err := protocol.EncodeMessage(protocol.Data{Bytes: make([]byte, protocol.MaxDataLength)})
	if err != nil {
		b.Fatal(err)
	}
	request := transport.WriteRequest{Class: transport.FrameData, Encoded: encoded}
	flowID := protocol.FlowID{1}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runtime.submit(context.Background(), request, flowID, uint64(i+1), 1, protocol.MaxDataLength); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSessionRuntimeSubmitControl measures the same path for a small
// control frame through the control queue.
func BenchmarkSessionRuntimeSubmitControl(b *testing.B) {
	runtime, err := newSessionRuntime(context.Background(), benchConnection{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { runtime.close(transport.ErrClosed) })

	encoded, err := protocol.EncodeMessage(protocol.Probe{Token: 42})
	if err != nil {
		b.Fatal(err)
	}
	request := transport.WriteRequest{Class: transport.FrameControl, Encoded: encoded}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runtime.submit(context.Background(), request, protocol.FlowID{}, 0, 0, 0); err != nil {
			b.Fatal(err)
		}
	}
}
