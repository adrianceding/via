package flow

import (
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

// BenchmarkTxAppend64KB measures the unbound compatibility path: one payload
// clone, replaySegment allocation and the attempts map.
func BenchmarkTxAppend64KB(b *testing.B) {
	payload := make([]byte, protocol.MaxDataLength)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := NewTx()
		if _, err := tx.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTxAppendThenEncode64KB reproduces the previous production path:
// clone into replay storage, then allocate and copy a second complete DATA
// frame for the send attempt.
func BenchmarkTxAppendThenEncode64KB(b *testing.B) {
	payload := make([]byte, protocol.MaxDataLength)
	flowID := protocol.FlowID{1}
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := NewTx()
		item, err := tx.Append(payload)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := protocol.EncodeData(flowID, item.Offset, item); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTxAppendEncoded64KB measures the production path: one frame-sized
// replay allocation, one payload copy and in-place DATA header encoding. Taking
// the encoded frame view performs no additional payload allocation or copy.
func BenchmarkTxAppendEncoded64KB(b *testing.B) {
	payload := make([]byte, protocol.MaxDataLength)
	flowID := protocol.FlowID{1}
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := NewTx()
		if err := tx.BindFlowID(flowID); err != nil {
			b.Fatal(err)
		}
		item, err := tx.Append(payload)
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := item.EncodedDataFrame(); !ok {
			b.Fatal("missing encoded DATA frame")
		}
	}
}

// BenchmarkRxReceiveData64KB measures one maximum DATA interval into a fresh
// Receiver: overlap scan, range insert and the ACK snapshot ranges slice.
func BenchmarkRxReceiveData64KB(b *testing.B) {
	payload := make([]byte, protocol.MaxDataLength)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		receiver := NewReceiver()
		if _, err := receiver.ReceiveData(0, payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTxRetryDue measures the retransmission path: earliestGap scan plus
// segmentAt linear search across a full send window (80 x 4 KiB segments fills
// SendWindowSize without hitting MaxReplaySegments).
func BenchmarkTxRetryDue(b *testing.B) {
	tx := NewTx()
	chunk := make([]byte, 4<<10)
	for i := 0; i < int(SendWindowSize/(4<<10)); i++ {
		if _, err := tx.Append(chunk); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := tx.RetryDue(); !ok {
			b.Fatal("no retry due")
		}
	}
}
