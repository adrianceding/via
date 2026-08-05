package protocol

import (
	"encoding/binary"
	"testing"
)

// BenchmarkEncodeFrame64KB measures EncodeFrame for one maximum DATA frame.
// It includes the redundant DecodeMessage round-trip validation on the encode
// path; the hand-rolled variant below isolates that cost.
func BenchmarkEncodeFrame64KB(b *testing.B) {
	frame := Frame{Type: TypeData, Payload: make([]byte, MaxDataLength)}
	b.SetBytes(MaxFrameSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeFrame(frame); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeFrameWithoutMessageValidation is the same encoding without
// the DecodeMessage validation pass: header write plus payload copy only.
func BenchmarkEncodeFrameWithoutMessageValidation(b *testing.B) {
	frame := Frame{Type: TypeData, Payload: make([]byte, MaxDataLength)}
	b.SetBytes(MaxFrameSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded := make([]byte, HeaderSize+len(frame.Payload))
		encoded[0] = 'V'
		encoded[1] = 'I'
		encoded[2] = Version
		encoded[3] = byte(frame.Type)
		binary.BigEndian.PutUint32(encoded[4:8], uint32(len(frame.Payload)))
		copy(encoded[HeaderSize:], frame.Payload)
		_ = encoded
	}
}

// BenchmarkDecodeMessage64KB measures the validation pass alone, so its cost
// relative to the encode benchmarks is visible directly.
func BenchmarkDecodeMessage64KB(b *testing.B) {
	frame := Frame{Type: TypeData, Payload: make([]byte, MaxDataLength)}
	b.SetBytes(MaxFrameSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeMessage(frame); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeDataFrameInPlace64KB measures header encoding into an
// existing frame backing. It performs no allocation and does not copy payload.
func BenchmarkEncodeDataFrameInPlace64KB(b *testing.B) {
	encoded := make([]byte, DataFramePrefixSize+MaxDataLength)
	flowID := FlowID{1}
	b.SetBytes(MaxFrameSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := EncodeDataFrameInPlace(encoded, flowID, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseHeader measures the header validation used on the read path.
func BenchmarkParseHeader(b *testing.B) {
	header := [HeaderSize]byte{'V', 'I', Version, byte(TypeData)}
	binary.BigEndian.PutUint32(header[4:8], MaxDataLength)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := ParseHeader(header[:]); err != nil {
			b.Fatal(err)
		}
	}
}
