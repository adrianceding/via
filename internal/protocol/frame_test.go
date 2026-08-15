package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseHeaderValidatesBeforePayloadAllocation(t *testing.T) {
	header := []byte{'V', 'I', Version, byte(TypeData), 0, 0, 0, 25}
	frameType, payloadLength, err := ParseHeader(header)
	if err != nil || frameType != TypeData || payloadLength != 25 {
		t.Fatalf("ParseHeader(valid) = %v, %d, %v", frameType, payloadLength, err)
	}

	tests := []struct {
		name   string
		header []byte
		want   error
	}{
		{name: "short", header: header[:7], want: ErrInvalidLength},
		{name: "bad magic", header: []byte{'X', 'I', Version, byte(TypeData), 0, 0, 0, 25}, want: ErrBadMagic},
		{name: "version", header: []byte{'V', 'I', Version + 1, byte(TypeData), 0, 0, 0, 25}, want: ErrUnsupportedVersion},
		{name: "type", header: []byte{'V', 'I', Version, 0xff, 0, 0, 0, 25}, want: ErrUnknownType},
		{name: "message length", header: []byte{'V', 'I', Version, byte(TypeData), 0, 0, 0, 24}, want: ErrInvalidLength},
		{name: "global length", header: []byte{'V', 'I', Version, byte(TypeData), 0, 1, 0, 1}, want: ErrInvalidLength},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := ParseHeader(test.header); !errors.Is(err, test.want) {
				t.Fatalf("ParseHeader error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestDecodeEncodedFrameUsesExactCompleteFrame(t *testing.T) {
	encoded, err := EncodeMessage(Probe{Token: 9})
	if err != nil {
		t.Fatal(err)
	}
	frame, message, err := DecodeEncodedFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != TypeProbe || message != (Probe{Token: 9}) {
		t.Fatalf("decoded frame/message = %#v / %#v", frame, message)
	}
	if &frame.Payload[0] != &encoded[HeaderSize] {
		t.Fatal("DecodeEncodedFrame copied payload")
	}
	if _, _, err := DecodeEncodedFrame(encoded[:len(encoded)-1]); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("truncated frame error = %v", err)
	}
	if _, _, err := DecodeEncodedFrame(append(encoded, 0)); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("trailing byte error = %v", err)
	}
}

func TestMessageFixedVectors(t *testing.T) {
	var challenge AuthChallenge
	for index := range challenge.Challenge {
		challenge.Challenge[index] = byte(index)
	}
	pathGroupID := PathGroupID{1}
	open := Open{
		DeliveryMode:  DeliveryAdaptive,
		PathSelection: PathFastest,
		Target: Target{
			Address: netip.MustParseAddr("192.0.2.1"),
			Port:    443,
		},
	}
	distributedConstraints := DeliveryConstraints{
		MaxDeliveryDelay: 10 * time.Millisecond,
		MaxDelayGap:      20 * time.Millisecond,
		Fallback:         DeliveryFallbackPause,
	}
	distributedConstraintHex := "00000000009896800000000001312d0002"

	tests := []struct {
		name    string
		message Message
		hex     string
	}{
		{
			name:    "auth challenge",
			message: challenge,
			hex:     "5649030100000020" + "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
		},
		{
			name:    "auth proof",
			message: AuthProof{PrincipalID: "a", PathGroupID: pathGroupID},
			hex:     "5649030200000052" + "0161" + "01000000000000000000000000000000" + zeroHex(64),
		},
		{
			name:    "auth result",
			message: AuthResult{Result: AuthSuccess},
			hex:     "564903030000000100",
		},
		{
			name:    "probe",
			message: Probe{Token: 0x0102030405060708},
			hex:     "56490304000000080102030405060708",
		},
		{
			name:    "probe ack",
			message: ProbeACK{Token: 0x8877665544332211},
			hex:     "56490305000000208877665544332211" + zeroHex(24),
		},
		{
			name:    "open",
			message: open,
			hex:     "564903100000004a" + zeroHex(48) + "0201" + zeroHex(17) + "01c000020101bb",
		},
		{
			name: "open distributed constraints",
			message: Open{
				DeliveryMode:  DeliveryAdaptive,
				PathSelection: PathDistributed,
				Constraints:   distributedConstraints,
				Target:        open.Target,
			},
			hex: "564903100000004a" + zeroHex(48) + "0202" + distributedConstraintHex + "01c000020101bb",
		},
		{
			name: "open result",
			message: OpenResult{
				Result:        OpenSuccess,
				DeliveryMode:  DeliveryAdaptive,
				PathSelection: PathFastest,
			},
			hex: "5649031100000044" + zeroHex(16) + "00" + zeroHex(32) + "0201" + zeroHex(17),
		},
		{
			name: "open result distributed constraints",
			message: OpenResult{
				Result:        OpenSuccess,
				DeliveryMode:  DeliveryAdaptive,
				PathSelection: PathDistributed,
				Constraints:   distributedConstraints,
			},
			hex: "5649031100000044" + zeroHex(16) + "00" + zeroHex(32) + "0202" + distributedConstraintHex,
		},
		{
			name:    "join",
			message: Join{},
			hex:     "5649031200000030" + zeroHex(48),
		},
		{
			name:    "join result",
			message: JoinResult{Result: JoinSuccess},
			hex:     "5649031300000011" + zeroHex(16) + "00",
		},
		{
			name:    "data",
			message: Data{Offset: 1, Bytes: []byte{0xaa}},
			hex:     "5649032000000019" + zeroHex(16) + "0000000000000001aa",
		},
		{
			name: "ack",
			message: ACK{
				NextOffset: 1,
				Ranges:     []ACKRange{{Start: 3, End: 5}},
			},
			hex: "5649032100000029" + zeroHex(16) + "00000000000000010100000000000000030000000000000005",
		},
		{
			name:    "fin",
			message: FIN{FinalOffset: 9},
			hex:     "5649032200000018" + zeroHex(16) + "0000000000000009",
		},
		{
			name:    "fin ack",
			message: FINACK{FinalOffset: 9},
			hex:     "5649032300000018" + zeroHex(16) + "0000000000000009",
		},
		{
			name:    "reset",
			message: Reset{Reason: ResetProtocolConflict},
			hex:     "5649032400000012" + zeroHex(16) + "0002",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want, err := hex.DecodeString(test.hex)
			if err != nil {
				t.Fatalf("invalid test vector: %v", err)
			}
			encoded, err := EncodeMessage(test.message)
			if err != nil {
				t.Fatalf("EncodeMessage() error = %v", err)
			}
			if !bytes.Equal(encoded, want) {
				t.Fatalf("encoded = %x, want %x", encoded, want)
			}

			decoder := NewDecoder(&oneByteReader{reader: bytes.NewReader(want)})
			frame, err := decoder.ReadFrame()
			if err != nil {
				t.Fatalf("ReadFrame() error = %v", err)
			}
			decoded, err := DecodeMessage(frame)
			if err != nil {
				t.Fatalf("DecodeMessage() error = %v", err)
			}
			if !reflect.DeepEqual(decoded, test.message) {
				t.Fatalf("decoded = %#v, want %#v", decoded, test.message)
			}
		})
	}
}

func TestProbeACKSendCapacityFixedVector(t *testing.T) {
	message := ProbeACK{
		Token:                      0x0102030405060708,
		SendCapacityBytesSec:       0x1112131415161718,
		SendCapacitySampleAgeNanos: 0x2122232425262728,
		SendCapacityFreshForNanos:  0x3132333435363738,
	}
	encoded, err := EncodeMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	want := "5649030500000020" +
		"0102030405060708" +
		"1112131415161718" +
		"2122232425262728" +
		"3132333435363738"
	if hex.EncodeToString(encoded) != want {
		t.Fatalf("encoded probe ack = %x, want %s", encoded, want)
	}
	frame, decoded, err := DecodeEncodedFrame(encoded)
	if err != nil || frame.Type != TypeProbeACK || decoded != message {
		t.Fatalf("decoded probe ack = %#v / %#v / %v", frame, decoded, err)
	}
}

func TestProbeACKRejectsInvalidSendCapacity(t *testing.T) {
	for _, message := range []ProbeACK{
		{Token: 1, SendCapacitySampleAgeNanos: 1},
		{Token: 1, SendCapacityFreshForNanos: 1},
		{Token: 1, SendCapacityBytesSec: 1},
		{Token: 1, SendCapacityBytesSec: 1, SendCapacitySampleAgeNanos: math.MaxInt64 + 1, SendCapacityFreshForNanos: 1},
		{Token: 1, SendCapacityBytesSec: 1, SendCapacityFreshForNanos: math.MaxInt64 + 1},
	} {
		if _, err := EncodeMessage(message); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("EncodeMessage(%#v) error = %v, want invalid payload", message, err)
		}
	}
}

func TestDecoderReadsConcatenatedFrames(t *testing.T) {
	first, err := EncodeMessage(Probe{Token: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeMessage(AuthResult{Result: AuthFailure})
	if err != nil {
		t.Fatal(err)
	}
	decoder := NewDecoder(bytes.NewReader(append(first, second...)))

	for index, wantType := range []Type{TypeProbe, TypeAuthResult} {
		frame, readErr := decoder.ReadFrame()
		if readErr != nil {
			t.Fatalf("frame %d error = %v", index, readErr)
		}
		if frame.Type != wantType {
			t.Fatalf("frame %d type = %v, want %v", index, frame.Type, wantType)
		}
	}
	if _, err := decoder.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("final ReadFrame() error = %v, want EOF", err)
	}
}

func TestDecoderRejectsHeaderBeforePayloadAllocation(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "magic", data: rawHeader('X', 'I', Version, TypeProbe, 8), want: ErrBadMagic},
		{name: "version", data: rawHeader('V', 'I', Version+1, TypeProbe, 8), want: ErrUnsupportedVersion},
		{name: "type", data: rawHeader('V', 'I', Version, Type(0xff), 8), want: ErrUnknownType},
		{name: "global length", data: rawHeader('V', 'I', Version, TypeData, MaxPayloadSize+1), want: ErrInvalidLength},
		{name: "message length", data: rawHeader('V', 'I', Version, TypeProbe, 9), want: ErrInvalidLength},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &countingReader{reader: bytes.NewReader(test.data)}
			_, err := NewDecoder(reader).ReadFrame()
			if !errors.Is(err, test.want) {
				t.Fatalf("ReadFrame() error = %v, want %v", err, test.want)
			}
			if reader.read != HeaderSize {
				t.Fatalf("reader consumed %d bytes, want header only", reader.read)
			}
		})
	}
}

func TestDecoderRejectsTruncatedInput(t *testing.T) {
	valid, err := EncodeMessage(Probe{Token: 1})
	if err != nil {
		t.Fatal(err)
	}
	for length := 1; length < len(valid); length++ {
		_, readErr := NewDecoder(bytes.NewReader(valid[:length])).ReadFrame()
		if readErr == nil {
			t.Fatalf("prefix length %d unexpectedly accepted", length)
		}
	}
}

func TestDataBoundariesAndOffsetOverflow(t *testing.T) {
	maximum := Data{Bytes: make([]byte, MaxDataLength)}
	encoded, err := EncodeMessage(maximum)
	if err != nil {
		t.Fatalf("maximum DATA error = %v", err)
	}
	if len(encoded) != MaxFrameSize {
		t.Fatalf("maximum frame size = %d, want %d", len(encoded), MaxFrameSize)
	}

	for name, message := range map[string]Data{
		"empty":     {},
		"too large": {Bytes: make([]byte, MaxDataLength+1)},
		"overflow":  {Offset: math.MaxUint64, Bytes: []byte{1}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := EncodeMessage(message); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("EncodeMessage() error = %v, want invalid payload", err)
			}
		})
	}
}

func TestEncodeDataBuildsOneOwnedFrame(t *testing.T) {
	flowID := FlowID{1, 2, 3}
	source := testDataSource{data: bytes.Repeat([]byte{0xa5}, MaxDataLength)}
	allocations := testing.AllocsPerRun(100, func() {
		encoded, err := EncodeData(flowID, 17, source)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != MaxFrameSize || encoded[3] != byte(TypeData) ||
			binary.BigEndian.Uint32(encoded[4:8]) != MaxPayloadSize ||
			!bytes.Equal(encoded[8:24], flowID[:]) || binary.BigEndian.Uint64(encoded[24:32]) != 17 ||
			!bytes.Equal(encoded[32:], source.data) {
			t.Fatal("direct DATA encoding changed the wire representation")
		}
	})
	if allocations != 1 {
		t.Fatalf("EncodeData allocations = %.0f, want 1", allocations)
	}

	encoded, err := EncodeData(flowID, 17, source)
	if err != nil {
		t.Fatal(err)
	}
	source.data[0] = 0
	if encoded[32] != 0xa5 {
		t.Fatal("encoded DATA aliases the source payload")
	}
}

func TestEncodeDataFrameInPlace(t *testing.T) {
	flowID := FlowID{1, 2, 3}
	payload := []byte("payload")
	encoded := make([]byte, DataFramePrefixSize+len(payload))
	copy(encoded[DataFramePrefixSize:], payload)
	if err := EncodeDataFrameInPlace(encoded, flowID, 17); err != nil {
		t.Fatal(err)
	}
	if encoded[3] != byte(TypeData) || binary.BigEndian.Uint32(encoded[4:8]) != uint32(24+len(payload)) ||
		!bytes.Equal(encoded[8:24], flowID[:]) || binary.BigEndian.Uint64(encoded[24:32]) != 17 ||
		!bytes.Equal(encoded[DataFramePrefixSize:], payload) {
		t.Fatalf("in-place DATA frame = %x", encoded)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if err := EncodeDataFrameInPlace(encoded, flowID, 17); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("EncodeDataFrameInPlace allocations = %.0f, want 0", allocations)
	}
}

func TestEncodeDataFrameInPlaceRejectsInvalidFrame(t *testing.T) {
	for name, test := range map[string]struct {
		encoded []byte
		offset  uint64
	}{
		"missing payload": {encoded: make([]byte, DataFramePrefixSize)},
		"too large":       {encoded: make([]byte, DataFramePrefixSize+MaxDataLength+1)},
		"offset overflow": {encoded: make([]byte, DataFramePrefixSize+1), offset: math.MaxUint64},
	} {
		t.Run(name, func(t *testing.T) {
			before := bytes.Clone(test.encoded)
			if err := EncodeDataFrameInPlace(test.encoded, FlowID{1}, test.offset); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("error = %v, want invalid payload", err)
			}
			if !bytes.Equal(test.encoded, before) {
				t.Fatal("invalid frame was partially modified")
			}
		})
	}
}

type testDataSource struct {
	data []byte
}

func (source testDataSource) DataLen() int { return len(source.data) }

func (source testDataSource) AppendData(destination []byte) []byte {
	return append(destination, source.data...)
}

func TestACKRangeBoundaries(t *testing.T) {
	ranges := make([]ACKRange, MaxACKRanges)
	for index := range ranges {
		start := uint64(index*2 + 1)
		ranges[index] = ACKRange{Start: start, End: start + 1}
	}
	encoded, err := EncodeMessage(ACK{Ranges: ranges})
	if err != nil {
		t.Fatalf("maximum ACK error = %v", err)
	}
	if got, want := len(encoded), HeaderSize+281; got != want {
		t.Fatalf("maximum ACK frame = %d, want %d", got, want)
	}

	tests := []struct {
		name   string
		next   uint64
		ranges []ACKRange
	}{
		{name: "starts at cumulative", next: 2, ranges: []ACKRange{{Start: 2, End: 3}}},
		{name: "empty", ranges: []ACKRange{{Start: 2, End: 2}}},
		{name: "overlap", ranges: []ACKRange{{Start: 2, End: 5}, {Start: 4, End: 6}}},
		{name: "adjacent", ranges: []ACKRange{{Start: 2, End: 5}, {Start: 5, End: 6}}},
		{name: "too many", ranges: append(ranges, ACKRange{Start: 40, End: 41})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EncodeMessage(ACK{NextOffset: test.next, Ranges: test.ranges}); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("EncodeMessage() error = %v, want invalid payload", err)
			}
		})
	}
}

func TestDecoderRejectsACKCountMismatch(t *testing.T) {
	payload := make([]byte, 25)
	payload[24] = 1
	encoded := append(rawHeader('V', 'I', Version, TypeACK, uint32(len(payload))), payload...)
	if _, err := NewDecoder(bytes.NewReader(encoded)).ReadFrame(); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("ReadFrame() error = %v, want invalid payload", err)
	}
}

func TestOpenPolicyAndTargetValidation(t *testing.T) {
	validTarget := Target{DNSName: "example.test", Port: 443}
	tests := []struct {
		name    string
		message Open
	}{
		{name: "redundant with selector", message: Open{DeliveryMode: DeliveryRedundant, PathSelection: PathFastest, Target: validTarget}},
		{name: "adaptive without selector", message: Open{DeliveryMode: DeliveryAdaptive, PathSelection: PathNone, Target: validTarget}},
		{name: "zero port", message: Open{DeliveryMode: DeliveryAdaptive, PathSelection: PathFastest, Target: Target{DNSName: "example.test"}}},
		{name: "uppercase dns", message: Open{DeliveryMode: DeliveryAdaptive, PathSelection: PathFastest, Target: Target{DNSName: "Example.test", Port: 443}}},
		{name: "target union", message: Open{DeliveryMode: DeliveryAdaptive, PathSelection: PathFastest, Target: Target{Address: netip.MustParseAddr("192.0.2.1"), DNSName: "example.test", Port: 443}}},
		{name: "zoned ipv6", message: Open{DeliveryMode: DeliveryAdaptive, PathSelection: PathFastest, Target: Target{Address: netip.MustParseAddr("fe80::1%eth0"), Port: 443}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EncodeMessage(test.message); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("EncodeMessage() error = %v, want invalid payload", err)
			}
		})
	}
}

func TestDeliveryConstraintsRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		constraints DeliveryConstraints
	}{
		{
			name: "configured",
			constraints: DeliveryConstraints{
				MaxDeliveryDelay: 80 * time.Millisecond,
				MaxDelayGap:      30 * time.Millisecond,
				Fallback:         DeliveryFallbackPause,
			},
		},
		{
			name: "minimum",
			constraints: DeliveryConstraints{
				MaxDeliveryDelay: MinimumDeliveryConstraint,
				MaxDelayGap:      MinimumDeliveryConstraint,
				Fallback:         DeliveryFallbackFastest,
			},
		},
		{
			name: "maximum",
			constraints: DeliveryConstraints{
				MaxDeliveryDelay: MaximumDeliveryConstraint,
				MaxDelayGap:      MaximumDeliveryConstraint,
				Fallback:         DeliveryFallbackPause,
			},
		},
	}
	target := Target{DNSName: "example.test", Port: 443}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			messages := []Message{
				Open{
					DeliveryMode:  DeliveryAdaptive,
					PathSelection: PathDistributed,
					Constraints:   test.constraints,
					Target:        target,
				},
				OpenResult{
					Result:        OpenSuccess,
					DeliveryMode:  DeliveryAdaptive,
					PathSelection: PathDistributed,
					Constraints:   test.constraints,
				},
			}

			for _, message := range messages {
				encoded, err := EncodeMessage(message)
				if err != nil {
					t.Fatalf("EncodeMessage(%T) error = %v", message, err)
				}
				_, decoded, err := DecodeEncodedFrame(encoded)
				if err != nil {
					t.Fatalf("DecodeEncodedFrame(%T) error = %v", message, err)
				}
				if !reflect.DeepEqual(decoded, message) {
					t.Fatalf("decoded %T = %#v, want %#v", message, decoded, message)
				}
			}
		})
	}
	if !ValidDeliveryPolicy(
		DeliveryAdaptive,
		PathDistributed,
		DeliveryConstraints{Fallback: DeliveryFallbackFastest},
	) {
		t.Fatal("zero duration constraints unexpectedly rejected")
	}
}

func TestDeliveryConstraintsRejectInvalidCombinations(t *testing.T) {
	validTarget := Target{DNSName: "example.test", Port: 443}
	tests := []struct {
		name        string
		mode        DeliveryMode
		selection   PathSelection
		constraints DeliveryConstraints
	}{
		{
			name:        "redundant carries constraints",
			mode:        DeliveryRedundant,
			selection:   PathNone,
			constraints: DeliveryConstraints{Fallback: DeliveryFallbackFastest},
		},
		{
			name:        "fastest carries constraints",
			mode:        DeliveryAdaptive,
			selection:   PathFastest,
			constraints: DeliveryConstraints{Fallback: DeliveryFallbackFastest},
		},
		{
			name:      "distributed missing fallback",
			mode:      DeliveryAdaptive,
			selection: PathDistributed,
		},
		{
			name:        "distributed unknown fallback",
			mode:        DeliveryAdaptive,
			selection:   PathDistributed,
			constraints: DeliveryConstraints{Fallback: DeliveryConstraintFallback(3)},
		},
		{
			name:        "delivery delay below minimum",
			mode:        DeliveryAdaptive,
			selection:   PathDistributed,
			constraints: DeliveryConstraints{MaxDeliveryDelay: MinimumDeliveryConstraint - time.Nanosecond, Fallback: DeliveryFallbackFastest},
		},
		{
			name:        "delivery delay above maximum",
			mode:        DeliveryAdaptive,
			selection:   PathDistributed,
			constraints: DeliveryConstraints{MaxDeliveryDelay: MaximumDeliveryConstraint + time.Nanosecond, Fallback: DeliveryFallbackFastest},
		},
		{
			name:        "delivery delay negative",
			mode:        DeliveryAdaptive,
			selection:   PathDistributed,
			constraints: DeliveryConstraints{MaxDeliveryDelay: -time.Nanosecond, Fallback: DeliveryFallbackFastest},
		},
		{
			name:        "delay gap below minimum",
			mode:        DeliveryAdaptive,
			selection:   PathDistributed,
			constraints: DeliveryConstraints{MaxDelayGap: MinimumDeliveryConstraint - time.Nanosecond, Fallback: DeliveryFallbackPause},
		},
		{
			name:        "delay gap above maximum",
			mode:        DeliveryAdaptive,
			selection:   PathDistributed,
			constraints: DeliveryConstraints{MaxDelayGap: MaximumDeliveryConstraint + time.Nanosecond, Fallback: DeliveryFallbackPause},
		},
		{
			name:        "delay gap negative",
			mode:        DeliveryAdaptive,
			selection:   PathDistributed,
			constraints: DeliveryConstraints{MaxDelayGap: -time.Nanosecond, Fallback: DeliveryFallbackPause},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if ValidDeliveryPolicy(test.mode, test.selection, test.constraints) {
				t.Fatal("ValidDeliveryPolicy() = true, want false")
			}
			messages := []Message{
				Open{
					DeliveryMode:  test.mode,
					PathSelection: test.selection,
					Constraints:   test.constraints,
					Target:        validTarget,
				},
				OpenResult{
					Result:        OpenSuccess,
					DeliveryMode:  test.mode,
					PathSelection: test.selection,
					Constraints:   test.constraints,
				},
			}
			for _, message := range messages {
				if _, err := EncodeMessage(message); !errors.Is(err, ErrInvalidPayload) {
					t.Fatalf("EncodeMessage(%T) error = %v, want invalid payload", message, err)
				}
			}
		})
	}
}

func TestOpenResultFailureRejectsSuccessOnlyFields(t *testing.T) {
	tests := []OpenResult{
		{Result: OpenConnectFailed, Capability: Capability{1}},
		{Result: OpenConnectFailed, DeliveryMode: DeliveryAdaptive},
		{Result: OpenConnectFailed, PathSelection: PathFastest},
		{Result: OpenConnectFailed, Constraints: DeliveryConstraints{Fallback: DeliveryFallbackFastest}},
		{Result: OpenConnectFailed, ImplicitAttachment: true},
	}
	for _, message := range tests {
		if _, err := EncodeMessage(message); !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("EncodeMessage(%#v) error = %v, want invalid payload", message, err)
		}
	}
}

func TestDeliveryConstraintsDecodeRejectsOverflow(t *testing.T) {
	message := Open{
		DeliveryMode:  DeliveryAdaptive,
		PathSelection: PathDistributed,
		Constraints:   DeliveryConstraints{Fallback: DeliveryFallbackFastest},
		Target:        Target{DNSName: "a", Port: 443},
	}
	encoded, err := EncodeMessage(message)
	if err != nil {
		t.Fatal(err)
	}

	for _, offset := range []int{50, 58} {
		t.Run(map[int]string{50: "delivery delay", 58: "delay gap"}[offset], func(t *testing.T) {
			malformed := append([]byte(nil), encoded...)
			malformed[HeaderSize+offset] = 0x80
			if _, _, err := DecodeEncodedFrame(malformed); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("DecodeEncodedFrame() error = %v, want invalid payload", err)
			}
		})
	}
}

func TestOpenFrameLengthBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		frameType Type
		length    uint32
		valid     bool
	}{
		{name: "open below minimum", frameType: TypeOpen, length: 71},
		{name: "open minimum", frameType: TypeOpen, length: 72, valid: true},
		{name: "open maximum", frameType: TypeOpen, length: 324, valid: true},
		{name: "open above maximum", frameType: TypeOpen, length: 325},
		{name: "open result failure", frameType: TypeOpenResult, length: 17, valid: true},
		{name: "open result old success", frameType: TypeOpenResult, length: 51},
		{name: "open result success", frameType: TypeOpenResult, length: 68, valid: true},
		{name: "open result implicit attachment", frameType: TypeOpenResult, length: 69, valid: true},
		{name: "open result above success", frameType: TypeOpenResult, length: 70},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := ParseHeader(rawHeader('V', 'I', Version, test.frameType, test.length))
			if test.valid && err != nil {
				t.Fatalf("ParseHeader() error = %v", err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidLength) {
				t.Fatalf("ParseHeader() error = %v, want invalid length", err)
			}
		})
	}
}

func TestDNSBoundary(t *testing.T) {
	name := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	if len(name) != 253 {
		t.Fatalf("test name length = %d", len(name))
	}
	message := Open{
		DeliveryMode:  DeliveryAdaptive,
		PathSelection: PathDistributed,
		Constraints:   DeliveryConstraints{Fallback: DeliveryFallbackFastest},
		Target:        Target{DNSName: name, Port: 65535},
	}
	encoded, err := EncodeMessage(message)
	if err != nil {
		t.Fatalf("maximum DNS OPEN error = %v", err)
	}
	if got, want := len(encoded), HeaderSize+324; got != want {
		t.Fatalf("maximum DNS frame = %d, want %d", got, want)
	}

	message.Target.DNSName += "e"
	if _, err := EncodeMessage(message); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("oversized DNS error = %v, want invalid payload", err)
	}
}

func TestWriteFrameHandlesShortWriter(t *testing.T) {
	frame, err := FrameForMessage(Probe{Token: 1})
	if err != nil {
		t.Fatal(err)
	}
	var destination bytes.Buffer
	if err := WriteFrame(&limitedWriter{writer: &destination, maximum: 1}, frame); err != nil {
		t.Fatalf("WriteFrame() error = %v", err)
	}
	want, err := EncodeFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(destination.Bytes(), want) {
		t.Fatalf("written = %x, want %x", destination.Bytes(), want)
	}
	if err := WriteFrame(zeroWriter{}, frame); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero writer error = %v, want short write", err)
	}
}

func FuzzDecoder(f *testing.F) {
	for _, message := range []Message{
		Probe{Token: 1},
		Data{Offset: 4, Bytes: []byte("payload")},
		ACK{NextOffset: 2, Ranges: []ACKRange{{Start: 4, End: 8}}},
	} {
		encoded, err := EncodeMessage(message)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
	}
	f.Add([]byte("not a frame"))

	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := NewDecoder(bytes.NewReader(data)).ReadFrame()
		if err != nil {
			return
		}
		if len(frame.Payload) > MaxPayloadSize {
			t.Fatalf("payload length = %d", len(frame.Payload))
		}
		if _, err := DecodeMessage(frame); err != nil {
			t.Fatalf("decoder accepted frame that message decoder rejected: %v", err)
		}
	})
}

type oneByteReader struct {
	reader io.Reader
}

func (reader *oneByteReader) Read(buffer []byte) (int, error) {
	if len(buffer) > 1 {
		buffer = buffer[:1]
	}
	return reader.reader.Read(buffer)
}

type countingReader struct {
	reader io.Reader
	read   int
}

func (reader *countingReader) Read(buffer []byte) (int, error) {
	n, err := reader.reader.Read(buffer)
	reader.read += n
	return n, err
}

type limitedWriter struct {
	writer  io.Writer
	maximum int
}

func (writer *limitedWriter) Write(buffer []byte) (int, error) {
	if len(buffer) > writer.maximum {
		buffer = buffer[:writer.maximum]
	}
	return writer.writer.Write(buffer)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func rawHeader(first, second byte, version byte, frameType Type, payloadLength uint32) []byte {
	header := make([]byte, HeaderSize)
	header[0] = first
	header[1] = second
	header[2] = version
	header[3] = byte(frameType)
	binary.BigEndian.PutUint32(header[4:], payloadLength)
	return header
}

func zeroHex(bytes int) string {
	return strings.Repeat("00", bytes)
}
