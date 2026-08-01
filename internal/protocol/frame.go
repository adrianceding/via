package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	Version        = 1
	HeaderSize     = 8
	MaxPayloadSize = 65_536
	MaxFrameSize   = HeaderSize + MaxPayloadSize
)

var (
	ErrBadMagic           = errors.New("protocol: bad magic")
	ErrUnsupportedVersion = errors.New("protocol: unsupported version")
	ErrUnknownType        = errors.New("protocol: unknown frame type")
	ErrInvalidLength      = errors.New("protocol: invalid payload length")
	ErrInvalidPayload     = errors.New("protocol: invalid payload")
)

type Type uint8

const (
	TypeAuthChallenge Type = 0x01
	TypeAuthProof     Type = 0x02
	TypeAuthResult    Type = 0x03
	TypeProbe         Type = 0x04
	TypeProbeACK      Type = 0x05

	TypeOpen       Type = 0x10
	TypeOpenResult Type = 0x11
	TypeJoin       Type = 0x12
	TypeJoinResult Type = 0x13

	TypeData   Type = 0x20
	TypeACK    Type = 0x21
	TypeFIN    Type = 0x22
	TypeFINACK Type = 0x23
	TypeReset  Type = 0x24
)

type Frame struct {
	Type    Type
	Payload []byte
}

func (t Type) Valid() bool {
	switch t {
	case TypeAuthChallenge, TypeAuthProof, TypeAuthResult, TypeProbe, TypeProbeACK,
		TypeOpen, TypeOpenResult, TypeJoin, TypeJoinResult,
		TypeData, TypeACK, TypeFIN, TypeFINACK, TypeReset:
		return true
	default:
		return false
	}
}

func EncodeFrame(frame Frame) ([]byte, error) {
	if err := validatePayloadLength(frame.Type, len(frame.Payload)); err != nil {
		return nil, err
	}
	if _, err := DecodeMessage(frame); err != nil {
		return nil, err
	}

	encoded := make([]byte, HeaderSize+len(frame.Payload))
	encoded[0] = 'V'
	encoded[1] = 'I'
	encoded[2] = Version
	encoded[3] = byte(frame.Type)
	binary.BigEndian.PutUint32(encoded[4:8], uint32(len(frame.Payload)))
	copy(encoded[HeaderSize:], frame.Payload)
	return encoded, nil
}

func WriteFrame(writer io.Writer, frame Frame) error {
	encoded, err := EncodeFrame(frame)
	if err != nil {
		return err
	}
	for len(encoded) > 0 {
		n, writeErr := writer.Write(encoded)
		if n < 0 || n > len(encoded) {
			return fmt.Errorf("protocol: invalid writer count %d: %w", n, io.ErrShortWrite)
		}
		encoded = encoded[n:]
		if writeErr != nil {
			return writeErr
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

type Decoder struct {
	reader io.Reader
	header [HeaderSize]byte
}

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{reader: reader}
}

// ParseHeader validates a complete v1 header, including the message-specific
// payload length, before a transport allocates or reads the payload.
func ParseHeader(header []byte) (Type, uint32, error) {
	if len(header) != HeaderSize {
		return 0, 0, fmt.Errorf("%w: header length %d", ErrInvalidLength, len(header))
	}
	if header[0] != 'V' || header[1] != 'I' {
		return 0, 0, ErrBadMagic
	}
	if header[2] != Version {
		return 0, 0, fmt.Errorf("%w: %d", ErrUnsupportedVersion, header[2])
	}
	frameType := Type(header[3])
	if !frameType.Valid() {
		return 0, 0, fmt.Errorf("%w: 0x%02x", ErrUnknownType, byte(frameType))
	}
	payloadLength := binary.BigEndian.Uint32(header[4:8])
	if payloadLength > MaxPayloadSize {
		return 0, 0, fmt.Errorf("%w: %d", ErrInvalidLength, payloadLength)
	}
	if err := validatePayloadLength(frameType, int(payloadLength)); err != nil {
		return 0, 0, err
	}
	return frameType, payloadLength, nil
}

// DecodeEncodedFrame validates one complete encoded frame without copying its
// payload. The caller must keep encoded immutable while using the result.
func DecodeEncodedFrame(encoded []byte) (Frame, Message, error) {
	if len(encoded) < HeaderSize {
		return Frame{}, nil, fmt.Errorf("%w: encoded frame length %d", ErrInvalidLength, len(encoded))
	}
	frameType, payloadLength, err := ParseHeader(encoded[:HeaderSize])
	if err != nil {
		return Frame{}, nil, err
	}
	if uint64(len(encoded)) != uint64(HeaderSize)+uint64(payloadLength) {
		return Frame{}, nil, fmt.Errorf("%w: encoded frame length %d", ErrInvalidLength, len(encoded))
	}
	frame := Frame{Type: frameType, Payload: encoded[HeaderSize:]}
	message, err := DecodeMessage(frame)
	if err != nil {
		return Frame{}, nil, err
	}
	return frame, message, nil
}

func (decoder *Decoder) ReadFrame() (Frame, error) {
	if decoder == nil || decoder.reader == nil {
		return Frame{}, fmt.Errorf("protocol: nil decoder reader: %w", ErrInvalidPayload)
	}

	n, err := io.ReadFull(decoder.reader, decoder.header[:])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return Frame{}, io.EOF
		}
		return Frame{}, err
	}
	frameType, payloadLength, err := ParseHeader(decoder.header[:])
	if err != nil {
		return Frame{}, err
	}

	payload := make([]byte, int(payloadLength))
	if _, err := io.ReadFull(decoder.reader, payload); err != nil {
		return Frame{}, err
	}
	frame := Frame{Type: frameType, Payload: payload}
	if _, err := DecodeMessage(frame); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func validatePayloadLength(frameType Type, length int) error {
	valid := false
	switch frameType {
	case TypeAuthChallenge:
		valid = length == 32
	case TypeAuthProof:
		valid = length >= 66 && length <= 129
	case TypeAuthResult:
		valid = length == 1
	case TypeProbe, TypeProbeACK:
		valid = length == 8
	case TypeOpen:
		valid = length >= 72 && length <= 324
	case TypeOpenResult:
		valid = length == 17 || length == 68 || length == 69
	case TypeJoin:
		valid = length == 48
	case TypeJoinResult:
		valid = length == 17
	case TypeData:
		valid = length >= 25 && length <= MaxPayloadSize
	case TypeACK:
		valid = length >= 25 && length <= 281 && (length-25)%16 == 0
	case TypeFIN, TypeFINACK:
		valid = length == 24
	case TypeReset:
		valid = length == 18
	default:
		return fmt.Errorf("%w: 0x%02x", ErrUnknownType, byte(frameType))
	}
	if !valid {
		return fmt.Errorf("%w: type 0x%02x length %d", ErrInvalidLength, byte(frameType), length)
	}
	return nil
}
