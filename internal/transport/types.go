package transport

import (
	"context"
	"errors"
	"fmt"
)

const (
	// TCPName is the only carrier registry name enabled by the v1 configuration.
	TCPName = "tcp"

	V1OutputQueueFrameLimit    uint32 = 256
	V1OutputQueueByteLimit     uint64 = 1 << 20
	V1ControlReserveFrameLimit uint32 = 16
	V1ControlReserveByteLimit  uint64 = 16 << 10
)

var (
	ErrInvalidCapabilities  = errors.New("transport: invalid capabilities")
	ErrInvalidQueueLimits   = errors.New("transport: invalid queue limits")
	ErrInvalidFrame         = errors.New("transport: invalid encoded frame")
	ErrFrameTooLarge        = errors.New("transport: encoded frame exceeds capability")
	ErrQueueFull            = errors.New("transport: output queue is full")
	ErrClosed               = errors.New("transport: connection is closed")
	ErrHalfCloseUnsupported = errors.New("transport: half-close is unsupported")
)

// CapabilitySpec is the input snapshot used to construct immutable Capabilities.
type CapabilitySpec struct {
	MaxEncodedFrame uint32
	Reliable        bool
	Ordered         bool
	Encrypted       bool
	Multiplexed     bool
	HalfClose       bool
}

// Capabilities describes carrier connection properties that remain constant for
// the entire connection lifetime. Fields cannot be modified directly; a Factory
// and its Connections must always return the same value.
type Capabilities struct {
	maxEncodedFrame uint32
	reliable        bool
	ordered         bool
	encrypted       bool
	multiplexed     bool
	halfClose       bool
}

func NewCapabilities(spec CapabilitySpec) (Capabilities, error) {
	if spec.MaxEncodedFrame == 0 {
		return Capabilities{}, fmt.Errorf("%w: maximum encoded frame must be positive", ErrInvalidCapabilities)
	}
	return Capabilities{
		maxEncodedFrame: spec.MaxEncodedFrame,
		reliable:        spec.Reliable,
		ordered:         spec.Ordered,
		encrypted:       spec.Encrypted,
		multiplexed:     spec.Multiplexed,
		halfClose:       spec.HalfClose,
	}, nil
}

func (capabilities Capabilities) MaxEncodedFrame() uint32 { return capabilities.maxEncodedFrame }
func (capabilities Capabilities) Reliable() bool          { return capabilities.reliable }
func (capabilities Capabilities) Ordered() bool           { return capabilities.ordered }
func (capabilities Capabilities) Encrypted() bool         { return capabilities.encrypted }
func (capabilities Capabilities) Multiplexed() bool       { return capabilities.multiplexed }
func (capabilities Capabilities) HalfClose() bool         { return capabilities.halfClose }

func (capabilities Capabilities) valid() bool {
	return capabilities.maxEncodedFrame != 0
}

// QueueLimits bounds both frame count and encoded bytes in one connection's
// output queue. Control reserves are included in the total limits and cannot be
// consumed by ordinary data.
type QueueLimits struct {
	MaxFrames             uint32
	MaxBytes              uint64
	ReservedControlFrames uint32
	ReservedControlBytes  uint64
}

// V1QueueLimits returns the fixed v1 output queue limits for one transport session.
func V1QueueLimits() QueueLimits {
	return QueueLimits{
		MaxFrames:             V1OutputQueueFrameLimit,
		MaxBytes:              V1OutputQueueByteLimit,
		ReservedControlFrames: V1ControlReserveFrameLimit,
		ReservedControlBytes:  V1ControlReserveByteLimit,
	}
}

// Validate checks the queue bounds and ensures the data capacity can hold at least one maximum encoded frame.
func (limits QueueLimits) Validate(capabilities Capabilities) error {
	if !capabilities.valid() {
		return fmt.Errorf("%w: zero capabilities", ErrInvalidQueueLimits)
	}
	if limits.MaxFrames == 0 || limits.MaxBytes == 0 {
		return fmt.Errorf("%w: total capacity must be positive", ErrInvalidQueueLimits)
	}
	if limits.ReservedControlFrames >= limits.MaxFrames {
		return fmt.Errorf("%w: control frame reserve leaves no data slot", ErrInvalidQueueLimits)
	}
	if limits.ReservedControlBytes >= limits.MaxBytes {
		return fmt.Errorf("%w: control byte reserve leaves no data capacity", ErrInvalidQueueLimits)
	}
	dataBytes := limits.MaxBytes - limits.ReservedControlBytes
	if uint64(capabilities.MaxEncodedFrame()) > dataBytes {
		return fmt.Errorf("%w: maximum encoded frame does not fit data capacity", ErrInvalidQueueLimits)
	}
	return nil
}

// FrameClass is supplied by the protocol coordinator; carrier adapters must not parse frame types themselves.
type FrameClass uint8

const (
	FrameData FrameClass = iota + 1
	FrameControl
)

// WriteRequest is one local send request containing a complete encoded frame.
// The caller must not modify Encoded before WriteFrame returns; nil means only
// that the local carrier write completed.
type WriteRequest struct {
	Class   FrameClass
	Encoded []byte
}

func (request WriteRequest) Validate(capabilities Capabilities) error {
	if !capabilities.valid() {
		return fmt.Errorf("%w: zero capabilities", ErrInvalidFrame)
	}
	if request.Class != FrameData && request.Class != FrameControl {
		return fmt.Errorf("%w: unknown frame class", ErrInvalidFrame)
	}
	if len(request.Encoded) == 0 {
		return fmt.Errorf("%w: empty frame", ErrInvalidFrame)
	}
	if uint64(len(request.Encoded)) > uint64(capabilities.MaxEncodedFrame()) {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(request.Encoded))
	}
	return nil
}

// Connection reads and writes only complete encoded frames. It must not interpret
// protocol messages or modify flow state. One ReadFrame and one WriteFrame may run
// concurrently on the same connection; the implementation serializes calls in
// the same direction. Close must be idempotent and unblock all pending calls.
type Connection interface {
	Capabilities() Capabilities
	QueueLimits() QueueLimits
	LocalEndpoint() string
	RemoteEndpoint() string
	ReadFrame(context.Context) ([]byte, error)
	WriteFrame(context.Context, WriteRequest) error
	CloseWrite() error
	Close() error
}

// DialOptions fully describes one dialer instance; the concrete Factory strictly validates endpoint syntax.
type DialOptions struct {
	RemoteEndpoint string
	LocalEndpoint  string
	InterfaceName  string
	QueueLimits    QueueLimits
}

// ListenOptions fully describes one listener instance; the concrete Factory strictly validates endpoint syntax.
type ListenOptions struct {
	LocalEndpoint string
	QueueLimits   QueueLimits
}

type Dialer interface {
	Dial(context.Context) (Connection, error)
}

type Listener interface {
	Accept(context.Context) (Connection, error)
	Close() error
}

// Factory constructs the dial and listen boundaries for one carrier. Every
// connection it creates must return the same Capabilities as the factory;
// NewDialer and NewListener must validate explicit limits before any network I/O.
type Factory interface {
	Capabilities() Capabilities
	NewDialer(DialOptions) (Dialer, error)
	NewListener(ListenOptions) (Listener, error)
}
