package flow

import (
	"errors"
	"math"
)

const (
	SendWindowSize      uint64 = 256 << 10
	ReceiveWindowSize   uint64 = 256 << 10
	MaxReplaySegments          = 128
	MaxReassemblyRanges        = 128
	MaxAttachments             = 64
)

var (
	ErrOffsetOverflow      = errors.New("flow offset overflow")
	ErrWindowExceeded      = errors.New("flow window exceeded")
	ErrSegmentLimit        = errors.New("flow segment limit exceeded")
	ErrSegmentTooLarge     = errors.New("flow segment exceeds protocol data limit")
	ErrRangeLimit          = errors.New("flow range limit exceeded")
	ErrDataConflict        = errors.New("flow data conflict")
	ErrACKBeyondAllocated  = errors.New("flow ack beyond allocated data")
	ErrACKNotCanonical     = errors.New("flow ack ranges are not canonical")
	ErrFinalOffsetConflict = errors.New("flow final offset conflict")
	ErrDataBeyondFinal     = errors.New("flow data beyond final offset")
	ErrInvalidWriteResult  = errors.New("flow invalid write result")
	ErrInvalidState        = errors.New("flow invalid state")
	ErrAttachmentLimit     = errors.New("flow attachment limit exceeded")
	ErrAttachmentExists    = errors.New("flow attachment already exists")
	ErrAttachmentUnknown   = errors.New("flow attachment is unknown")
	ErrStaleGeneration     = errors.New("flow stale generation")
)

type TxState uint8

const (
	TxOpen TxState = iota
	TxFinPending
	TxComplete
)

type RxState uint8

const (
	RxOpen RxState = iota
	RxFinSeen
	RxClosingWrite
	RxComplete
)

type LifecycleState uint8

const (
	AwaitingAttachment LifecycleState = iota
	Relaying
	Recovering
	Closing
	Closed
	Resetting
	Reset
)

type ByteRange struct {
	Start uint64
	End   uint64
}

func (r ByteRange) Len() uint64 {
	if r.End <= r.Start {
		return 0
	}
	return r.End - r.Start
}

func checkedEnd(offset uint64, length int) (uint64, error) {
	if length < 0 || uint64(length) > math.MaxUint64-offset {
		return 0, ErrOffsetOverflow
	}
	return offset + uint64(length), nil
}

type AttachmentKey struct {
	SessionGeneration    uint64
	AttachmentGeneration uint64
}
