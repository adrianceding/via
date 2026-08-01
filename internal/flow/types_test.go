package flow

import (
	"errors"
	"math"
	"testing"
)

func TestCheckedEnd(t *testing.T) {
	end, err := checkedEnd(41, 1)
	if err != nil || end != 42 {
		t.Fatalf("checkedEnd(41, 1) = %d, %v", end, err)
	}

	if _, err := checkedEnd(math.MaxUint64, 1); !errors.Is(err, ErrOffsetOverflow) {
		t.Fatalf("overflow error = %v", err)
	}
}

func TestFixedLimits(t *testing.T) {
	if SendWindowSize != 256<<10 || ReceiveWindowSize != 256<<10 {
		t.Fatal("fixed byte windows changed")
	}
	if MaxReplaySegments != 128 || MaxReassemblyRanges != 128 || MaxAttachments != 64 {
		t.Fatal("fixed count limits changed")
	}
}

func TestByteRangeLen(t *testing.T) {
	if got := (ByteRange{Start: 10, End: 15}).Len(); got != 5 {
		t.Fatalf("range length = %d", got)
	}
	if got := (ByteRange{Start: 15, End: 10}).Len(); got != 0 {
		t.Fatalf("invalid range length = %d", got)
	}
}
