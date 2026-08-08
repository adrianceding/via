package flow

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestReceiverReordersWritesAndAcknowledgesActualProgress(t *testing.T) {
	rx := NewReceiver()

	actions, err := rx.ReceiveData(4, []byte("ef"))
	if err != nil {
		t.Fatal(err)
	}
	assertACK(t, actions, 0, []ByteRange{{Start: 4, End: 6}})
	assertNoAction(t, actions, RxWriteLocal)

	actions, err = rx.ReceiveData(0, []byte("abcd"))
	if err != nil {
		t.Fatal(err)
	}
	write := requireAction(t, actions, RxWriteLocal)
	if write.Generation != 1 || write.Offset != 0 || string(write.CopyData()) != "abcdef" {
		t.Fatalf("first write = %+v", write)
	}
	assertACK(t, actions, 0, []ByteRange{{Start: 1, End: 6}})

	actions, err = rx.HandleWriteResult(write.Generation, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertACK(t, actions, 2, []ByteRange{{Start: 3, End: 6}})
	write = requireAction(t, actions, RxWriteLocal)
	if write.Generation != 2 || write.Offset != 2 || string(write.CopyData()) != "cdef" {
		t.Fatalf("short-write continuation = %+v", write)
	}

	actions, err = rx.HandleWriteResult(write.Generation, write.DataLen(), nil)
	if err != nil {
		t.Fatal(err)
	}
	assertACK(t, actions, 6, nil)
	if rx.WrittenOffset() != 6 || rx.BufferedBytes() != 0 || rx.RangeCount() != 0 {
		t.Fatalf("receiver after writes: offset=%d bytes=%d ranges=%d", rx.WrittenOffset(), rx.BufferedBytes(), rx.RangeCount())
	}
}

func TestReceiverCopiesInputActionsAndSnapshots(t *testing.T) {
	rx := NewReceiver()
	input := []byte("abcd")
	actions, err := rx.ReceiveData(0, input)
	if err != nil {
		t.Fatal(err)
	}
	write := requireAction(t, actions, RxWriteLocal)
	input[1] = 'X'
	writeCopy := write.CopyData()
	writeCopy[2] = 'Y'

	actions, err = rx.HandleWriteResult(write.Generation, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	next := requireAction(t, actions, RxWriteLocal)
	if string(next.CopyData()) != "bcd" {
		t.Fatalf("mutable slice escaped into receiver: %q", next.CopyData())
	}

	rx = NewReceiver()
	if _, err := rx.ReceiveData(4, []byte("e")); err != nil {
		t.Fatal(err)
	}
	snapshot := rx.ACKSnapshot()
	snapshot.Ranges[0].Start = 99
	if got := rx.ACKSnapshot().Ranges[0].Start; got != 4 {
		t.Fatalf("mutable ACK range escaped into receiver: %d", got)
	}
}

func TestReceiverWriteActionSurvivesDiscard(t *testing.T) {
	rx := NewReceiver()
	actions, err := rx.ReceiveData(0, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	write := requireAction(t, actions, RxWriteLocal)
	rx.Discard()
	if got := string(write.CopyData()); got != "payload" {
		t.Fatalf("discard changed detached write action: %q", got)
	}
}

func TestReceiverOverlapMergeConflictAndWrittenPrefixCropping(t *testing.T) {
	rx := NewReceiver()
	for _, data := range []struct {
		offset uint64
		bytes  string
	}{
		{offset: 2, bytes: "cdef"},
		{offset: 6, bytes: "gh"},
		{offset: 4, bytes: "efg"},
	} {
		if _, err := rx.ReceiveData(data.offset, []byte(data.bytes)); err != nil {
			t.Fatalf("ReceiveData(%d, %q): %v", data.offset, data.bytes, err)
		}
	}
	if rx.RangeCount() != 1 || rx.BufferedBytes() != 6 {
		t.Fatalf("merged ranges: count=%d bytes=%d", rx.RangeCount(), rx.BufferedBytes())
	}
	before := rx.ACKSnapshot()
	if _, err := rx.ReceiveData(3, []byte("X")); !errors.Is(err, ErrDataConflict) {
		t.Fatalf("overlap conflict error = %v", err)
	}
	if got := rx.ACKSnapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("conflict mutated snapshot: got %+v want %+v", got, before)
	}

	rx = NewReceiver()
	actions, err := rx.ReceiveData(0, []byte("abcd"))
	if err != nil {
		t.Fatal(err)
	}
	write := requireAction(t, actions, RxWriteLocal)
	if _, err := rx.HandleWriteResult(write.Generation, 4, nil); err != nil {
		t.Fatal(err)
	}
	actions, err = rx.ReceiveData(0, []byte("WXYZ"))
	if err != nil {
		t.Fatalf("released prefix was compared: %v", err)
	}
	assertACK(t, actions, 4, nil)
	actions, err = rx.ReceiveData(2, []byte("XXef"))
	if err != nil {
		t.Fatalf("cropped prefix receive: %v", err)
	}
	write = requireAction(t, actions, RxWriteLocal)
	if write.Offset != 4 || string(write.CopyData()) != "ef" {
		t.Fatalf("cropped write = %+v", write)
	}
}

func TestReceiverWindowOverflowAndRangeLimits(t *testing.T) {
	t.Run("full window", func(t *testing.T) {
		rx := NewReceiver()
		data := make([]byte, int(ReceiveWindowSize))
		actions, err := rx.ReceiveData(0, data)
		if err != nil {
			t.Fatalf("full receive window: %v", err)
		}
		write := requireAction(t, actions, RxWriteLocal)
		if write.DataLen() != int(ReceiveWindowSize) || rx.BufferedBytes() != ReceiveWindowSize {
			t.Fatalf("full window: write=%d buffered=%d", write.DataLen(), rx.BufferedBytes())
		}
	})
	t.Run("window boundary", func(t *testing.T) {
		rx := NewReceiver()
		if _, err := rx.ReceiveData(ReceiveWindowSize-1, []byte{1}); err != nil {
			t.Fatalf("exact window end: %v", err)
		}
	})
	t.Run("over window", func(t *testing.T) {
		rx := NewReceiver()
		if _, err := rx.ReceiveData(ReceiveWindowSize, []byte{1}); !errors.Is(err, ErrWindowExceeded) {
			t.Fatalf("window error = %v", err)
		}
	})
	t.Run("offset overflow", func(t *testing.T) {
		rx := NewReceiver()
		if _, err := rx.ReceiveData(math.MaxUint64, []byte{1, 2}); !errors.Is(err, ErrOffsetOverflow) {
			t.Fatalf("overflow error = %v", err)
		}
	})
	t.Run("window addition saturates", func(t *testing.T) {
		rx := NewReceiver()
		rx.writtenOffset = math.MaxUint64 - 2
		if _, err := rx.ReceiveData(math.MaxUint64-1, []byte{1}); err != nil {
			t.Fatalf("saturated window boundary: %v", err)
		}
	})
	t.Run("range count", func(t *testing.T) {
		rx := NewReceiver()
		for i := 0; i < MaxReassemblyRanges; i++ {
			offset := uint64(1 + i*2)
			if _, err := rx.ReceiveData(offset, []byte{byte(i)}); err != nil {
				t.Fatalf("range %d: %v", i, err)
			}
		}
		if rx.RangeCount() != MaxReassemblyRanges {
			t.Fatalf("range count = %d", rx.RangeCount())
		}
		if _, err := rx.ReceiveData(uint64(1+MaxReassemblyRanges*2), []byte{1}); !errors.Is(err, ErrRangeLimit) {
			t.Fatalf("range limit error = %v", err)
		}
	})
}

func TestReceiverSequentialMaximumWindowRemainsCanonical(t *testing.T) {
	const window = 4 << 20
	rx := NewReceiverWithWindow(window)
	payload := make([]byte, 16<<10)
	for offset := uint64(0); offset < window; offset += uint64(len(payload)) {
		if _, err := rx.ReceiveData(offset, payload); err != nil {
			t.Fatalf("receive offset %d: %v", offset, err)
		}
	}
	if snapshot := rx.Snapshot(); snapshot.BufferedBytes != window || snapshot.RangeCount != 1 {
		t.Fatalf("sequential window snapshot = %#v", snapshot)
	}
}

func TestReceiverACKSnapshotIsCanonicalAndBounded(t *testing.T) {
	rx := NewReceiver()
	for i := 0; i < 20; i++ {
		offset := uint64(2 + i*2)
		if _, err := rx.ReceiveData(offset, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := rx.ACKSnapshot()
	if snapshot.NextOffset != 0 || len(snapshot.Ranges) != MaxACKRanges {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	for i, r := range snapshot.Ranges {
		want := ByteRange{Start: uint64(2 + i*2), End: uint64(3 + i*2)}
		if r != want {
			t.Fatalf("range %d = %+v, want %+v", i, r, want)
		}
		if r.Start <= snapshot.NextOffset || r.Start >= r.End {
			t.Fatalf("non-canonical range %d: %+v", i, r)
		}
		if i > 0 && snapshot.Ranges[i-1].End >= r.Start {
			t.Fatalf("ranges overlap or touch: %+v", snapshot.Ranges)
		}
	}
}

func TestReceiverACKSnapshotCoalescesAdjacentInternalRanges(t *testing.T) {
	rx := NewReceiver()
	rx.ranges = []receiveRange{
		{start: 0, data: []byte("ab")},
		{start: 2, data: []byte("cd")},
		{start: 4, data: []byte("ef")},
	}
	snapshot := rx.ACKSnapshot()
	if snapshot.NextOffset != 0 || !reflect.DeepEqual(snapshot.Ranges, []ByteRange{{Start: 1, End: 6}}) {
		t.Fatalf("canonical ACK snapshot = %#v", snapshot)
	}
}

func TestReceiverACKSnapshotKeepsOneUnwrittenByteAsGap(t *testing.T) {
	rx := NewReceiver()
	actions, err := rx.ReceiveData(0, []byte("abcd"))
	if err != nil {
		t.Fatal(err)
	}
	assertACK(t, actions, 0, []ByteRange{{Start: 1, End: 4}})

	rx = NewReceiver()
	actions, err = rx.ReceiveData(0, []byte{'a'})
	if err != nil {
		t.Fatal(err)
	}
	assertACK(t, actions, 0, nil)
}

func TestReceiverWriteResultMatrix(t *testing.T) {
	writeError := errors.New("write failed")
	tests := []struct {
		name       string
		n          int
		err        error
		wantOffset uint64
		wantErr    error
	}{
		{name: "zero without error", n: 0, wantErr: ErrInvalidWriteResult},
		{name: "negative", n: -1, wantErr: ErrInvalidWriteResult},
		{name: "too large", n: 5, wantErr: ErrInvalidWriteResult},
		{name: "zero with error", n: 0, err: writeError, wantErr: writeError},
		{name: "progress with error", n: 2, err: writeError, wantOffset: 2, wantErr: writeError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rx := NewReceiver()
			actions, err := rx.ReceiveData(0, []byte("data"))
			if err != nil {
				t.Fatal(err)
			}
			write := requireAction(t, actions, RxWriteLocal)
			actions, err = rx.HandleWriteResult(write.Generation, test.n, test.err)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if rx.WrittenOffset() != test.wantOffset {
				t.Fatalf("offset = %d, want %d", rx.WrittenOffset(), test.wantOffset)
			}
			if test.wantOffset > 0 {
				assertACK(t, actions, test.wantOffset, []ByteRange{{Start: 3, End: 4}})
			} else if len(actions) != 0 {
				t.Fatalf("failure without progress emitted actions: %+v", actions)
			}
			if rx.Failed() == nil {
				t.Fatal("write failure did not poison receiver")
			}
			if later, laterErr := rx.ReceiveData(4, []byte("x")); !errors.Is(laterErr, ErrInvalidState) || len(later) != 0 {
				t.Fatalf("poisoned receiver accepted data: actions=%+v err=%v", later, laterErr)
			}
		})
	}
}

func TestReceiverStaleAndTerminalResultsHaveNoEffect(t *testing.T) {
	rx := NewReceiver()
	actions, err := rx.ReceiveData(0, []byte("abcd"))
	if err != nil {
		t.Fatal(err)
	}
	write := requireAction(t, actions, RxWriteLocal)
	before := rx.Snapshot()
	if actions, err := rx.HandleWriteResult(write.Generation+1, 4, nil); err != nil || len(actions) != 0 {
		t.Fatalf("stale result: actions=%+v err=%v", actions, err)
	}
	if got := rx.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("stale result mutated receiver: got %+v want %+v", got, before)
	}

	actions, err = rx.HandleWriteResult(write.Generation, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	next := requireAction(t, actions, RxWriteLocal)
	before = rx.Snapshot()
	if actions, err := rx.HandleWriteResult(write.Generation, 3, nil); err != nil || len(actions) != 0 {
		t.Fatalf("late old result: actions=%+v err=%v", actions, err)
	}
	if got := rx.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("late result mutated receiver: got %+v want %+v", got, before)
	}

	if _, err := rx.ReceiveFIN(4); err != nil {
		t.Fatal(err)
	}
	actions, err = rx.HandleWriteResult(next.Generation, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	closeWrite := requireAction(t, actions, RxCloseWriteLocal)
	before = rx.Snapshot()
	if actions, err := rx.HandleCloseWriteResult(closeWrite.Generation+1, nil); err != nil || len(actions) != 0 {
		t.Fatalf("stale close result: actions=%+v err=%v", actions, err)
	}
	if got := rx.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("stale close result mutated receiver: got %+v want %+v", got, before)
	}
	if _, err := rx.HandleCloseWriteResult(closeWrite.Generation, nil); err != nil {
		t.Fatal(err)
	}
	before = rx.Snapshot()
	if actions, err := rx.HandleWriteResult(next.Generation, 3, nil); err != nil || len(actions) != 0 {
		t.Fatalf("terminal write result: actions=%+v err=%v", actions, err)
	}
	if actions, err := rx.HandleCloseWriteResult(closeWrite.Generation, nil); err != nil || len(actions) != 0 {
		t.Fatalf("terminal close result: actions=%+v err=%v", actions, err)
	}
	if got := rx.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("terminal result mutated receiver: got %+v want %+v", got, before)
	}
}

func TestReceiverFINStateMatrixAndIdempotence(t *testing.T) {
	rx := NewReceiver()
	actions, err := rx.ReceiveData(0, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	requireAction(t, actions, RxWriteLocal)

	actions, err = rx.ReceiveFIN(3)
	if err != nil {
		t.Fatal(err)
	}
	if rx.State() != RxFinSeen {
		t.Fatalf("state = %v", rx.State())
	}
	assertACK(t, actions, 0, []ByteRange{{Start: 1, End: 3}})
	assertNoAction(t, actions, RxCloseWriteLocal)

	actions, err = rx.ReceiveFIN(3)
	if err != nil {
		t.Fatalf("duplicate FIN: %v", err)
	}
	assertACK(t, actions, 0, []ByteRange{{Start: 1, End: 3}})
	assertNoAction(t, actions, RxSendFINACK)
	if final, ok := rx.FinalOffset(); !ok || final != 3 {
		t.Fatalf("final offset = %d, %v", final, ok)
	}
	if _, err := rx.ReceiveFIN(4); !errors.Is(err, ErrFinalOffsetConflict) {
		t.Fatalf("conflicting FIN error = %v", err)
	}
}

func TestReceiverFINClosesAfterAllBytesAreWritten(t *testing.T) {
	rx := NewReceiver()
	actions, err := rx.ReceiveData(0, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	write := requireAction(t, actions, RxWriteLocal)
	if _, err := rx.ReceiveFIN(3); err != nil {
		t.Fatal(err)
	}
	actions, err = rx.HandleWriteResult(write.Generation, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertACK(t, actions, 3, nil)
	closeWrite := requireAction(t, actions, RxCloseWriteLocal)
	if rx.State() != RxClosingWrite || closeWrite.FinalOffset != 3 {
		t.Fatalf("close action/state = %+v/%v", closeWrite, rx.State())
	}

	actions, err = rx.HandleCloseWriteResult(closeWrite.Generation, nil)
	if err != nil {
		t.Fatal(err)
	}
	finACK := requireAction(t, actions, RxSendFINACK)
	if rx.State() != RxComplete || finACK.FinalOffset != 3 {
		t.Fatalf("FIN_ACK/state = %+v/%v", finACK, rx.State())
	}

	actions, err = rx.ReceiveFIN(3)
	if err != nil {
		t.Fatal(err)
	}
	if requireAction(t, actions, RxSendFINACK).FinalOffset != 3 {
		t.Fatalf("duplicate FIN_ACK = %+v", actions)
	}
	actions, err = rx.ReceiveData(0, []byte("XXX"))
	if err != nil {
		t.Fatalf("released closing data was compared: %v", err)
	}
	assertACK(t, actions, 3, nil)
	requireAction(t, actions, RxSendFINACK)
}

func TestReceiverEmptyFINAndCloseFailure(t *testing.T) {
	rx := NewReceiver()
	actions, err := rx.ReceiveFIN(0)
	if err != nil {
		t.Fatal(err)
	}
	closeWrite := requireAction(t, actions, RxCloseWriteLocal)
	if rx.State() != RxClosingWrite {
		t.Fatalf("state = %v", rx.State())
	}
	closeErr := errors.New("close write failed")
	actions, err = rx.HandleCloseWriteResult(closeWrite.Generation, closeErr)
	if !errors.Is(err, closeErr) || len(actions) != 0 || rx.Failed() == nil {
		t.Fatalf("close failure: actions=%+v err=%v failed=%v", actions, err, rx.Failed())
	}
}

func TestReceiverRejectsDataBeyondFinal(t *testing.T) {
	t.Run("buffer already beyond first FIN", func(t *testing.T) {
		rx := NewReceiver()
		if _, err := rx.ReceiveData(4, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := rx.ReceiveFIN(4); !errors.Is(err, ErrDataBeyondFinal) {
			t.Fatalf("FIN error = %v", err)
		}
	})
	t.Run("new data beyond known FIN", func(t *testing.T) {
		rx := NewReceiver()
		if _, err := rx.ReceiveFIN(4); err != nil {
			t.Fatal(err)
		}
		if _, err := rx.ReceiveData(3, []byte("xy")); !errors.Is(err, ErrDataBeyondFinal) {
			t.Fatalf("data error = %v", err)
		}
	})
	t.Run("FIN behind written prefix", func(t *testing.T) {
		rx := NewReceiver()
		actions, err := rx.ReceiveData(0, []byte("abc"))
		if err != nil {
			t.Fatal(err)
		}
		write := requireAction(t, actions, RxWriteLocal)
		if _, err := rx.HandleWriteResult(write.Generation, 3, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := rx.ReceiveFIN(2); !errors.Is(err, ErrDataBeyondFinal) {
			t.Fatalf("FIN error = %v", err)
		}
	})
}

func TestReceiverRejectsEmptyDATA(t *testing.T) {
	rx := NewReceiver()
	if _, err := rx.ReceiveData(0, nil); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("empty DATA error = %v", err)
	}
}

func TestReceiverStateEventMatrix(t *testing.T) {
	t.Run("open ignores unrelated results", func(t *testing.T) {
		rx := NewReceiver()
		before := rx.Snapshot()
		if actions, err := rx.HandleWriteResult(1, 1, nil); err != nil || len(actions) != 0 {
			t.Fatalf("write result: actions=%+v err=%v", actions, err)
		}
		if actions, err := rx.HandleCloseWriteResult(1, nil); err != nil || len(actions) != 0 {
			t.Fatalf("close result: actions=%+v err=%v", actions, err)
		}
		if got := rx.Snapshot(); !reflect.DeepEqual(got, before) {
			t.Fatalf("open state changed: got %+v want %+v", got, before)
		}
	})

	t.Run("fin seen accepts data and ignores close result", func(t *testing.T) {
		rx := NewReceiver()
		if _, err := rx.ReceiveFIN(2); err != nil {
			t.Fatal(err)
		}
		before := rx.Snapshot()
		if actions, err := rx.HandleCloseWriteResult(1, nil); err != nil || len(actions) != 0 {
			t.Fatalf("close result: actions=%+v err=%v", actions, err)
		}
		if got := rx.Snapshot(); !reflect.DeepEqual(got, before) {
			t.Fatalf("FIN-seen state changed: got %+v want %+v", got, before)
		}
		actions, err := rx.ReceiveData(0, []byte("ab"))
		if err != nil {
			t.Fatal(err)
		}
		requireAction(t, actions, RxWriteLocal)
		if rx.State() != RxFinSeen {
			t.Fatalf("state after DATA = %v", rx.State())
		}
	})

	t.Run("closing accepts duplicates only", func(t *testing.T) {
		rx, closeWrite := receiverAwaitingClose(t, "ab")
		before := rx.Snapshot()
		actions, err := rx.ReceiveData(0, []byte("XX"))
		if err != nil {
			t.Fatal(err)
		}
		assertACK(t, actions, 2, nil)
		assertNoAction(t, actions, RxCloseWriteLocal)
		actions, err = rx.ReceiveFIN(2)
		if err != nil {
			t.Fatal(err)
		}
		assertACK(t, actions, 2, nil)
		assertNoAction(t, actions, RxCloseWriteLocal)
		if actions, err := rx.HandleWriteResult(closeWrite.Generation, 1, nil); err != nil || len(actions) != 0 {
			t.Fatalf("late write result: actions=%+v err=%v", actions, err)
		}
		if got := rx.Snapshot(); !reflect.DeepEqual(got, before) {
			t.Fatalf("closing state changed: got %+v want %+v", got, before)
		}
	})

	t.Run("complete handles duplicates and ignores results", func(t *testing.T) {
		rx, closeWrite := receiverAwaitingClose(t, "ab")
		if _, err := rx.HandleCloseWriteResult(closeWrite.Generation, nil); err != nil {
			t.Fatal(err)
		}
		before := rx.Snapshot()
		actions, err := rx.ReceiveData(0, []byte("XX"))
		if err != nil {
			t.Fatal(err)
		}
		assertACK(t, actions, 2, nil)
		requireAction(t, actions, RxSendFINACK)
		actions, err = rx.ReceiveFIN(2)
		if err != nil {
			t.Fatal(err)
		}
		requireAction(t, actions, RxSendFINACK)
		if actions, err := rx.HandleWriteResult(1, 1, nil); err != nil || len(actions) != 0 {
			t.Fatalf("late write result: actions=%+v err=%v", actions, err)
		}
		if actions, err := rx.HandleCloseWriteResult(closeWrite.Generation, nil); err != nil || len(actions) != 0 {
			t.Fatalf("late close result: actions=%+v err=%v", actions, err)
		}
		if got := rx.Snapshot(); !reflect.DeepEqual(got, before) {
			t.Fatalf("complete state changed: got %+v want %+v", got, before)
		}
	})
}

func TestReceiverPreservesFirstFailure(t *testing.T) {
	rx := NewReceiver()
	if _, err := rx.ReceiveData(ReceiveWindowSize, []byte{1}); !errors.Is(err, ErrWindowExceeded) {
		t.Fatalf("first error = %v", err)
	}
	if _, err := rx.ReceiveData(0, nil); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("later error = %v", err)
	}
	if !errors.Is(rx.Failed(), ErrWindowExceeded) {
		t.Fatalf("stored failure = %v", rx.Failed())
	}
}

func receiverAwaitingClose(t *testing.T, data string) (*Receiver, RxAction) {
	t.Helper()
	rx := NewReceiver()
	actions, err := rx.ReceiveData(0, []byte(data))
	if err != nil {
		t.Fatal(err)
	}
	write := requireAction(t, actions, RxWriteLocal)
	if _, err := rx.ReceiveFIN(uint64(len(data))); err != nil {
		t.Fatal(err)
	}
	actions, err = rx.HandleWriteResult(write.Generation, len(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	return rx, requireAction(t, actions, RxCloseWriteLocal)
}

func requireAction(t *testing.T, actions []RxAction, kind RxActionKind) RxAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("action %v missing from %+v", kind, actions)
	return RxAction{}
}

func assertNoAction(t *testing.T, actions []RxAction, kind RxActionKind) {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			t.Fatalf("unexpected action %v in %+v", kind, actions)
		}
	}
}

func assertACK(t *testing.T, actions []RxAction, next uint64, ranges []ByteRange) {
	t.Helper()
	action := requireAction(t, actions, RxSendACK)
	want := ACKSnapshot{NextOffset: next, Ranges: ranges}
	if !reflect.DeepEqual(action.ACK, want) {
		t.Fatalf("ACK = %+v, want %+v", action.ACK, want)
	}
}
