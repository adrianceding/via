package flow

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

func TestTxDataSegmentProtocolBoundaryAndAvailableWindow(t *testing.T) {
	tx := NewTx()
	if got := tx.AvailableWindow(); got != SendWindowSize {
		t.Fatalf("initial available window = %d", got)
	}
	data := make([]byte, protocol.MaxDataLength)
	if _, err := tx.Append(data); err != nil {
		t.Fatalf("maximum DATA segment: %v", err)
	}
	if got := tx.AvailableWindow(); got != SendWindowSize-uint64(len(data)) {
		t.Fatalf("available window after append = %d", got)
	}
	if _, err := tx.Append(make([]byte, protocol.MaxDataLength+1)); !errors.Is(err, ErrSegmentTooLarge) {
		t.Fatalf("oversized DATA segment error = %v", err)
	}
}

func TestTxAppendIsContiguousImmutableAndBounded(t *testing.T) {
	tx := NewTx()
	input := []byte("abc")
	first, err := tx.Append(input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Kind != TxItemData || first.ItemID != 1 || first.AttemptGeneration != 1 || first.Offset != 0 || string(first.CopyData()) != "abc" {
		t.Fatalf("first item = %+v", first)
	}
	input[0] = 'x'
	firstCopy := first.CopyData()
	firstCopy[1] = 'x'
	retry, ok := tx.RetryDue()
	if !ok || retry.ItemID != first.ItemID || retry.AttemptGeneration != 2 || retry.Offset != 0 || string(retry.CopyData()) != "abc" {
		t.Fatalf("retry item = %+v, %t", retry, ok)
	}
	second, err := tx.Append([]byte("def"))
	if err != nil {
		t.Fatal(err)
	}
	if second.ItemID != 2 || second.Offset != 3 || string(second.CopyData()) != "def" || tx.AllocatedOffset() != 6 {
		t.Fatalf("second item = %+v, allocated = %d", second, tx.AllocatedOffset())
	}

	full := NewTx()
	remaining := SendWindowSize
	for remaining > 0 {
		chunk := min(remaining, uint64(protocol.MaxDataLength))
		if _, err := full.Append(make([]byte, int(chunk))); err != nil {
			t.Fatalf("append exact window chunk: %v", err)
		}
		remaining -= chunk
	}
	if _, err := full.Append([]byte{1}); !errors.Is(err, ErrWindowExceeded) {
		t.Fatalf("over-window error = %v", err)
	}
	if _, err := full.ApplyACK(SendWindowSize, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := full.Append([]byte{1}); err != nil {
		t.Fatalf("append after cumulative release: %v", err)
	}

	overflow := NewTx()
	overflow.allocated = math.MaxUint64
	overflow.acknowledged = math.MaxUint64
	if _, err := overflow.Append([]byte{1}); !errors.Is(err, ErrOffsetOverflow) {
		t.Fatalf("offset overflow error = %v", err)
	}
	if _, err := tx.Append(nil); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("empty append error = %v", err)
	}
}

func TestTxReplaySegmentLimit(t *testing.T) {
	tx := NewTx()
	for index := 0; index < MaxReplaySegments; index++ {
		if _, err := tx.Append([]byte{byte(index)}); err != nil {
			t.Fatalf("append %d: %v", index, err)
		}
	}
	if _, err := tx.Append([]byte{0}); !errors.Is(err, ErrSegmentLimit) {
		t.Fatalf("segment limit error = %v", err)
	}
	if _, err := tx.ApplyACK(1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Append([]byte{0}); err != nil {
		t.Fatalf("append after releasing one segment: %v", err)
	}
	if tx.ReplaySegments() != MaxReplaySegments {
		t.Fatalf("replay segment count = %d", tx.ReplaySegments())
	}
}

func TestTxRejectsInvalidACKAtomically(t *testing.T) {
	tooMany := make([]ByteRange, MaxACKRanges+1)
	for index := range tooMany {
		tooMany[index] = ByteRange{Start: uint64(index*3 + 1), End: uint64(index*3 + 2)}
	}
	tests := []struct {
		name   string
		next   uint64
		ranges []ByteRange
		want   error
	}{
		{name: "beyond allocated", next: 65, want: ErrACKBeyondAllocated},
		{name: "too many ranges", ranges: tooMany, want: ErrACKNotCanonical},
		{name: "range touches cumulative", ranges: []ByteRange{{Start: 0, End: 1}}, want: ErrACKNotCanonical},
		{name: "empty range", ranges: []ByteRange{{Start: 2, End: 2}}, want: ErrACKNotCanonical},
		{name: "overlap", ranges: []ByteRange{{Start: 2, End: 5}, {Start: 4, End: 6}}, want: ErrACKNotCanonical},
		{name: "adjacent not merged", ranges: []ByteRange{{Start: 2, End: 5}, {Start: 5, End: 6}}, want: ErrACKNotCanonical},
		{name: "range beyond allocated", ranges: []ByteRange{{Start: 2, End: 65}}, want: ErrACKBeyondAllocated},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := NewTx()
			if _, err := tx.Append(make([]byte, 64)); err != nil {
				t.Fatal(err)
			}
			progress, err := tx.ApplyACK(test.next, test.ranges)
			if progress || !errors.Is(err, test.want) {
				t.Fatalf("ApplyACK() = %t, %v; want false, %v", progress, err, test.want)
			}
			if tx.AcknowledgedOffset() != 0 || tx.ReplayBytes() != 64 || len(tx.SelectiveACKs()) != 0 {
				t.Fatalf("invalid ACK changed state: ack=%d replay=%d sacks=%v", tx.AcknowledgedOffset(), tx.ReplayBytes(), tx.SelectiveACKs())
			}
		})
	}
}

func TestTxACKReleasesOnlyCumulativePrefix(t *testing.T) {
	tx := NewTx()
	if _, err := tx.Append([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Append([]byte("efgh")); err != nil {
		t.Fatal(err)
	}
	progress, err := tx.ApplyACK(2, nil)
	if err != nil || !progress {
		t.Fatalf("partial cumulative ACK = %t, %v", progress, err)
	}
	if tx.ReplayBytes() != 6 || tx.ReplaySegments() != 2 {
		t.Fatalf("after partial ACK: bytes=%d segments=%d", tx.ReplayBytes(), tx.ReplaySegments())
	}
	retry, ok := tx.RetryDue()
	if !ok || retry.Offset != 2 || string(retry.CopyData()) != "cd" {
		t.Fatalf("trimmed retry = %+v, %t", retry, ok)
	}

	progress, err = tx.ApplyACK(4, []ByteRange{{Start: 6, End: 8}})
	if err != nil || !progress {
		t.Fatalf("cumulative plus selective ACK = %t, %v", progress, err)
	}
	if tx.ReplayBytes() != 4 || tx.ReplaySegments() != 1 {
		t.Fatalf("SACK released payload: bytes=%d segments=%d", tx.ReplayBytes(), tx.ReplaySegments())
	}
	retry, ok = tx.RetryDue()
	if !ok || retry.Offset != 4 || string(retry.CopyData()) != "ef" {
		t.Fatalf("gap retry = %+v, %t", retry, ok)
	}

	before := tx.SelectiveACKs()
	progress, err = tx.ApplyACK(2, []ByteRange{{Start: 3, End: 5}})
	if err != nil || progress {
		t.Fatalf("old ACK = %t, %v", progress, err)
	}
	if tx.AcknowledgedOffset() != 4 || !reflect.DeepEqual(tx.SelectiveACKs(), before) {
		t.Fatalf("old ACK changed state: ack=%d sacks=%v", tx.AcknowledgedOffset(), tx.SelectiveACKs())
	}

	progress, err = tx.ApplyACK(8, nil)
	if err != nil || !progress || tx.ReplayBytes() != 0 || tx.ReplaySegments() != 0 {
		t.Fatalf("full cumulative ACK = %t, %v; bytes=%d segments=%d", progress, err, tx.ReplayBytes(), tx.ReplaySegments())
	}
}

func TestTxMergesSelectiveACKKnowledgeWithinFixedLimit(t *testing.T) {
	tx := NewTx()
	if _, err := tx.Append(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	if progress, err := tx.ApplyACK(0, []ByteRange{{Start: 2, End: 4}}); err != nil || !progress {
		t.Fatalf("first SACK = %t, %v", progress, err)
	}
	if tx.ReplayBytes() != 64 {
		t.Fatalf("SACK changed replay bytes to %d", tx.ReplayBytes())
	}
	if progress, err := tx.ApplyACK(0, []ByteRange{{Start: 4, End: 6}}); err != nil || !progress {
		t.Fatalf("adjacent SACK union = %t, %v", progress, err)
	}
	want := []ByteRange{{Start: 2, End: 6}}
	if got := tx.SelectiveACKs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("merged SACKs = %v, want %v", got, want)
	}
	copyOfRanges := tx.SelectiveACKs()
	copyOfRanges[0].Start = 40
	if got := tx.SelectiveACKs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("caller mutated internal SACKs: %v", got)
	}
	if progress, err := tx.ApplyACK(0, want); err != nil || progress {
		t.Fatalf("duplicate SACK = %t, %v", progress, err)
	}

	limited := NewTx()
	if _, err := limited.Append(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	ranges := make([]ByteRange, MaxACKRanges)
	for index := range ranges {
		ranges[index] = ByteRange{Start: uint64(index*2 + 1), End: uint64(index*2 + 2)}
	}
	if _, err := limited.ApplyACK(0, ranges); err != nil {
		t.Fatal(err)
	}
	before := limited.SelectiveACKs()
	if progress, err := limited.ApplyACK(0, []ByteRange{{Start: 33, End: 34}}); progress || !errors.Is(err, ErrACKNotCanonical) {
		t.Fatalf("seventeenth SACK = %t, %v", progress, err)
	}
	if !reflect.DeepEqual(limited.SelectiveACKs(), before) {
		t.Fatal("failed SACK union changed state")
	}
}

func TestTxRetryDueReturnsEarliestExactGap(t *testing.T) {
	tx := NewTx()
	if _, err := tx.Append([]byte("abcde")); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Append([]byte("fghij")); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		next   uint64
		ranges []ByteRange
		offset uint64
		data   string
	}{
		{next: 0, ranges: []ByteRange{{Start: 2, End: 4}, {Start: 6, End: 9}}, offset: 0, data: "ab"},
		{next: 2, ranges: []ByteRange{{Start: 3, End: 4}, {Start: 6, End: 9}}, offset: 2, data: "c"},
		{next: 5, ranges: []ByteRange{{Start: 6, End: 9}}, offset: 5, data: "f"},
		{next: 9, offset: 9, data: "j"},
	}
	for _, test := range tests {
		if _, err := tx.ApplyACK(test.next, test.ranges); err != nil {
			t.Fatalf("ApplyACK(%d): %v", test.next, err)
		}
		item, ok := tx.RetryDue()
		if !ok || item.Offset != test.offset || string(item.CopyData()) != test.data {
			t.Fatalf("RetryDue after ACK %d = %+v, %t; want offset=%d data=%q", test.next, item, ok, test.offset, test.data)
		}
	}
}

func TestTxAttemptGenerationAndAttachmentBookkeeping(t *testing.T) {
	tx := NewTx()
	item, err := tx.Append([]byte("lost"))
	if err != nil {
		t.Fatal(err)
	}
	a := AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 10}
	b := AttachmentKey{SessionGeneration: 2, AttachmentGeneration: 20}
	attachments := []AttachmentKey{a, b}
	attempt, err := tx.StartAttempt(item.ItemID, item.AttemptGeneration, a)
	if err != nil {
		t.Fatal(err)
	}
	attemptCopy := attempt.Item.CopyData()
	attemptCopy[0] = 'X'
	if _, err := tx.StartAttempt(item.ItemID, item.AttemptGeneration, a); !errors.Is(err, ErrAttachmentExists) {
		t.Fatalf("duplicate attachment error = %v", err)
	}
	if _, err := tx.StartAttempt(item.ItemID, item.AttemptGeneration, b); err != nil {
		t.Fatal(err)
	}
	for sessionGeneration := uint64(3); sessionGeneration <= uint64(MaxAttachments); sessionGeneration++ {
		attachment := AttachmentKey{SessionGeneration: sessionGeneration, AttachmentGeneration: sessionGeneration * 10}
		if _, err := tx.StartAttempt(item.ItemID, item.AttemptGeneration, attachment); err != nil {
			t.Fatalf("attachment %d error = %v", sessionGeneration, err)
		}
		attachments = append(attachments, attachment)
	}
	overflow := AttachmentKey{SessionGeneration: uint64(MaxAttachments) + 1, AttachmentGeneration: (uint64(MaxAttachments) + 1) * 10}
	if _, err := tx.StartAttempt(item.ItemID, item.AttemptGeneration, overflow); !errors.Is(err, ErrAttachmentLimit) {
		t.Fatalf("overflow attachment error = %v", err)
	}
	if !tx.RecordAttemptResult(item.ItemID, item.AttemptGeneration, a, AttemptFailed) {
		t.Fatal("first attachment result rejected")
	}
	if tx.RecordAttemptResult(item.ItemID, item.AttemptGeneration, a, AttemptSucceeded) {
		t.Fatal("duplicate attachment result accepted")
	}
	if !tx.RecordAttemptResult(item.ItemID, item.AttemptGeneration, b, AttemptTimedOut) {
		t.Fatal("second attachment result rejected")
	}
	for _, attachment := range attachments[2:] {
		if !tx.RecordAttemptResult(item.ItemID, item.AttemptGeneration, attachment, AttemptFailed) {
			t.Fatalf("attachment result rejected: %#v", attachment)
		}
	}

	secondGeneration, ok := tx.RetryDue()
	if !ok || secondGeneration.ItemID != item.ItemID || secondGeneration.AttemptGeneration != item.AttemptGeneration+1 || string(secondGeneration.CopyData()) != "lost" {
		t.Fatalf("second generation = %+v, %t", secondGeneration, ok)
	}
	if tx.RecordAttemptResult(item.ItemID, item.AttemptGeneration, b, AttemptFailed) {
		t.Fatal("late first-generation result changed current state")
	}
	if _, err := tx.StartAttempt(item.ItemID, item.AttemptGeneration, a); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale StartAttempt error = %v", err)
	}
	if _, err := tx.StartAttempt(secondGeneration.ItemID, secondGeneration.AttemptGeneration, a); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.StartAttempt(secondGeneration.ItemID, secondGeneration.AttemptGeneration, b); err != nil {
		t.Fatal(err)
	}
	if !tx.RecordAttemptResult(secondGeneration.ItemID, secondGeneration.AttemptGeneration, a, AttemptFailed) ||
		!tx.RecordAttemptResult(secondGeneration.ItemID, secondGeneration.AttemptGeneration, b, AttemptFailed) {
		t.Fatal("all-path failure results were not accepted")
	}
	thirdGeneration, ok := tx.RetryDue()
	if !ok || thirdGeneration.AttemptGeneration != secondGeneration.AttemptGeneration+1 {
		t.Fatalf("new generation after all paths lost DATA = %+v, %t", thirdGeneration, ok)
	}
	if tx.RecordAttemptResult(secondGeneration.ItemID, secondGeneration.AttemptGeneration, a, AttemptSucceeded) {
		t.Fatal("late success from superseded generation was accepted")
	}
}

func TestTxSelectiveACKInvalidatesCoveredQueuedItem(t *testing.T) {
	tx := NewTx()
	item, err := tx.Append([]byte("abcd"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ApplyACK(0, []ByteRange{{Start: 2, End: 4}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.StartAttempt(item.ItemID, item.AttemptGeneration, AttachmentKey{AttachmentGeneration: 1}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("covered queued item error = %v", err)
	}
	retry, ok := tx.RetryDue()
	if !ok || retry.AttemptGeneration != 2 || retry.Offset != 0 || string(retry.CopyData()) != "ab" {
		t.Fatalf("precise replacement item = %+v, %t", retry, ok)
	}
}

func TestTxFINIsReliableAndConverges(t *testing.T) {
	tx := NewTx()
	if _, err := tx.Append([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	fin, err := tx.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if fin.Kind != TxItemFIN || fin.FinalOffset != 3 || fin.AttemptGeneration != 1 || tx.State() != TxFinPending {
		t.Fatalf("FIN item/state = %+v, %v", fin, tx.State())
	}
	finAttachment := AttachmentKey{SessionGeneration: 7, AttachmentGeneration: 8}
	if _, err := tx.StartAttempt(fin.ItemID, fin.AttemptGeneration, finAttachment); err != nil {
		t.Fatalf("start initial FIN attempt: %v", err)
	}
	repeated, err := tx.Finish()
	if err != nil || !reflect.DeepEqual(repeated, fin) {
		t.Fatalf("repeated Finish = %+v, %v", repeated, err)
	}
	if _, err := tx.Append([]byte{1}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("append after EOF error = %v", err)
	}
	dataRetry, ok := tx.RetryDue()
	if !ok || dataRetry.Kind != TxItemData {
		t.Fatalf("DATA must precede FIN retry: %+v, %t", dataRetry, ok)
	}
	if _, err := tx.ApplyACK(3, nil); err != nil {
		t.Fatal(err)
	}
	finRetry, ok := tx.RetryDue()
	if !ok || finRetry.Kind != TxItemFIN || finRetry.ItemID != fin.ItemID || finRetry.AttemptGeneration != 2 || finRetry.FinalOffset != 3 {
		t.Fatalf("FIN retry = %+v, %t", finRetry, ok)
	}
	if tx.RecordAttemptResult(fin.ItemID, fin.AttemptGeneration, finAttachment, AttemptSucceeded) {
		t.Fatal("late initial FIN result was accepted")
	}
	if _, err := tx.StartAttempt(finRetry.ItemID, finRetry.AttemptGeneration, finAttachment); err != nil {
		t.Fatalf("start retried FIN attempt: %v", err)
	}
	if progress, err := tx.ApplyFINACK(2); progress || !errors.Is(err, ErrFinalOffsetConflict) || tx.State() != TxFinPending {
		t.Fatalf("conflicting FIN_ACK = %t, %v; state=%v", progress, err, tx.State())
	}
	progress, err := tx.ApplyFINACK(3)
	if err != nil || !progress || tx.State() != TxComplete || tx.HasPending() || tx.ReplayBytes() != 0 || tx.ReplaySegments() != 0 {
		t.Fatalf("matching FIN_ACK = %t, %v; state=%v pending=%t bytes=%d segments=%d", progress, err, tx.State(), tx.HasPending(), tx.ReplayBytes(), tx.ReplaySegments())
	}
	if final, ok := tx.FinalOffset(); !ok || final != 3 {
		t.Fatalf("retained final offset = %d, %t", final, ok)
	}
	if progress, err := tx.ApplyFINACK(3); err != nil || progress {
		t.Fatalf("duplicate FIN_ACK = %t, %v", progress, err)
	}
	if _, ok := tx.RetryDue(); ok {
		t.Fatal("completed Tx produced retry")
	}
	if _, err := tx.Finish(); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Finish after completion error = %v", err)
	}

	withoutEOF := NewTx()
	if progress, err := withoutEOF.ApplyFINACK(0); progress || !errors.Is(err, ErrFinalOffsetConflict) {
		t.Fatalf("FIN_ACK before EOF = %t, %v", progress, err)
	}

	direct := NewTx()
	if _, err := direct.Append(bytes.Repeat([]byte{'x'}, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := direct.Finish(); err != nil {
		t.Fatal(err)
	}
	if progress, err := direct.ApplyFINACK(8); err != nil || !progress || direct.AcknowledgedOffset() != 8 || direct.ReplayBytes() != 0 {
		t.Fatalf("direct FIN_ACK cumulative effect = %t, %v; ack=%d replay=%d", progress, err, direct.AcknowledgedOffset(), direct.ReplayBytes())
	}
}
