package flow

import (
	"bytes"
	"sort"

	"github.com/adrianceding/via/internal/protocol"
)

const MaxACKRanges = 16

type TxItemKind uint8

const (
	TxItemData TxItemKind = iota + 1
	TxItemFIN
)

type TxItem struct {
	Kind              TxItemKind
	ItemID            uint64
	AttemptGeneration uint64
	Offset            uint64
	FinalOffset       uint64
	data              []byte
}

func (item TxItem) DataLen() int { return len(item.data) }

// AppendData appends the immutable replay payload to dst. The returned bytes
// are owned by the caller; the policy descriptor itself never copies payload.
func (item TxItem) AppendData(dst []byte) []byte { return append(dst, item.data...) }

func (item TxItem) CopyData() []byte { return bytes.Clone(item.data) }

type TxAttempt struct {
	Item       TxItem
	Attachment AttachmentKey
}

type AttemptOutcome uint8

const (
	AttemptSucceeded AttemptOutcome = iota + 1
	AttemptFailed
	AttemptTimedOut
)

type attemptRecord struct {
	outcome AttemptOutcome
	done    bool
}

type replaySegment struct {
	id         uint64
	start      uint64
	data       []byte
	generation uint64
	current    ByteRange
	attempts   map[AttachmentKey]attemptRecord
}

func (s *replaySegment) end() uint64 {
	return s.start + uint64(len(s.data))
}

type finReplay struct {
	id         uint64
	offset     uint64
	generation uint64
	attempts   map[AttachmentKey]attemptRecord
}

// Tx owns one flow's local sending direction. It performs no network I/O.
type Tx struct {
	state        TxState
	allocated    uint64
	acknowledged uint64
	nextItemID   uint64
	segments     []*replaySegment
	selective    []ByteRange
	fin          *finReplay
}

func NewTx() *Tx {
	return &Tx{state: TxOpen}
}

func (t *Tx) State() TxState { return t.state }

func (t *Tx) AllocatedOffset() uint64 { return t.allocated }

func (t *Tx) AcknowledgedOffset() uint64 { return t.acknowledged }

func (t *Tx) ReplayBytes() uint64 { return t.allocated - t.acknowledged }

func (t *Tx) ReplaySegments() int { return len(t.segments) }

func (t *Tx) AvailableWindow() uint64 {
	if t.state != TxOpen || len(t.segments) >= MaxReplaySegments {
		return 0
	}
	return SendWindowSize - t.ReplayBytes()
}

func (t *Tx) Discard() {
	for _, segment := range t.segments {
		clear(segment.data)
		segment.data = nil
		clear(segment.attempts)
	}
	clear(t.segments)
	t.segments = nil
	t.selective = nil
	if t.fin != nil {
		clear(t.fin.attempts)
	}
	t.fin = nil
	t.acknowledged = t.allocated
	t.state = TxComplete
}

func (t *Tx) HasPending() bool {
	return t.allocated != t.acknowledged || t.state == TxFinPending
}

func (t *Tx) SelectiveACKs() []ByteRange {
	return append([]ByteRange(nil), t.selective...)
}

func (t *Tx) FinalOffset() (uint64, bool) {
	if t.fin == nil {
		return 0, false
	}
	return t.fin.offset, true
}

// Append allocates the next contiguous DATA range and returns a detached copy.
func (t *Tx) Append(data []byte) (TxItem, error) {
	if t.state != TxOpen || len(data) == 0 {
		return TxItem{}, ErrInvalidState
	}
	if len(data) > protocol.MaxDataLength {
		return TxItem{}, ErrSegmentTooLarge
	}
	end, err := checkedEnd(t.allocated, len(data))
	if err != nil {
		return TxItem{}, err
	}
	if end-t.acknowledged > SendWindowSize {
		return TxItem{}, ErrWindowExceeded
	}
	if len(t.segments) >= MaxReplaySegments {
		return TxItem{}, ErrSegmentLimit
	}
	id, err := t.allocateItemID()
	if err != nil {
		return TxItem{}, err
	}
	segment := &replaySegment{
		id:         id,
		start:      t.allocated,
		data:       bytes.Clone(data),
		generation: 1,
		current:    ByteRange{Start: t.allocated, End: end},
		attempts:   make(map[AttachmentKey]attemptRecord),
	}
	t.segments = append(t.segments, segment)
	t.allocated = end
	return t.dataItem(segment, segment.current), nil
}

// Finish records local EOF. Repeated calls while FIN is pending are idempotent.
func (t *Tx) Finish() (TxItem, error) {
	switch t.state {
	case TxOpen:
		id, err := t.allocateItemID()
		if err != nil {
			return TxItem{}, err
		}
		t.state = TxFinPending
		t.fin = &finReplay{
			id:         id,
			offset:     t.allocated,
			generation: 1,
			attempts:   make(map[AttachmentKey]attemptRecord),
		}
		return t.finItem(), nil
	case TxFinPending:
		return t.finItem(), nil
	default:
		return TxItem{}, ErrInvalidState
	}
}

// RetryDue advances the generation of the earliest DATA gap, or FIN when all
// DATA is cumulatively acknowledged. SACK ranges suppress DATA retry only.
func (t *Tx) RetryDue() (TxItem, bool) {
	if gap, ok := t.earliestGap(); ok {
		segment := t.segmentAt(gap.Start)
		if segment == nil {
			return TxItem{}, false
		}
		if segment.end() < gap.End {
			gap.End = segment.end()
		}
		segment.generation++
		segment.current = gap
		segment.attempts = make(map[AttachmentKey]attemptRecord)
		return t.dataItem(segment, gap), true
	}
	if t.state == TxFinPending && t.fin != nil && t.acknowledged == t.fin.offset {
		t.fin.generation++
		t.fin.attempts = make(map[AttachmentKey]attemptRecord)
		return t.finItem(), true
	}
	return TxItem{}, false
}

// StartAttempt reserves one attachment for the item's current generation.
func (t *Tx) StartAttempt(itemID, generation uint64, attachment AttachmentKey) (TxAttempt, error) {
	item, attempts, ok := t.currentItem(itemID, generation)
	if !ok {
		return TxAttempt{}, ErrStaleGeneration
	}
	if _, exists := attempts[attachment]; exists {
		return TxAttempt{}, ErrAttachmentExists
	}
	if len(attempts) >= MaxAttachments {
		return TxAttempt{}, ErrAttachmentLimit
	}
	attempts[attachment] = attemptRecord{}
	return TxAttempt{Item: item, Attachment: attachment}, nil
}

// RecordAttemptResult accepts a result exactly once for the current item,
// generation, and attachment. Stale and duplicate results have no side effect.
func (t *Tx) RecordAttemptResult(itemID, generation uint64, attachment AttachmentKey, outcome AttemptOutcome) bool {
	if outcome < AttemptSucceeded || outcome > AttemptTimedOut {
		return false
	}
	_, attempts, ok := t.currentItem(itemID, generation)
	if !ok {
		return false
	}
	record, ok := attempts[attachment]
	if !ok || record.done {
		return false
	}
	record.done = true
	record.outcome = outcome
	attempts[attachment] = record
	return true
}

// ApplyACK validates and applies a cumulative plus selective ACK atomically.
// Only cumulative progress releases replay bytes.
func (t *Tx) ApplyACK(nextOffset uint64, ranges []ByteRange) (bool, error) {
	progress, _, err := t.ApplyACKWithCoverage(nextOffset, ranges)
	return progress, err
}

// ApplyACKWithCoverage returns only logical byte ranges newly covered by this ACK.
func (t *Tx) ApplyACKWithCoverage(nextOffset uint64, ranges []ByteRange) (bool, []ByteRange, error) {
	if err := t.validateACK(nextOffset, ranges); err != nil {
		return false, nil, err
	}
	if nextOffset < t.acknowledged {
		return false, nil, nil
	}
	before := acknowledgedRanges(t.acknowledged, t.selective)

	if nextOffset > t.acknowledged {
		t.acknowledged = nextOffset
		t.releasePrefix(nextOffset)
		t.selective = append([]ByteRange(nil), ranges...)
		t.invalidateCoveredItems()
		return true, subtractRanges(acknowledgedRanges(t.acknowledged, t.selective), before), nil
	}

	merged, err := mergeSelective(t.selective, ranges, nextOffset)
	if err != nil {
		return false, nil, err
	}
	progress := !equalRanges(merged, t.selective)
	if progress {
		t.selective = merged
		t.invalidateCoveredItems()
	}
	return progress, subtractRanges(acknowledgedRanges(t.acknowledged, t.selective), before), nil
}

func acknowledgedRanges(nextOffset uint64, selective []ByteRange) []ByteRange {
	ranges := make([]ByteRange, 0, len(selective)+1)
	if nextOffset != 0 {
		ranges = append(ranges, ByteRange{End: nextOffset})
	}
	ranges = append(ranges, selective...)
	return ranges
}

func subtractRanges(source, covered []ByteRange) []ByteRange {
	if len(source) == 0 {
		return nil
	}
	result := make([]ByteRange, 0, len(source))
	for _, current := range source {
		cursor := current.Start
		for _, existing := range covered {
			if existing.End <= cursor {
				continue
			}
			if existing.Start >= current.End {
				break
			}
			if existing.Start > cursor {
				end := existing.Start
				if end > current.End {
					end = current.End
				}
				if end > cursor {
					result = append(result, ByteRange{Start: cursor, End: end})
				}
			}
			if existing.End > cursor {
				cursor = existing.End
			}
			if cursor >= current.End {
				break
			}
		}
		if cursor < current.End {
			result = append(result, ByteRange{Start: cursor, End: current.End})
		}
	}
	return result
}

// ApplyFINACK completes Tx only for the final offset fixed by Finish.
func (t *Tx) ApplyFINACK(finalOffset uint64) (bool, error) {
	if t.fin == nil || finalOffset != t.fin.offset {
		return false, ErrFinalOffsetConflict
	}
	if t.state == TxComplete {
		return false, nil
	}
	if t.state != TxFinPending {
		return false, ErrInvalidState
	}
	t.acknowledged = t.fin.offset
	t.segments = nil
	t.selective = nil
	t.fin.attempts = nil
	t.state = TxComplete
	return true, nil
}

func (t *Tx) allocateItemID() (uint64, error) {
	if t.nextItemID == ^uint64(0) {
		return 0, ErrOffsetOverflow
	}
	t.nextItemID++
	return t.nextItemID, nil
}

func (t *Tx) dataItem(segment *replaySegment, byteRange ByteRange) TxItem {
	start := byteRange.Start - segment.start
	end := byteRange.End - segment.start
	return TxItem{
		Kind:              TxItemData,
		ItemID:            segment.id,
		AttemptGeneration: segment.generation,
		Offset:            byteRange.Start,
		data:              segment.data[start:end:end],
	}
}

func (t *Tx) finItem() TxItem {
	return TxItem{
		Kind:              TxItemFIN,
		ItemID:            t.fin.id,
		AttemptGeneration: t.fin.generation,
		FinalOffset:       t.fin.offset,
	}
}

func (t *Tx) currentItem(itemID, generation uint64) (TxItem, map[AttachmentKey]attemptRecord, bool) {
	for _, segment := range t.segments {
		if segment.id == itemID {
			if segment.generation != generation || segment.current.Len() == 0 {
				return TxItem{}, nil, false
			}
			return t.dataItem(segment, segment.current), segment.attempts, true
		}
	}
	if t.state == TxFinPending && t.fin != nil && t.fin.id == itemID && t.fin.generation == generation {
		return t.finItem(), t.fin.attempts, true
	}
	return TxItem{}, nil, false
}

func (t *Tx) earliestGap() (ByteRange, bool) {
	if t.acknowledged == t.allocated {
		return ByteRange{}, false
	}
	cursor := t.acknowledged
	for _, ackRange := range t.selective {
		if cursor < ackRange.Start {
			return ByteRange{Start: cursor, End: ackRange.Start}, true
		}
		if cursor < ackRange.End {
			cursor = ackRange.End
		}
	}
	if cursor < t.allocated {
		return ByteRange{Start: cursor, End: t.allocated}, true
	}
	return ByteRange{}, false
}

func (t *Tx) segmentAt(offset uint64) *replaySegment {
	for _, segment := range t.segments {
		if segment.start <= offset && offset < segment.end() {
			return segment
		}
	}
	return nil
}

func (t *Tx) releasePrefix(nextOffset uint64) {
	first := 0
	for first < len(t.segments) && t.segments[first].end() <= nextOffset {
		first++
	}
	if first > 0 {
		remaining := len(t.segments) - first
		copy(t.segments, t.segments[first:])
		clear(t.segments[remaining:])
		t.segments = t.segments[:remaining]
	}
	if len(t.segments) == 0 || t.segments[0].start >= nextOffset {
		return
	}
	segment := t.segments[0]
	trim := nextOffset - segment.start
	segment.data = segment.data[trim:len(segment.data):len(segment.data)]
	segment.start = nextOffset
	segment.current = ByteRange{}
}

func (t *Tx) invalidateCoveredItems() {
	for _, segment := range t.segments {
		current := segment.current
		if current.Len() == 0 {
			continue
		}
		if current.Start < t.acknowledged {
			segment.current = ByteRange{}
			continue
		}
		for _, ackRange := range t.selective {
			if current.Start < ackRange.End && ackRange.Start < current.End {
				segment.current = ByteRange{}
				break
			}
		}
	}
}

func (t *Tx) validateACK(nextOffset uint64, ranges []ByteRange) error {
	if nextOffset > t.allocated {
		return ErrACKBeyondAllocated
	}
	if len(ranges) > MaxACKRanges {
		return ErrACKNotCanonical
	}
	previousEnd := nextOffset
	for _, ackRange := range ranges {
		if ackRange.Start <= previousEnd || ackRange.End <= ackRange.Start {
			return ErrACKNotCanonical
		}
		if ackRange.End > t.allocated {
			return ErrACKBeyondAllocated
		}
		previousEnd = ackRange.End
	}
	return nil
}

func mergeSelective(left, right []ByteRange, nextOffset uint64) ([]ByteRange, error) {
	all := make([]ByteRange, 0, len(left)+len(right))
	all = append(all, left...)
	all = append(all, right...)
	if len(all) == 0 {
		return nil, nil
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Start == all[j].Start {
			return all[i].End < all[j].End
		}
		return all[i].Start < all[j].Start
	})
	merged := make([]ByteRange, 0, len(all))
	for _, current := range all {
		if current.End <= nextOffset {
			continue
		}
		if len(merged) == 0 || current.Start > merged[len(merged)-1].End {
			merged = append(merged, current)
			continue
		}
		if current.End > merged[len(merged)-1].End {
			merged[len(merged)-1].End = current.End
		}
	}
	if len(merged) > MaxACKRanges {
		return nil, ErrACKNotCanonical
	}
	return merged, nil
}

func equalRanges(left, right []ByteRange) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
