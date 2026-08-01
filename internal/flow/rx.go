package flow

import (
	"bytes"
	"math"
)

type RxActionKind uint8

const (
	RxWriteLocal RxActionKind = iota + 1
	RxCloseWriteLocal
	RxSendACK
	RxSendFINACK
)

// ACKSnapshot is the receive direction's detached cumulative/selective ACK.
type ACKSnapshot struct {
	NextOffset uint64
	Ranges     []ByteRange
}

// RxAction describes work for an executor. Receiver never performs network I/O.
type RxAction struct {
	Kind        RxActionKind
	Generation  uint64
	Offset      uint64
	ACK         ACKSnapshot
	FinalOffset uint64
	data        []byte
}

func (action RxAction) DataLen() int { return len(action.data) }

func (action RxAction) AppendData(dst []byte) []byte { return append(dst, action.data...) }

func (action RxAction) CopyData() []byte { return bytes.Clone(action.data) }

// RxSnapshot contains bounded scalar state suitable for tests and diagnostics.
type RxSnapshot struct {
	State                  RxState
	WrittenOffset          uint64
	BufferedBytes          uint64
	RangeCount             int
	FinalOffset            uint64
	HasFinalOffset         bool
	PendingWriteGeneration uint64
	PendingCloseGeneration uint64
	Failed                 bool
}

type receiveRange struct {
	start uint64
	data  []byte
}

func (r receiveRange) end() uint64 {
	return r.start + uint64(len(r.data))
}

type pendingRxWrite struct {
	generation uint64
	offset     uint64
	data       []byte
}

// Receiver owns one flow's local receive direction.
//
// Event/state table:
//
//	RxOpen       + DATA              -> RxOpen       + ACK[, WriteLocal]
//	RxOpen       + FIN               -> RxFinSeen    + ACK[, CloseWriteLocal]
//	RxFinSeen    + DATA/WriteResult  -> RxFinSeen    + ACK[, WriteLocal]
//	RxFinSeen    + all bytes written -> RxClosingWrite + CloseWriteLocal
//	RxClosingWrite + CloseResult     -> RxComplete   + FIN_ACK
//	RxComplete   + duplicate DATA/FIN -> RxComplete  + ACK/FIN_ACK
//
// Any semantic or executor-contract error is reported to the flow owner, which
// enters Resetting. Results for an old generation or terminal direction are
// ignored without side effects.
type Receiver struct {
	state          RxState
	writtenOffset  uint64
	ranges         []receiveRange
	finalOffset    uint64
	hasFinalOffset bool
	nextGeneration uint64
	pendingWrite   *pendingRxWrite
	pendingClose   uint64
	failure        error
}

func NewReceiver() *Receiver {
	return &Receiver{state: RxOpen}
}

func (r *Receiver) State() RxState {
	return r.state
}

func (r *Receiver) WrittenOffset() uint64 {
	return r.writtenOffset
}

func (r *Receiver) BufferedBytes() uint64 {
	var total uint64
	for _, buffered := range r.ranges {
		total += uint64(len(buffered.data))
	}
	return total
}

func (r *Receiver) RangeCount() int {
	return len(r.ranges)
}

func (r *Receiver) FinalOffset() (uint64, bool) {
	return r.finalOffset, r.hasFinalOffset
}

func (r *Receiver) Failed() error {
	return r.failure
}

func (r *Receiver) Discard() {
	for index := range r.ranges {
		clear(r.ranges[index].data)
		r.ranges[index] = receiveRange{}
	}
	r.ranges = nil
	if r.pendingWrite != nil {
		clear(r.pendingWrite.data)
	}
	r.pendingWrite = nil
	r.pendingClose = 0
	r.state = RxComplete
}

func (r *Receiver) Snapshot() RxSnapshot {
	snapshot := RxSnapshot{
		State:          r.state,
		WrittenOffset:  r.writtenOffset,
		BufferedBytes:  r.BufferedBytes(),
		RangeCount:     len(r.ranges),
		FinalOffset:    r.finalOffset,
		HasFinalOffset: r.hasFinalOffset,
		Failed:         r.failure != nil,
	}
	if r.pendingWrite != nil {
		snapshot.PendingWriteGeneration = r.pendingWrite.generation
	}
	snapshot.PendingCloseGeneration = r.pendingClose
	return snapshot
}

// ACKSnapshot returns a detached canonical snapshot. When buffered data starts
// exactly at NextOffset, that first unwritten byte remains the represented gap
// and the received suffix is selectively acknowledged.
func (r *Receiver) ACKSnapshot() ACKSnapshot {
	var ranges []ByteRange
	for _, buffered := range r.ranges {
		start := buffered.start
		if start == r.writtenOffset {
			start++
		}
		if start <= r.writtenOffset || start >= buffered.end() {
			continue
		}
		ranges = append(ranges, ByteRange{Start: start, End: buffered.end()})
		if len(ranges) == MaxACKRanges {
			break
		}
	}
	return ACKSnapshot{NextOffset: r.writtenOffset, Ranges: ranges}
}

// ReceiveData validates, crops and inserts one immutable DATA interval.
func (r *Receiver) ReceiveData(offset uint64, data []byte) ([]RxAction, error) {
	if r.failure != nil || len(data) == 0 {
		return r.fail(ErrInvalidState)
	}
	end, err := checkedEnd(offset, len(data))
	if err != nil {
		return r.fail(err)
	}
	if r.hasFinalOffset && end > r.finalOffset {
		return r.fail(ErrDataBeyondFinal)
	}

	if end <= r.writtenOffset {
		return r.duplicateActions(), nil
	}
	if offset < r.writtenOffset {
		trim := r.writtenOffset - offset
		data = data[trim:]
		offset = r.writtenOffset
	}
	if end > saturatingAdd(r.writtenOffset, ReceiveWindowSize) {
		return r.fail(ErrWindowExceeded)
	}
	if err := r.insertRange(offset, end, data); err != nil {
		return r.fail(err)
	}

	actions := []RxAction{r.ackAction()}
	write, err := r.scheduleWrite()
	if err != nil {
		return actions, r.recordFailure(err)
	}
	if write != nil {
		actions = append(actions, *write)
	}
	return actions, nil
}

// HandleWriteResult applies only the currently in-flight write generation.
// Progress is committed and acknowledged before a simultaneous error is
// returned to the flow owner.
func (r *Receiver) HandleWriteResult(generation uint64, n int, writeErr error) ([]RxAction, error) {
	if r.failure != nil || r.state == RxComplete || r.pendingWrite == nil || r.pendingWrite.generation != generation {
		return nil, nil
	}
	pending := r.pendingWrite
	if n < 0 || n > len(pending.data) || (n == 0 && writeErr == nil) {
		r.pendingWrite = nil
		return r.fail(ErrInvalidWriteResult)
	}
	if n == 0 {
		r.pendingWrite = nil
		return r.fail(writeErr)
	}

	r.pendingWrite = nil
	r.advanceWritten(uint64(n))
	actions := []RxAction{r.ackAction()}
	if writeErr != nil {
		return actions, r.recordFailure(writeErr)
	}

	if r.hasFinalOffset && r.writtenOffset == r.finalOffset {
		closeWrite, err := r.scheduleCloseWrite()
		if err != nil {
			return actions, r.recordFailure(err)
		}
		if closeWrite != nil {
			actions = append(actions, *closeWrite)
		}
		return actions, nil
	}

	write, err := r.scheduleWrite()
	if err != nil {
		return actions, r.recordFailure(err)
	}
	if write != nil {
		actions = append(actions, *write)
	}
	return actions, nil
}

// ReceiveFIN records the sole final offset. Repeated matching FIN is
// idempotent; FIN_ACK is emitted only after CloseWrite succeeds.
func (r *Receiver) ReceiveFIN(finalOffset uint64) ([]RxAction, error) {
	if r.failure != nil {
		return r.fail(ErrInvalidState)
	}
	if r.hasFinalOffset {
		if finalOffset != r.finalOffset {
			return r.fail(ErrFinalOffsetConflict)
		}
		if r.state == RxComplete {
			return []RxAction{r.finACKAction()}, nil
		}
		return []RxAction{r.ackAction()}, nil
	}
	if finalOffset < r.writtenOffset {
		return r.fail(ErrDataBeyondFinal)
	}
	for _, buffered := range r.ranges {
		if buffered.end() > finalOffset {
			return r.fail(ErrDataBeyondFinal)
		}
	}

	r.finalOffset = finalOffset
	r.hasFinalOffset = true
	r.state = RxFinSeen
	actions := []RxAction{r.ackAction()}
	if r.writtenOffset == finalOffset {
		closeWrite, err := r.scheduleCloseWrite()
		if err != nil {
			return actions, r.recordFailure(err)
		}
		if closeWrite != nil {
			actions = append(actions, *closeWrite)
		}
	}
	return actions, nil
}

// HandleCloseWriteResult completes Rx only for the current close generation.
func (r *Receiver) HandleCloseWriteResult(generation uint64, closeErr error) ([]RxAction, error) {
	if r.failure != nil || r.state == RxComplete || r.pendingClose == 0 || r.pendingClose != generation {
		return nil, nil
	}
	r.pendingClose = 0
	if closeErr != nil {
		return r.fail(closeErr)
	}
	r.state = RxComplete
	return []RxAction{r.finACKAction()}, nil
}

func (r *Receiver) insertRange(start, end uint64, data []byte) error {
	for _, existing := range r.ranges {
		overlapStart := max(start, existing.start)
		overlapEnd := min(end, existing.end())
		if overlapStart >= overlapEnd {
			continue
		}
		incomingStart := overlapStart - start
		existingStart := overlapStart - existing.start
		length := overlapEnd - overlapStart
		if !bytes.Equal(data[incomingStart:incomingStart+length], existing.data[existingStart:existingStart+length]) {
			return ErrDataConflict
		}
	}

	first := 0
	for first < len(r.ranges) && r.ranges[first].end() < start {
		first++
	}
	mergedStart := start
	mergedEnd := end
	last := first
	for last < len(r.ranges) && r.ranges[last].start <= mergedEnd {
		mergedStart = min(mergedStart, r.ranges[last].start)
		mergedEnd = max(mergedEnd, r.ranges[last].end())
		last++
	}
	newCount := len(r.ranges) - (last - first) + 1
	if newCount > MaxReassemblyRanges {
		return ErrRangeLimit
	}

	merged := make([]byte, int(mergedEnd-mergedStart))
	for index := first; index < last; index++ {
		existing := r.ranges[index]
		copy(merged[existing.start-mergedStart:], existing.data)
	}
	copy(merged[start-mergedStart:], data)

	updated := make([]receiveRange, 0, newCount)
	updated = append(updated, r.ranges[:first]...)
	updated = append(updated, receiveRange{start: mergedStart, data: merged})
	updated = append(updated, r.ranges[last:]...)
	r.ranges = updated
	return nil
}

func (r *Receiver) advanceWritten(n uint64) {
	r.writtenOffset += n
	for len(r.ranges) > 0 && r.ranges[0].end() <= r.writtenOffset {
		clear(r.ranges[0].data)
		r.ranges[0] = receiveRange{}
		r.ranges = r.ranges[1:]
	}
	if len(r.ranges) == 0 || r.ranges[0].start >= r.writtenOffset {
		return
	}
	trim := r.writtenOffset - r.ranges[0].start
	r.ranges[0].data = bytes.Clone(r.ranges[0].data[trim:])
	r.ranges[0].start = r.writtenOffset
}

func (r *Receiver) scheduleWrite() (*RxAction, error) {
	if r.pendingWrite != nil || r.pendingClose != 0 || r.state == RxComplete || len(r.ranges) == 0 || r.ranges[0].start != r.writtenOffset {
		return nil, nil
	}
	generation, err := r.allocateGeneration()
	if err != nil {
		return nil, err
	}
	r.pendingWrite = &pendingRxWrite{
		generation: generation,
		offset:     r.writtenOffset,
		data:       bytes.Clone(r.ranges[0].data),
	}
	return &RxAction{
		Kind:       RxWriteLocal,
		Generation: generation,
		Offset:     r.pendingWrite.offset,
		data:       r.pendingWrite.data[:len(r.pendingWrite.data):len(r.pendingWrite.data)],
	}, nil
}

func (r *Receiver) scheduleCloseWrite() (*RxAction, error) {
	if r.pendingClose != 0 || r.state == RxComplete {
		return nil, nil
	}
	if r.pendingWrite != nil || !r.hasFinalOffset || r.writtenOffset != r.finalOffset {
		return nil, ErrInvalidState
	}
	generation, err := r.allocateGeneration()
	if err != nil {
		return nil, err
	}
	r.pendingClose = generation
	r.state = RxClosingWrite
	return &RxAction{
		Kind:        RxCloseWriteLocal,
		Generation:  generation,
		FinalOffset: r.finalOffset,
	}, nil
}

func (r *Receiver) allocateGeneration() (uint64, error) {
	if r.nextGeneration == math.MaxUint64 {
		return 0, ErrOffsetOverflow
	}
	r.nextGeneration++
	return r.nextGeneration, nil
}

func (r *Receiver) duplicateActions() []RxAction {
	actions := []RxAction{r.ackAction()}
	if r.state == RxComplete {
		actions = append(actions, r.finACKAction())
	}
	return actions
}

func (r *Receiver) ackAction() RxAction {
	return RxAction{Kind: RxSendACK, ACK: r.ACKSnapshot()}
}

func (r *Receiver) finACKAction() RxAction {
	return RxAction{Kind: RxSendFINACK, FinalOffset: r.finalOffset}
}

func (r *Receiver) fail(err error) ([]RxAction, error) {
	return nil, r.recordFailure(err)
}

func (r *Receiver) recordFailure(err error) error {
	if err == nil {
		err = ErrInvalidState
	}
	if r.failure == nil {
		r.failure = err
	}
	r.pendingWrite = nil
	r.pendingClose = 0
	return err
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}
