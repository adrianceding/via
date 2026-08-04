package flow

import (
	"errors"

	"github.com/adrianceding/via/internal/protocol"
)

type FlowEventKind uint8

const (
	FlowStarted FlowEventKind = iota + 1
	FlowLocalData
	FlowLocalEOF
	FlowRemoteData
	FlowRemoteACK
	FlowRemoteFIN
	FlowRemoteFINACK
	FlowWriteResult
	FlowCloseWriteResult
	FlowRetryDeadline
	FlowNoProgressDeadline
	FlowStartAttempt
	FlowAttemptResult
	FlowLifecycle
)

type FlowEvent struct {
	Kind              FlowEventKind
	Offset            uint64
	Data              []byte
	Ranges            []ByteRange
	FinalOffset       uint64
	Generation        uint64
	ItemID            uint64
	AttemptGeneration uint64
	Attachment        AttachmentKey
	AttemptOutcome    AttemptOutcome
	N                 int
	Err               error
	Lifecycle         LifecycleEvent
}

type FlowActionKind uint8

const (
	FlowActionTxItem FlowActionKind = iota + 1
	FlowActionTxAttempt
	FlowActionTxAcknowledged
	FlowActionRx
	FlowActionLifecycle
	FlowActionArmRetryDeadline
	FlowActionCancelRetryDeadline
	FlowActionArmNoProgressDeadline
	FlowActionCancelNoProgressDeadline
)

type FlowAction struct {
	Kind       FlowActionKind
	TxItem     TxItem
	TxAttempt  TxAttempt
	ACKRanges  []ByteRange
	Rx         RxAction
	Lifecycle  LifecycleAction
	Generation uint64
}

type FlowSnapshot struct {
	Started              bool
	Lifecycle            LifecycleSnapshot
	TxState              TxState
	TxAllocatedOffset    uint64
	TxAcknowledgedOffset uint64
	TxReplayBytes        uint64
	TxReplaySegments     int
	TxAvailableWindow    uint64
	Rx                   RxSnapshot
	RetryGeneration      uint64
	NoProgressGeneration uint64
}

type StatusSnapshot struct {
	LifecycleState       LifecycleState
	PublishedAttachments int
	TxAllocatedOffset    uint64
	TxAcknowledged       uint64
	RxWrittenOffset      uint64
}

// Flow is the single mutable owner of both byte directions, attachments and
// data-protocol deadlines. Handle performs no network or local socket I/O.
type Flow struct {
	tx             *Tx
	rx             *Receiver
	lifecycle      *Lifecycle
	started        bool
	nextGeneration uint64
	retryDeadline  uint64
	noProgress     uint64
}

func NewFlow() *Flow {
	return &Flow{
		tx:        NewTx(),
		rx:        NewReceiver(),
		lifecycle: NewLifecycle(),
	}
}

func (flow *Flow) Snapshot() FlowSnapshot {
	if flow == nil {
		return FlowSnapshot{Lifecycle: LifecycleSnapshot{State: Reset}, TxState: TxComplete, Rx: RxSnapshot{State: RxComplete}}
	}
	return FlowSnapshot{
		Started:              flow.started,
		Lifecycle:            flow.lifecycle.Snapshot(),
		TxState:              flow.tx.State(),
		TxAllocatedOffset:    flow.tx.AllocatedOffset(),
		TxAcknowledgedOffset: flow.tx.AcknowledgedOffset(),
		TxReplayBytes:        flow.tx.ReplayBytes(),
		TxReplaySegments:     flow.tx.ReplaySegments(),
		TxAvailableWindow:    flow.tx.AvailableWindow(),
		Rx:                   flow.rx.Snapshot(),
		RetryGeneration:      flow.retryDeadline,
		NoProgressGeneration: flow.noProgress,
	}
}

func (flow *Flow) StatusSnapshot() StatusSnapshot {
	if flow == nil {
		return StatusSnapshot{LifecycleState: Reset}
	}
	return StatusSnapshot{
		LifecycleState:       flow.lifecycle.State(),
		PublishedAttachments: flow.lifecycle.PublishedAttachmentCount(),
		TxAllocatedOffset:    flow.tx.AllocatedOffset(),
		TxAcknowledged:       flow.tx.AcknowledgedOffset(),
		RxWrittenOffset:      flow.rx.WrittenOffset(),
	}
}

func (flow *Flow) LifecycleState() LifecycleState {
	if flow == nil {
		return Reset
	}
	return flow.lifecycle.State()
}

func (flow *Flow) TxState() TxState {
	if flow == nil {
		return TxComplete
	}
	return flow.tx.State()
}

func (flow *Flow) TxReplayBytes() uint64 {
	if flow == nil {
		return 0
	}
	return flow.tx.ReplayBytes()
}

func (flow *Flow) TxAvailableWindow() uint64 {
	if flow == nil {
		return 0
	}
	return flow.tx.AvailableWindow()
}

func (flow *Flow) TxAcknowledgedOffset() uint64 {
	if flow == nil {
		return 0
	}
	return flow.tx.AcknowledgedOffset()
}

func (flow *Flow) IsAttachmentPublished(attachment AttachmentKey) bool {
	return flow != nil && flow.lifecycle.IsPublished(attachment)
}

func (flow *Flow) HasPublishedAttachments() bool {
	return flow != nil && flow.lifecycle.HasPublishedAttachments()
}

func (flow *Flow) Handle(event FlowEvent) ([]FlowAction, error) {
	if flow == nil {
		return nil, ErrInvalidState
	}

	switch event.Kind {
	case FlowStarted:
		if flow.started {
			return nil, ErrInvalidState
		}
		flow.started = true
		return flow.applyLifecycle(LifecycleEvent{Kind: LifecycleStarted}), nil
	case FlowLifecycle:
		return flow.applyLifecycle(event.Lifecycle), nil
	case FlowLocalData:
		return flow.handleLocalData(event.Data)
	case FlowLocalEOF:
		return flow.handleLocalEOF()
	case FlowRemoteData:
		return flow.handleRemoteData(event.Offset, event.Data)
	case FlowRemoteACK:
		return flow.handleRemoteACK(event.Offset, event.Ranges)
	case FlowRemoteFIN:
		return flow.handleRemoteFIN(event.FinalOffset)
	case FlowRemoteFINACK:
		return flow.handleRemoteFINACK(event.FinalOffset)
	case FlowWriteResult:
		return flow.handleWriteResult(event.Generation, event.N, event.Err)
	case FlowCloseWriteResult:
		return flow.handleCloseWriteResult(event.Generation, event.Err)
	case FlowRetryDeadline:
		return flow.handleRetryDeadline(event.Generation), nil
	case FlowNoProgressDeadline:
		return flow.handleNoProgressDeadline(event.Generation), nil
	case FlowStartAttempt:
		return flow.handleStartAttempt(event)
	case FlowAttemptResult:
		if flow.isTerminalOrConverging() {
			return nil, nil
		}
		flow.tx.RecordAttemptResult(event.ItemID, event.AttemptGeneration, event.Attachment, event.AttemptOutcome)
		return nil, nil
	default:
		return nil, ErrInvalidState
	}
}

func (flow *Flow) handleLocalData(data []byte) ([]FlowAction, error) {
	if !flow.mayAcceptLocalReadResult() {
		return nil, ErrInvalidState
	}
	item, err := flow.tx.Append(data)
	if err != nil {
		return nil, err
	}
	actions := []FlowAction{{Kind: FlowActionTxItem, TxItem: item}}
	return flow.syncDataDeadlines(actions, false, false), nil
}

func (flow *Flow) handleLocalEOF() ([]FlowAction, error) {
	if !flow.mayAcceptLocalReadResult() {
		return nil, ErrInvalidState
	}
	before := flow.tx.State()
	item, err := flow.tx.Finish()
	if err != nil {
		return nil, err
	}
	actions := []FlowAction{{Kind: FlowActionTxItem, TxItem: item}}
	return flow.syncDataDeadlines(actions, false, flow.tx.State() != before), nil
}

func (flow *Flow) handleRemoteData(offset uint64, data []byte) ([]FlowAction, error) {
	if !flow.mayAcceptRemoteFrame() {
		return nil, ErrInvalidState
	}
	rxActions, err := flow.rx.ReceiveData(offset, data)
	actions := wrapRxActions(rxActions)
	if err != nil {
		return append(actions, flow.resetForError(remoteDataResetReason(err))...), err
	}
	return flow.syncDataDeadlines(actions, false, false), nil
}

func (flow *Flow) handleRemoteACK(nextOffset uint64, ranges []ByteRange) ([]FlowAction, error) {
	if !flow.mayAcceptRemoteFrame() {
		return nil, ErrInvalidState
	}
	before := flow.tx.AcknowledgedOffset()
	_, newlyCovered, err := flow.tx.ApplyACKWithCoverage(nextOffset, ranges)
	if err != nil {
		return flow.resetForError(protocol.ResetProtocolConflict), err
	}
	var actions []FlowAction
	if len(newlyCovered) != 0 {
		actions = append(actions, FlowAction{Kind: FlowActionTxAcknowledged, ACKRanges: newlyCovered})
	}
	cumulativeProgress := flow.tx.AcknowledgedOffset() > before
	return flow.syncDataDeadlines(actions, cumulativeProgress, cumulativeProgress), nil
}

func (flow *Flow) handleRemoteFIN(finalOffset uint64) ([]FlowAction, error) {
	if !flow.mayAcceptRemoteFrame() {
		return nil, ErrInvalidState
	}
	before := flow.rx.State()
	rxActions, err := flow.rx.ReceiveFIN(finalOffset)
	actions := wrapRxActions(rxActions)
	if err != nil {
		return append(actions, flow.resetForError(protocol.ResetProtocolConflict)...), err
	}
	progress := flow.rx.State() != before
	actions = flow.syncDataDeadlines(actions, false, progress)
	return flow.maybeDirectionsComplete(actions), nil
}

func (flow *Flow) handleRemoteFINACK(finalOffset uint64) ([]FlowAction, error) {
	if !flow.mayAcceptRemoteFrame() {
		return nil, ErrInvalidState
	}
	progress, err := flow.tx.ApplyFINACK(finalOffset)
	if err != nil {
		return flow.resetForError(protocol.ResetProtocolConflict), err
	}
	actions := flow.syncDataDeadlines(nil, progress, progress)
	return flow.maybeDirectionsComplete(actions), nil
}

func (flow *Flow) handleWriteResult(generation uint64, n int, writeErr error) ([]FlowAction, error) {
	if flow.isTerminalOrConverging() {
		return nil, nil
	}
	before := flow.rx.WrittenOffset()
	rxActions, err := flow.rx.HandleWriteResult(generation, n, writeErr)
	actions := wrapRxActions(rxActions)
	progress := flow.rx.WrittenOffset() > before
	if err != nil {
		return append(actions, flow.resetForError(protocol.ResetLocalIOFailure)...), err
	}
	actions = flow.syncDataDeadlines(actions, false, progress)
	return flow.maybeDirectionsComplete(actions), nil
}

func (flow *Flow) handleCloseWriteResult(generation uint64, closeErr error) ([]FlowAction, error) {
	if flow.isTerminalOrConverging() {
		return nil, nil
	}
	before := flow.rx.State()
	rxActions, err := flow.rx.HandleCloseWriteResult(generation, closeErr)
	actions := wrapRxActions(rxActions)
	progress := flow.rx.State() != before
	if err != nil {
		return append(actions, flow.resetForError(protocol.ResetLocalIOFailure)...), err
	}
	actions = flow.syncDataDeadlines(actions, false, progress)
	return flow.maybeDirectionsComplete(actions), nil
}

func (flow *Flow) handleRetryDeadline(generation uint64) []FlowAction {
	if generation == 0 || generation != flow.retryDeadline || flow.isTerminalOrConverging() {
		return nil
	}
	flow.retryDeadline = 0
	var actions []FlowAction
	if item, ok := flow.tx.RetryDue(); ok {
		actions = append(actions, FlowAction{Kind: FlowActionTxItem, TxItem: item})
	}
	return flow.syncDataDeadlines(actions, false, false)
}

func (flow *Flow) handleNoProgressDeadline(generation uint64) []FlowAction {
	if generation == 0 || generation != flow.noProgress || flow.isTerminalOrConverging() {
		return nil
	}
	flow.noProgress = 0
	return flow.resetForError(protocol.ResetDeadlineExceeded)
}

func (flow *Flow) handleStartAttempt(event FlowEvent) ([]FlowAction, error) {
	if flow.isTerminalOrConverging() {
		return nil, ErrInvalidState
	}
	if !flow.hasPublishedAttachment(event.Attachment) {
		return nil, ErrAttachmentUnknown
	}
	attempt, err := flow.tx.StartAttempt(event.ItemID, event.AttemptGeneration, event.Attachment)
	if err != nil {
		return nil, err
	}
	return []FlowAction{{Kind: FlowActionTxAttempt, TxAttempt: attempt}}, nil
}

func (flow *Flow) hasPublishedAttachment(attachment AttachmentKey) bool {
	return flow.lifecycle.IsPublished(attachment)
}

func (flow *Flow) applyLifecycle(event LifecycleEvent) []FlowAction {
	before := flow.lifecycle.Snapshot()
	lifecycleActions := flow.lifecycle.Handle(event)
	actions := make([]FlowAction, 0, len(lifecycleActions)+3)
	for _, action := range lifecycleActions {
		actions = append(actions, FlowAction{Kind: FlowActionLifecycle, Lifecycle: action})
		if action.Kind == LifecycleActionSendAcknowledgementSnapshot {
			actions = append(actions, FlowAction{Kind: FlowActionRx, Rx: flow.rx.ackAction()})
			if flow.rx.State() == RxComplete {
				actions = append(actions, FlowAction{Kind: FlowActionRx, Rx: flow.rx.finACKAction()})
			}
		}
	}
	after := flow.lifecycle.Snapshot()
	// Closing is a bounded linger state. Keep final offsets and acknowledgement
	// snapshots until the linger ends so duplicates from another attachment can
	// be answered idempotently instead of being mistaken for protocol conflicts.
	if after.State == Closed || after.State == Resetting || after.State == Reset {
		flow.tx.Discard()
		flow.rx.Discard()
	}
	if (event.Kind == LifecycleAttachmentLost || event.Kind == LifecycleSessionClosed) &&
		len(after.Published) > 0 && flow.tx.HasPending() {
		if item, ok := flow.tx.RetryDue(); ok {
			actions = append(actions, FlowAction{Kind: FlowActionTxItem, TxItem: item})
		}
	}
	if before.State == Recovering && after.State == Relaying && flow.tx.HasPending() {
		if item, ok := flow.tx.RetryDue(); ok {
			actions = append(actions, FlowAction{Kind: FlowActionTxItem, TxItem: item})
		}
	}
	if flow.isTerminalOrConverging() {
		return flow.cancelDataDeadlines(actions)
	}
	return flow.syncDataDeadlines(actions, false, false)
}

func (flow *Flow) maybeDirectionsComplete(actions []FlowAction) []FlowAction {
	if flow.tx.State() != TxComplete || flow.rx.State() != RxComplete || flow.isTerminalOrConverging() {
		return actions
	}
	return append(actions, flow.applyLifecycle(LifecycleEvent{Kind: LifecycleDirectionsComplete})...)
}

func (flow *Flow) syncDataDeadlines(actions []FlowAction, resetRetry, resetNoProgress bool) []FlowAction {
	if flow.isTerminalOrConverging() {
		return flow.cancelDataDeadlines(actions)
	}
	if flow.tx.HasPending() {
		if resetRetry && flow.retryDeadline != 0 {
			actions = append(actions, FlowAction{Kind: FlowActionCancelRetryDeadline, Generation: flow.retryDeadline})
			flow.retryDeadline = 0
		}
		if flow.retryDeadline == 0 {
			flow.retryDeadline = flow.allocateGeneration()
			actions = append(actions, FlowAction{Kind: FlowActionArmRetryDeadline, Generation: flow.retryDeadline})
		}
	} else if flow.retryDeadline != 0 {
		actions = append(actions, FlowAction{Kind: FlowActionCancelRetryDeadline, Generation: flow.retryDeadline})
		flow.retryDeadline = 0
	}

	if flow.hasNoProgressWork() {
		if resetNoProgress && flow.noProgress != 0 {
			actions = append(actions, FlowAction{Kind: FlowActionCancelNoProgressDeadline, Generation: flow.noProgress})
			flow.noProgress = 0
		}
		if flow.noProgress == 0 {
			flow.noProgress = flow.allocateGeneration()
			actions = append(actions, FlowAction{Kind: FlowActionArmNoProgressDeadline, Generation: flow.noProgress})
		}
	} else if flow.noProgress != 0 {
		actions = append(actions, FlowAction{Kind: FlowActionCancelNoProgressDeadline, Generation: flow.noProgress})
		flow.noProgress = 0
	}
	return actions
}

func (flow *Flow) cancelDataDeadlines(actions []FlowAction) []FlowAction {
	if flow.retryDeadline != 0 {
		actions = append(actions, FlowAction{Kind: FlowActionCancelRetryDeadline, Generation: flow.retryDeadline})
		flow.retryDeadline = 0
	}
	if flow.noProgress != 0 {
		actions = append(actions, FlowAction{Kind: FlowActionCancelNoProgressDeadline, Generation: flow.noProgress})
		flow.noProgress = 0
	}
	return actions
}

func (flow *Flow) hasNoProgressWork() bool {
	rx := flow.rx.Snapshot()
	return flow.tx.HasPending() || rx.BufferedBytes != 0 || rx.PendingWriteGeneration != 0 ||
		rx.PendingCloseGeneration != 0 || rx.State == RxFinSeen || rx.State == RxClosingWrite
}

func (flow *Flow) allocateGeneration() uint64 {
	if flow.nextGeneration == ^uint64(0) {
		panic("flow deadline generation exhausted")
	}
	flow.nextGeneration++
	return flow.nextGeneration
}

func (flow *Flow) mayAcceptLocalReadResult() bool {
	state := flow.lifecycle.State()
	return flow.started && (state == Relaying || state == Recovering)
}

func (flow *Flow) mayAcceptRemoteFrame() bool {
	state := flow.lifecycle.State()
	return flow.started && (state == Relaying || state == Recovering || state == Closing)
}

func (flow *Flow) isTerminalOrConverging() bool {
	state := flow.lifecycle.State()
	return state == Closing || state == Closed || state == Resetting || state == Reset
}

func (flow *Flow) resetForError(reason protocol.ResetReason) []FlowAction {
	return flow.applyLifecycle(LifecycleEvent{Kind: LifecycleResetRequested, Reason: reason})
}

func remoteDataResetReason(err error) protocol.ResetReason {
	if errors.Is(err, ErrWindowExceeded) || errors.Is(err, ErrRangeLimit) {
		return protocol.ResetResourceLimit
	}
	return protocol.ResetProtocolConflict
}

func wrapRxActions(rxActions []RxAction) []FlowAction {
	actions := make([]FlowAction, 0, len(rxActions))
	for _, action := range rxActions {
		actions = append(actions, FlowAction{Kind: FlowActionRx, Rx: action})
	}
	return actions
}
