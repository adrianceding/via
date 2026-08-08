package server

import (
	"bytes"
	"errors"
	"io"
	"math"
	"sort"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

const (
	// MaxRelayPendingSends bounds the per-flow ledger between producing a
	// transport send action and receiving its executor result. Reaching this
	// boundary is transport backpressure, not a flow failure.
	MaxRelayPendingSends = flow.MaxAttachments * 4
	MaxRelayAttemptItems = flow.MaxReplaySegments + 1
	MaxTargetReadBytes   = protocol.MaxDataLength
	MaxTargetWriteBytes  = int(flow.ReceiveWindowSize)
)

var (
	ErrInvalidRelay           = errors.New("server: invalid relay")
	ErrInvalidRelayEvent      = errors.New("server: invalid relay event")
	ErrRelaySendLimit         = errors.New("server: relay send limit exceeded")
	ErrInvalidTargetRead      = errors.New("server: invalid target read result")
	ErrInvalidTargetWrite     = errors.New("server: invalid target write result")
	ErrRelayAttemptLimit      = errors.New("server: relay attempt history limit exceeded")
	ErrRelayGenerationExhaust = errors.New("server: relay generation exhausted")
)

type RelayEventKind uint8

const (
	RelayStart RelayEventKind = iota + 1
	RelayApplyFlowActions
	RelayRemoteMessage
	RelayTargetReadResult
	RelayTargetWriteResult
	RelayTargetCloseWriteResult
	RelaySendAdmitted
	RelaySendResult
	RelayRetryDeadline
	RelayNoProgressDeadline
	RelayRecoveryDeadline
	RelayClosingDeadline
	RelayResetDeadline
	RelayResetRequested
	RelayObserveDataQuality
	RelayObserveProbeQuality
	RelaySetAttachmentLoad
	RelaySetStallPenalty
	RelaySetSessionQuality
	RelaySetSessionQualities
)

// RelayEvent is one serialized input to the target relay owner. FlowActions is
// used for actions already produced by the JOIN coordinator from the same Flow.
type RelayEvent struct {
	Kind             RelayEventKind
	FlowActions      []flow.FlowAction
	Message          protocol.Message
	Attachment       flow.AttachmentKey
	Generation       uint64
	Data             []byte
	N                int
	Err              error
	AttemptOutcome   flow.AttemptOutcome
	ResetReason      protocol.ResetReason
	RTT              time.Duration
	Interval         time.Duration
	Bytes            uint64
	QueuedBytes      uint64
	InFlightBytes    uint64
	StallPenalty     time.Duration
	WriteCompletedAt time.Time
	Quality          policy.QualitySnapshot
	SessionQualities map[flow.AttachmentKey]policy.QualitySnapshot
}

type RelayActionKind uint8

const (
	RelayActionReadTarget RelayActionKind = iota + 1
	RelayActionWriteTarget
	RelayActionCloseWriteTarget
	RelayActionCloseTarget
	RelayActionSendMessage
	RelayActionArmRetryDeadline
	RelayActionCancelRetryDeadline
	RelayActionArmNoProgressDeadline
	RelayActionCancelNoProgressDeadline
	RelayActionArmRecoveryDeadline
	RelayActionCancelRecoveryDeadline
	RelayActionArmClosingDeadline
	RelayActionCancelClosingDeadline
	RelayActionArmResetDeadline
	RelayActionCancelResetDeadline
	RelayActionAttachmentPublished
	RelayActionAttachmentWithdrawn
	RelayActionDataCredit
)

// RelayAction is an I/O-free executor description. Write payloads are exposed
// only through copying helpers so an executor cannot mutate Flow-owned bytes.
type RelayAction struct {
	Kind              RelayActionKind
	Generation        uint64
	MaxBytes          int
	Attachment        flow.AttachmentKey
	Message           protocol.Message
	Encoded           []byte
	Class             transport.FrameClass
	ItemID            uint64
	AttemptGeneration uint64
	SendDataBytes     int
	Offset            uint64
	FinalOffset       uint64
	After             time.Duration
	DataCreditBytes   uint64
	WriteCompletedAt  time.Time
	AcknowledgedAt    time.Time
	data              []byte
}

func (action RelayAction) DataLen() int { return len(action.data) }

func (action RelayAction) AppendData(dst []byte) []byte { return append(dst, action.data...) }

func (action RelayAction) CopyData() []byte { return bytes.Clone(action.data) }

type RelaySnapshot struct {
	Flow                       flow.FlowSnapshot
	Policy                     policy.Snapshot
	Started                    bool
	TargetReadPaused           bool
	TargetReadGeneration       uint64
	TargetReadLimit            int
	TargetWriteGeneration      uint64
	TargetCloseWriteGeneration uint64
	TargetCloseIssued          bool
	PendingSends               int
}

type relayPendingSendKind uint8

const (
	relayPendingAttempt relayPendingSendKind = iota + 1
	relayPendingControl
	relayPendingReset
)

type relayPendingSend struct {
	kind              relayPendingSendKind
	attachment        flow.AttachmentKey
	itemID            uint64
	attemptGeneration uint64
	resetGeneration   uint64
	dataBytes         uint64
	admissionResolved bool
}

type relayAttemptHistory struct {
	generation  uint64
	end         uint64
	fin         bool
	retryAfter  time.Duration
	attempted   []flow.AttachmentKey
	attempts    []relayAttempt
	invalidated bool
}

type relayAttempt struct {
	attachment       flow.AttachmentKey
	start            uint64
	end              uint64
	writeSucceeded   bool
	invalid          bool
	writeCompletedAt time.Time
	credited         []flow.ByteRange
}

// Relay owns target-side read/write action generations and maps a single
// server Flow to policy placements. It never performs target or transport I/O.
//
// Event/action table:
//
//	Start / published JOIN       -> arm lifecycle timers / resume ReadTarget
//	target Read(data)            -> FlowLocalData -> placed DATA attempts
//	target Read(EOF)             -> FlowLocalEOF  -> placed FIN attempts
//	remote DATA                  -> WriteTarget + placed ACK
//	WriteTarget result           -> next short-write action or ACK/RESET
//	remote FIN                   -> CloseWriteTarget -> placed FIN_ACK
//	remote ACK/FIN_ACK           -> release replay / converge directions
//	remote RESET or local error  -> close target; never reply to RESET
//	stale generation result      -> no state change
//
// Read, Write and CloseWrite each have at most one in-flight generation. The
// send-result ledger is capped at MaxRelayPendingSends, while policy and Flow
// independently cap placements, replay, reassembly and attachments.
type Relay struct {
	flowID               protocol.FlowID
	machine              *flow.Flow
	policy               *policy.Policy
	started              bool
	readPaused           bool
	nextGeneration       uint64
	readGeneration       uint64
	readLimit            int
	writeGeneration      uint64
	closeWriteGeneration uint64
	closeIssued          bool
	pendingSends         map[uint64]relayPendingSend
	controlPending       map[flow.AttachmentKey]struct{}
	deferredControls     map[flow.AttachmentKey]protocol.Message
	resetGeneration      uint64
	resetPending         int
	attemptHistory       map[uint64]*relayAttemptHistory
	retryAfter           time.Duration
	now                  func() time.Time
}

func NewRelay(flowID protocol.FlowID, config policy.Config, machine *flow.Flow) (*Relay, error) {
	return NewRelayWithClock(flowID, config, machine, time.Now)
}

func NewRelayWithClock(flowID protocol.FlowID, config policy.Config, machine *flow.Flow, now func() time.Time) (*Relay, error) {
	if flowID == (protocol.FlowID{}) || machine == nil {
		return nil, ErrInvalidRelay
	}
	if now == nil {
		return nil, ErrInvalidRelay
	}
	sender, err := policy.NewWithClock(flowID, config, now)
	if err != nil {
		return nil, err
	}
	snapshot := machine.Snapshot()
	if snapshot.Lifecycle.State == flow.Closed || snapshot.Lifecycle.State == flow.Reset ||
		len(snapshot.Lifecycle.Published) != 0 || len(snapshot.Lifecycle.Provisional) != 0 {
		return nil, ErrInvalidRelay
	}
	if err := machine.BindFlowID(flowID); err != nil {
		return nil, ErrInvalidRelay
	}
	return &Relay{
		flowID:           flowID,
		machine:          machine,
		policy:           sender,
		started:          snapshot.Started,
		readPaused:       true,
		pendingSends:     make(map[uint64]relayPendingSend, MaxRelayPendingSends),
		controlPending:   make(map[flow.AttachmentKey]struct{}, flow.MaxAttachments),
		deferredControls: make(map[flow.AttachmentKey]protocol.Message, flow.MaxAttachments),
		attemptHistory:   make(map[uint64]*relayAttemptHistory, MaxRelayAttemptItems),
		now:              now,
	}, nil
}

func (relay *Relay) Snapshot() RelaySnapshot {
	if relay == nil {
		return RelaySnapshot{TargetCloseIssued: true}
	}
	return RelaySnapshot{
		Flow:                       relay.machine.Snapshot(),
		Policy:                     relay.policy.Snapshot(),
		Started:                    relay.started,
		TargetReadPaused:           relay.readPaused,
		TargetReadGeneration:       relay.readGeneration,
		TargetReadLimit:            relay.readLimit,
		TargetWriteGeneration:      relay.writeGeneration,
		TargetCloseWriteGeneration: relay.closeWriteGeneration,
		TargetCloseIssued:          relay.closeIssued,
		PendingSends:               len(relay.pendingSends),
	}
}

func (relay *Relay) AdaptiveState() policy.AdaptiveState {
	if relay == nil || relay.policy == nil {
		return policy.AdaptiveWaiting
	}
	return relay.policy.State()
}

// Attachments returns the published attachment keys in stable order without
// building a full snapshot. Used by refresh loops that only need the key set.
func (relay *Relay) Attachments() []flow.AttachmentKey {
	if relay == nil || relay.policy == nil {
		return nil
	}
	return relay.policy.Attachments()
}

func (relay *Relay) StatusSnapshot() (flow.StatusSnapshot, policy.StatusSnapshot) {
	if relay == nil || relay.machine == nil || relay.policy == nil {
		return flow.StatusSnapshot{LifecycleState: flow.Reset}, policy.StatusSnapshot{State: policy.AdaptiveWaiting}
	}
	return relay.machine.StatusSnapshot(), relay.policy.StatusSnapshot()
}

func (relay *Relay) Handle(event RelayEvent) ([]RelayAction, error) {
	if relay == nil || relay.machine == nil || relay.policy == nil {
		return nil, ErrInvalidRelay
	}
	var actions []RelayAction
	var err error
	switch event.Kind {
	case RelayStart:
		if relay.started {
			return nil, ErrInvalidRelayEvent
		}
		relay.started = true
		err = relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowStarted}, &actions)
	case RelayApplyFlowActions:
		relay.retryAfter = 0
		err = relay.consumeFlowActions(event.FlowActions, &actions)
		relay.retryAfter = 0
	case RelayRemoteMessage:
		err = relay.handleRemoteMessage(event.Message, event.Attachment, &actions)
	case RelayTargetReadResult:
		err = relay.handleTargetRead(event, &actions)
	case RelayTargetWriteResult:
		err = relay.handleTargetWrite(event, &actions)
	case RelayTargetCloseWriteResult:
		err = relay.handleTargetCloseWrite(event, &actions)
	case RelaySendAdmitted:
		err = relay.handleSendAdmitted(event)
	case RelaySendResult:
		err = relay.handleSendResult(event, &actions)
	case RelayRetryDeadline:
		err = relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowRetryDeadline, Generation: event.Generation}, &actions)
	case RelayNoProgressDeadline:
		err = relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowNoProgressDeadline, Generation: event.Generation}, &actions)
	case RelayRecoveryDeadline:
		err = relay.applyLifecycleEvent(flow.LifecycleRecoveryDeadline, event.Generation, 0, &actions)
	case RelayClosingDeadline:
		err = relay.applyLifecycleEvent(flow.LifecycleClosingDeadline, event.Generation, 0, &actions)
	case RelayResetDeadline:
		err = relay.applyLifecycleEvent(flow.LifecycleResetDeadline, event.Generation, 0, &actions)
	case RelayResetRequested:
		err = relay.applyLifecycleEvent(flow.LifecycleResetRequested, 0, event.ResetReason, &actions)
	case RelayObserveDataQuality:
		err = ignoreWithdrawnAttachmentTelemetry(relay.observeDataQuality(event))
	case RelayObserveProbeQuality:
		err = ignoreWithdrawnAttachmentTelemetry(relay.observeProbeQuality(event))
	case RelaySetAttachmentLoad:
		err = ignoreWithdrawnAttachmentTelemetry(relay.setAttachmentLoad(event))
	case RelaySetStallPenalty:
		err = ignoreWithdrawnAttachmentTelemetry(relay.setStallPenalty(event))
	case RelaySetSessionQuality:
		err = ignoreWithdrawnAttachmentTelemetry(relay.setSessionQuality(event))
	case RelaySetSessionQualities:
		err = relay.policy.SetQualitySnapshots(event.SessionQualities)
	default:
		return nil, ErrInvalidRelayEvent
	}

	if errors.Is(err, ErrRelayAttemptLimit) && !relay.converging() {
		resetErr := relay.applyLifecycleEvent(flow.LifecycleResetRequested, 0, protocol.ResetResourceLimit, &actions)
		err = errors.Join(err, resetErr)
	}
	if relay.terminal() {
		clear(relay.pendingSends)
		relay.clearControlState()
		clear(relay.attemptHistory)
		relay.resetGeneration = 0
		relay.resetPending = 0
	}
	readErr := relay.scheduleRead(&actions)
	return actions, errors.Join(err, readErr)
}

func ignoreWithdrawnAttachmentTelemetry(err error) error {
	if errors.Is(err, policy.ErrUnknownAttachment) {
		return nil
	}
	return err
}

func (relay *Relay) handleRemoteMessage(message protocol.Message, attachment flow.AttachmentKey, actions *[]RelayAction) error {
	if message == nil || !relay.published(attachment) {
		return ErrInvalidRelayEvent
	}
	var event flow.FlowEvent
	switch typed := message.(type) {
	case protocol.Data:
		if typed.FlowID != relay.flowID {
			return ErrInvalidRelayEvent
		}
		event = flow.FlowEvent{Kind: flow.FlowRemoteData, Offset: typed.Offset, Data: typed.Bytes}
	case protocol.ACK:
		if typed.FlowID != relay.flowID {
			return ErrInvalidRelayEvent
		}
		ranges := make([]flow.ByteRange, len(typed.Ranges))
		for index, ackRange := range typed.Ranges {
			ranges[index] = flow.ByteRange{Start: ackRange.Start, End: ackRange.End}
		}
		before := relay.machine.TxAcknowledgedOffset()
		err := relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowRemoteACK, Offset: typed.NextOffset, Ranges: ranges}, actions)
		after := relay.machine.TxAcknowledgedOffset()
		if after > before {
			relay.policy.RecordCumulativeProgress(after - before)
			relay.pruneAttemptHistory(after, false)
		}
		return err
	case protocol.FIN:
		if typed.FlowID != relay.flowID {
			return ErrInvalidRelayEvent
		}
		event = flow.FlowEvent{Kind: flow.FlowRemoteFIN, FinalOffset: typed.FinalOffset}
	case protocol.FINACK:
		if typed.FlowID != relay.flowID {
			return ErrInvalidRelayEvent
		}
		err := relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowRemoteFINACK, FinalOffset: typed.FinalOffset}, actions)
		if relay.machine.TxState() == flow.TxComplete {
			relay.pruneAttemptHistory(typed.FinalOffset, true)
		}
		return err
	case protocol.Reset:
		if typed.FlowID != relay.flowID {
			return ErrInvalidRelayEvent
		}
		event = flow.FlowEvent{Kind: flow.FlowLifecycle, Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleRemoteReset}}
	default:
		return ErrInvalidRelayEvent
	}
	return relay.applyFlowEvent(event, actions)
}

func (relay *Relay) handleTargetRead(event RelayEvent, actions *[]RelayAction) error {
	if event.Generation == 0 || event.Generation != relay.readGeneration {
		return nil
	}
	limit := relay.readLimit
	relay.readGeneration = 0
	relay.readLimit = 0
	if relay.closeIssued || relay.terminal() {
		return nil
	}
	if len(event.Data) > limit || len(event.Data) == 0 && event.Err == nil {
		resetErr := relay.applyLifecycleEvent(flow.LifecycleResetRequested, 0, protocol.ResetLocalIOFailure, actions)
		return errors.Join(ErrInvalidTargetRead, resetErr)
	}

	var result error
	if len(event.Data) != 0 {
		result = relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowLocalData, Data: event.Data}, actions)
	}
	if event.Err == nil {
		return result
	}
	if errors.Is(event.Err, io.EOF) {
		return errors.Join(result, relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowLocalEOF}, actions))
	}
	return errors.Join(result, relay.applyLifecycleEvent(flow.LifecycleResetRequested, 0, protocol.ResetLocalIOFailure, actions))
}

func (relay *Relay) handleTargetWrite(event RelayEvent, actions *[]RelayAction) error {
	if event.Generation == 0 || event.Generation != relay.writeGeneration {
		return nil
	}
	relay.writeGeneration = 0
	if relay.closeIssued || relay.terminal() {
		return nil
	}
	flowErr := relay.applyFlowEvent(flow.FlowEvent{
		Kind: flow.FlowWriteResult, Generation: event.Generation, N: event.N, Err: event.Err,
	}, actions)
	if errors.Is(flowErr, flow.ErrInvalidWriteResult) {
		return errors.Join(ErrInvalidTargetWrite, flowErr)
	}
	return flowErr
}

func (relay *Relay) handleTargetCloseWrite(event RelayEvent, actions *[]RelayAction) error {
	if event.Generation == 0 || event.Generation != relay.closeWriteGeneration {
		return nil
	}
	relay.closeWriteGeneration = 0
	if relay.closeIssued || relay.terminal() {
		return nil
	}
	return relay.applyFlowEvent(flow.FlowEvent{
		Kind: flow.FlowCloseWriteResult, Generation: event.Generation, Err: event.Err,
	}, actions)
}

func (relay *Relay) handleSendResult(event RelayEvent, actions *[]RelayAction) error {
	pending, ok := relay.pendingSends[event.Generation]
	if !ok || event.Generation == 0 {
		return nil
	}
	outcome := event.AttemptOutcome
	if outcome < flow.AttemptSucceeded || outcome > flow.AttemptTimedOut {
		return ErrInvalidRelayEvent
	}
	if err := relay.resolveSendAdmission(event.Generation, nil); err != nil {
		return err
	}
	delete(relay.pendingSends, event.Generation)
	var result error
	switch pending.kind {
	case relayPendingAttempt:
		relay.recordAttemptResult(pending, outcome, event.WriteCompletedAt)
		result = relay.applyFlowEvent(flow.FlowEvent{
			Kind: flow.FlowAttemptResult, ItemID: pending.itemID,
			AttemptGeneration: pending.attemptGeneration,
			Attachment:        pending.attachment, AttemptOutcome: outcome,
		}, actions)
	case relayPendingControl:
		delete(relay.controlPending, pending.attachment)
	case relayPendingReset:
		if pending.resetGeneration != relay.resetGeneration || relay.resetPending == 0 {
			return nil
		}
		relay.resetPending--
		if outcome == flow.AttemptSucceeded {
			relay.resetPending = 0
			result = relay.applyLifecycleEvent(flow.LifecycleResetSendCompleted, pending.resetGeneration, 0, actions)
			break
		}
		if relay.resetPending == 0 {
			result = relay.applyLifecycleEvent(flow.LifecycleResetSendFailed, pending.resetGeneration, 0, actions)
		}
	}
	if relay.terminal() || relay.converging() {
		return result
	}
	return errors.Join(result, relay.drainDeferredControls(actions))
}

func (relay *Relay) handleSendAdmitted(event RelayEvent) error {
	return relay.resolveSendAdmission(event.Generation, &event.Quality)
}

func (relay *Relay) resolveSendAdmission(generation uint64, quality *policy.QualitySnapshot) error {
	pending, ok := relay.pendingSends[generation]
	if !ok || generation == 0 || pending.kind != relayPendingAttempt || pending.dataBytes == 0 || pending.admissionResolved {
		return nil
	}
	if err := relay.policy.ResolveAssigned(pending.attachment, pending.dataBytes); err != nil {
		return ignoreWithdrawnAttachmentTelemetry(err)
	}
	pending.admissionResolved = true
	relay.pendingSends[generation] = pending
	if quality == nil {
		return nil
	}
	return ignoreWithdrawnAttachmentTelemetry(relay.policy.SetQualitySnapshot(pending.attachment, *quality))
}

func (relay *Relay) applyLifecycleEvent(kind flow.LifecycleEventKind, generation uint64, reason protocol.ResetReason, actions *[]RelayAction) error {
	return relay.applyFlowEvent(flow.FlowEvent{
		Kind:      flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{Kind: kind, Generation: generation, Reason: reason},
	}, actions)
}

func (relay *Relay) applyFlowEvent(event flow.FlowEvent, actions *[]RelayAction) error {
	relay.retryAfter = 0
	flowActions, flowErr := relay.machine.Handle(event)
	consumeErr := relay.consumeFlowActions(flowActions, actions)
	relay.retryAfter = 0
	return errors.Join(flowErr, consumeErr)
}

func (relay *Relay) consumeFlowActions(flowActions []flow.FlowAction, actions *[]RelayAction) error {
	var result error
	for _, action := range flowActions {
		var err error
		switch action.Kind {
		case flow.FlowActionTxItem:
			err = relay.placeTxItem(action.TxItem, actions)
		case flow.FlowActionTxAttempt:
			err = relay.emitAttempt(action.TxAttempt, actions)
		case flow.FlowActionTxAcknowledged:
			err = relay.consumeAcknowledgedRanges(action.ACKRanges, actions)
		case flow.FlowActionRx:
			err = relay.consumeRxAction(action.Rx, actions)
		case flow.FlowActionLifecycle:
			err = relay.consumeLifecycleAction(action.Lifecycle, actions)
		case flow.FlowActionArmRetryDeadline:
			*actions = append(*actions, RelayAction{
				Kind: RelayActionArmRetryDeadline, Generation: action.Generation, After: relay.currentRetryAfter(),
			})
		case flow.FlowActionCancelRetryDeadline:
			*actions = append(*actions, RelayAction{Kind: RelayActionCancelRetryDeadline, Generation: action.Generation})
		case flow.FlowActionArmNoProgressDeadline:
			*actions = append(*actions, RelayAction{Kind: RelayActionArmNoProgressDeadline, Generation: action.Generation})
		case flow.FlowActionCancelNoProgressDeadline:
			*actions = append(*actions, RelayAction{Kind: RelayActionCancelNoProgressDeadline, Generation: action.Generation})
		default:
			err = ErrInvalidRelayEvent
		}
		result = errors.Join(result, err)
	}
	relay.policy.SetPending(relay.machine.TxReplayBytes() != 0 || relay.machine.TxState() == flow.TxFinPending)
	return result
}

func (relay *Relay) placeTxItem(item flow.TxItem, actions *[]RelayAction) error {
	history, previousAttempts, err := relay.beginAttemptGeneration(item)
	if err != nil {
		return err
	}
	request := policy.PlacementRequest{
		ItemID: item.ItemID, AttemptGeneration: item.AttemptGeneration, Bytes: uint64(item.DataLen()),
	}
	var placements []policy.Placement
	switch item.AttemptGeneration {
	case 0:
		return ErrInvalidRelayEvent
	case 1:
		placements = relay.policy.PlaceNew(request)
	case 2:
		if relay.policy.Snapshot().Config.Mode == protocol.DeliveryAdaptive {
			request.Attempted = previousAttempts
			placements = relay.policy.GapDue(request)
		} else {
			placements = relay.policy.RetryDue(request)
		}
	default:
		placements = relay.policy.RetryDue(request)
	}
	if len(placements) > flow.MaxAttachments {
		return ErrInvalidRelay
	}
	var result error
	for _, placement := range placements {
		retryAfter := placement.RetryAfter
		if relay.retryAfter == 0 || retryAfter < relay.retryAfter {
			relay.retryAfter = retryAfter
		}
		if history.retryAfter == 0 || retryAfter < history.retryAfter {
			history.retryAfter = retryAfter
		}
		if attemptedAttachment(history.attempted, placement.Attachment) {
			continue
		}
		// A full send ledger is expected when the transport is slower than the
		// target. Leave the item in Flow's replay window and let its retry
		// deadline retry placement after a send completion frees a slot.
		if len(relay.pendingSends) >= MaxRelayPendingSends {
			continue
		}
		attemptActions, err := relay.machine.Handle(flow.FlowEvent{
			Kind: flow.FlowStartAttempt, ItemID: item.ItemID,
			AttemptGeneration: item.AttemptGeneration, Attachment: placement.Attachment,
		})
		consumeErr := relay.consumeFlowActions(attemptActions, actions)
		result = errors.Join(result, err, consumeErr)
		if err == nil && consumeErr == nil {
			history.attempted = append(history.attempted, placement.Attachment)
		}
	}
	return result
}

func (relay *Relay) emitAttempt(attempt flow.TxAttempt, actions *[]RelayAction) error {
	item := attempt.Item
	switch item.Kind {
	case flow.TxItemData:
		encoded, ok := item.EncodedDataFrame()
		if !ok {
			var err error
			encoded, err = protocol.EncodeData(relay.flowID, item.Offset, item)
			if err != nil {
				return err
			}
		}
		relay.recordAttemptStart(item, attempt.Attachment)
		return relay.emitEncodedData(encoded, item.DataLen(), attempt.Attachment, relayPendingSend{
			kind: relayPendingAttempt, attachment: attempt.Attachment,
			itemID: item.ItemID, attemptGeneration: item.AttemptGeneration,
		}, actions)
	case flow.TxItemFIN:
		return relay.emitSend(protocol.FIN{FlowID: relay.flowID, FinalOffset: item.FinalOffset}, transport.FrameControl, attempt.Attachment, relayPendingSend{
			kind: relayPendingAttempt, attachment: attempt.Attachment,
			itemID: item.ItemID, attemptGeneration: item.AttemptGeneration,
		}, actions)
	default:
		return ErrInvalidRelayEvent
	}
}

func (relay *Relay) consumeRxAction(action flow.RxAction, actions *[]RelayAction) error {
	switch action.Kind {
	case flow.RxWriteLocal:
		if relay.writeGeneration != 0 || relay.closeWriteGeneration != 0 || action.Generation == 0 {
			return ErrInvalidRelayEvent
		}
		relay.writeGeneration = action.Generation
		*actions = append(*actions, RelayAction{
			Kind: RelayActionWriteTarget, Generation: action.Generation,
			Offset: action.Offset, data: action.CopyData(),
		})
	case flow.RxCloseWriteLocal:
		if relay.writeGeneration != 0 || relay.closeWriteGeneration != 0 || action.Generation == 0 {
			return ErrInvalidRelayEvent
		}
		relay.closeWriteGeneration = action.Generation
		*actions = append(*actions, RelayAction{
			Kind: RelayActionCloseWriteTarget, Generation: action.Generation, FinalOffset: action.FinalOffset,
		})
	case flow.RxSendACK:
		ranges := make([]protocol.ACKRange, len(action.ACK.Ranges))
		for index, ackRange := range action.ACK.Ranges {
			ranges[index] = protocol.ACKRange{Start: ackRange.Start, End: ackRange.End}
		}
		return relay.emitControl(protocol.ACK{FlowID: relay.flowID, NextOffset: action.ACK.NextOffset, Ranges: ranges}, actions)
	case flow.RxSendFINACK:
		return relay.emitControl(protocol.FINACK{FlowID: relay.flowID, FinalOffset: action.FinalOffset}, actions)
	default:
		return ErrInvalidRelayEvent
	}
	return nil
}

func (relay *Relay) consumeLifecycleAction(action flow.LifecycleAction, actions *[]RelayAction) error {
	switch action.Kind {
	case flow.LifecycleActionPublishAttachment:
		if err := relay.policy.AddAttachment(action.Attachment); err != nil {
			return err
		}
		generation, err := relay.allocateGeneration()
		if err != nil {
			return err
		}
		*actions = append(*actions, RelayAction{
			Kind: RelayActionAttachmentPublished, Generation: generation, Attachment: action.Attachment,
		})
	case flow.LifecycleActionWithdrawAttachment:
		relay.policy.RemoveAttachment(action.Attachment)
		delete(relay.controlPending, action.Attachment)
		delete(relay.deferredControls, action.Attachment)
		generation, err := relay.allocateGeneration()
		if err != nil {
			return err
		}
		*actions = append(*actions, RelayAction{
			Kind: RelayActionAttachmentWithdrawn, Generation: generation, Attachment: action.Attachment,
		})
	case flow.LifecycleActionSendAcknowledgementSnapshot:
		// Flow emits the detached ACK/FIN_ACK actions immediately after this marker.
	case flow.LifecycleActionPauseLocalRead:
		relay.readPaused = true
	case flow.LifecycleActionResumeLocalRead:
		relay.readPaused = false
	case flow.LifecycleActionCloseLocal:
		return relay.closeTarget(actions)
	case flow.LifecycleActionArmRecoveryDeadline:
		*actions = append(*actions, RelayAction{Kind: RelayActionArmRecoveryDeadline, Generation: action.Generation})
	case flow.LifecycleActionCancelRecoveryDeadline:
		*actions = append(*actions, RelayAction{Kind: RelayActionCancelRecoveryDeadline, Generation: action.Generation})
	case flow.LifecycleActionArmClosingDeadline:
		*actions = append(*actions, RelayAction{Kind: RelayActionArmClosingDeadline, Generation: action.Generation})
	case flow.LifecycleActionCancelClosingDeadline:
		*actions = append(*actions, RelayAction{Kind: RelayActionCancelClosingDeadline, Generation: action.Generation})
	case flow.LifecycleActionSendReset:
		return relay.emitReset(action.Generation, action.Reason, actions)
	case flow.LifecycleActionArmResetDeadline:
		*actions = append(*actions, RelayAction{Kind: RelayActionArmResetDeadline, Generation: action.Generation})
	case flow.LifecycleActionCancelResetDeadline:
		*actions = append(*actions, RelayAction{Kind: RelayActionCancelResetDeadline, Generation: action.Generation})
	case flow.LifecycleActionSendJoinSuccess, flow.LifecycleActionSendJoinFailure:
		return ErrInvalidRelayEvent
	default:
		return ErrInvalidRelayEvent
	}
	return nil
}

func (relay *Relay) emitControl(message protocol.Message, actions *[]RelayAction) error {
	placements := relay.policy.ControlPlacements()
	var result error
	for _, placement := range placements {
		result = errors.Join(result, relay.emitControlOnAttachment(message, placement.Attachment, actions))
	}
	return result
}

func (relay *Relay) emitControlOnAttachment(message protocol.Message, attachment flow.AttachmentKey, actions *[]RelayAction) error {
	if _, pending := relay.controlPending[attachment]; pending {
		relay.deferredControls[attachment] = cloneMessage(message)
		return nil
	}
	if len(relay.pendingSends) >= MaxRelayPendingSends {
		relay.deferredControls[attachment] = cloneMessage(message)
		return nil
	}
	err := relay.emitSend(message, transport.FrameControl, attachment, relayPendingSend{
		kind: relayPendingControl, attachment: attachment,
	}, actions)
	if err == nil {
		relay.controlPending[attachment] = struct{}{}
	}
	return err
}

func (relay *Relay) emitReset(generation uint64, reason protocol.ResetReason, actions *[]RelayAction) error {
	if generation == 0 {
		return ErrInvalidRelayEvent
	}
	// Reset supersedes all data/control bookkeeping. Executor completions for
	// already returned actions are subsequently stale and release no Flow state.
	flushErr := relay.flushDeferredControls(actions)
	clear(relay.pendingSends)
	relay.clearControlState()
	clear(relay.attemptHistory)
	relay.resetGeneration = generation
	relay.resetPending = 0
	placements := relay.policy.ControlPlacements()
	result := flushErr
	for _, placement := range placements {
		err := relay.emitSend(protocol.Reset{FlowID: relay.flowID, Reason: reason}, transport.FrameControl, placement.Attachment, relayPendingSend{
			kind: relayPendingReset, attachment: placement.Attachment, resetGeneration: generation,
		}, actions)
		if err == nil {
			relay.resetPending++
		}
		result = errors.Join(result, err)
	}
	return result
}

func (relay *Relay) emitSend(message protocol.Message, class transport.FrameClass, attachment flow.AttachmentKey, pending relayPendingSend, actions *[]RelayAction) error {
	if len(relay.pendingSends) >= MaxRelayPendingSends {
		return ErrRelaySendLimit
	}
	generation, err := relay.allocateGeneration()
	if err != nil {
		return err
	}
	relay.pendingSends[generation] = pending
	*actions = append(*actions, RelayAction{
		Kind: RelayActionSendMessage, Generation: generation, Attachment: attachment,
		Message: cloneMessage(message), Class: class,
		ItemID: pending.itemID, AttemptGeneration: pending.attemptGeneration,
	})
	return nil
}

func (relay *Relay) emitEncodedData(encoded []byte, dataBytes int, attachment flow.AttachmentKey, pending relayPendingSend, actions *[]RelayAction) error {
	if len(relay.pendingSends) >= MaxRelayPendingSends {
		return ErrRelaySendLimit
	}
	if len(encoded) == 0 || dataBytes < 1 {
		return ErrInvalidRelayEvent
	}
	generation, err := relay.allocateGeneration()
	if err != nil {
		return err
	}
	pending.dataBytes = uint64(dataBytes)
	relay.pendingSends[generation] = pending
	*actions = append(*actions, RelayAction{
		Kind: RelayActionSendMessage, Generation: generation, Attachment: attachment,
		Encoded: encoded, Class: transport.FrameData, SendDataBytes: dataBytes,
		ItemID: pending.itemID, AttemptGeneration: pending.attemptGeneration,
	})
	return nil
}

func (relay *Relay) closeTarget(actions *[]RelayAction) error {
	if relay.closeIssued {
		return nil
	}
	generation, err := relay.allocateGeneration()
	if err != nil {
		return err
	}
	relay.closeIssued = true
	relay.readPaused = true
	relay.readGeneration = 0
	relay.readLimit = 0
	relay.writeGeneration = 0
	relay.closeWriteGeneration = 0
	flushErr := relay.flushDeferredControls(actions)
	clear(relay.pendingSends)
	relay.clearControlState()
	clear(relay.attemptHistory)
	*actions = append(*actions, RelayAction{Kind: RelayActionCloseTarget, Generation: generation})
	return flushErr
}

func (relay *Relay) flushDeferredControls(actions *[]RelayAction) error {
	var result error
	for _, placement := range relay.policy.ControlPlacements() {
		message, ok := relay.deferredControls[placement.Attachment]
		if !ok {
			continue
		}
		generation, err := relay.allocateGeneration()
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		*actions = append(*actions, RelayAction{
			Kind: RelayActionSendMessage, Generation: generation, Attachment: placement.Attachment,
			Message: cloneMessage(message), Class: transport.FrameControl,
		})
	}
	return result
}

func (relay *Relay) drainDeferredControls(actions *[]RelayAction) error {
	var result error
	for _, placement := range relay.policy.ControlPlacements() {
		message, ok := relay.deferredControls[placement.Attachment]
		if !ok {
			continue
		}
		if len(relay.pendingSends) >= MaxRelayPendingSends {
			break
		}
		if err := relay.emitControlOnAttachment(message, placement.Attachment, actions); err != nil {
			result = errors.Join(result, err)
			continue
		}
		delete(relay.deferredControls, placement.Attachment)
	}
	return result
}

func (relay *Relay) clearControlState() {
	clear(relay.controlPending)
	clear(relay.deferredControls)
}

func (relay *Relay) scheduleRead(actions *[]RelayAction) error {
	if !relay.started || relay.readPaused || relay.closeIssued || relay.readGeneration != 0 {
		return nil
	}
	if relay.machine.LifecycleState() != flow.Relaying || relay.machine.TxState() != flow.TxOpen || relay.machine.TxAvailableWindow() == 0 {
		return nil
	}
	limit := int(min(relay.machine.TxAvailableWindow(), uint64(MaxTargetReadBytes)))
	if limit < 1 {
		return nil
	}
	generation, err := relay.allocateGeneration()
	if err != nil {
		return err
	}
	relay.readGeneration = generation
	relay.readLimit = limit
	*actions = append(*actions, RelayAction{Kind: RelayActionReadTarget, Generation: generation, MaxBytes: limit})
	return nil
}

func (relay *Relay) allocateGeneration() (uint64, error) {
	if relay.nextGeneration == math.MaxUint64 {
		return 0, ErrRelayGenerationExhaust
	}
	relay.nextGeneration++
	return relay.nextGeneration, nil
}

func (relay *Relay) published(attachment flow.AttachmentKey) bool {
	return relay.machine.IsAttachmentPublished(attachment)
}

func (relay *Relay) observeDataQuality(event RelayEvent) error {
	quality, err := relay.policy.Quality(event.Attachment)
	if err != nil {
		return err
	}
	return quality.ObserveData(event.RTT, event.Bytes, event.Interval)
}

func (relay *Relay) observeProbeQuality(event RelayEvent) error {
	quality, err := relay.policy.Quality(event.Attachment)
	if err != nil {
		return err
	}
	return quality.ObserveProbe(event.RTT)
}

func (relay *Relay) setAttachmentLoad(event RelayEvent) error {
	quality, err := relay.policy.Quality(event.Attachment)
	if err != nil {
		return err
	}
	quality.SetLoad(event.QueuedBytes, event.InFlightBytes)
	return nil
}

func (relay *Relay) setStallPenalty(event RelayEvent) error {
	quality, err := relay.policy.Quality(event.Attachment)
	if err != nil {
		return err
	}
	return quality.SetStallPenalty(event.StallPenalty)
}

func (relay *Relay) setSessionQuality(event RelayEvent) error {
	return relay.policy.SetQualitySnapshot(event.Attachment, event.Quality)
}

func (relay *Relay) currentRetryAfter() time.Duration {
	if relay.retryAfter > 0 {
		return relay.retryAfter
	}
	var after time.Duration
	acknowledged := relay.machine.TxAcknowledgedOffset()
	for _, history := range relay.attemptHistory {
		if history.retryAfter > 0 && (history.fin || history.end > acknowledged) && (after == 0 || history.retryAfter < after) {
			after = history.retryAfter
		}
	}
	if after > 0 {
		return after
	}
	for _, attachment := range relay.policy.Snapshot().Attachments {
		retry := attachment.Quality.RetryEstimate
		if retry > 0 && (after == 0 || retry < after) {
			after = retry
		}
	}
	if after == 0 {
		return policy.InitialRetryEstimate
	}
	return after
}

func (relay *Relay) beginAttemptGeneration(item flow.TxItem) (*relayAttemptHistory, []flow.AttachmentKey, error) {
	if item.ItemID == 0 || item.AttemptGeneration == 0 {
		return nil, nil, ErrInvalidRelayEvent
	}
	history := relay.attemptHistory[item.ItemID]
	if history == nil {
		if len(relay.attemptHistory) >= MaxRelayAttemptItems {
			return nil, nil, ErrRelayAttemptLimit
		}
		history = &relayAttemptHistory{}
		relay.attemptHistory[item.ItemID] = history
	}
	if item.AttemptGeneration < history.generation {
		return nil, nil, ErrInvalidRelayEvent
	}
	previous := append([]flow.AttachmentKey(nil), history.attempted...)
	if item.AttemptGeneration != history.generation {
		history.generation = item.AttemptGeneration
		history.retryAfter = 0
		history.attempted = nil
		history.attempts = nil
		history.invalidated = item.AttemptGeneration > 1
	}
	if item.Kind == flow.TxItemFIN {
		history.end = item.FinalOffset
		history.fin = true
	} else {
		history.end = item.Offset + uint64(item.DataLen())
		history.fin = false
	}
	return history, previous, nil
}

func (relay *Relay) recordAttemptStart(item flow.TxItem, attachment flow.AttachmentKey) {
	if item.Kind != flow.TxItemData {
		return
	}
	history := relay.attemptHistory[item.ItemID]
	if history == nil || history.generation != item.AttemptGeneration || history.invalidated || len(history.attempts) >= flow.MaxAttachments {
		return
	}
	attempt := relayAttempt{
		attachment: attachment,
		start:      item.Offset,
		end:        item.Offset + uint64(item.DataLen()),
	}
	for index := range history.attempts {
		current := &history.attempts[index]
		if current.start < attempt.end && attempt.start < current.end {
			current.invalid = true
			attempt.invalid = true
		}
	}
	history.attempts = append(history.attempts, attempt)
}

func (relay *Relay) recordAttemptResult(pending relayPendingSend, outcome flow.AttemptOutcome, completedAt time.Time) {
	history := relay.attemptHistory[pending.itemID]
	if history == nil || history.generation != pending.attemptGeneration {
		return
	}
	for index := range history.attempts {
		attempt := &history.attempts[index]
		if attempt.attachment != pending.attachment {
			continue
		}
		if outcome != flow.AttemptSucceeded {
			attempt.invalid = true
			return
		}
		if completedAt.IsZero() {
			completedAt = relay.now()
		}
		attempt.writeSucceeded = true
		attempt.writeCompletedAt = completedAt
		return
	}
}

func (relay *Relay) consumeAcknowledgedRanges(ranges []flow.ByteRange, actions *[]RelayAction) error {
	if len(ranges) == 0 {
		return nil
	}
	itemIDs := make([]uint64, 0, len(relay.attemptHistory))
	for itemID := range relay.attemptHistory {
		itemIDs = append(itemIDs, itemID)
	}
	sort.Slice(itemIDs, func(left, right int) bool { return itemIDs[left] < itemIDs[right] })
	acknowledgedAt := relay.now()
	for _, itemID := range itemIDs {
		history := relay.attemptHistory[itemID]
		for index := range history.attempts {
			attempt := &history.attempts[index]
			if attempt.invalid || !attempt.writeSucceeded || attempt.writeCompletedAt.IsZero() {
				continue
			}
			for _, acknowledged := range ranges {
				start := acknowledged.Start
				if start < attempt.start {
					start = attempt.start
				}
				end := acknowledged.End
				if end > attempt.end {
					end = attempt.end
				}
				for _, credited := range attempt.credited {
					if credited.Start <= start && start < credited.End {
						start = credited.End
					}
				}
				if start >= end {
					continue
				}
				credited := flow.ByteRange{Start: start, End: end}
				attempt.credited = append(attempt.credited, credited)
				*actions = append(*actions, RelayAction{
					Kind:             RelayActionDataCredit,
					Attachment:       attempt.attachment,
					DataCreditBytes:  credited.End - credited.Start,
					WriteCompletedAt: attempt.writeCompletedAt,
					AcknowledgedAt:   acknowledgedAt,
				})
			}
		}
	}
	return nil
}

func (relay *Relay) pruneAttemptHistory(acknowledged uint64, includeFIN bool) {
	for itemID, history := range relay.attemptHistory {
		if history.fin {
			if includeFIN && history.end == acknowledged {
				delete(relay.attemptHistory, itemID)
			}
			continue
		}
		if history.end <= acknowledged {
			delete(relay.attemptHistory, itemID)
		}
	}
}

func attemptedAttachment(attempted []flow.AttachmentKey, attachment flow.AttachmentKey) bool {
	for _, candidate := range attempted {
		if candidate == attachment {
			return true
		}
	}
	return false
}

func (relay *Relay) converging() bool {
	state := relay.machine.LifecycleState()
	return state == flow.Closing || state == flow.Closed || state == flow.Resetting || state == flow.Reset
}

func (relay *Relay) terminal() bool {
	state := relay.machine.LifecycleState()
	return state == flow.Closed || state == flow.Reset
}

func transportClass(message protocol.Message) transport.FrameClass {
	if _, ok := message.(protocol.Data); ok {
		return transport.FrameData
	}
	return transport.FrameControl
}

func cloneMessage(message protocol.Message) protocol.Message {
	switch typed := message.(type) {
	case protocol.Data:
		typed.Bytes = bytes.Clone(typed.Bytes)
		return typed
	case protocol.ACK:
		typed.Ranges = append([]protocol.ACKRange(nil), typed.Ranges...)
		return typed
	default:
		return message
	}
}
