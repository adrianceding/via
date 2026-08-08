package client

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
	// MaxApplicationRelayPendingSends bounds per-flow ledger entries between
	// send action emission and executor result feedback. Reaching this
	// boundary is transport backpressure, not a flow failure.
	MaxApplicationRelayPendingSends = flow.MaxAttachments * 4
	MaxApplicationRelayAttemptItems = flow.MaxReplaySegments + 1
	MaxApplicationRelayFlowActions  = 64
	MaxApplicationRelayReservations = flow.MaxAttachments
	MaxApplicationReadBytes         = protocol.MaxDataLength
	MaxApplicationWriteBytes        = int(flow.ReceiveWindowSize)
)

var (
	ErrInvalidApplicationRelay           = errors.New("client: invalid application relay")
	ErrInvalidApplicationRelayEvent      = errors.New("client: invalid application relay event")
	ErrApplicationRelaySendLimit         = errors.New("client: application relay send limit exceeded")
	ErrApplicationRelayActionLimit       = errors.New("client: application relay flow action limit exceeded")
	ErrApplicationRelayAttachment        = errors.New("client: application relay attachment rejected")
	ErrInvalidApplicationRead            = errors.New("client: invalid application read result")
	ErrInvalidApplicationWrite           = errors.New("client: invalid application write result")
	ErrApplicationRelayAttemptLimit      = errors.New("client: application relay attempt history limit exceeded")
	ErrApplicationRelayGenerationExhaust = errors.New("client: application relay generation exhausted")
)

type ApplicationRelayEventKind uint8

const (
	ApplicationRelayStart ApplicationRelayEventKind = iota + 1
	ApplicationRelayApplyFlowActions
	ApplicationRelayReserveAttachment
	ApplicationRelayPublishAttachment
	ApplicationRelayReleaseAttachment
	ApplicationRelaySessionClosed
	ApplicationRelayEnableApplication
	ApplicationRelayRemoteMessage
	ApplicationRelayReadResult
	ApplicationRelayWriteResult
	ApplicationRelayCloseWriteResult
	ApplicationRelaySendAdmitted
	ApplicationRelaySendResult
	ApplicationRelayRetryDeadline
	ApplicationRelayNoProgressDeadline
	ApplicationRelayRecoveryDeadline
	ApplicationRelayClosingDeadline
	ApplicationRelayResetDeadline
	ApplicationRelayResetRequested
	ApplicationRelayObserveDataQuality
	ApplicationRelayObserveProbeQuality
	ApplicationRelaySetAttachmentLoad
	ApplicationRelaySetStallPenalty
	ApplicationRelaySetSessionQuality
	ApplicationRelaySetSessionQualities
)

// ApplicationRelayEvent is one serial input to the application relay state owner.
// FlowActions carries actions already emitted by the JOIN coordinator for the same flow.
type ApplicationRelayEvent struct {
	Kind             ApplicationRelayEventKind
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
	ObservedAt       time.Time
	Quality          policy.QualitySnapshot
	SessionQualities map[flow.AttachmentKey]policy.QualitySnapshot
}

type ApplicationRelayActionKind uint8

const (
	ApplicationRelayActionRead ApplicationRelayActionKind = iota + 1
	ApplicationRelayActionWrite
	ApplicationRelayActionCloseWrite
	ApplicationRelayActionClose
	ApplicationRelayActionSendMessage
	ApplicationRelayActionArmRetryDeadline
	ApplicationRelayActionCancelRetryDeadline
	ApplicationRelayActionArmNoProgressDeadline
	ApplicationRelayActionCancelNoProgressDeadline
	ApplicationRelayActionArmRecoveryDeadline
	ApplicationRelayActionCancelRecoveryDeadline
	ApplicationRelayActionArmClosingDeadline
	ApplicationRelayActionCancelClosingDeadline
	ApplicationRelayActionArmResetDeadline
	ApplicationRelayActionCancelResetDeadline
	ApplicationRelayActionAttachmentPublished
	ApplicationRelayActionAttachmentWithdrawn
	ApplicationRelayActionDataCredit
)

// ApplicationRelayAction describes work without performing I/O. Write payloads
// are exposed only through copying methods, so executors cannot modify bytes held by the flow.
type ApplicationRelayAction struct {
	Kind              ApplicationRelayActionKind
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

func (action ApplicationRelayAction) DataLen() int { return len(action.data) }

func (action ApplicationRelayAction) AppendData(dst []byte) []byte {
	return append(dst, action.data...)
}

func (action ApplicationRelayAction) CopyData() []byte {
	return bytes.Clone(action.data)
}

type ApplicationRelaySnapshot struct {
	Flow                            flow.FlowSnapshot
	Policy                          policy.Snapshot
	Started                         bool
	ApplicationReadPaused           bool
	ApplicationReadGeneration       uint64
	ApplicationReadLimit            int
	ApplicationWriteGeneration      uint64
	ApplicationCloseWriteGeneration uint64
	ApplicationCloseIssued          bool
	ApplicationEnabled              bool
	PendingSends                    int
}

type applicationRelayPendingSendKind uint8

const (
	applicationRelayPendingAttempt applicationRelayPendingSendKind = iota + 1
	applicationRelayPendingControl
	applicationRelayPendingReset
)

type applicationRelayPendingSend struct {
	kind              applicationRelayPendingSendKind
	attachment        flow.AttachmentKey
	itemID            uint64
	attemptGeneration uint64
	resetGeneration   uint64
	dataBytes         uint64
	admissionResolved bool
}

type applicationRelayAttemptHistory struct {
	generation uint64
	end        uint64
	fin        bool
	retryAfter time.Duration
	attempted  []flow.AttachmentKey
	attempts   []applicationRelayAttempt
}

type applicationRelayAttempt struct {
	attachment             flow.AttachmentKey
	start                  uint64
	end                    uint64
	writeSucceeded         bool
	invalid                bool
	writeCompletedAt       time.Time
	credited               []flow.ByteRange
	pendingAcknowledgments []applicationRelayAcknowledgment
}

type applicationRelayAcknowledgment struct {
	rangeValue     flow.ByteRange
	acknowledgedAt time.Time
}

type applicationRelayReservation struct {
	requestGeneration   uint64
	lifecycleGeneration uint64
}

// ApplicationRelay owns application-side read and write action generations and
// maps one client flow to send-policy placement results. It performs no application
// or transport connection I/O.
//
// Event/action table:
//
//	start/published JOIN    -> start lifecycle deadline/resume application reads
//	application read data  -> FlowLocalData -> place DATA by policy
//	application read EOF   -> FlowLocalEOF  -> place FIN by policy
//	remote DATA            -> write application connection + send ACK by policy
//	application write result -> follow-up short write or ACK/RESET
//	remote FIN             -> half-close application write side + send FIN_ACK
//	remote ACK/FIN_ACK     -> release retransmission data/converge both directions
//	remote RESET/local error -> close application connection; do not answer RESET
//	stale generation result -> no state change
//
// Reads, writes, and half-closes each have at most one generation in flight. The
// send-result ledger is bounded by MaxApplicationRelayPendingSends; send policy
// and flow state separately bound placement, retransmission, reordering, and attachment counts.
type ApplicationRelay struct {
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
	applicationEnabled   bool
	deferredApplication  *ApplicationRelayAction
	pendingSends         map[uint64]applicationRelayPendingSend
	controlPending       map[flow.AttachmentKey]struct{}
	deferredControls     map[flow.AttachmentKey]protocol.Message
	reservations         map[flow.AttachmentKey]applicationRelayReservation
	resetGeneration      uint64
	resetPending         int
	attemptHistory       map[uint64]*applicationRelayAttemptHistory
	retryAfter           time.Duration
	now                  func() time.Time
}

func NewApplicationRelay(flowID protocol.FlowID, config policy.Config, machine *flow.Flow) (*ApplicationRelay, error) {
	return NewApplicationRelayWithClock(flowID, config, machine, time.Now)
}

func NewApplicationRelayWithClock(flowID protocol.FlowID, config policy.Config, machine *flow.Flow, now func() time.Time) (*ApplicationRelay, error) {
	if flowID == (protocol.FlowID{}) || machine == nil {
		return nil, ErrInvalidApplicationRelay
	}
	if now == nil {
		return nil, ErrInvalidApplicationRelay
	}
	sender, err := policy.NewWithClock(flowID, config, now)
	if err != nil {
		return nil, err
	}
	snapshot := machine.Snapshot()
	if snapshot.Lifecycle.State == flow.Closed || snapshot.Lifecycle.State == flow.Reset ||
		len(snapshot.Lifecycle.Published) != 0 || len(snapshot.Lifecycle.Provisional) != 0 {
		return nil, ErrInvalidApplicationRelay
	}
	if err := machine.BindFlowID(flowID); err != nil {
		return nil, ErrInvalidApplicationRelay
	}
	return &ApplicationRelay{
		flowID:           flowID,
		machine:          machine,
		policy:           sender,
		started:          snapshot.Started,
		readPaused:       true,
		pendingSends:     make(map[uint64]applicationRelayPendingSend, MaxApplicationRelayPendingSends),
		controlPending:   make(map[flow.AttachmentKey]struct{}, flow.MaxAttachments),
		deferredControls: make(map[flow.AttachmentKey]protocol.Message, flow.MaxAttachments),
		reservations:     make(map[flow.AttachmentKey]applicationRelayReservation, MaxApplicationRelayReservations),
		attemptHistory:   make(map[uint64]*applicationRelayAttemptHistory, MaxApplicationRelayAttemptItems),
		now:              now,
	}, nil
}

func (relay *ApplicationRelay) Snapshot() ApplicationRelaySnapshot {
	if relay == nil {
		return ApplicationRelaySnapshot{ApplicationCloseIssued: true}
	}
	return ApplicationRelaySnapshot{
		Flow:                            relay.machine.Snapshot(),
		Policy:                          relay.policy.Snapshot(),
		Started:                         relay.started,
		ApplicationReadPaused:           relay.readPaused,
		ApplicationReadGeneration:       relay.readGeneration,
		ApplicationReadLimit:            relay.readLimit,
		ApplicationWriteGeneration:      relay.writeGeneration,
		ApplicationCloseWriteGeneration: relay.closeWriteGeneration,
		ApplicationCloseIssued:          relay.closeIssued,
		ApplicationEnabled:              relay.applicationEnabled,
		PendingSends:                    len(relay.pendingSends),
	}
}

func (relay *ApplicationRelay) AdaptiveState() policy.AdaptiveState {
	if relay == nil || relay.policy == nil {
		return policy.AdaptiveWaiting
	}
	return relay.policy.State()
}

// Attachments returns the published attachment keys in stable order without
// building a full snapshot. Used by refresh loops that only need the key set.
func (relay *ApplicationRelay) Attachments() []flow.AttachmentKey {
	if relay == nil || relay.policy == nil {
		return nil
	}
	return relay.policy.Attachments()
}

func (relay *ApplicationRelay) StatusSnapshot() (flow.StatusSnapshot, policy.StatusSnapshot) {
	if relay == nil || relay.machine == nil || relay.policy == nil {
		return flow.StatusSnapshot{LifecycleState: flow.Reset}, policy.StatusSnapshot{State: policy.AdaptiveWaiting}
	}
	return relay.machine.StatusSnapshot(), relay.policy.StatusSnapshot()
}

func (relay *ApplicationRelay) Handle(event ApplicationRelayEvent) ([]ApplicationRelayAction, error) {
	if relay == nil || relay.machine == nil || relay.policy == nil {
		return nil, ErrInvalidApplicationRelay
	}
	var actions []ApplicationRelayAction
	var err error
	qualityMaySignalStall := event.Kind == ApplicationRelaySetStallPenalty ||
		event.Kind == ApplicationRelaySetSessionQuality || event.Kind == ApplicationRelaySetSessionQualities
	wasIncumbentStalled := qualityMaySignalStall && relay.incumbentStalled()
	switch event.Kind {
	case ApplicationRelayStart:
		if relay.started {
			return nil, ErrInvalidApplicationRelayEvent
		}
		relay.started = true
		err = relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowStarted}, &actions)
	case ApplicationRelayApplyFlowActions:
		if len(event.FlowActions) > MaxApplicationRelayFlowActions {
			return nil, ErrApplicationRelayActionLimit
		}
		relay.retryAfter = 0
		err = relay.consumeFlowActions(event.FlowActions, &actions)
		relay.retryAfter = 0
	case ApplicationRelayReserveAttachment:
		err = relay.reserveAttachment(event.Attachment, event.Generation)
	case ApplicationRelayPublishAttachment:
		err = relay.publishAttachment(event.Attachment, event.Generation, &actions)
	case ApplicationRelayReleaseAttachment:
		err = relay.releaseAttachment(event.Attachment, &actions)
	case ApplicationRelaySessionClosed:
		err = relay.sessionClosed(event.Generation, &actions)
	case ApplicationRelayEnableApplication:
		if relay.applicationEnabled || !relay.machine.HasPublishedAttachments() || relay.closeIssued || relay.terminal() {
			return nil, ErrInvalidApplicationRelayEvent
		}
		relay.applicationEnabled = true
		if relay.deferredApplication != nil {
			actions = append(actions, *relay.deferredApplication)
			relay.deferredApplication = nil
		}
	case ApplicationRelayRemoteMessage:
		err = relay.handleRemoteMessage(event.Message, event.Attachment, &actions)
	case ApplicationRelayReadResult:
		err = relay.handleApplicationRead(event, &actions)
	case ApplicationRelayWriteResult:
		err = relay.handleApplicationWrite(event, &actions)
	case ApplicationRelayCloseWriteResult:
		err = relay.handleApplicationCloseWrite(event, &actions)
	case ApplicationRelaySendAdmitted:
		err = relay.handleSendAdmitted(event)
	case ApplicationRelaySendResult:
		err = relay.handleSendResult(event, &actions)
	case ApplicationRelayRetryDeadline:
		err = relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowRetryDeadline, Generation: event.Generation}, &actions)
	case ApplicationRelayNoProgressDeadline:
		err = relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowNoProgressDeadline, Generation: event.Generation}, &actions)
	case ApplicationRelayRecoveryDeadline:
		err = relay.applyLifecycleEvent(flow.LifecycleRecoveryDeadline, event.Generation, 0, &actions)
	case ApplicationRelayClosingDeadline:
		err = relay.applyLifecycleEvent(flow.LifecycleClosingDeadline, event.Generation, 0, &actions)
	case ApplicationRelayResetDeadline:
		err = relay.applyLifecycleEvent(flow.LifecycleResetDeadline, event.Generation, 0, &actions)
	case ApplicationRelayResetRequested:
		err = relay.applyLifecycleEvent(flow.LifecycleResetRequested, 0, event.ResetReason, &actions)
	case ApplicationRelayObserveDataQuality:
		err = ignoreWithdrawnAttachmentTelemetry(relay.observeDataQuality(event))
	case ApplicationRelayObserveProbeQuality:
		err = ignoreWithdrawnAttachmentTelemetry(relay.observeProbeQuality(event))
	case ApplicationRelaySetAttachmentLoad:
		err = ignoreWithdrawnAttachmentTelemetry(relay.setAttachmentLoad(event))
	case ApplicationRelaySetStallPenalty:
		err = ignoreWithdrawnAttachmentTelemetry(relay.setStallPenalty(event))
	case ApplicationRelaySetSessionQuality:
		err = ignoreWithdrawnAttachmentTelemetry(relay.setSessionQuality(event))
	case ApplicationRelaySetSessionQualities:
		err = relay.policy.SetQualitySnapshots(event.SessionQualities)
	default:
		return nil, ErrInvalidApplicationRelayEvent
	}
	if err == nil && qualityMaySignalStall && !wasIncumbentStalled && relay.incumbentStalled() {
		generation := relay.machine.Snapshot().RetryGeneration
		if generation != 0 {
			err = relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowRetryDeadline, Generation: generation}, &actions)
		}
	}

	if errors.Is(err, ErrApplicationRelayAttemptLimit) && !relay.converging() {
		resetErr := relay.applyLifecycleEvent(flow.LifecycleResetRequested, 0, protocol.ResetResourceLimit, &actions)
		err = errors.Join(err, resetErr)
	}
	state := relay.machine.LifecycleState()
	if state == flow.Resetting || state == flow.Reset {
		clear(relay.reservations)
	}
	if relay.terminal() {
		relay.discardDeferredApplication()
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

func (relay *ApplicationRelay) reserveAttachment(attachment flow.AttachmentKey, requestGeneration uint64) error {
	if attachment.SessionGeneration == 0 || attachment.AttachmentGeneration == 0 || requestGeneration == 0 {
		return ErrInvalidApplicationRelayEvent
	}
	if current, ok := relay.reservations[attachment]; ok {
		if current.requestGeneration == requestGeneration {
			return nil
		}
		return ErrInvalidApplicationRelayEvent
	}
	if len(relay.reservations) >= MaxApplicationRelayReservations {
		return ErrApplicationRelayAttachment
	}

	flowActions, err := relay.machine.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind:       flow.LifecycleJoinRequested,
			Attachment: attachment,
		},
	})
	if err != nil {
		return err
	}
	var lifecycleGeneration uint64
	for _, action := range flowActions {
		if action.Kind != flow.FlowActionLifecycle || action.Lifecycle.Attachment != attachment {
			continue
		}
		switch action.Lifecycle.Kind {
		case flow.LifecycleActionSendJoinSuccess:
			lifecycleGeneration = action.Lifecycle.Generation
		case flow.LifecycleActionSendJoinFailure:
			return ErrApplicationRelayAttachment
		}
	}
	if lifecycleGeneration == 0 {
		return ErrInvalidApplicationRelayEvent
	}
	relay.reservations[attachment] = applicationRelayReservation{
		requestGeneration:   requestGeneration,
		lifecycleGeneration: lifecycleGeneration,
	}
	return nil
}

func (relay *ApplicationRelay) publishAttachment(attachment flow.AttachmentKey, requestGeneration uint64, actions *[]ApplicationRelayAction) error {
	reservation, ok := relay.reservations[attachment]
	if !ok || requestGeneration == 0 || reservation.requestGeneration != requestGeneration {
		return nil
	}
	delete(relay.reservations, attachment)
	return relay.applyLifecycleEventWithAttachment(
		flow.LifecycleJoinResultSendCompleted,
		attachment,
		reservation.lifecycleGeneration,
		actions,
	)
}

func (relay *ApplicationRelay) releaseAttachment(attachment flow.AttachmentKey, actions *[]ApplicationRelayAction) error {
	if reservation, ok := relay.reservations[attachment]; ok {
		delete(relay.reservations, attachment)
		return relay.applyLifecycleEventWithAttachment(
			flow.LifecycleJoinResultSendFailed,
			attachment,
			reservation.lifecycleGeneration,
			actions,
		)
	}
	if !relay.published(attachment) {
		return nil
	}
	return relay.applyLifecycleEventWithAttachment(flow.LifecycleAttachmentLost, attachment, 0, actions)
}

func (relay *ApplicationRelay) sessionClosed(sessionGeneration uint64, actions *[]ApplicationRelayAction) error {
	if sessionGeneration == 0 {
		return ErrInvalidApplicationRelayEvent
	}
	for attachment := range relay.reservations {
		if attachment.SessionGeneration == sessionGeneration {
			delete(relay.reservations, attachment)
		}
	}
	return relay.applyLifecycleEventWithAttachment(
		flow.LifecycleSessionClosed,
		flow.AttachmentKey{SessionGeneration: sessionGeneration},
		0,
		actions,
	)
}

func (relay *ApplicationRelay) handleRemoteMessage(message protocol.Message, attachment flow.AttachmentKey, actions *[]ApplicationRelayAction) error {
	if message == nil || !relay.published(attachment) {
		return ErrInvalidApplicationRelayEvent
	}
	var event flow.FlowEvent
	switch typed := message.(type) {
	case protocol.Data:
		if typed.FlowID != relay.flowID {
			return ErrInvalidApplicationRelayEvent
		}
		event = flow.FlowEvent{Kind: flow.FlowRemoteData, Offset: typed.Offset, Data: typed.Bytes}
	case protocol.ACK:
		if typed.FlowID != relay.flowID {
			return ErrInvalidApplicationRelayEvent
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
			return ErrInvalidApplicationRelayEvent
		}
		event = flow.FlowEvent{Kind: flow.FlowRemoteFIN, FinalOffset: typed.FinalOffset}
	case protocol.FINACK:
		if typed.FlowID != relay.flowID {
			return ErrInvalidApplicationRelayEvent
		}
		err := relay.applyFlowEvent(flow.FlowEvent{Kind: flow.FlowRemoteFINACK, FinalOffset: typed.FinalOffset}, actions)
		if relay.machine.TxState() == flow.TxComplete {
			relay.pruneAttemptHistory(typed.FinalOffset, true)
		}
		return err
	case protocol.Reset:
		if typed.FlowID != relay.flowID {
			return ErrInvalidApplicationRelayEvent
		}
		event = flow.FlowEvent{Kind: flow.FlowLifecycle, Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleRemoteReset}}
	default:
		return ErrInvalidApplicationRelayEvent
	}
	return relay.applyFlowEvent(event, actions)
}

func (relay *ApplicationRelay) handleApplicationRead(event ApplicationRelayEvent, actions *[]ApplicationRelayAction) error {
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
		return errors.Join(ErrInvalidApplicationRead, resetErr)
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

func (relay *ApplicationRelay) handleApplicationWrite(event ApplicationRelayEvent, actions *[]ApplicationRelayAction) error {
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
		return errors.Join(ErrInvalidApplicationWrite, flowErr)
	}
	return flowErr
}

func (relay *ApplicationRelay) handleApplicationCloseWrite(event ApplicationRelayEvent, actions *[]ApplicationRelayAction) error {
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

func (relay *ApplicationRelay) handleSendResult(event ApplicationRelayEvent, actions *[]ApplicationRelayAction) error {
	pending, ok := relay.pendingSends[event.Generation]
	if !ok || event.Generation == 0 {
		return nil
	}
	outcome := event.AttemptOutcome
	if outcome < flow.AttemptSucceeded || outcome > flow.AttemptTimedOut {
		return ErrInvalidApplicationRelayEvent
	}
	if err := relay.resolveSendAdmission(event.Generation, nil); err != nil {
		return err
	}
	delete(relay.pendingSends, event.Generation)
	var result error
	switch pending.kind {
	case applicationRelayPendingAttempt:
		relay.recordAttemptResult(pending, outcome, event.WriteCompletedAt, actions)
		relay.pruneAttemptHistory(relay.machine.TxAcknowledgedOffset(), false)
		result = relay.applyFlowEvent(flow.FlowEvent{
			Kind: flow.FlowAttemptResult, ItemID: pending.itemID,
			AttemptGeneration: pending.attemptGeneration,
			Attachment:        pending.attachment, AttemptOutcome: outcome,
		}, actions)
	case applicationRelayPendingControl:
		delete(relay.controlPending, pending.attachment)
	case applicationRelayPendingReset:
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

func (relay *ApplicationRelay) handleSendAdmitted(event ApplicationRelayEvent) error {
	return relay.resolveSendAdmission(event.Generation, &event.Quality)
}

func (relay *ApplicationRelay) resolveSendAdmission(generation uint64, quality *policy.QualitySnapshot) error {
	pending, ok := relay.pendingSends[generation]
	if !ok || generation == 0 || pending.kind != applicationRelayPendingAttempt || pending.dataBytes == 0 || pending.admissionResolved {
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

func (relay *ApplicationRelay) applyLifecycleEvent(kind flow.LifecycleEventKind, generation uint64, reason protocol.ResetReason, actions *[]ApplicationRelayAction) error {
	return relay.applyFlowEvent(flow.FlowEvent{
		Kind:      flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{Kind: kind, Generation: generation, Reason: reason},
	}, actions)
}

func (relay *ApplicationRelay) applyLifecycleEventWithAttachment(kind flow.LifecycleEventKind, attachment flow.AttachmentKey, generation uint64, actions *[]ApplicationRelayAction) error {
	return relay.applyFlowEvent(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind:       kind,
			Attachment: attachment,
			Generation: generation,
		},
	}, actions)
}

func (relay *ApplicationRelay) applyFlowEvent(event flow.FlowEvent, actions *[]ApplicationRelayAction) error {
	relay.retryAfter = 0
	flowActions, flowErr := relay.machine.Handle(event)
	consumeErr := relay.consumeFlowActions(flowActions, actions)
	relay.retryAfter = 0
	return errors.Join(flowErr, consumeErr)
}

func (relay *ApplicationRelay) consumeFlowActions(flowActions []flow.FlowAction, actions *[]ApplicationRelayAction) error {
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
			*actions = append(*actions, ApplicationRelayAction{
				Kind: ApplicationRelayActionArmRetryDeadline, Generation: action.Generation, After: relay.currentRetryAfter(),
			})
		case flow.FlowActionCancelRetryDeadline:
			*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionCancelRetryDeadline, Generation: action.Generation})
		case flow.FlowActionArmNoProgressDeadline:
			*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionArmNoProgressDeadline, Generation: action.Generation})
		case flow.FlowActionCancelNoProgressDeadline:
			*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionCancelNoProgressDeadline, Generation: action.Generation})
		default:
			err = ErrInvalidApplicationRelayEvent
		}
		result = errors.Join(result, err)
	}
	relay.policy.SetPending(relay.machine.TxReplayBytes() != 0 || relay.machine.TxState() == flow.TxFinPending)
	return result
}

func (relay *ApplicationRelay) placeTxItem(item flow.TxItem, actions *[]ApplicationRelayAction) error {
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
		return ErrInvalidApplicationRelayEvent
	case 1:
		placements = relay.policy.PlaceNew(request)
	case 2:
		if relay.policy.Mode() == protocol.DeliveryAdaptive {
			request.Attempted = previousAttempts
			placements = relay.policy.GapDue(request)
		} else {
			placements = relay.policy.RetryDue(request)
		}
	default:
		placements = relay.policy.RetryDue(request)
	}
	if len(placements) > flow.MaxAttachments {
		return ErrInvalidApplicationRelay
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
		if applicationRelayAttemptedAttachment(history.attempted, placement.Attachment) {
			continue
		}
		// A full send ledger is expected when the transport is slower than the
		// application. Leave the item in Flow's replay window and let its retry
		// deadline retry placement after a send completion frees a slot.
		if len(relay.pendingSends) >= MaxApplicationRelayPendingSends {
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

func (relay *ApplicationRelay) emitAttempt(attempt flow.TxAttempt, actions *[]ApplicationRelayAction) error {
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
		return relay.emitEncodedData(encoded, item.DataLen(), attempt.Attachment, applicationRelayPendingSend{
			kind: applicationRelayPendingAttempt, attachment: attempt.Attachment,
			itemID: item.ItemID, attemptGeneration: item.AttemptGeneration,
		}, actions)
	case flow.TxItemFIN:
		return relay.emitSend(protocol.FIN{FlowID: relay.flowID, FinalOffset: item.FinalOffset}, transport.FrameControl, attempt.Attachment, applicationRelayPendingSend{
			kind: applicationRelayPendingAttempt, attachment: attempt.Attachment,
			itemID: item.ItemID, attemptGeneration: item.AttemptGeneration,
		}, actions)
	default:
		return ErrInvalidApplicationRelayEvent
	}
}

func (relay *ApplicationRelay) consumeRxAction(action flow.RxAction, actions *[]ApplicationRelayAction) error {
	switch action.Kind {
	case flow.RxWriteLocal:
		if relay.writeGeneration != 0 || relay.closeWriteGeneration != 0 || action.Generation == 0 {
			return ErrInvalidApplicationRelayEvent
		}
		relay.writeGeneration = action.Generation
		if err := relay.emitApplicationAction(ApplicationRelayAction{
			Kind: ApplicationRelayActionWrite, Generation: action.Generation,
			Offset: action.Offset, data: action.CopyData(),
		}, actions); err != nil {
			return err
		}
	case flow.RxCloseWriteLocal:
		if relay.writeGeneration != 0 || relay.closeWriteGeneration != 0 || action.Generation == 0 {
			return ErrInvalidApplicationRelayEvent
		}
		relay.closeWriteGeneration = action.Generation
		if err := relay.emitApplicationAction(ApplicationRelayAction{
			Kind: ApplicationRelayActionCloseWrite, Generation: action.Generation, FinalOffset: action.FinalOffset,
		}, actions); err != nil {
			return err
		}
	case flow.RxSendACK:
		ranges := make([]protocol.ACKRange, len(action.ACK.Ranges))
		for index, ackRange := range action.ACK.Ranges {
			ranges[index] = protocol.ACKRange{Start: ackRange.Start, End: ackRange.End}
		}
		return relay.emitControl(protocol.ACK{FlowID: relay.flowID, NextOffset: action.ACK.NextOffset, Ranges: ranges}, actions)
	case flow.RxSendFINACK:
		return relay.emitControl(protocol.FINACK{FlowID: relay.flowID, FinalOffset: action.FinalOffset}, actions)
	default:
		return ErrInvalidApplicationRelayEvent
	}
	return nil
}

func (relay *ApplicationRelay) consumeLifecycleAction(action flow.LifecycleAction, actions *[]ApplicationRelayAction) error {
	switch action.Kind {
	case flow.LifecycleActionPublishAttachment:
		generation, err := relay.allocateGeneration()
		if err != nil {
			return err
		}
		if err := relay.policy.AddAttachment(action.Attachment); err != nil {
			return err
		}
		*actions = append(*actions, ApplicationRelayAction{
			Kind: ApplicationRelayActionAttachmentPublished, Generation: generation, Attachment: action.Attachment,
		})
	case flow.LifecycleActionWithdrawAttachment:
		generation, err := relay.allocateGeneration()
		if err != nil {
			return err
		}
		relay.policy.RemoveAttachment(action.Attachment)
		delete(relay.controlPending, action.Attachment)
		delete(relay.deferredControls, action.Attachment)
		*actions = append(*actions, ApplicationRelayAction{
			Kind: ApplicationRelayActionAttachmentWithdrawn, Generation: generation, Attachment: action.Attachment,
		})
	case flow.LifecycleActionSendAcknowledgementSnapshot:
		// The flow emits a separate ACK/FIN_ACK action immediately after this marker.
	case flow.LifecycleActionPauseLocalRead:
		relay.readPaused = true
	case flow.LifecycleActionResumeLocalRead:
		relay.readPaused = false
	case flow.LifecycleActionCloseLocal:
		return relay.closeApplication(actions)
	case flow.LifecycleActionArmRecoveryDeadline:
		*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionArmRecoveryDeadline, Generation: action.Generation})
	case flow.LifecycleActionCancelRecoveryDeadline:
		*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionCancelRecoveryDeadline, Generation: action.Generation})
	case flow.LifecycleActionArmClosingDeadline:
		*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionArmClosingDeadline, Generation: action.Generation})
	case flow.LifecycleActionCancelClosingDeadline:
		*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionCancelClosingDeadline, Generation: action.Generation})
	case flow.LifecycleActionSendReset:
		return relay.emitReset(action.Generation, action.Reason, actions)
	case flow.LifecycleActionArmResetDeadline:
		*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionArmResetDeadline, Generation: action.Generation})
	case flow.LifecycleActionCancelResetDeadline:
		*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionCancelResetDeadline, Generation: action.Generation})
	case flow.LifecycleActionSendJoinSuccess, flow.LifecycleActionSendJoinFailure:
		return ErrInvalidApplicationRelayEvent
	default:
		return ErrInvalidApplicationRelayEvent
	}
	return nil
}

func (relay *ApplicationRelay) emitControl(message protocol.Message, actions *[]ApplicationRelayAction) error {
	placements := relay.policy.ControlPlacements()
	var result error
	for _, placement := range placements {
		result = errors.Join(result, relay.emitControlOnAttachment(message, placement.Attachment, actions))
	}
	return result
}

func (relay *ApplicationRelay) emitControlOnAttachment(message protocol.Message, attachment flow.AttachmentKey, actions *[]ApplicationRelayAction) error {
	if _, pending := relay.controlPending[attachment]; pending {
		relay.deferredControls[attachment] = cloneApplicationRelayMessage(message)
		return nil
	}
	if len(relay.pendingSends) >= MaxApplicationRelayPendingSends {
		relay.deferredControls[attachment] = cloneApplicationRelayMessage(message)
		return nil
	}
	err := relay.emitSend(message, transport.FrameControl, attachment, applicationRelayPendingSend{
		kind: applicationRelayPendingControl, attachment: attachment,
	}, actions)
	if err == nil {
		relay.controlPending[attachment] = struct{}{}
	}
	return err
}

func (relay *ApplicationRelay) emitReset(generation uint64, reason protocol.ResetReason, actions *[]ApplicationRelayAction) error {
	if generation == 0 {
		return ErrInvalidApplicationRelayEvent
	}
	// RESET replaces all data and control ledger entries. Results from previously
	// returned actions are subsequently stale and cannot release or alter flow state.
	flushErr := relay.flushDeferredControls(actions)
	clear(relay.pendingSends)
	relay.clearControlState()
	clear(relay.attemptHistory)
	relay.resetGeneration = generation
	relay.resetPending = 0
	placements := relay.policy.ControlPlacements()
	result := flushErr
	for _, placement := range placements {
		err := relay.emitSend(protocol.Reset{FlowID: relay.flowID, Reason: reason}, transport.FrameControl, placement.Attachment, applicationRelayPendingSend{
			kind: applicationRelayPendingReset, attachment: placement.Attachment, resetGeneration: generation,
		}, actions)
		if err == nil {
			relay.resetPending++
		}
		result = errors.Join(result, err)
	}
	return result
}

func (relay *ApplicationRelay) emitSend(message protocol.Message, class transport.FrameClass, attachment flow.AttachmentKey, pending applicationRelayPendingSend, actions *[]ApplicationRelayAction) error {
	if len(relay.pendingSends) >= MaxApplicationRelayPendingSends {
		return ErrApplicationRelaySendLimit
	}
	generation, err := relay.allocateGeneration()
	if err != nil {
		return err
	}
	relay.pendingSends[generation] = pending
	*actions = append(*actions, ApplicationRelayAction{
		Kind: ApplicationRelayActionSendMessage, Generation: generation, Attachment: attachment,
		Message: cloneApplicationRelayMessage(message), Class: class,
		ItemID: pending.itemID, AttemptGeneration: pending.attemptGeneration,
	})
	return nil
}

func (relay *ApplicationRelay) emitEncodedData(encoded []byte, dataBytes int, attachment flow.AttachmentKey, pending applicationRelayPendingSend, actions *[]ApplicationRelayAction) error {
	if len(relay.pendingSends) >= MaxApplicationRelayPendingSends {
		return ErrApplicationRelaySendLimit
	}
	if len(encoded) == 0 || dataBytes < 1 {
		return ErrInvalidApplicationRelayEvent
	}
	generation, err := relay.allocateGeneration()
	if err != nil {
		return err
	}
	pending.dataBytes = uint64(dataBytes)
	relay.pendingSends[generation] = pending
	*actions = append(*actions, ApplicationRelayAction{
		Kind: ApplicationRelayActionSendMessage, Generation: generation, Attachment: attachment,
		Encoded: encoded, Class: transport.FrameData, SendDataBytes: dataBytes,
		ItemID: pending.itemID, AttemptGeneration: pending.attemptGeneration,
	})
	return nil
}

func (relay *ApplicationRelay) closeApplication(actions *[]ApplicationRelayAction) error {
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
	relay.discardDeferredApplication()
	flushErr := relay.flushDeferredControls(actions)
	clear(relay.pendingSends)
	relay.clearControlState()
	clear(relay.attemptHistory)
	*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionClose, Generation: generation})
	return flushErr
}

func (relay *ApplicationRelay) flushDeferredControls(actions *[]ApplicationRelayAction) error {
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
		*actions = append(*actions, ApplicationRelayAction{
			Kind: ApplicationRelayActionSendMessage, Generation: generation, Attachment: placement.Attachment,
			Message: cloneApplicationRelayMessage(message), Class: transport.FrameControl,
		})
	}
	return result
}

func (relay *ApplicationRelay) drainDeferredControls(actions *[]ApplicationRelayAction) error {
	var result error
	for _, placement := range relay.policy.ControlPlacements() {
		message, ok := relay.deferredControls[placement.Attachment]
		if !ok {
			continue
		}
		if len(relay.pendingSends) >= MaxApplicationRelayPendingSends {
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

func (relay *ApplicationRelay) clearControlState() {
	clear(relay.controlPending)
	clear(relay.deferredControls)
}

func (relay *ApplicationRelay) emitApplicationAction(action ApplicationRelayAction, actions *[]ApplicationRelayAction) error {
	if relay.applicationEnabled {
		*actions = append(*actions, action)
		return nil
	}
	if relay.deferredApplication != nil {
		return ErrInvalidApplicationRelayEvent
	}
	deferred := action
	relay.deferredApplication = &deferred
	return nil
}

func (relay *ApplicationRelay) discardDeferredApplication() {
	if relay.deferredApplication == nil {
		return
	}
	clear(relay.deferredApplication.data)
	relay.deferredApplication = nil
}

func (relay *ApplicationRelay) scheduleRead(actions *[]ApplicationRelayAction) error {
	if !relay.started || !relay.applicationEnabled || relay.readPaused || relay.closeIssued || relay.readGeneration != 0 {
		return nil
	}
	if relay.machine.LifecycleState() != flow.Relaying || relay.machine.TxState() != flow.TxOpen || relay.machine.TxAvailableWindow() == 0 {
		return nil
	}
	limit := int(min(relay.machine.TxAvailableWindow(), uint64(MaxApplicationReadBytes)))
	if limit < 1 {
		return nil
	}
	generation, err := relay.allocateGeneration()
	if err != nil {
		return err
	}
	relay.readGeneration = generation
	relay.readLimit = limit
	*actions = append(*actions, ApplicationRelayAction{Kind: ApplicationRelayActionRead, Generation: generation, MaxBytes: limit})
	return nil
}

func (relay *ApplicationRelay) allocateGeneration() (uint64, error) {
	if relay.nextGeneration == math.MaxUint64 {
		return 0, ErrApplicationRelayGenerationExhaust
	}
	relay.nextGeneration++
	return relay.nextGeneration, nil
}

func (relay *ApplicationRelay) published(attachment flow.AttachmentKey) bool {
	return relay.machine.IsAttachmentPublished(attachment)
}

func (relay *ApplicationRelay) observeDataQuality(event ApplicationRelayEvent) error {
	quality, err := relay.policy.Quality(event.Attachment)
	if err != nil {
		return err
	}
	return quality.ObserveData(event.RTT, event.Bytes, event.Interval)
}

func (relay *ApplicationRelay) observeProbeQuality(event ApplicationRelayEvent) error {
	quality, err := relay.policy.Quality(event.Attachment)
	if err != nil {
		return err
	}
	return quality.ObserveProbe(event.RTT)
}

func (relay *ApplicationRelay) setAttachmentLoad(event ApplicationRelayEvent) error {
	return relay.policy.SetAttachmentLoad(event.Attachment, event.QueuedBytes, event.InFlightBytes)
}

func (relay *ApplicationRelay) setStallPenalty(event ApplicationRelayEvent) error {
	return relay.policy.SetStallPenalty(event.Attachment, event.StallPenalty)
}

func (relay *ApplicationRelay) setSessionQuality(event ApplicationRelayEvent) error {
	return relay.policy.SetQualitySnapshot(event.Attachment, event.Quality)
}

func (relay *ApplicationRelay) incumbentStalled() bool {
	return relay.policy.IncumbentStalled()
}

func (relay *ApplicationRelay) currentRetryAfter() time.Duration {
	if relay.retryAfter > 0 {
		return relay.retryAfter
	}
	acknowledged := relay.machine.TxAcknowledgedOffset()
	var earliestEnd uint64
	var earliestAfter time.Duration
	foundEarliest := false
	for _, history := range relay.attemptHistory {
		if history.fin {
			if history.end < acknowledged {
				continue
			}
		} else if history.end <= acknowledged {
			continue
		}
		if !foundEarliest || history.end < earliestEnd {
			foundEarliest = true
			earliestEnd = history.end
			earliestAfter = history.retryAfter
		}
	}
	if earliestAfter > 0 {
		return earliestAfter
	}
	var after time.Duration
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

func (relay *ApplicationRelay) beginAttemptGeneration(item flow.TxItem) (*applicationRelayAttemptHistory, []flow.AttachmentKey, error) {
	if item.ItemID == 0 || item.AttemptGeneration == 0 {
		return nil, nil, ErrInvalidApplicationRelayEvent
	}
	history := relay.attemptHistory[item.ItemID]
	if history == nil {
		if len(relay.attemptHistory) >= MaxApplicationRelayAttemptItems {
			return nil, nil, ErrApplicationRelayAttemptLimit
		}
		history = &applicationRelayAttemptHistory{}
		relay.attemptHistory[item.ItemID] = history
	}
	if item.AttemptGeneration < history.generation {
		return nil, nil, ErrInvalidApplicationRelayEvent
	}
	previous := append([]flow.AttachmentKey(nil), history.attempted...)
	if item.AttemptGeneration != history.generation {
		history.generation = item.AttemptGeneration
		history.retryAfter = 0
		history.attempted = nil
		history.attempts = nil
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

func (relay *ApplicationRelay) recordAttemptStart(item flow.TxItem, attachment flow.AttachmentKey) {
	if item.Kind != flow.TxItemData {
		return
	}
	history := relay.attemptHistory[item.ItemID]
	if history == nil || history.generation != item.AttemptGeneration || len(history.attempts) >= flow.MaxAttachments {
		return
	}
	attempt := applicationRelayAttempt{
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

func (relay *ApplicationRelay) recordAttemptResult(pending applicationRelayPendingSend, outcome flow.AttemptOutcome, completedAt time.Time, actions *[]ApplicationRelayAction) {
	history := relay.attemptHistory[pending.itemID]
	if history == nil || history.generation != pending.attemptGeneration {
		return
	}
	for index := range history.attempts {
		attempt := &history.attempts[index]
		if attempt.attachment != pending.attachment {
			continue
		}
		if attempt.invalid {
			attempt.pendingAcknowledgments = nil
			return
		}
		if outcome != flow.AttemptSucceeded {
			attempt.invalid = true
			attempt.pendingAcknowledgments = nil
			return
		}
		if completedAt.IsZero() {
			completedAt = relay.now()
		}
		attempt.writeSucceeded = true
		attempt.writeCompletedAt = completedAt
		for _, acknowledgment := range attempt.pendingAcknowledgments {
			relay.appendDataCredit(attempt, acknowledgment, actions)
		}
		attempt.pendingAcknowledgments = nil
		return
	}
}

func (relay *ApplicationRelay) consumeAcknowledgedRanges(ranges []flow.ByteRange, actions *[]ApplicationRelayAction) error {
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
			if attempt.invalid {
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
				acknowledgment := applicationRelayAcknowledgment{rangeValue: credited, acknowledgedAt: acknowledgedAt}
				if !attempt.writeSucceeded || attempt.writeCompletedAt.IsZero() {
					attempt.pendingAcknowledgments = append(attempt.pendingAcknowledgments, acknowledgment)
					continue
				}
				relay.appendDataCredit(attempt, acknowledgment, actions)
			}
		}
	}
	return nil
}

func (relay *ApplicationRelay) pruneAttemptHistory(acknowledged uint64, includeFIN bool) {
	for itemID, history := range relay.attemptHistory {
		if history.fin {
			if includeFIN && history.end == acknowledged {
				delete(relay.attemptHistory, itemID)
			}
			continue
		}
		if history.end <= acknowledged {
			if relay.hasPendingAttempt(itemID, history.generation) {
				continue
			}
			delete(relay.attemptHistory, itemID)
		}
	}
}

func (relay *ApplicationRelay) hasPendingAttempt(itemID, generation uint64) bool {
	for _, pending := range relay.pendingSends {
		if pending.kind == applicationRelayPendingAttempt && pending.itemID == itemID && pending.attemptGeneration == generation {
			return true
		}
	}
	return false
}

func (relay *ApplicationRelay) appendDataCredit(attempt *applicationRelayAttempt, acknowledgment applicationRelayAcknowledgment, actions *[]ApplicationRelayAction) {
	if attempt == nil || actions == nil || acknowledgment.rangeValue.Len() == 0 {
		return
	}
	acknowledgedAt := acknowledgment.acknowledgedAt
	if acknowledgedAt.Before(attempt.writeCompletedAt) {
		acknowledgedAt = attempt.writeCompletedAt
	}
	*actions = append(*actions, ApplicationRelayAction{
		Kind:             ApplicationRelayActionDataCredit,
		Attachment:       attempt.attachment,
		DataCreditBytes:  acknowledgment.rangeValue.Len(),
		WriteCompletedAt: attempt.writeCompletedAt,
		AcknowledgedAt:   acknowledgedAt,
	})
}

func applicationRelayAttemptedAttachment(attempted []flow.AttachmentKey, attachment flow.AttachmentKey) bool {
	for _, candidate := range attempted {
		if candidate == attachment {
			return true
		}
	}
	return false
}

func (relay *ApplicationRelay) converging() bool {
	state := relay.machine.LifecycleState()
	return state == flow.Closing || state == flow.Closed || state == flow.Resetting || state == flow.Reset
}

func (relay *ApplicationRelay) terminal() bool {
	state := relay.machine.LifecycleState()
	return state == flow.Closed || state == flow.Reset
}

func cloneApplicationRelayMessage(message protocol.Message) protocol.Message {
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
