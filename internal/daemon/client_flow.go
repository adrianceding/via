package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adrianceding/via/internal/auth"
	clientcore "github.com/adrianceding/via/internal/client"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	statusapi "github.com/adrianceding/via/internal/status"
)

type openJoinTimerKind uint8

const (
	openJoinTimerOpenDeadline openJoinTimerKind = iota + 1
	openJoinTimerOpenRetry
	openJoinTimerJoinDeadline
	openJoinTimerJoinRetry
)

type openJoinTimerKey struct {
	kind       openJoinTimerKind
	generation uint64
}

type clientFlowRemote struct {
	message           protocol.Message
	sessionGeneration uint64
	attachment        flow.AttachmentKey
}

type clientFlowSessionReady struct {
	generation uint64
	rtt        time.Duration
	stall      time.Duration
}
type clientFlowSessionQuality struct {
	generation uint64
	rtt        time.Duration
	stall      time.Duration
}
type clientFlowSessionLost struct{ generation uint64 }
type clientFlowApplicationEnabled struct{}

type clientFlowSnapshotRequest struct {
	response chan clientcore.ApplicationRelaySnapshot
}

type clientFlowOpenJoinTimer struct {
	key   openJoinTimerKey
	event clientcore.OpenJoinEvent
}

type clientFlowRelayTimer struct {
	key   relayTimerKey
	event clientcore.ApplicationRelayEvent
}

type clientFlow struct {
	host          *clientDaemon
	application   *clientcore.ApplicationIOExecutor
	openJoin      *clientcore.OpenJoinCoordinator
	machine       *flow.Flow
	relay         *clientcore.ApplicationRelay
	flowID        protocol.FlowID
	correlationID string
	target        protocol.Target
	ctx           context.Context
	cancel        context.CancelFunc

	events     chan any
	openResult chan protocol.OpenResultCode
	done       chan struct{}
	resultOnce sync.Once
	doneOnce   sync.Once
	cancelCode atomic.Uint32

	openJoinTimers map[openJoinTimerKey]*time.Timer
	relayTimers    map[relayTimerKey]*time.Timer
	publications   map[flow.AttachmentKey]uint64
	recoveringSlot bool
	lastReason     statusapi.TransitionReason
}

func newClientFlow(host *clientDaemon, connection net.Conn, target protocol.Target) (*clientFlow, error) {
	if host == nil || connection == nil || len(host.readySessions()) == 0 {
		return nil, ErrWireProtocol
	}
	coordinator, err := clientcore.NewOpenJoinCoordinator(clientcore.OpenJoinSpec{
		Target: target, DeliveryMode: host.configuration.Delivery.Mode, PathSelection: host.configuration.Delivery.Selection,
		Constraints: host.configuration.Delivery.Constraints,
	})
	if err != nil {
		return nil, err
	}
	application, err := clientcore.NewApplicationIOExecutor(connection)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(host.runtimeCtx)
	instance := &clientFlow{
		host: host, application: application, openJoin: coordinator, target: target, ctx: ctx, cancel: cancel,
		events: make(chan any, 128), openResult: make(chan protocol.OpenResultCode, 1), done: make(chan struct{}),
		openJoinTimers: make(map[openJoinTimerKey]*time.Timer, 4), relayTimers: make(map[relayTimerKey]*time.Timer, 5),
		publications: make(map[flow.AttachmentKey]uint64, flow.MaxAttachments),
	}
	host.flowsMu.Lock()
	host.actors[instance] = struct{}{}
	host.flowsMu.Unlock()
	host.wg.Add(1)
	go func() {
		defer host.wg.Done()
		instance.run()
	}()
	return instance, nil
}

func (instance *clientFlow) run() {
	defer instance.cleanup()
	for _, session := range instance.host.readySessions() {
		rtt, stall := session.probeQuality()
		instance.handleOpenJoin(clientcore.OpenJoinEvent{
			Kind: clientcore.OpenJoinSessionReady, SessionGeneration: session.generation, SessionRTT: rtt, SessionStall: stall,
		})
	}
	instance.handleOpenJoin(clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinStart})
	for {
		select {
		case <-instance.ctx.Done():
			instance.shutdown()
			return
		default:
		}
		select {
		case <-instance.ctx.Done():
			instance.shutdown()
			return
		case raw := <-instance.events:
			switch event := raw.(type) {
			case clientcore.OpenJoinEvent:
				instance.handleOpenJoin(event)
			case clientcore.ApplicationRelayEvent:
				instance.handleRelay(event)
			case clientFlowOpenJoinTimer:
				instance.consumeOpenJoinTimer(event.key)
				instance.handleOpenJoin(event.event)
			case clientFlowRelayTimer:
				instance.consumeRelayTimer(event.key)
				instance.handleRelay(event.event)
			case clientFlowRemote:
				instance.handleRemote(event)
			case clientFlowSessionReady:
				instance.handleOpenJoin(clientcore.OpenJoinEvent{
					Kind: clientcore.OpenJoinSessionReady, SessionGeneration: event.generation,
					SessionRTT: event.rtt, SessionStall: event.stall,
				})
			case clientFlowSessionQuality:
				instance.handleOpenJoin(clientcore.OpenJoinEvent{
					Kind: clientcore.OpenJoinSessionQuality, SessionGeneration: event.generation,
					SessionRTT: event.rtt, SessionStall: event.stall,
				})
			case clientFlowSessionLost:
				if instance.relay != nil {
					instance.handleRelay(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelaySessionClosed, Generation: event.generation})
				}
				instance.handleOpenJoin(clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinSessionLost, SessionGeneration: event.generation})
			case clientFlowApplicationEnabled:
				if instance.relay != nil {
					instance.handleRelay(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayEnableApplication})
				}
			case clientFlowSnapshotRequest:
				if instance.relay != nil {
					event.response <- instance.relay.Snapshot()
				}
			}
		}
		if instance.relay != nil {
			state := instance.machine.LifecycleState()
			if state == flow.Closed || state == flow.Reset {
				return
			}
		}
	}
}

func (instance *clientFlow) snapshot(ctx context.Context) (clientcore.ApplicationRelaySnapshot, bool) {
	if instance == nil || ctx == nil {
		return clientcore.ApplicationRelaySnapshot{}, false
	}
	response := make(chan clientcore.ApplicationRelaySnapshot, 1)
	select {
	case instance.events <- clientFlowSnapshotRequest{response: response}:
	case <-instance.done:
		return clientcore.ApplicationRelaySnapshot{}, false
	case <-ctx.Done():
		return clientcore.ApplicationRelaySnapshot{}, false
	}
	select {
	case snapshot := <-response:
		return snapshot, true
	case <-instance.done:
		return clientcore.ApplicationRelaySnapshot{}, false
	case <-ctx.Done():
		return clientcore.ApplicationRelaySnapshot{}, false
	}
}

func (instance *clientFlow) shutdown() {
	instance.handleOpenJoin(clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinCancelled})
	if instance.relay != nil {
		reason := protocol.ResetReason(instance.cancelCode.Load())
		if reason == 0 {
			reason = protocol.ResetCancelled
		}
		instance.handleRelay(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayResetRequested, ResetReason: reason})
		generation := instance.relay.Snapshot().Flow.Lifecycle.ResetDeadlineGeneration
		if generation != 0 {
			instance.handleRelay(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayResetDeadline, Generation: generation})
			instance.publishStatus(clientStatusReasonForReset(reason))
		}
	}
}

func (instance *clientFlow) emit(event any) {
	if instance == nil {
		return
	}
	select {
	case <-instance.ctx.Done():
		return
	default:
	}
	select {
	case instance.events <- event:
	case <-instance.ctx.Done():
	default:
		instance.requestCancel(protocol.ResetResourceLimit)
	}
}

func (instance *clientFlow) tryEmitQuality(event clientcore.ApplicationRelayEvent) {
	if instance == nil {
		return
	}
	select {
	case instance.events <- event:
	case <-instance.ctx.Done():
	default:
	}
}

func (instance *clientFlow) tryEmitSessionQuality(generation uint64, rtt, stall time.Duration) {
	if instance == nil || generation == 0 || rtt < 0 || stall < 0 || rtt == 0 && stall == 0 {
		return
	}
	select {
	case instance.events <- clientFlowSessionQuality{generation: generation, rtt: rtt, stall: stall}:
	case <-instance.ctx.Done():
	default:
	}
}

func (instance *clientFlow) requestCancel(reason protocol.ResetReason) {
	if instance == nil {
		return
	}
	if reason == 0 {
		reason = protocol.ResetCancelled
	}
	instance.cancelCode.CompareAndSwap(0, uint32(reason))
	instance.cancel()
}

func (instance *clientFlow) handleOpenJoin(event clientcore.OpenJoinEvent) {
	actions, err := instance.openJoin.Handle(event)
	if err != nil {
		instance.signalResult(protocol.OpenInternalFailure)
		instance.cancel()
		return
	}
	instance.executeOpenJoin(actions)
}

func (instance *clientFlow) executeOpenJoin(actions []clientcore.OpenJoinAction) {
	failedReservations := make(map[uint64]struct{})
	for _, action := range actions {
		if _, failed := failedReservations[action.Generation]; failed && action.Kind == clientcore.OpenJoinActionSendJoin {
			continue
		}
		switch action.Kind {
		case clientcore.OpenJoinActionGenerateIdentity:
			instance.host.wg.Add(1)
			go func(action clientcore.OpenJoinAction) {
				defer instance.host.wg.Done()
				var flowID protocol.FlowID
				var token protocol.OpenToken
				_, flowErr := rand.Read(flowID[:])
				_, tokenErr := rand.Read(token[:])
				event := clientcore.OpenJoinEvent{
					Kind: clientcore.OpenJoinIdentityGenerated, Generation: action.Generation, FlowID: flowID, OpenToken: token,
				}
				if flowErr != nil || tokenErr != nil {
					event = clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinIdentityFailed, Generation: action.Generation}
				}
				instance.emit(event)
			}(action)
		case clientcore.OpenJoinActionArmOpenDeadline:
			instance.armOpenJoinTimer(openJoinTimerOpenDeadline, action.Generation, action.Duration)
		case clientcore.OpenJoinActionCancelOpenDeadline:
			instance.cancelOpenJoinTimer(openJoinTimerOpenDeadline, action.Generation)
		case clientcore.OpenJoinActionArmOpenRetry:
			instance.armOpenJoinTimer(openJoinTimerOpenRetry, action.Generation, action.Duration)
		case clientcore.OpenJoinActionCancelOpenRetry:
			instance.cancelOpenJoinTimer(openJoinTimerOpenRetry, action.Generation)
		case clientcore.OpenJoinActionArmJoinDeadline:
			instance.armOpenJoinTimer(openJoinTimerJoinDeadline, action.Generation, action.Duration)
		case clientcore.OpenJoinActionCancelJoinDeadline:
			instance.cancelOpenJoinTimer(openJoinTimerJoinDeadline, action.Generation)
		case clientcore.OpenJoinActionArmJoinRetry:
			instance.armOpenJoinTimer(openJoinTimerJoinRetry, action.Generation, action.Duration)
		case clientcore.OpenJoinActionCancelJoinRetry:
			instance.cancelOpenJoinTimer(openJoinTimerJoinRetry, action.Generation)
		case clientcore.OpenJoinActionSendOpen:
			if instance.relay == nil && !instance.initializeRelay(action.Open.FlowID) {
				instance.signalResult(protocol.OpenInternalFailure)
				instance.cancel()
				return
			}
			instance.send(action.SessionGeneration, action.Open)
		case clientcore.OpenJoinActionReserveAttachment:
			session := instance.host.session(action.SessionGeneration)
			if session == nil {
				failedReservations[action.Generation] = struct{}{}
				instance.emit(clientcore.OpenJoinEvent{
					Kind: clientcore.OpenJoinJoinReservationFailed, Generation: action.Generation, SessionGeneration: action.SessionGeneration,
				})
				continue
			}
			relayActions, relayErr := instance.relay.Handle(clientcore.ApplicationRelayEvent{
				Kind: clientcore.ApplicationRelayReserveAttachment, Generation: action.Generation, Attachment: action.Attachment,
			})
			instance.executeRelay(relayActions)
			if relayErr != nil || session.reserve(instance.flowID, action.Attachment) != nil {
				failedReservations[action.Generation] = struct{}{}
				instance.handleRelay(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayReleaseAttachment, Attachment: action.Attachment})
				session.release(instance.flowID, action.Attachment)
				instance.emit(clientcore.OpenJoinEvent{
					Kind: clientcore.OpenJoinJoinReservationFailed, Generation: action.Generation, SessionGeneration: action.SessionGeneration,
				})
			}
		case clientcore.OpenJoinActionSendJoin:
			instance.send(action.SessionGeneration, action.Join)
		case clientcore.OpenJoinActionPublishAttachment:
			instance.publications[action.Attachment] = action.Generation
			instance.handleRelay(clientcore.ApplicationRelayEvent{
				Kind: clientcore.ApplicationRelayPublishAttachment, Generation: action.Generation, Attachment: action.Attachment,
			})
		case clientcore.OpenJoinActionReleaseAttachment:
			delete(instance.publications, action.Attachment)
			if instance.relay != nil {
				instance.handleRelay(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayReleaseAttachment, Attachment: action.Attachment})
			}
			if session := instance.host.session(action.SessionGeneration); session != nil {
				session.release(instance.flowID, action.Attachment)
			}
		case clientcore.OpenJoinActionReplyApplicationSuccess:
			instance.signalResult(protocol.OpenSuccess)
		case clientcore.OpenJoinActionFailFlow:
			instance.signalResult(action.OpenResult)
			if instance.relay != nil {
				instance.handleRelay(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayResetRequested, ResetReason: protocol.ResetInternalFailure})
			}
			instance.cancel()
		}
	}
}

func (instance *clientFlow) initializeRelay(flowID protocol.FlowID) bool {
	if flowID == (protocol.FlowID{}) {
		return false
	}
	machine := flow.NewFlow()
	relay, err := clientcore.NewApplicationRelay(flowID, policy.Config{
		Mode: instance.host.configuration.Delivery.Mode, Selection: instance.host.configuration.Delivery.Selection,
		Constraints: instance.host.configuration.Delivery.Constraints,
	}, machine)
	if err != nil {
		return false
	}
	correlationID, err := auth.DeriveFlowID(
		auth.Key(instance.host.configuration.PSK), instance.host.configuration.PrincipalID, flowID,
	)
	if err != nil {
		return false
	}
	instance.host.flowsMu.Lock()
	if instance.host.flows[flowID] != nil {
		instance.host.flowsMu.Unlock()
		return false
	}
	instance.host.flows[flowID] = instance
	instance.host.flowsMu.Unlock()
	instance.flowID = flowID
	instance.correlationID = correlationID.String()
	instance.machine = machine
	instance.relay = relay
	instance.handleRelay(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayStart})
	return true
}

func (instance *clientFlow) send(sessionGeneration uint64, message protocol.Message) {
	instance.host.wg.Add(1)
	go func() {
		defer instance.host.wg.Done()
		session := instance.host.session(sessionGeneration)
		if session == nil || session.send(message) != nil {
			instance.host.emitPoolEvent(poolEvent{manager: clientcore.SessionManagerEvent{
				Kind: clientcore.SessionConnectionLost, Generation: sessionGeneration,
			}})
		}
	}()
}

func (instance *clientFlow) handleRemote(event clientFlowRemote) {
	switch message := event.message.(type) {
	case protocol.OpenResult:
		if attempt, ok := instance.openJoin.PendingAttempt(event.sessionGeneration, clientcore.OpenJoinAttemptOpen); ok {
			instance.handleOpenJoin(clientcore.OpenJoinEvent{
				Kind: clientcore.OpenJoinOpenResultReceived, Generation: attempt.Generation,
				SessionGeneration: event.sessionGeneration, OpenResult: message,
			})
		}
	case protocol.JoinResult:
		if attempt, ok := instance.openJoin.PendingAttempt(event.sessionGeneration, clientcore.OpenJoinAttemptJoin); ok {
			instance.handleOpenJoin(clientcore.OpenJoinEvent{
				Kind: clientcore.OpenJoinJoinResultReceived, Generation: attempt.Generation,
				SessionGeneration: event.sessionGeneration, JoinResult: message,
			})
		}
	case protocol.Data, protocol.ACK, protocol.FIN, protocol.FINACK, protocol.Reset:
		if instance.relay != nil {
			instance.handleRelay(clientcore.ApplicationRelayEvent{
				Kind: clientcore.ApplicationRelayRemoteMessage, Message: event.message, Attachment: event.attachment,
			})
		}
	}
}

func (instance *clientFlow) handleRelay(event clientcore.ApplicationRelayEvent) {
	if instance.relay == nil {
		return
	}
	refreshErr := instance.refreshSessionQualities(event.Kind)
	actions, err := instance.relay.Handle(event)
	err = errors.Join(refreshErr, err)
	if err != nil {
		log.Printf("client flow event failed: %s, %s", clientRelayEventCategory(event), clientRelayErrorCategory(err))
	}
	reason := clientRelayReason(event)
	if reason == statusapi.ReasonNone {
		reason = clientStatusReasonFromRelayActions(actions)
	}
	instance.recordRelayCounters(actions)
	state := instance.machine.LifecycleState()
	if !instance.syncRecoveringSlot(state) {
		instance.executeRelay(actions)
		more, _ := instance.relay.Handle(clientcore.ApplicationRelayEvent{
			Kind: clientcore.ApplicationRelayResetRequested, ResetReason: protocol.ResetResourceLimit,
		})
		instance.recordRelayCounters(more)
		instance.syncRecoveringSlot(instance.machine.LifecycleState())
		instance.executeRelay(more)
		instance.publishStatus(statusapi.ReasonResourceLimit)
		return
	}
	instance.executeRelay(actions)
	if err != nil && !errors.Is(err, flow.ErrInvalidState) {
		if reason == statusapi.ReasonNone {
			reason = clientStatusReasonForReset(protocol.ResetInternalFailure)
		}
		state := instance.machine.LifecycleState()
		if state != flow.Resetting && state != flow.Reset && state != flow.Closed {
			more, _ := instance.relay.Handle(clientcore.ApplicationRelayEvent{
				Kind: clientcore.ApplicationRelayResetRequested, ResetReason: protocol.ResetInternalFailure,
			})
			instance.recordRelayCounters(more)
			instance.syncRecoveringSlot(instance.machine.LifecycleState())
			instance.executeRelay(more)
		}
	}
	instance.publishStatus(reason)
}

func (instance *clientFlow) refreshSessionQualities(kind clientcore.ApplicationRelayEventKind) error {
	switch kind {
	case clientcore.ApplicationRelayObserveDataQuality, clientcore.ApplicationRelayObserveProbeQuality,
		clientcore.ApplicationRelaySetAttachmentLoad, clientcore.ApplicationRelaySetStallPenalty,
		clientcore.ApplicationRelaySetSessionQuality, clientcore.ApplicationRelaySetSessionQualities,
		clientcore.ApplicationRelaySendAdmitted:
		return nil
	}
	attachments := instance.relay.Attachments()
	qualities := make(map[flow.AttachmentKey]policy.QualitySnapshot, len(attachments))
	for _, attachment := range attachments {
		session := instance.host.session(attachment.SessionGeneration)
		if session == nil {
			return nil
		}
		qualities[attachment] = session.qualitySnapshot()
	}
	_, err := instance.relay.Handle(clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelaySetSessionQualities, SessionQualities: qualities,
	})
	return err
}

func clientRelayEventCategory(event clientcore.ApplicationRelayEvent) string {
	switch event.Kind {
	case clientcore.ApplicationRelayReserveAttachment:
		return "attachment reservation"
	case clientcore.ApplicationRelayPublishAttachment:
		return "attachment publication"
	case clientcore.ApplicationRelayReleaseAttachment:
		return "attachment withdrawal"
	case clientcore.ApplicationRelaySessionClosed:
		return "transport session closure"
	case clientcore.ApplicationRelayRemoteMessage:
		return "remote " + remoteMessageCategory(event.Message) + " message"
	case clientcore.ApplicationRelayReadResult:
		return "application read result"
	case clientcore.ApplicationRelayWriteResult:
		return "application write result"
	case clientcore.ApplicationRelayCloseWriteResult:
		return "application half-close result"
	case clientcore.ApplicationRelaySendAdmitted:
		return "transport send admission"
	case clientcore.ApplicationRelaySendResult:
		return "transport send result"
	case clientcore.ApplicationRelayRetryDeadline:
		return "retry deadline"
	case clientcore.ApplicationRelayNoProgressDeadline:
		return "no-progress deadline"
	case clientcore.ApplicationRelayRecoveryDeadline:
		return "recovery deadline"
	case clientcore.ApplicationRelayClosingDeadline:
		return "closing deadline"
	case clientcore.ApplicationRelayResetDeadline:
		return "reset deadline"
	case clientcore.ApplicationRelayResetRequested:
		return "reset request"
	case clientcore.ApplicationRelayObserveDataQuality, clientcore.ApplicationRelayObserveProbeQuality,
		clientcore.ApplicationRelaySetAttachmentLoad, clientcore.ApplicationRelaySetStallPenalty,
		clientcore.ApplicationRelaySetSessionQuality:
		return "path quality"
	default:
		return "internal event"
	}
}

func clientRelayErrorCategory(err error) string {
	categories := []struct {
		target error
		name   string
	}{
		{clientcore.ErrApplicationRelaySendLimit, "pending send action limit exceeded"},
		{clientcore.ErrApplicationRelayAttemptLimit, "send attempt record limit exceeded"},
		{clientcore.ErrApplicationRelayActionLimit, "state action batch limit exceeded"},
		{clientcore.ErrApplicationRelayAttachment, "attachment rejected"},
		{clientcore.ErrInvalidApplicationRead, "invalid application read result"},
		{clientcore.ErrInvalidApplicationWrite, "invalid application write result"},
		{clientcore.ErrApplicationRelayGenerationExhaust, "event generation exhausted"},
		{clientcore.ErrInvalidApplicationRelayEvent, "invalid relay event"},
	}
	for _, category := range categories {
		if errors.Is(err, category.target) {
			return category.name
		}
	}
	if category := flowErrorCategory(err); category != "internal error" {
		return category
	}
	if errors.Is(err, policy.ErrInvalidSample) {
		return "invalid path quality sample"
	}
	if errors.Is(err, policy.ErrUnknownAttachment) {
		return "unknown path quality attachment"
	}
	return "internal error"
}

func (instance *clientFlow) publishStatus(reason statusapi.TransitionReason) {
	if instance == nil || instance.relay == nil || instance.host.statusObserver == nil {
		return
	}
	flowStatus, policyStatus := instance.relay.StatusSnapshot()
	state := statusFlowState(flowStatus.LifecycleState)
	instance.lastReason = stableClientTerminalReason(state, instance.lastReason, reason)
	if state == statusapi.FlowClosed || state == statusapi.FlowReset {
		reason = instance.lastReason
		if state == statusapi.FlowClosed && reason == statusapi.ReasonNone {
			reason = statusapi.ReasonCompleted
		}
		instance.host.statusObserver.terminalFlow(instance.flowID, state, reason)
		return
	}
	if instance.lastReason != statusapi.ReasonNone {
		reason = instance.lastReason
	}
	instance.host.statusObserver.upsertFlowObservation(
		instance.flowID, instance.target, instance.host.configuration.Delivery.Mode,
		instance.host.configuration.Delivery.Selection, runtimeFlowObservation{
			correlationID: instance.correlationID, lifecycleState: flowStatus.LifecycleState,
			adaptiveState: policyStatus.State, adaptiveTransition: policyStatus.Transition,
			publishedAttachments: flowStatus.PublishedAttachments, policyAttachments: policyStatus.Attachments,
			preferredAttachment: policyStatus.Preferred, hasPreferred: policyStatus.HasPreferred,
			txAllocatedOffset: flowStatus.TxAllocatedOffset, txAcknowledged: flowStatus.TxAcknowledged,
			rxWrittenOffset: flowStatus.RxWrittenOffset,
		}, reason,
	)
}

func stableClientTerminalReason(state statusapi.FlowState, current, candidate statusapi.TransitionReason) statusapi.TransitionReason {
	if current != statusapi.ReasonNone {
		return current
	}
	if state == statusapi.FlowClosed {
		if candidate == statusapi.ReasonCompleted {
			return candidate
		}
		return statusapi.ReasonNone
	}
	if state != statusapi.FlowResetting && state != statusapi.FlowReset {
		return statusapi.ReasonNone
	}
	switch candidate {
	case statusapi.ReasonAuthenticationFailed, statusapi.ReasonProtocolConflict,
		statusapi.ReasonResourceLimit, statusapi.ReasonLocalIOFailure,
		statusapi.ReasonDeadlineExceeded, statusapi.ReasonCancelled,
		statusapi.ReasonRemoteReset, statusapi.ReasonInternalFailure:
		return candidate
	default:
		return statusapi.ReasonNone
	}
}

func clientStatusReasonFromRelayActions(actions []clientcore.ApplicationRelayAction) statusapi.TransitionReason {
	for _, action := range actions {
		reset, ok := action.Message.(protocol.Reset)
		if ok {
			return clientStatusReasonForReset(reset.Reason)
		}
	}
	return statusapi.ReasonNone
}

func (instance *clientFlow) recordRelayCounters(actions []clientcore.ApplicationRelayAction) {
	if instance == nil || instance.host.statusObserver == nil {
		return
	}
	type attemptKey struct {
		item       uint64
		generation uint64
	}
	var copies map[attemptKey]uint8
	var retransmitted, redundant uint64
	for _, action := range actions {
		if action.Kind != clientcore.ApplicationRelayActionSendMessage {
			continue
		}
		if action.SendDataBytes == 0 {
			continue
		}
		if copies == nil {
			copies = make(map[attemptKey]uint8, flow.MaxAttachments)
		}
		bytes := uint64(action.SendDataBytes)
		if action.AttemptGeneration > 1 {
			retransmitted += bytes
		}
		key := attemptKey{item: action.ItemID, generation: action.AttemptGeneration}
		if copies[key] != 0 {
			redundant += bytes
		}
		copies[key]++
	}
	instance.host.statusObserver.recordFlowTraffic(instance.flowID, retransmitted, redundant)
}

func clientRelayReason(event clientcore.ApplicationRelayEvent) statusapi.TransitionReason {
	switch event.Kind {
	case clientcore.ApplicationRelayStart:
		return statusapi.ReasonStarted
	case clientcore.ApplicationRelayPublishAttachment:
		return statusapi.ReasonPathAdded
	case clientcore.ApplicationRelayReleaseAttachment, clientcore.ApplicationRelaySessionClosed:
		return statusapi.ReasonPathRemoved
	case clientcore.ApplicationRelayRetryDeadline, clientcore.ApplicationRelayNoProgressDeadline,
		clientcore.ApplicationRelayRecoveryDeadline, clientcore.ApplicationRelayResetDeadline:
		return statusapi.ReasonDeadlineExceeded
	case clientcore.ApplicationRelayClosingDeadline:
		return statusapi.ReasonCompleted
	case clientcore.ApplicationRelayResetRequested:
		return clientStatusReasonForReset(event.ResetReason)
	case clientcore.ApplicationRelayReadResult:
		if event.Err != nil && !errors.Is(event.Err, io.EOF) {
			return statusapi.ReasonLocalIOFailure
		}
	case clientcore.ApplicationRelayWriteResult, clientcore.ApplicationRelayCloseWriteResult:
		if event.Err != nil {
			return statusapi.ReasonLocalIOFailure
		}
	case clientcore.ApplicationRelayRemoteMessage:
		if _, ok := event.Message.(protocol.Reset); ok {
			return statusapi.ReasonRemoteReset
		}
	}
	return statusapi.ReasonNone
}

func (instance *clientFlow) syncRecoveringSlot(state flow.LifecycleState) bool {
	if state != flow.Recovering {
		if instance.recoveringSlot {
			<-instance.host.recoveringSlots
			instance.recoveringSlot = false
		}
		return true
	}
	if instance.recoveringSlot {
		return true
	}
	select {
	case instance.host.recoveringSlots <- struct{}{}:
		instance.recoveringSlot = true
		return true
	default:
		return false
	}
}

func (instance *clientFlow) executeRelay(actions []clientcore.ApplicationRelayAction) {
	for _, action := range actions {
		switch action.Kind {
		case clientcore.ApplicationRelayActionRead, clientcore.ApplicationRelayActionWrite, clientcore.ApplicationRelayActionCloseWrite:
			instance.host.wg.Add(1)
			go func(action clientcore.ApplicationRelayAction) {
				defer instance.host.wg.Done()
				event, hasResult, err := instance.application.Execute(action)
				if err != nil {
					instance.emit(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayResetRequested, ResetReason: protocol.ResetLocalIOFailure})
				} else if hasResult {
					instance.emit(event)
				}
			}(action)
		case clientcore.ApplicationRelayActionClose:
			_, _, _ = instance.application.Execute(action)
		case clientcore.ApplicationRelayActionSendMessage:
			pending, ok := instance.admitRelaySend(action)
			if !ok {
				instance.emit(clientcore.ApplicationRelayEvent{
					Kind: clientcore.ApplicationRelaySendResult, Generation: action.Generation, AttemptOutcome: flow.AttemptFailed,
				})
				continue
			}
			quality := policy.QualitySnapshot{}
			if pending.session != nil {
				quality = pending.session.qualitySnapshot()
			}
			instance.handleRelay(clientcore.ApplicationRelayEvent{
				Kind: clientcore.ApplicationRelaySendAdmitted, Generation: action.Generation, Quality: quality,
			})
			instance.host.wg.Add(1)
			go func(action clientcore.ApplicationRelayAction, pending *pendingSessionWrite) {
				defer instance.host.wg.Done()
				outcome := flow.AttemptFailed
				writeCompletedAt, err := pending.wait()
				if err == nil {
					outcome = flow.AttemptSucceeded
				}
				instance.emit(clientcore.ApplicationRelayEvent{
					Kind: clientcore.ApplicationRelaySendResult, Generation: action.Generation, AttemptOutcome: outcome,
					WriteCompletedAt: writeCompletedAt,
				})
			}(action, pending)
		case clientcore.ApplicationRelayActionDataCredit:
			session := instance.host.session(action.Attachment.SessionGeneration)
			if session != nil {
				session.runtime.observeDataCredit(action.DataCreditBytes, action.WriteCompletedAt, action.AcknowledgedAt)
			}
		case clientcore.ApplicationRelayActionArmRetryDeadline:
			instance.armRelayTimer(relayTimerRetry, action.Generation, action.After)
		case clientcore.ApplicationRelayActionCancelRetryDeadline:
			instance.cancelRelayTimer(relayTimerRetry, action.Generation)
		case clientcore.ApplicationRelayActionArmNoProgressDeadline:
			instance.armRelayTimer(relayTimerNoProgress, action.Generation, 30*time.Second)
		case clientcore.ApplicationRelayActionCancelNoProgressDeadline:
			instance.cancelRelayTimer(relayTimerNoProgress, action.Generation)
		case clientcore.ApplicationRelayActionArmRecoveryDeadline:
			instance.armRelayTimer(relayTimerRecovery, action.Generation, 30*time.Second)
		case clientcore.ApplicationRelayActionCancelRecoveryDeadline:
			instance.cancelRelayTimer(relayTimerRecovery, action.Generation)
		case clientcore.ApplicationRelayActionArmClosingDeadline:
			instance.armRelayTimer(relayTimerClosing, action.Generation, 60*time.Second)
		case clientcore.ApplicationRelayActionCancelClosingDeadline:
			instance.cancelRelayTimer(relayTimerClosing, action.Generation)
		case clientcore.ApplicationRelayActionArmResetDeadline:
			instance.armRelayTimer(relayTimerReset, action.Generation, sendTimeout)
		case clientcore.ApplicationRelayActionCancelResetDeadline:
			instance.cancelRelayTimer(relayTimerReset, action.Generation)
		case clientcore.ApplicationRelayActionAttachmentPublished:
			session := instance.host.session(action.Attachment.SessionGeneration)
			generation := instance.publications[action.Attachment]
			if session == nil || generation == 0 || session.publish(instance.flowID, action.Attachment) != nil {
				instance.emit(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayReleaseAttachment, Attachment: action.Attachment})
				instance.emit(clientcore.OpenJoinEvent{
					Kind: clientcore.OpenJoinAttachmentPublishFailed, Generation: generation,
					SessionGeneration: action.Attachment.SessionGeneration,
				})
			} else {
				instance.handleRelay(clientcore.ApplicationRelayEvent{
					Kind: clientcore.ApplicationRelaySetSessionQuality, Attachment: action.Attachment,
					Quality: session.qualitySnapshot(),
				})
				instance.emit(clientcore.OpenJoinEvent{
					Kind: clientcore.OpenJoinAttachmentPublished, Generation: generation,
					SessionGeneration: action.Attachment.SessionGeneration,
				})
			}
			delete(instance.publications, action.Attachment)
		case clientcore.ApplicationRelayActionAttachmentWithdrawn:
			if session := instance.host.session(action.Attachment.SessionGeneration); session != nil {
				session.release(instance.flowID, action.Attachment)
			}
		}
	}
}

func (instance *clientFlow) admitRelaySend(action clientcore.ApplicationRelayAction) (*pendingSessionWrite, bool) {
	session := instance.host.session(action.Attachment.SessionGeneration)
	if session == nil {
		return nil, false
	}
	attachment, ok := session.attachment(instance.flowID)
	if !ok || attachment != action.Attachment {
		return nil, false
	}
	var pending *pendingSessionWrite
	var err error
	if len(action.Encoded) != 0 {
		pending, err = session.admitEncodedContextMetadata(instance.ctx, action.Class, action.Encoded,
			instance.flowID, action.ItemID, action.AttemptGeneration, uint64(action.SendDataBytes))
	} else {
		pending, err = session.admitMessageContext(instance.ctx, action.Message)
	}
	return pending, err == nil
}

func (instance *clientFlow) armOpenJoinTimer(kind openJoinTimerKind, generation uint64, duration time.Duration) {
	key := openJoinTimerKey{kind: kind, generation: generation}
	if generation == 0 || duration <= 0 || instance.openJoinTimers[key] != nil {
		return
	}
	instance.openJoinTimers[key] = time.AfterFunc(duration, func() {
		var event clientcore.OpenJoinEvent
		switch kind {
		case openJoinTimerOpenDeadline:
			event = clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinOpenDeadlineReached, Generation: generation}
		case openJoinTimerOpenRetry:
			event = clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinOpenRetryDue, Generation: generation}
		case openJoinTimerJoinDeadline:
			event = clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinJoinDeadlineReached, Generation: generation}
		case openJoinTimerJoinRetry:
			event = clientcore.OpenJoinEvent{Kind: clientcore.OpenJoinJoinRetryDue, Generation: generation}
		}
		instance.emit(clientFlowOpenJoinTimer{key: key, event: event})
	})
}

func (instance *clientFlow) consumeOpenJoinTimer(key openJoinTimerKey) {
	delete(instance.openJoinTimers, key)
}

func (instance *clientFlow) cancelOpenJoinTimer(kind openJoinTimerKind, generation uint64) {
	key := openJoinTimerKey{kind: kind, generation: generation}
	if timer := instance.openJoinTimers[key]; timer != nil {
		timer.Stop()
		delete(instance.openJoinTimers, key)
	}
}

func (instance *clientFlow) armRelayTimer(kind relayTimerKind, generation uint64, duration time.Duration) {
	key := relayTimerKey{kind: kind, generation: generation}
	if generation == 0 || duration <= 0 || instance.relayTimers[key] != nil {
		return
	}
	instance.relayTimers[key] = time.AfterFunc(duration, func() {
		var event clientcore.ApplicationRelayEvent
		switch kind {
		case relayTimerRetry:
			event = clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayRetryDeadline, Generation: generation}
		case relayTimerNoProgress:
			event = clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayNoProgressDeadline, Generation: generation}
		case relayTimerRecovery:
			event = clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayRecoveryDeadline, Generation: generation}
		case relayTimerClosing:
			event = clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayClosingDeadline, Generation: generation}
		case relayTimerReset:
			event = clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayResetDeadline, Generation: generation}
		}
		instance.emit(clientFlowRelayTimer{key: key, event: event})
	})
}

func (instance *clientFlow) consumeRelayTimer(key relayTimerKey) {
	delete(instance.relayTimers, key)
}

func (instance *clientFlow) cancelRelayTimer(kind relayTimerKind, generation uint64) {
	key := relayTimerKey{kind: kind, generation: generation}
	if timer := instance.relayTimers[key]; timer != nil {
		timer.Stop()
		delete(instance.relayTimers, key)
	}
}

func (instance *clientFlow) signalResult(result protocol.OpenResultCode) {
	if result == protocol.OpenSuccess && instance.relay == nil {
		result = protocol.OpenInternalFailure
	}
	instance.resultOnce.Do(func() { instance.openResult <- result })
}

func (instance *clientFlow) cleanup() {
	instance.signalResult(protocol.OpenInternalFailure)
	for key, timer := range instance.openJoinTimers {
		timer.Stop()
		delete(instance.openJoinTimers, key)
	}
	for key, timer := range instance.relayTimers {
		timer.Stop()
		delete(instance.relayTimers, key)
	}
	if instance.recoveringSlot {
		<-instance.host.recoveringSlots
		instance.recoveringSlot = false
	}
	instance.host.flowsMu.Lock()
	if instance.flowID != (protocol.FlowID{}) {
		if instance.host.flows[instance.flowID] == instance {
			delete(instance.host.flows, instance.flowID)
		}
	}
	delete(instance.host.actors, instance)
	instance.host.flowsMu.Unlock()
	instance.cancel()
	instance.doneOnce.Do(func() {
		close(instance.done)
		<-instance.host.flowSlots
	})
}

func (instance *clientFlow) close() {
	if instance != nil {
		instance.requestCancel(protocol.ResetCancelled)
	}
}
