package daemon

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	servercore "github.com/adrianceding/via/internal/server"
	statusapi "github.com/adrianceding/via/internal/status"
)

type relayTimerKind uint8

const (
	relayTimerRetry relayTimerKind = iota + 1
	relayTimerNoProgress
	relayTimerRecovery
	relayTimerClosing
	relayTimerReset
)

type relayTimerKey struct {
	kind       relayTimerKind
	generation uint64
}

type serverRelayTimer struct {
	generation uint64
	timer      *time.Timer
	done       sync.Once
}

type serverFlow struct {
	host          *serverDaemon
	key           servercore.FlowKey
	owner         *flow.Flow
	relay         *servercore.Relay
	target        *servercore.TargetIOExecutor
	destination   protocol.Target
	mode          protocol.DeliveryMode
	selection     protocol.PathSelection
	correlationID string
	ctx           context.Context
	cancel        context.CancelFunc

	mu             sync.Mutex
	timersMu       sync.Mutex
	timers         map[relayTimerKind]*serverRelayTimer
	timersClosed   bool
	terminal       bool
	recoveringSlot bool
	statusActive   bool
	lastReason     statusapi.TransitionReason
}

func newServerFlow(host *serverDaemon, key servercore.FlowKey, flowID protocol.FlowID, destination protocol.Target, mode protocol.DeliveryMode, selection protocol.PathSelection, constraints protocol.DeliveryConstraints, owner *flow.Flow, connection net.Conn) (*serverFlow, error) {
	if host == nil || owner == nil || connection == nil {
		return nil, ErrWireProtocol
	}
	relay, err := servercore.NewRelay(flowID, policy.Config{
		Mode: mode, Selection: selection, Constraints: constraints,
	}, owner)
	if err != nil {
		return nil, err
	}
	target, err := servercore.NewTargetIOExecutor(connection)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(host.runtimeCtx)
	principalKey, ok := host.principalKeys[key.PrincipalID]
	if !ok {
		cancel()
		return nil, ErrWireProtocol
	}
	correlationID, err := auth.DeriveFlowID(principalKey, key.PrincipalID, flowID)
	if err != nil {
		cancel()
		return nil, ErrWireProtocol
	}
	instance := &serverFlow{
		host: host, key: key, owner: owner, relay: relay, target: target,
		destination: destination, mode: mode, selection: selection, correlationID: correlationID.String(),
		ctx: ctx, cancel: cancel, timers: make(map[relayTimerKind]*serverRelayTimer, 5),
	}
	return instance, nil
}

func (instance *serverFlow) start() error {
	if instance == nil {
		return ErrWireProtocol
	}
	return instance.handle(servercore.RelayEvent{Kind: servercore.RelayStart})
}

func (instance *serverFlow) handle(event servercore.RelayEvent) error {
	if instance == nil {
		return ErrWireProtocol
	}
	instance.mu.Lock()
	if instance.terminal {
		instance.mu.Unlock()
		return nil
	}
	beforeState := instance.owner.LifecycleState()
	actions, err := instance.relay.Handle(event)
	reason := statusReasonForRelayEvent(event, err)
	if actionReason := serverStatusReasonFromRelayActions(actions); actionReason != statusapi.ReasonNone &&
		(reason == statusapi.ReasonNone || err != nil) {
		reason = actionReason
	}
	reason = stableServerRelayReason(beforeState, reason)
	followups, lost, closes := instance.finishTransitionLocked(actions, reason)
	instance.mu.Unlock()
	instance.runEffects(followups, lost, closes)
	return err
}

func (instance *serverFlow) snapshot() servercore.RelaySnapshot {
	if instance == nil {
		return servercore.RelaySnapshot{}
	}
	instance.mu.Lock()
	snapshot := instance.relay.Snapshot()
	instance.mu.Unlock()
	return snapshot
}

func (instance *serverFlow) finishTransitionLocked(actions []servercore.RelayAction, reason statusapi.TransitionReason) ([]servercore.RelayEvent, []flow.AttachmentKey, []servercore.RelayAction) {
	instance.recordRelayCountersLocked(actions)
	flowStatus := instance.owner.StatusSnapshot()
	state := flowStatus.LifecycleState
	terminal := state == flow.Closed || state == flow.Reset
	if terminal {
		instance.terminal = true
	}
	if reason != statusapi.ReasonNone {
		instance.lastReason = reason
	}
	effectiveReason := instance.lastReason
	instance.host.registry.UpdateLifecycle(instance.key, instance.owner, state)
	followups, lost, closes := instance.executeLocked(actions)
	if !instance.syncRecoveringSlotLocked(state) {
		followups = append(followups, servercore.RelayEvent{
			Kind: servercore.RelayResetRequested, ResetReason: protocol.ResetResourceLimit,
		})
	}
	if terminal {
		kind := servercore.TerminalReset
		terminalState := statusapi.FlowReset
		if state == flow.Closed {
			kind = servercore.TerminalClosed
			terminalState = statusapi.FlowClosed
		}
		instance.host.registry.MarkTerminal(instance.key, instance.owner, kind)
		instance.host.statusObserver.setTombstones(uint64(instance.host.registry.Snapshot().Tombstones))
		if instance.statusActive {
			instance.host.statusObserver.terminalFlow(instance.key.FlowID, terminalState, effectiveReason)
		}
		instance.stopTimers()
		instance.cancel()
		instance.host.removeFlow(instance.key, instance)
	} else if instance.statusActive {
		flowStatus, policyStatus := instance.relay.StatusSnapshot()
		instance.host.statusObserver.upsertFlowObservation(
			instance.key.FlowID, instance.destination, instance.mode, instance.selection,
			runtimeFlowObservation{
				correlationID: instance.correlationID, lifecycleState: flowStatus.LifecycleState,
				adaptiveState: policyStatus.State, adaptiveTransition: policyStatus.Transition,
				publishedAttachments: flowStatus.PublishedAttachments, policyAttachments: policyStatus.Attachments,
				preferredAttachment: policyStatus.Preferred, hasPreferred: policyStatus.HasPreferred,
				txAllocatedOffset: flowStatus.TxAllocatedOffset, txAcknowledged: flowStatus.TxAcknowledged,
				rxWrittenOffset: flowStatus.RxWrittenOffset,
			}, effectiveReason,
		)
	}
	return followups, lost, closes
}

func (instance *serverFlow) recordRelayCountersLocked(actions []servercore.RelayAction) {
	if instance == nil || !instance.statusActive || instance.host.statusObserver == nil {
		return
	}
	type attemptKey struct {
		item       uint64
		generation uint64
	}
	copies := make(map[attemptKey]uint8, flow.MaxAttachments)
	var retransmitted, redundant uint64
	for _, action := range actions {
		if action.Kind != servercore.RelayActionSendMessage {
			continue
		}
		if action.SendDataBytes == 0 {
			continue
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
	instance.host.statusObserver.recordFlowTraffic(instance.key.FlowID, retransmitted, redundant)
}

func (instance *serverFlow) activateStatus() {
	if instance == nil {
		return
	}
	instance.mu.Lock()
	if !instance.terminal && !instance.statusActive {
		instance.statusActive = true
		instance.lastReason = statusapi.ReasonStarted
		flowStatus, policyStatus := instance.relay.StatusSnapshot()
		instance.host.statusObserver.upsertFlowObservation(
			instance.key.FlowID, instance.destination, instance.mode, instance.selection,
			runtimeFlowObservation{
				correlationID: instance.correlationID, lifecycleState: flowStatus.LifecycleState,
				adaptiveState: policyStatus.State, adaptiveTransition: policyStatus.Transition,
				publishedAttachments: flowStatus.PublishedAttachments, policyAttachments: policyStatus.Attachments,
				preferredAttachment: policyStatus.Preferred, hasPreferred: policyStatus.HasPreferred,
				txAllocatedOffset: flowStatus.TxAllocatedOffset, txAcknowledged: flowStatus.TxAcknowledged,
				rxWrittenOffset: flowStatus.RxWrittenOffset,
			}, instance.lastReason,
		)
	}
	instance.mu.Unlock()
}

func (instance *serverFlow) syncRecoveringSlotLocked(state flow.LifecycleState) bool {
	if state == flow.Recovering && !instance.recoveringSlot {
		select {
		case instance.host.recoveringSlots <- struct{}{}:
			instance.recoveringSlot = true
		default:
			return false
		}
	}
	if state != flow.Recovering && instance.recoveringSlot {
		<-instance.host.recoveringSlots
		instance.recoveringSlot = false
	}
	return true
}

func (instance *serverFlow) executeLocked(actions []servercore.RelayAction) ([]servercore.RelayEvent, []flow.AttachmentKey, []servercore.RelayAction) {
	var followups []servercore.RelayEvent
	var lost []flow.AttachmentKey
	var closes []servercore.RelayAction
	for _, action := range actions {
		switch action.Kind {
		case servercore.RelayActionReadTarget, servercore.RelayActionWriteTarget, servercore.RelayActionCloseWriteTarget:
			if !instance.host.startWorker(func() { instance.executeTarget(action) }) {
				followups = append(followups, servercore.RelayEvent{Kind: servercore.RelayResetRequested, ResetReason: protocol.ResetCancelled})
			}
		case servercore.RelayActionCloseTarget:
			closes = append(closes, action)
		case servercore.RelayActionSendMessage:
			if !instance.host.startWorker(func() { instance.executeSend(action) }) {
				followups = append(followups, servercore.RelayEvent{
					Kind: servercore.RelaySendResult, Generation: action.Generation, AttemptOutcome: flow.AttemptFailed,
				})
			}
		case servercore.RelayActionArmRetryDeadline:
			instance.armTimer(relayTimerRetry, action.Generation, action.After)
		case servercore.RelayActionCancelRetryDeadline:
			instance.cancelTimer(relayTimerRetry, action.Generation)
		case servercore.RelayActionArmNoProgressDeadline:
			instance.armTimer(relayTimerNoProgress, action.Generation, 30*time.Second)
		case servercore.RelayActionCancelNoProgressDeadline:
			instance.cancelTimer(relayTimerNoProgress, action.Generation)
		case servercore.RelayActionArmRecoveryDeadline:
			instance.armTimer(relayTimerRecovery, action.Generation, 30*time.Second)
		case servercore.RelayActionCancelRecoveryDeadline:
			instance.cancelTimer(relayTimerRecovery, action.Generation)
		case servercore.RelayActionArmClosingDeadline:
			instance.armTimer(relayTimerClosing, action.Generation, 60*time.Second)
		case servercore.RelayActionCancelClosingDeadline:
			instance.cancelTimer(relayTimerClosing, action.Generation)
		case servercore.RelayActionArmResetDeadline:
			instance.armTimer(relayTimerReset, action.Generation, sendTimeout)
		case servercore.RelayActionCancelResetDeadline:
			instance.cancelTimer(relayTimerReset, action.Generation)
		case servercore.RelayActionAttachmentPublished:
			session := instance.host.session(action.Attachment.SessionGeneration)
			if session == nil || session.publish(instance.key.FlowID, action.Attachment) != nil {
				lost = append(lost, action.Attachment)
			}
		case servercore.RelayActionAttachmentWithdrawn:
			if session := instance.host.session(action.Attachment.SessionGeneration); session != nil {
				session.release(instance.key.FlowID, action.Attachment)
			}
		}
	}
	return followups, lost, closes
}

func (instance *serverFlow) runEffects(events []servercore.RelayEvent, lost []flow.AttachmentKey, closes []servercore.RelayAction) {
	for _, action := range closes {
		_, _, _ = instance.target.Execute(action)
	}
	for _, event := range events {
		_ = instance.handle(event)
	}
	for _, attachment := range lost {
		instance.attachmentLost(attachment)
	}
}

func (instance *serverFlow) executeTarget(action servercore.RelayAction) {
	event, hasResult, err := instance.target.Execute(action)
	if err != nil {
		_ = instance.handle(servercore.RelayEvent{Kind: servercore.RelayResetRequested, ResetReason: protocol.ResetLocalIOFailure})
		return
	}
	if hasResult {
		_ = instance.handle(event)
	}
}

func (instance *serverFlow) executeSend(action servercore.RelayAction) {
	outcome := flow.AttemptFailed
	session := instance.host.session(action.Attachment.SessionGeneration)
	if session != nil {
		if attachment, ok := session.attachment(instance.key.FlowID); ok && attachment == action.Attachment {
			var err error
			if len(action.Encoded) != 0 {
				err = session.sendEncodedContext(instance.ctx, action.Class, action.Encoded)
			} else {
				err = session.sendContext(instance.ctx, action.Message)
			}
			if err == nil {
				outcome = flow.AttemptSucceeded
			}
		}
	}
	_ = instance.handle(servercore.RelayEvent{
		Kind: servercore.RelaySendResult, Generation: action.Generation, AttemptOutcome: outcome,
	})
}

func (instance *serverFlow) remote(message protocol.Message, attachment flow.AttachmentKey) error {
	err := instance.handle(servercore.RelayEvent{
		Kind: servercore.RelayRemoteMessage, Message: message, Attachment: attachment,
	})
	if err != nil {
		log.Printf("server flow rejected remote %s message: %s", remoteMessageCategory(message), flowErrorCategory(err))
	}
	if err != nil && !errors.Is(err, servercore.ErrInvalidRelayEvent) {
		_ = instance.handle(servercore.RelayEvent{Kind: servercore.RelayResetRequested, ResetReason: protocol.ResetInternalFailure})
	}
	// A data-flow semantic failure resets only this Flow; it must not close the
	// shared authenticated transport session used by unrelated Flows.
	return nil
}

func remoteMessageCategory(message protocol.Message) string {
	switch message.(type) {
	case protocol.Data:
		return "data"
	case protocol.ACK:
		return "acknowledgement"
	case protocol.FIN:
		return "finish"
	case protocol.FINACK:
		return "finish acknowledgement"
	case protocol.Reset:
		return "reset"
	default:
		return "unknown"
	}
}

func flowErrorCategory(err error) string {
	categories := []struct {
		target error
		name   string
	}{
		{flow.ErrWindowExceeded, "receive window exceeded"},
		{flow.ErrRangeLimit, "reorder range limit exceeded"},
		{flow.ErrDataConflict, "overlapping data conflict"},
		{flow.ErrACKBeyondAllocated, "acknowledgement exceeds allocated offset"},
		{flow.ErrACKNotCanonical, "non-canonical acknowledgement ranges"},
		{flow.ErrFinalOffsetConflict, "final offset conflict"},
		{flow.ErrDataBeyondFinal, "data exceeds final offset"},
		{flow.ErrInvalidState, "message rejected in current state"},
		{flow.ErrAttachmentUnknown, "unknown attachment"},
		{servercore.ErrRelaySendLimit, "pending send action limit exceeded"},
		{servercore.ErrRelayAttemptLimit, "send attempt record limit exceeded"},
		{servercore.ErrRelayGenerationExhaust, "event generation exhausted"},
		{servercore.ErrInvalidTargetRead, "invalid target read result"},
		{servercore.ErrInvalidTargetWrite, "invalid target write result"},
		{servercore.ErrInvalidRelayEvent, "invalid relay event"},
		{servercore.ErrInvalidRelay, "invalid relay state"},
		{policy.ErrInvalidSample, "invalid path quality sample"},
		{policy.ErrUnknownAttachment, "unknown path quality attachment"},
	}
	for _, category := range categories {
		if errors.Is(err, category.target) {
			return category.name
		}
	}
	return "internal error"
}

func (instance *serverFlow) beginJoin(attachment flow.AttachmentKey) (uint64, bool) {
	instance.mu.Lock()
	if instance.terminal {
		instance.mu.Unlock()
		return 0, false
	}
	actions, err := instance.owner.Handle(flow.FlowEvent{
		Kind:      flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleJoinRequested, Attachment: attachment},
	})
	instance.mu.Unlock()
	if err != nil {
		return 0, false
	}
	action, ok := joinLifecycleAction(actions, flow.LifecycleActionSendJoinSuccess)
	return action.Generation, ok && action.Generation != 0
}

func (instance *serverFlow) completeJoin(attachment flow.AttachmentKey, generation uint64, succeeded bool) bool {
	kind := flow.LifecycleJoinResultSendFailed
	if succeeded {
		kind = flow.LifecycleJoinResultSendCompleted
	}
	instance.applyLifecycle(flow.LifecycleEvent{
		Kind: kind, Attachment: attachment, Generation: generation,
	})
	instance.mu.Lock()
	published := false
	for _, current := range instance.relay.Snapshot().Flow.Lifecycle.Published {
		if current == attachment {
			published = true
			break
		}
	}
	instance.mu.Unlock()
	return published
}

func (instance *serverFlow) attachmentLost(attachment flow.AttachmentKey) {
	instance.applyLifecycle(flow.LifecycleEvent{Kind: flow.LifecycleAttachmentLost, Attachment: attachment})
}

func (instance *serverFlow) sessionClosed(generation uint64) {
	instance.applyLifecycle(flow.LifecycleEvent{
		Kind: flow.LifecycleSessionClosed, Attachment: flow.AttachmentKey{SessionGeneration: generation},
	})
}

func (instance *serverFlow) observeProbe(attachment flow.AttachmentKey, rtt time.Duration) {
	_ = instance.handle(servercore.RelayEvent{
		Kind: servercore.RelayObserveProbeQuality, Attachment: attachment, RTT: rtt,
	})
}

func (instance *serverFlow) setStallPenalty(attachment flow.AttachmentKey, penalty time.Duration) {
	_ = instance.handle(servercore.RelayEvent{
		Kind: servercore.RelaySetStallPenalty, Attachment: attachment, StallPenalty: penalty,
	})
}

func (instance *serverFlow) applyLifecycle(event flow.LifecycleEvent) {
	if instance == nil {
		return
	}
	instance.mu.Lock()
	if instance.terminal {
		instance.mu.Unlock()
		return
	}
	flowActions, flowErr := instance.owner.Handle(flow.FlowEvent{
		Kind:      flow.FlowLifecycle,
		Lifecycle: event,
	})
	relayActions, relayErr := instance.relay.Handle(servercore.RelayEvent{Kind: servercore.RelayApplyFlowActions, FlowActions: flowActions})
	if event.Kind == flow.LifecycleJoinResultSendCompleted && relayErr == nil {
		if session := instance.host.session(event.Attachment.SessionGeneration); session != nil {
			rtt, stall := session.probeQuality()
			if rtt > 0 {
				qualityActions, qualityErr := instance.relay.Handle(servercore.RelayEvent{
					Kind: servercore.RelayObserveProbeQuality, Attachment: event.Attachment, RTT: rtt,
				})
				relayActions = append(relayActions, qualityActions...)
				relayErr = errors.Join(relayErr, qualityErr)
			}
			if stall > 0 {
				qualityActions, qualityErr := instance.relay.Handle(servercore.RelayEvent{
					Kind: servercore.RelaySetStallPenalty, Attachment: event.Attachment, StallPenalty: stall,
				})
				relayActions = append(relayActions, qualityActions...)
				relayErr = errors.Join(relayErr, qualityErr)
			}
		}
	}
	reason := statusReasonForLifecycleEvent(event, errors.Join(flowErr, relayErr))
	followups, lost, closes := instance.finishTransitionLocked(relayActions, reason)
	instance.mu.Unlock()
	instance.runEffects(followups, lost, closes)
	if flowErr != nil || relayErr != nil {
		_ = instance.handle(servercore.RelayEvent{Kind: servercore.RelayResetRequested, ResetReason: protocol.ResetInternalFailure})
	}
}

func (instance *serverFlow) armTimer(kind relayTimerKind, generation uint64, duration time.Duration) {
	if generation == 0 || duration <= 0 {
		return
	}
	instance.timersMu.Lock()
	if instance.timersClosed {
		instance.timersMu.Unlock()
		return
	}
	if current, exists := instance.timers[kind]; exists && current.generation == generation {
		instance.timersMu.Unlock()
		return
	}
	if current, exists := instance.timers[kind]; exists {
		if current.timer.Stop() {
			instance.finishTimer(current)
		}
	}
	current := &serverRelayTimer{generation: generation}
	instance.host.timerCallbacks.Add(1)
	current.timer = time.AfterFunc(duration, func() {
		defer instance.finishTimer(current)
		instance.timersMu.Lock()
		active, exists := instance.timers[kind]
		if !exists || active != current {
			instance.timersMu.Unlock()
			return
		}
		delete(instance.timers, kind)
		instance.timersMu.Unlock()
		var event servercore.RelayEvent
		switch kind {
		case relayTimerRetry:
			event = servercore.RelayEvent{Kind: servercore.RelayRetryDeadline, Generation: generation}
		case relayTimerNoProgress:
			event = servercore.RelayEvent{Kind: servercore.RelayNoProgressDeadline, Generation: generation}
		case relayTimerRecovery:
			event = servercore.RelayEvent{Kind: servercore.RelayRecoveryDeadline, Generation: generation}
		case relayTimerClosing:
			event = servercore.RelayEvent{Kind: servercore.RelayClosingDeadline, Generation: generation}
		case relayTimerReset:
			event = servercore.RelayEvent{Kind: servercore.RelayResetDeadline, Generation: generation}
		}
		_ = instance.handle(event)
	})
	instance.timers[kind] = current
	instance.timersMu.Unlock()
}

func (instance *serverFlow) cancelTimer(kind relayTimerKind, generation uint64) {
	instance.timersMu.Lock()
	if current, exists := instance.timers[kind]; exists && current.generation == generation {
		if current.timer.Stop() {
			instance.finishTimer(current)
		}
		delete(instance.timers, kind)
	}
	instance.timersMu.Unlock()
}

func (instance *serverFlow) stopTimers() {
	instance.timersMu.Lock()
	instance.timersClosed = true
	for kind, current := range instance.timers {
		if current.timer.Stop() {
			instance.finishTimer(current)
		}
		delete(instance.timers, kind)
	}
	instance.timersMu.Unlock()
}

func (instance *serverFlow) finishTimer(timer *serverRelayTimer) {
	if instance == nil || timer == nil || instance.host == nil {
		return
	}
	timer.done.Do(instance.host.timerCallbacks.Done)
}

func (instance *serverFlow) close() {
	if instance == nil {
		return
	}
	_ = instance.handle(servercore.RelayEvent{Kind: servercore.RelayResetRequested, ResetReason: protocol.ResetCancelled})
	instance.mu.Lock()
	resetGeneration := instance.relay.Snapshot().Flow.Lifecycle.ResetDeadlineGeneration
	instance.mu.Unlock()
	if resetGeneration != 0 {
		_ = instance.handle(servercore.RelayEvent{Kind: servercore.RelayResetDeadline, Generation: resetGeneration})
	}
	instance.stopTimers()
	instance.cancel()
}

func (instance *serverFlow) abort() {
	if instance == nil {
		return
	}
	instance.mu.Lock()
	instance.syncRecoveringSlotLocked(flow.Reset)
	instance.mu.Unlock()
	instance.stopTimers()
	instance.cancel()
}

func statusReasonForRelayEvent(event servercore.RelayEvent, eventErr error) statusapi.TransitionReason {
	switch event.Kind {
	case servercore.RelayStart:
		return statusapi.ReasonStarted
	case servercore.RelayNoProgressDeadline, servercore.RelayRecoveryDeadline:
		return statusapi.ReasonDeadlineExceeded
	case servercore.RelayClosingDeadline:
		return statusapi.ReasonCompleted
	case servercore.RelayResetRequested:
		return statusReasonForReset(event.ResetReason)
	case servercore.RelayTargetReadResult:
		if eventErr != nil || event.Err != nil && !errors.Is(event.Err, io.EOF) {
			return statusapi.ReasonLocalIOFailure
		}
	case servercore.RelayTargetWriteResult, servercore.RelayTargetCloseWriteResult:
		if eventErr != nil || event.Err != nil {
			return statusapi.ReasonLocalIOFailure
		}
	case servercore.RelayRemoteMessage:
		if _, ok := event.Message.(protocol.Reset); ok {
			return statusapi.ReasonRemoteReset
		}
		if eventErr != nil {
			return statusapi.ReasonProtocolConflict
		}
	}
	return statusapi.ReasonNone
}

func stableServerRelayReason(before flow.LifecycleState, reason statusapi.TransitionReason) statusapi.TransitionReason {
	if before == flow.Resetting {
		return statusapi.ReasonNone
	}
	return reason
}

func statusReasonForLifecycleEvent(event flow.LifecycleEvent, eventErr error) statusapi.TransitionReason {
	switch event.Kind {
	case flow.LifecycleJoinResultSendCompleted:
		return statusapi.ReasonPathAdded
	case flow.LifecycleAttachmentLost, flow.LifecycleSessionClosed:
		return statusapi.ReasonPathRemoved
	case flow.LifecycleRecoveryDeadline:
		return statusapi.ReasonDeadlineExceeded
	case flow.LifecycleClosingDeadline:
		return statusapi.ReasonCompleted
	case flow.LifecycleResetRequested:
		return statusReasonForReset(event.Reason)
	case flow.LifecycleRemoteReset:
		return statusapi.ReasonRemoteReset
	}
	if eventErr != nil {
		return statusapi.ReasonProtocolConflict
	}
	return statusapi.ReasonNone
}

func statusReasonForReset(reason protocol.ResetReason) statusapi.TransitionReason {
	switch reason {
	case protocol.ResetCancelled:
		return statusapi.ReasonCancelled
	case protocol.ResetProtocolConflict:
		return statusapi.ReasonProtocolConflict
	case protocol.ResetResourceLimit:
		return statusapi.ReasonResourceLimit
	case protocol.ResetLocalIOFailure:
		return statusapi.ReasonLocalIOFailure
	case protocol.ResetDeadlineExceeded:
		return statusapi.ReasonDeadlineExceeded
	case protocol.ResetInternalFailure:
		return statusapi.ReasonInternalFailure
	default:
		return statusapi.ReasonNone
	}
}

func serverStatusReasonFromRelayActions(actions []servercore.RelayAction) statusapi.TransitionReason {
	for _, action := range actions {
		reset, ok := action.Message.(protocol.Reset)
		if ok {
			return statusReasonForReset(reset.Reason)
		}
	}
	return statusapi.ReasonNone
}

func joinLifecycleAction(actions []flow.FlowAction, kind flow.LifecycleActionKind) (flow.LifecycleAction, bool) {
	for _, action := range actions {
		if action.Kind == flow.FlowActionLifecycle && action.Lifecycle.Kind == kind {
			return action.Lifecycle, true
		}
	}
	return flow.LifecycleAction{}, false
}
