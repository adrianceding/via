package flow

import (
	"sort"

	"github.com/adrianceding/via/internal/protocol"
)

// LifecycleEventKind identifies an external event accepted by the flow lifecycle owner.
type LifecycleEventKind uint8

const (
	LifecycleStarted LifecycleEventKind = iota + 1
	LifecycleJoinRequested
	LifecycleJoinResultSendCompleted
	LifecycleJoinResultSendFailed
	LifecycleAttachmentLost
	LifecycleSessionClosed
	LifecycleRecoveryDeadline
	LifecycleDirectionsComplete
	LifecycleClosingDeadline
	LifecycleResetRequested
	LifecycleResetSendCompleted
	LifecycleResetSendFailed
	LifecycleResetDeadline
	LifecycleRemoteReset
)

// LifecycleEvent is one input to the lifecycle state machine. Attachment applies
// only to JOIN and attachment events; Generation applies to JOIN_RESULT, deadline,
// and RESET send results.
type LifecycleEvent struct {
	Kind       LifecycleEventKind
	Attachment AttachmentKey
	Generation uint64
	Reason     protocol.ResetReason
}

// LifecycleActionKind identifies an external action the coordinator must execute in return order.
type LifecycleActionKind uint8

const (
	LifecycleActionSendJoinSuccess LifecycleActionKind = iota + 1
	LifecycleActionSendJoinFailure
	LifecycleActionPublishAttachment
	LifecycleActionWithdrawAttachment
	LifecycleActionSendAcknowledgementSnapshot
	LifecycleActionPauseLocalRead
	LifecycleActionResumeLocalRead
	LifecycleActionCloseLocal
	LifecycleActionArmRecoveryDeadline
	LifecycleActionCancelRecoveryDeadline
	LifecycleActionArmClosingDeadline
	LifecycleActionCancelClosingDeadline
	LifecycleActionSendReset
	LifecycleActionArmResetDeadline
	LifecycleActionCancelResetDeadline
)

// LifecycleAction describes work the state owner requires the executor to perform.
type LifecycleAction struct {
	Kind       LifecycleActionKind
	Attachment AttachmentKey
	Generation uint64
	Reason     protocol.ResetReason
}

// LifecycleSnapshot is a deterministic lifecycle snapshot with no mutable internal collections.
type LifecycleSnapshot struct {
	State                      LifecycleState
	RecoveryReturnState        LifecycleState
	Published                  []AttachmentKey
	Provisional                []AttachmentKey
	RecoveryDeadlineGeneration uint64
	ClosingDeadlineGeneration  uint64
	ResetDeadlineGeneration    uint64
	ResetAttemptGeneration     uint64
}

type joinReplyState uint8

const (
	joinReplyProvisional joinReplyState = iota + 1
	joinReplyPublishedDuplicate
)

type joinReply struct {
	state      joinReplyState
	generation uint64
}

// Lifecycle is the pure lifecycle state machine for one flow. A single state owner
// must call Handle serially; this type performs no network I/O and reads no wall clock.
type Lifecycle struct {
	state               LifecycleState
	recoveryReturnState LifecycleState
	published           map[AttachmentKey]struct{}
	joinReplies         map[AttachmentKey]joinReply
	nextGeneration      uint64
	recoveryDeadline    uint64
	closingDeadline     uint64
	resetDeadline       uint64
	resetAttempt        uint64
	localReadPaused     bool
	localCloseIssued    bool
}

func NewLifecycle() *Lifecycle {
	return &Lifecycle{
		state:       AwaitingAttachment,
		published:   make(map[AttachmentKey]struct{}, MaxAttachments),
		joinReplies: make(map[AttachmentKey]joinReply, MaxAttachments),
		// Do not read application or target bytes before the first JOIN is published.
		// Publication emits the action that resumes local reads.
		localReadPaused: true,
	}
}

func (lifecycle *Lifecycle) State() LifecycleState {
	if lifecycle == nil {
		return Reset
	}
	return lifecycle.state
}

func (lifecycle *Lifecycle) IsPublished(attachment AttachmentKey) bool {
	if lifecycle == nil {
		return false
	}
	_, published := lifecycle.published[attachment]
	return published
}

func (lifecycle *Lifecycle) HasPublishedAttachments() bool {
	return lifecycle != nil && len(lifecycle.published) != 0
}

func (lifecycle *Lifecycle) PublishedAttachmentCount() int {
	if lifecycle == nil {
		return 0
	}
	return len(lifecycle.published)
}

func (lifecycle *Lifecycle) Snapshot() LifecycleSnapshot {
	if lifecycle == nil {
		return LifecycleSnapshot{State: Reset}
	}
	snapshot := LifecycleSnapshot{
		State:                      lifecycle.state,
		RecoveryReturnState:        lifecycle.recoveryReturnState,
		RecoveryDeadlineGeneration: lifecycle.recoveryDeadline,
		ClosingDeadlineGeneration:  lifecycle.closingDeadline,
		ResetDeadlineGeneration:    lifecycle.resetDeadline,
		ResetAttemptGeneration:     lifecycle.resetAttempt,
	}
	for attachment := range lifecycle.published {
		snapshot.Published = append(snapshot.Published, attachment)
	}
	for attachment, reply := range lifecycle.joinReplies {
		if reply.state == joinReplyProvisional {
			snapshot.Provisional = append(snapshot.Provisional, attachment)
		}
	}
	sortAttachmentKeys(snapshot.Published)
	sortAttachmentKeys(snapshot.Provisional)
	return snapshot
}

func sortAttachmentKeys(attachments []AttachmentKey) {
	sort.Slice(attachments, func(i, j int) bool {
		if attachments[i].SessionGeneration != attachments[j].SessionGeneration {
			return attachments[i].SessionGeneration < attachments[j].SessionGeneration
		}
		return attachments[i].AttachmentGeneration < attachments[j].AttachmentGeneration
	})
}

func (lifecycle *Lifecycle) nextActionGeneration() uint64 {
	if lifecycle.nextGeneration == ^uint64(0) {
		panic("flow lifecycle action generation exhausted")
	}
	lifecycle.nextGeneration++
	return lifecycle.nextGeneration
}

// Handle applies one event and returns actions in coordinator execution order.
// Results and deadline events carrying an inactive generation are ignored.
func (lifecycle *Lifecycle) Handle(event LifecycleEvent) []LifecycleAction {
	if lifecycle == nil || lifecycle.state == Closed || lifecycle.state == Reset {
		return nil
	}

	switch event.Kind {
	case LifecycleRemoteReset:
		return lifecycle.enterTerminal(Reset, false)
	case LifecycleResetRequested:
		if lifecycle.state == Resetting {
			return nil
		}
		return lifecycle.enterResetting(normalizeResetReason(event.Reason), false, false)
	case LifecycleAttachmentLost:
		return lifecycle.loseAttachment(event.Attachment)
	case LifecycleSessionClosed:
		return lifecycle.closeSession(event.Attachment.SessionGeneration)
	}

	switch lifecycle.state {
	case AwaitingAttachment:
		return lifecycle.handleAwaitingAttachment(event)
	case Relaying:
		return lifecycle.handleRelaying(event)
	case Recovering:
		return lifecycle.handleRecovering(event)
	case Closing:
		return lifecycle.handleClosing(event)
	case Resetting:
		return lifecycle.handleResetting(event)
	default:
		return nil
	}
}

func (lifecycle *Lifecycle) handleAwaitingAttachment(event LifecycleEvent) []LifecycleAction {
	switch event.Kind {
	case LifecycleStarted:
		return lifecycle.armRecoveryDeadline()
	case LifecycleJoinRequested:
		return lifecycle.beginJoin(event.Attachment)
	case LifecycleJoinResultSendCompleted:
		return lifecycle.completeJoin(event.Attachment, event.Generation)
	case LifecycleJoinResultSendFailed:
		lifecycle.failJoin(event.Attachment, event.Generation)
	case LifecycleRecoveryDeadline:
		if event.Generation == 0 || event.Generation != lifecycle.recoveryDeadline {
			return nil
		}
		lifecycle.recoveryDeadline = 0
		return lifecycle.enterResetting(protocol.ResetDeadlineExceeded, true, false)
	case LifecycleDirectionsComplete:
		return lifecycle.enterClosing()
	}
	return nil
}

func (lifecycle *Lifecycle) handleRelaying(event LifecycleEvent) []LifecycleAction {
	switch event.Kind {
	case LifecycleJoinRequested:
		return lifecycle.beginJoin(event.Attachment)
	case LifecycleJoinResultSendCompleted:
		return lifecycle.completeJoin(event.Attachment, event.Generation)
	case LifecycleJoinResultSendFailed:
		lifecycle.failJoin(event.Attachment, event.Generation)
	case LifecycleDirectionsComplete:
		return lifecycle.enterClosing()
	}
	return nil
}

func (lifecycle *Lifecycle) handleRecovering(event LifecycleEvent) []LifecycleAction {
	switch event.Kind {
	case LifecycleJoinRequested:
		return lifecycle.beginJoin(event.Attachment)
	case LifecycleJoinResultSendCompleted:
		return lifecycle.completeJoin(event.Attachment, event.Generation)
	case LifecycleJoinResultSendFailed:
		lifecycle.failJoin(event.Attachment, event.Generation)
	case LifecycleRecoveryDeadline:
		if event.Generation == 0 || event.Generation != lifecycle.recoveryDeadline {
			return nil
		}
		lifecycle.recoveryDeadline = 0
		return lifecycle.enterResetting(protocol.ResetDeadlineExceeded, true, false)
	case LifecycleDirectionsComplete:
		return lifecycle.enterClosing()
	}
	return nil
}

func (lifecycle *Lifecycle) handleClosing(event LifecycleEvent) []LifecycleAction {
	switch event.Kind {
	case LifecycleJoinRequested:
		return lifecycle.beginJoin(event.Attachment)
	case LifecycleJoinResultSendCompleted:
		return lifecycle.completeJoin(event.Attachment, event.Generation)
	case LifecycleJoinResultSendFailed:
		lifecycle.failJoin(event.Attachment, event.Generation)
	case LifecycleClosingDeadline:
		if event.Generation == 0 || event.Generation != lifecycle.closingDeadline {
			return nil
		}
		lifecycle.closingDeadline = 0
		return lifecycle.enterTerminal(Closed, false)
	}
	return nil
}

func (lifecycle *Lifecycle) handleResetting(event LifecycleEvent) []LifecycleAction {
	switch event.Kind {
	case LifecycleJoinRequested:
		return []LifecycleAction{{
			Kind:       LifecycleActionSendJoinFailure,
			Attachment: event.Attachment,
			Generation: lifecycle.nextActionGeneration(),
		}}
	case LifecycleResetSendCompleted, LifecycleResetSendFailed:
		if event.Generation == 0 || event.Generation != lifecycle.resetAttempt {
			return nil
		}
		lifecycle.resetAttempt = 0
		return lifecycle.enterTerminal(Reset, false)
	case LifecycleResetDeadline:
		if event.Generation == 0 || event.Generation != lifecycle.resetDeadline {
			return nil
		}
		lifecycle.resetDeadline = 0
		return lifecycle.enterTerminal(Reset, true)
	}
	return nil
}

func (lifecycle *Lifecycle) beginJoin(attachment AttachmentKey) []LifecycleAction {
	if !validAttachmentKey(attachment) {
		return []LifecycleAction{{
			Kind:       LifecycleActionSendJoinFailure,
			Attachment: attachment,
			Generation: lifecycle.nextActionGeneration(),
		}}
	}
	if _, exists := lifecycle.joinReplies[attachment]; exists {
		// The coordinator read barrier prevents a second JOIN while this result is in flight.
		return nil
	}
	if _, published := lifecycle.published[attachment]; published {
		generation := lifecycle.nextActionGeneration()
		lifecycle.joinReplies[attachment] = joinReply{
			state:      joinReplyPublishedDuplicate,
			generation: generation,
		}
		return []LifecycleAction{{
			Kind:       LifecycleActionSendJoinSuccess,
			Attachment: attachment,
			Generation: generation,
		}}
	}
	if lifecycle.hasSession(attachment.SessionGeneration) || lifecycle.reservedAttachmentCount() >= MaxAttachments {
		return []LifecycleAction{{
			Kind:       LifecycleActionSendJoinFailure,
			Attachment: attachment,
			Generation: lifecycle.nextActionGeneration(),
		}}
	}
	generation := lifecycle.nextActionGeneration()
	lifecycle.joinReplies[attachment] = joinReply{
		state:      joinReplyProvisional,
		generation: generation,
	}
	return []LifecycleAction{{
		Kind:       LifecycleActionSendJoinSuccess,
		Attachment: attachment,
		Generation: generation,
	}}
}

func (lifecycle *Lifecycle) completeJoin(attachment AttachmentKey, generation uint64) []LifecycleAction {
	reply, exists := lifecycle.joinReplies[attachment]
	if !exists || generation == 0 || generation != reply.generation {
		return nil
	}
	delete(lifecycle.joinReplies, attachment)
	if reply.state == joinReplyPublishedDuplicate {
		return []LifecycleAction{{
			Kind:       LifecycleActionSendAcknowledgementSnapshot,
			Attachment: attachment,
		}}
	}

	lifecycle.published[attachment] = struct{}{}
	actions := []LifecycleAction{
		{Kind: LifecycleActionPublishAttachment, Attachment: attachment},
		{Kind: LifecycleActionSendAcknowledgementSnapshot, Attachment: attachment},
	}
	if lifecycle.state != AwaitingAttachment && lifecycle.state != Recovering {
		return actions
	}

	if lifecycle.recoveryDeadline != 0 {
		actions = append(actions, LifecycleAction{
			Kind:       LifecycleActionCancelRecoveryDeadline,
			Generation: lifecycle.recoveryDeadline,
		})
		lifecycle.recoveryDeadline = 0
	}
	if lifecycle.state == Recovering && lifecycle.recoveryReturnState != 0 {
		lifecycle.state = lifecycle.recoveryReturnState
	} else {
		lifecycle.state = Relaying
	}
	lifecycle.recoveryReturnState = 0
	if lifecycle.localReadPaused {
		lifecycle.localReadPaused = false
		actions = append(actions, LifecycleAction{Kind: LifecycleActionResumeLocalRead})
	}
	return actions
}

func (lifecycle *Lifecycle) failJoin(attachment AttachmentKey, generation uint64) {
	reply, exists := lifecycle.joinReplies[attachment]
	if !exists || generation == 0 || generation != reply.generation {
		return
	}
	delete(lifecycle.joinReplies, attachment)
}

func (lifecycle *Lifecycle) loseAttachment(attachment AttachmentKey) []LifecycleAction {
	if !validAttachmentKey(attachment) {
		return nil
	}
	delete(lifecycle.joinReplies, attachment)
	if _, exists := lifecycle.published[attachment]; !exists {
		return nil
	}
	delete(lifecycle.published, attachment)
	actions := []LifecycleAction{{Kind: LifecycleActionWithdrawAttachment, Attachment: attachment}}
	return lifecycle.afterPublishedLoss(actions)
}

func (lifecycle *Lifecycle) closeSession(sessionGeneration uint64) []LifecycleAction {
	if sessionGeneration == 0 {
		return nil
	}
	for attachment := range lifecycle.joinReplies {
		if attachment.SessionGeneration == sessionGeneration {
			delete(lifecycle.joinReplies, attachment)
		}
	}
	var withdrawn []AttachmentKey
	for attachment := range lifecycle.published {
		if attachment.SessionGeneration == sessionGeneration {
			delete(lifecycle.published, attachment)
			withdrawn = append(withdrawn, attachment)
		}
	}
	if len(withdrawn) == 0 {
		return nil
	}
	sortAttachmentKeys(withdrawn)
	actions := make([]LifecycleAction, 0, len(withdrawn)+2)
	for _, attachment := range withdrawn {
		actions = append(actions, LifecycleAction{
			Kind:       LifecycleActionWithdrawAttachment,
			Attachment: attachment,
		})
	}
	return lifecycle.afterPublishedLoss(actions)
}

func (lifecycle *Lifecycle) afterPublishedLoss(actions []LifecycleAction) []LifecycleAction {
	if len(lifecycle.published) != 0 || lifecycle.state != Relaying {
		return actions
	}
	lifecycle.state = Recovering
	lifecycle.recoveryReturnState = Relaying
	if !lifecycle.localReadPaused {
		lifecycle.localReadPaused = true
		actions = append(actions, LifecycleAction{Kind: LifecycleActionPauseLocalRead})
	}
	return append(actions, lifecycle.armRecoveryDeadline()...)
}

func (lifecycle *Lifecycle) armRecoveryDeadline() []LifecycleAction {
	if lifecycle.recoveryDeadline != 0 {
		return nil
	}
	lifecycle.recoveryDeadline = lifecycle.nextActionGeneration()
	return []LifecycleAction{{
		Kind:       LifecycleActionArmRecoveryDeadline,
		Generation: lifecycle.recoveryDeadline,
	}}
}

func (lifecycle *Lifecycle) enterClosing() []LifecycleAction {
	if lifecycle.state == Closing || lifecycle.state == Closed || lifecycle.state == Resetting || lifecycle.state == Reset {
		return nil
	}
	lifecycle.state = Closing
	lifecycle.recoveryReturnState = 0
	actions := make([]LifecycleAction, 0, 3)
	if lifecycle.recoveryDeadline != 0 {
		actions = append(actions, LifecycleAction{
			Kind:       LifecycleActionCancelRecoveryDeadline,
			Generation: lifecycle.recoveryDeadline,
		})
		lifecycle.recoveryDeadline = 0
	}
	if !lifecycle.localCloseIssued {
		lifecycle.localCloseIssued = true
		actions = append(actions, LifecycleAction{Kind: LifecycleActionCloseLocal})
	}
	lifecycle.closingDeadline = lifecycle.nextActionGeneration()
	actions = append(actions, LifecycleAction{
		Kind:       LifecycleActionArmClosingDeadline,
		Generation: lifecycle.closingDeadline,
	})
	return actions
}

func (lifecycle *Lifecycle) enterResetting(reason protocol.ResetReason, recoveryDeadlineFired, closingDeadlineFired bool) []LifecycleAction {
	if lifecycle.state == Resetting || lifecycle.state == Reset || lifecycle.state == Closed {
		return nil
	}
	lifecycle.state = Resetting
	lifecycle.recoveryReturnState = 0
	clear(lifecycle.joinReplies)
	actions := make([]LifecycleAction, 0, 5)
	if lifecycle.recoveryDeadline != 0 {
		if !recoveryDeadlineFired {
			actions = append(actions, LifecycleAction{
				Kind:       LifecycleActionCancelRecoveryDeadline,
				Generation: lifecycle.recoveryDeadline,
			})
		}
		lifecycle.recoveryDeadline = 0
	}
	if lifecycle.closingDeadline != 0 {
		if !closingDeadlineFired {
			actions = append(actions, LifecycleAction{
				Kind:       LifecycleActionCancelClosingDeadline,
				Generation: lifecycle.closingDeadline,
			})
		}
		lifecycle.closingDeadline = 0
	}
	if !lifecycle.localCloseIssued {
		lifecycle.localCloseIssued = true
		actions = append(actions, LifecycleAction{Kind: LifecycleActionCloseLocal})
	}
	lifecycle.resetAttempt = lifecycle.nextActionGeneration()
	lifecycle.resetDeadline = lifecycle.nextActionGeneration()
	actions = append(actions,
		LifecycleAction{
			Kind:       LifecycleActionSendReset,
			Generation: lifecycle.resetAttempt,
			Reason:     reason,
		},
		LifecycleAction{
			Kind:       LifecycleActionArmResetDeadline,
			Generation: lifecycle.resetDeadline,
		},
	)
	return actions
}

func (lifecycle *Lifecycle) enterTerminal(state LifecycleState, resetDeadlineFired bool) []LifecycleAction {
	if state != Closed && state != Reset {
		panic("flow lifecycle terminal state must be Closed or Reset")
	}
	actions := make([]LifecycleAction, 0, len(lifecycle.published)+3)
	if lifecycle.recoveryDeadline != 0 {
		actions = append(actions, LifecycleAction{
			Kind:       LifecycleActionCancelRecoveryDeadline,
			Generation: lifecycle.recoveryDeadline,
		})
	}
	if lifecycle.closingDeadline != 0 {
		actions = append(actions, LifecycleAction{
			Kind:       LifecycleActionCancelClosingDeadline,
			Generation: lifecycle.closingDeadline,
		})
	}
	if lifecycle.resetDeadline != 0 && !resetDeadlineFired {
		actions = append(actions, LifecycleAction{
			Kind:       LifecycleActionCancelResetDeadline,
			Generation: lifecycle.resetDeadline,
		})
	}
	if !lifecycle.localCloseIssued {
		lifecycle.localCloseIssued = true
		actions = append(actions, LifecycleAction{Kind: LifecycleActionCloseLocal})
	}
	attachments := make([]AttachmentKey, 0, len(lifecycle.published))
	for attachment := range lifecycle.published {
		attachments = append(attachments, attachment)
	}
	sortAttachmentKeys(attachments)
	for _, attachment := range attachments {
		actions = append(actions, LifecycleAction{
			Kind:       LifecycleActionWithdrawAttachment,
			Attachment: attachment,
		})
	}

	lifecycle.state = state
	lifecycle.recoveryReturnState = 0
	lifecycle.recoveryDeadline = 0
	lifecycle.closingDeadline = 0
	lifecycle.resetDeadline = 0
	lifecycle.resetAttempt = 0
	clear(lifecycle.published)
	clear(lifecycle.joinReplies)
	return actions
}

func (lifecycle *Lifecycle) reservedAttachmentCount() int {
	count := len(lifecycle.published)
	for _, reply := range lifecycle.joinReplies {
		if reply.state == joinReplyProvisional {
			count++
		}
	}
	return count
}

func (lifecycle *Lifecycle) hasSession(sessionGeneration uint64) bool {
	for attachment := range lifecycle.published {
		if attachment.SessionGeneration == sessionGeneration {
			return true
		}
	}
	for attachment := range lifecycle.joinReplies {
		if attachment.SessionGeneration == sessionGeneration {
			return true
		}
	}
	return false
}

func validAttachmentKey(attachment AttachmentKey) bool {
	return attachment.SessionGeneration != 0 && attachment.AttachmentGeneration != 0
}

func normalizeResetReason(reason protocol.ResetReason) protocol.ResetReason {
	if reason < protocol.ResetCancelled || reason > protocol.ResetInternalFailure {
		return protocol.ResetInternalFailure
	}
	return reason
}
