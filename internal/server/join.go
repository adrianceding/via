package server

import (
	"bytes"
	"errors"
	"sort"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

const (
	DefaultMaxTrackedJoins = 2 * 1024
	HardMaxTrackedJoins    = 2 * 8192
)

var (
	ErrInvalidJoinCoordinator = errors.New("server: invalid join coordinator")
	ErrJoinResultUnavailable  = errors.New("server: join result unavailable")
)

// JoinFlow is the minimal I/O-free interface exposed by one server flow owner.
type JoinFlow interface {
	Snapshot() flow.FlowSnapshot
	Handle(flow.FlowEvent) ([]flow.FlowAction, error)
}

// JoinSession is the minimal I/O-free interface exposed by one transport session owner.
type JoinSession interface {
	Snapshot() transport.SessionSnapshot
	Handle(transport.SessionEvent) ([]transport.SessionAction, error)
}

// JoinAuthorization is an immutable authorization result. Directory entries must
// be indexed jointly by PrincipalID and FlowID; the coordinator still verifies
// both identifiers again.
type JoinAuthorization struct {
	PrincipalID string
	FlowID      protocol.FlowID
	Flow        JoinFlow
}

// JoinDirectory must compare suppliedCapability in constant time. Before returning
// false for an unknown key, it must perform the same fixed-length comparison using
// an in-process dummy digest.
type JoinDirectory interface {
	AuthorizeJoin(principalID string, flowID protocol.FlowID, suppliedCapability protocol.Capability) (JoinAuthorization, bool)
}

type JoinAuthorizeFunc func(principalID string, flowID protocol.FlowID, suppliedCapability protocol.Capability) (JoinAuthorization, bool)

func (authorize JoinAuthorizeFunc) AuthorizeJoin(principalID string, flowID protocol.FlowID, suppliedCapability protocol.Capability) (JoinAuthorization, bool) {
	return authorize(principalID, flowID, suppliedCapability)
}

// JoinOutput contains only executor actions emitted by pure state owners.
// External executors may write encoded results from SessionActions; FlowActions
// describe subsequent scheduling, deadline, and local connection work.
type JoinOutput struct {
	SessionActions []transport.SessionAction
	FlowActions    []flow.FlowAction
}

type joinAttachmentState uint8

const (
	joinProvisional joinAttachmentState = iota + 1
	joinPublished
)

type joinSessionFlowKey struct {
	SessionGeneration uint64
	FlowID            protocol.FlowID
}

type trackedJoin struct {
	key        joinSessionFlowKey
	attachment flow.AttachmentKey
	auth       JoinAuthorization
	session    JoinSession
	state      joinAttachmentState
}

type pendingJoinResult struct {
	tracked             *trackedJoin
	lifecycleGeneration uint64
}

// JoinCoordinator serially processes JOIN authorization and publication. It
// performs no network or target connection I/O and must be called by the sole
// server role owner.
type JoinCoordinator struct {
	directory      JoinDirectory
	maxTracked     int
	nextGeneration uint64
	tracked        map[joinSessionFlowKey]*trackedJoin
	pending        map[uint64]pendingJoinResult
}

func NewJoinCoordinator(directory JoinDirectory, maxTracked int) (*JoinCoordinator, error) {
	if directory == nil || maxTracked < 1 || maxTracked > HardMaxTrackedJoins {
		return nil, ErrInvalidJoinCoordinator
	}
	return &JoinCoordinator{
		directory:  directory,
		maxTracked: maxTracked,
		tracked:    make(map[joinSessionFlowKey]*trackedJoin, maxTracked),
		pending:    make(map[uint64]pendingJoinResult, maxTracked),
	}, nil
}

// Begin validates one JOIN and queues its uniform result in the pure transport
// session state owner. An empty output means the same result is already in flight.
func (coordinator *JoinCoordinator) Begin(session JoinSession, message protocol.Join) (JoinOutput, error) {
	if coordinator == nil || session == nil {
		return JoinOutput{}, ErrInvalidJoinCoordinator
	}
	sessionSnapshot := session.Snapshot()
	if sessionSnapshot.State != transport.SessionReady || sessionSnapshot.Generation == 0 || sessionSnapshot.PrincipalID == "" {
		return coordinator.sendFailure(session, message.FlowID)
	}

	authorization, authorized := coordinator.directory.AuthorizeJoin(sessionSnapshot.PrincipalID, message.FlowID, message.Capability)
	identityMatches := authorized && authorization.PrincipalID == sessionSnapshot.PrincipalID &&
		authorization.FlowID == message.FlowID && authorization.Flow != nil
	if !identityMatches || !joinableState(authorization.Flow.Snapshot().Lifecycle.State) {
		return coordinator.sendFailure(session, message.FlowID)
	}

	key := joinSessionFlowKey{SessionGeneration: sessionSnapshot.Generation, FlowID: message.FlowID}
	tracked := coordinator.tracked[key]
	created := false
	if tracked == nil {
		if len(coordinator.tracked) >= coordinator.maxTracked {
			return coordinator.sendFailure(session, message.FlowID)
		}
		attachmentGeneration, ok := coordinator.allocateGeneration()
		if !ok {
			return coordinator.sendFailure(session, message.FlowID)
		}
		attachment := flow.AttachmentKey{
			SessionGeneration:    sessionSnapshot.Generation,
			AttachmentGeneration: attachmentGeneration,
		}
		if _, err := session.Handle(transport.SessionEvent{
			Kind:                 transport.SessionReserveAttachment,
			FlowID:               message.FlowID,
			AttachmentGeneration: attachmentGeneration,
		}); err != nil {
			return coordinator.sendFailure(session, message.FlowID)
		}
		tracked = &trackedJoin{
			key:        key,
			attachment: attachment,
			auth:       authorization,
			session:    session,
			state:      joinProvisional,
		}
		coordinator.tracked[key] = tracked
		created = true
	}

	flowActions, err := tracked.auth.Flow.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind:       flow.LifecycleJoinRequested,
			Attachment: tracked.attachment,
		},
	})
	if err != nil {
		coordinator.rollbackProvisional(tracked, 0)
		return coordinator.sendFailure(session, message.FlowID)
	}
	resultKind, lifecycleGeneration, ok := joinResultAction(flowActions)
	if !ok {
		// The same JOIN_RESULT is already in flight for this attachment.
		if !created {
			return JoinOutput{}, nil
		}
		coordinator.rollbackProvisional(tracked, 0)
		return coordinator.sendFailure(session, message.FlowID)
	}
	if resultKind != flow.LifecycleActionSendJoinSuccess {
		coordinator.rollbackProvisional(tracked, lifecycleGeneration)
		return coordinator.sendFailure(session, message.FlowID)
	}

	correlation, ok := coordinator.allocateGeneration()
	if !ok || len(coordinator.pending) >= coordinator.maxTracked {
		coordinator.rollbackProvisional(tracked, lifecycleGeneration)
		return coordinator.sendFailure(session, message.FlowID)
	}
	coordinator.pending[correlation] = pendingJoinResult{
		tracked:             tracked,
		lifecycleGeneration: lifecycleGeneration,
	}
	sessionActions, err := session.Handle(transport.SessionEvent{
		Kind:        transport.SessionSendMessage,
		Message:     protocol.JoinResult{FlowID: message.FlowID, Result: protocol.JoinSuccess},
		Class:       transport.FrameControl,
		Correlation: correlation,
	})
	if err != nil {
		delete(coordinator.pending, correlation)
		coordinator.rollbackProvisional(tracked, lifecycleGeneration)
		return JoinOutput{}, ErrJoinResultUnavailable
	}
	return JoinOutput{SessionActions: sessionActions}, nil
}

// CompleteResult consumes a SessionActionSendResult correlation. The attachment
// is published only after the result is sent successfully; a stale correlation
// does not alter its replacement attempt.
func (coordinator *JoinCoordinator) CompleteResult(correlation uint64, succeeded bool) (JoinOutput, error) {
	if coordinator == nil {
		return JoinOutput{}, ErrInvalidJoinCoordinator
	}
	pending, ok := coordinator.pending[correlation]
	if !ok || correlation == 0 {
		return JoinOutput{}, nil
	}
	delete(coordinator.pending, correlation)
	tracked := pending.tracked
	if !succeeded {
		coordinator.rollbackProvisional(tracked, pending.lifecycleGeneration)
		return JoinOutput{}, nil
	}

	flowActions, err := tracked.auth.Flow.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind:       flow.LifecycleJoinResultSendCompleted,
			Attachment: tracked.attachment,
			Generation: pending.lifecycleGeneration,
		},
	})
	if err != nil {
		cleanupActions := coordinator.cleanupFlowAttachment(tracked)
		coordinator.cancelSessionReservation(tracked)
		coordinator.removeTracked(tracked)
		return JoinOutput{FlowActions: cleanupActions}, ErrJoinResultUnavailable
	}
	if tracked.state == joinProvisional && !hasLifecycleAction(flowActions, flow.LifecycleActionPublishAttachment) {
		cleanupActions := coordinator.cleanupFlowAttachment(tracked)
		coordinator.cancelSessionReservation(tracked)
		coordinator.removeTracked(tracked)
		return JoinOutput{FlowActions: cleanupActions}, nil
	}

	// The flow attachment set is authoritative. Update the derived transport
	// session index only after successful publication; keep the read barrier
	// closed until both steps complete.
	if _, err := tracked.session.Handle(transport.SessionEvent{
		Kind:                 transport.SessionPublishAttachment,
		FlowID:               tracked.key.FlowID,
		AttachmentGeneration: tracked.attachment.AttachmentGeneration,
	}); err != nil {
		coordinator.cancelSessionReservation(tracked)
		coordinator.withdrawSessionIndex(tracked)
		cleanupActions := coordinator.cleanupFlowAttachment(tracked)
		coordinator.removeTracked(tracked)
		return JoinOutput{FlowActions: appendPublicationRollback(flowActions, cleanupActions)}, ErrJoinResultUnavailable
	}
	tracked.state = joinPublished
	return JoinOutput{FlowActions: flowActions}, nil
}

// AttachmentLost withdraws a provisional or published attachment. It can process
// SessionActionAttachmentLost directly and remains idempotent for stale events.
func (coordinator *JoinCoordinator) AttachmentLost(attachment flow.AttachmentKey) (JoinOutput, error) {
	if coordinator == nil {
		return JoinOutput{}, ErrInvalidJoinCoordinator
	}
	tracked := coordinator.findAttachment(attachment)
	if tracked == nil {
		return JoinOutput{}, nil
	}
	coordinator.dropPendingFor(tracked)
	flowActions, err := tracked.auth.Flow.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind:       flow.LifecycleAttachmentLost,
			Attachment: tracked.attachment,
		},
	})
	coordinator.removeTracked(tracked)
	if err != nil {
		return JoinOutput{}, ErrJoinResultUnavailable
	}
	return JoinOutput{FlowActions: flowActions}, nil
}

// WithdrawAttachment consumes a withdrawal action already emitted by the flow
// owner and synchronously clears the derived transport session index and the
// coordinator entry. Duplicate or stale actions have no effect.
func (coordinator *JoinCoordinator) WithdrawAttachment(attachment flow.AttachmentKey) error {
	if coordinator == nil {
		return ErrInvalidJoinCoordinator
	}
	tracked := coordinator.findAttachment(attachment)
	if tracked == nil {
		return nil
	}
	coordinator.dropPendingFor(tracked)
	eventKind := transport.SessionWithdrawAttachment
	if tracked.state == joinProvisional {
		eventKind = transport.SessionCancelAttachment
	}
	_, err := tracked.session.Handle(transport.SessionEvent{
		Kind:                 eventKind,
		FlowID:               tracked.key.FlowID,
		AttachmentGeneration: tracked.attachment.AttachmentGeneration,
	})
	coordinator.removeTracked(tracked)
	if err != nil {
		return ErrJoinResultUnavailable
	}
	return nil
}

// FlowTerminal clears provisional or published indexes remaining for a terminal
// flow. The flow owner must complete the terminal transition first; this method
// only synchronizes coordinator and derived transport session state.
func (coordinator *JoinCoordinator) FlowTerminal(principalID string, flowID protocol.FlowID) error {
	if coordinator == nil {
		return ErrInvalidJoinCoordinator
	}
	entries := make([]*trackedJoin, 0, flow.MaxAttachments)
	for _, tracked := range coordinator.tracked {
		if tracked.auth.PrincipalID == principalID && tracked.auth.FlowID == flowID {
			entries = append(entries, tracked)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].attachment.SessionGeneration != entries[j].attachment.SessionGeneration {
			return entries[i].attachment.SessionGeneration < entries[j].attachment.SessionGeneration
		}
		return entries[i].attachment.AttachmentGeneration < entries[j].attachment.AttachmentGeneration
	})
	failed := false
	for _, tracked := range entries {
		coordinator.dropPendingFor(tracked)
		eventKind := transport.SessionWithdrawAttachment
		if tracked.state == joinProvisional {
			eventKind = transport.SessionCancelAttachment
		}
		if _, err := tracked.session.Handle(transport.SessionEvent{
			Kind:                 eventKind,
			FlowID:               tracked.key.FlowID,
			AttachmentGeneration: tracked.attachment.AttachmentGeneration,
		}); err != nil {
			failed = true
		}
		coordinator.removeTracked(tracked)
	}
	if failed {
		return ErrJoinResultUnavailable
	}
	return nil
}

// SessionClosed rolls back every provisional JOIN for one session and withdraws
// all of its published attachments. The scan is hard-bounded by maxTracked.
func (coordinator *JoinCoordinator) SessionClosed(sessionGeneration uint64) (JoinOutput, error) {
	if coordinator == nil {
		return JoinOutput{}, ErrInvalidJoinCoordinator
	}
	if sessionGeneration == 0 {
		return JoinOutput{}, nil
	}
	entries := make([]*trackedJoin, 0, flow.MaxAttachments)
	for _, tracked := range coordinator.tracked {
		if tracked.attachment.SessionGeneration == sessionGeneration {
			entries = append(entries, tracked)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		comparison := bytes.Compare(entries[i].key.FlowID[:], entries[j].key.FlowID[:])
		if comparison != 0 {
			return comparison < 0
		}
		return entries[i].attachment.AttachmentGeneration < entries[j].attachment.AttachmentGeneration
	})
	var output JoinOutput
	for _, tracked := range entries {
		coordinator.dropPendingFor(tracked)
		flowActions, err := tracked.auth.Flow.Handle(flow.FlowEvent{
			Kind: flow.FlowLifecycle,
			Lifecycle: flow.LifecycleEvent{
				Kind: flow.LifecycleSessionClosed,
				Attachment: flow.AttachmentKey{
					SessionGeneration: sessionGeneration,
				},
			},
		})
		coordinator.removeTracked(tracked)
		if err != nil {
			return output, ErrJoinResultUnavailable
		}
		output.FlowActions = append(output.FlowActions, flowActions...)
	}
	return output, nil
}

func (coordinator *JoinCoordinator) sendFailure(session JoinSession, flowID protocol.FlowID) (JoinOutput, error) {
	actions, err := session.Handle(transport.SessionEvent{
		Kind:    transport.SessionSendMessage,
		Message: protocol.JoinResult{FlowID: flowID, Result: protocol.JoinFailure},
		Class:   transport.FrameControl,
	})
	if err != nil {
		return JoinOutput{}, ErrJoinResultUnavailable
	}
	return JoinOutput{SessionActions: actions}, nil
}

func (coordinator *JoinCoordinator) rollbackProvisional(tracked *trackedJoin, lifecycleGeneration uint64) {
	if tracked == nil {
		return
	}
	if lifecycleGeneration != 0 {
		_, _ = tracked.auth.Flow.Handle(flow.FlowEvent{
			Kind: flow.FlowLifecycle,
			Lifecycle: flow.LifecycleEvent{
				Kind:       flow.LifecycleJoinResultSendFailed,
				Attachment: tracked.attachment,
				Generation: lifecycleGeneration,
			},
		})
	}
	if tracked.state != joinProvisional {
		return
	}
	_, _ = tracked.session.Handle(transport.SessionEvent{
		Kind:                 transport.SessionCancelAttachment,
		FlowID:               tracked.key.FlowID,
		AttachmentGeneration: tracked.attachment.AttachmentGeneration,
	})
	coordinator.removeTracked(tracked)
}

func (coordinator *JoinCoordinator) withdrawSessionIndex(tracked *trackedJoin) {
	_, _ = tracked.session.Handle(transport.SessionEvent{
		Kind:                 transport.SessionWithdrawAttachment,
		FlowID:               tracked.key.FlowID,
		AttachmentGeneration: tracked.attachment.AttachmentGeneration,
	})
}

func (coordinator *JoinCoordinator) cancelSessionReservation(tracked *trackedJoin) {
	_, _ = tracked.session.Handle(transport.SessionEvent{
		Kind:                 transport.SessionCancelAttachment,
		FlowID:               tracked.key.FlowID,
		AttachmentGeneration: tracked.attachment.AttachmentGeneration,
	})
}

func (coordinator *JoinCoordinator) cleanupFlowAttachment(tracked *trackedJoin) []flow.FlowAction {
	actions, _ := tracked.auth.Flow.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind:       flow.LifecycleAttachmentLost,
			Attachment: tracked.attachment,
		},
	})
	return actions
}

func appendPublicationRollback(published, cleanup []flow.FlowAction) []flow.FlowAction {
	actions := make([]flow.FlowAction, 0, len(published)+len(cleanup))
	for _, action := range published {
		switch action.Kind {
		case flow.FlowActionCancelRetryDeadline, flow.FlowActionCancelNoProgressDeadline:
			actions = append(actions, action)
		case flow.FlowActionLifecycle:
			switch action.Lifecycle.Kind {
			case flow.LifecycleActionCancelRecoveryDeadline,
				flow.LifecycleActionCancelClosingDeadline,
				flow.LifecycleActionCancelResetDeadline:
				actions = append(actions, action)
			}
		}
	}
	return append(actions, cleanup...)
}

func (coordinator *JoinCoordinator) dropPendingFor(tracked *trackedJoin) {
	for correlation, pending := range coordinator.pending {
		if pending.tracked == tracked {
			delete(coordinator.pending, correlation)
		}
	}
}

func (coordinator *JoinCoordinator) removeTracked(tracked *trackedJoin) {
	if current := coordinator.tracked[tracked.key]; current == tracked {
		delete(coordinator.tracked, tracked.key)
	}
}

func (coordinator *JoinCoordinator) findAttachment(attachment flow.AttachmentKey) *trackedJoin {
	if attachment.SessionGeneration == 0 || attachment.AttachmentGeneration == 0 {
		return nil
	}
	for _, tracked := range coordinator.tracked {
		if tracked.attachment == attachment {
			return tracked
		}
	}
	return nil
}

func (coordinator *JoinCoordinator) allocateGeneration() (uint64, bool) {
	if coordinator.nextGeneration == ^uint64(0) {
		return 0, false
	}
	coordinator.nextGeneration++
	return coordinator.nextGeneration, true
}

func joinableState(state flow.LifecycleState) bool {
	return state == flow.AwaitingAttachment || state == flow.Relaying ||
		state == flow.Recovering || state == flow.Closing
}

func joinResultAction(actions []flow.FlowAction) (flow.LifecycleActionKind, uint64, bool) {
	for _, action := range actions {
		if action.Kind != flow.FlowActionLifecycle {
			continue
		}
		if action.Lifecycle.Kind == flow.LifecycleActionSendJoinSuccess ||
			action.Lifecycle.Kind == flow.LifecycleActionSendJoinFailure {
			return action.Lifecycle.Kind, action.Lifecycle.Generation, action.Lifecycle.Generation != 0
		}
	}
	return 0, 0, false
}

func hasLifecycleAction(actions []flow.FlowAction, kind flow.LifecycleActionKind) bool {
	for _, action := range actions {
		if action.Kind == flow.FlowActionLifecycle && action.Lifecycle.Kind == kind {
			return true
		}
	}
	return false
}
