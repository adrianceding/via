package daemon

import (
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
	servercore "github.com/adrianceding/via/internal/server"
)

// Each lane can have one reader and one coalesced OPEN waiter, plus the
// originating OPEN worker. This is an I/O bound, not a new Flow attachment limit.
const maxConcurrentJoinWrites = 2*maxWirePathGroupLanes + 1

// Concurrent wire replies share one logical Lifecycle JOIN transaction:
//
//	first reply admitted  -> provisional, one in-flight write
//	additional reply     -> same attachment, increment in-flight count
//	first successful send -> publish once; subsequent failures cannot withdraw it
//	failed send          -> retain provisional while another write is in flight
//	last failed send     -> fail the logical JOIN and release its reservation
//	last result          -> remove the bounded write record
//
// Flow termination wins over every write result. instance.mu owns these records;
// network writes run outside it and only the Flow owns attachment publication.
type serverJoinWrites struct {
	attachment flow.AttachmentKey
	generation uint64
	inFlight   uint32
	completed  bool
}

func (instance *serverFlow) beginJoin(session *wireSession) (*serverJoinWrites, bool) {
	instance.mu.Lock()
	defer instance.mu.Unlock()
	if instance.terminal || instance.owner.LifecycleState() == flow.Resetting ||
		session == nil || session.principal != instance.key.PrincipalID || session.ctx.Err() != nil {
		return nil, false
	}
	group := session.attachmentGeneration
	if pending := instance.joinWrites[group]; pending != nil {
		if pending.inFlight >= maxConcurrentJoinWrites || pending.completed && !instance.owner.IsAttachmentPublished(pending.attachment) {
			return nil, false
		}
		pending.inFlight++
		return pending, true
	}
	if len(instance.joinWrites) >= flow.MaxAttachments {
		return nil, false
	}
	attachment, published := session.attachment(instance.key.FlowID)
	if !published {
		generation, ok := allocateDaemonGeneration(&instance.host.nextAttachment)
		if !ok {
			return nil, false
		}
		attachment = flow.AttachmentKey{SessionGeneration: group, AttachmentGeneration: generation}
		if session.reserve(instance.key.FlowID, attachment) != nil {
			return nil, false
		}
	}
	actions, err := instance.owner.Handle(flow.FlowEvent{
		Kind:      flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleJoinRequested, Attachment: attachment},
	})
	action, ok := joinLifecycleAction(actions, flow.LifecycleActionSendJoinSuccess)
	if err != nil || !ok || action.Generation == 0 {
		if !published {
			session.release(instance.key.FlowID, attachment)
		}
		return nil, false
	}
	pending := &serverJoinWrites{attachment: attachment, generation: action.Generation, inFlight: 1}
	if instance.joinWrites == nil {
		instance.joinWrites = make(map[uint64]*serverJoinWrites, flow.MaxAttachments)
	}
	instance.joinWrites[group] = pending
	return pending, true
}

func (instance *serverFlow) completeJoin(session *wireSession, pending *serverJoinWrites, succeeded bool) bool {
	instance.mu.Lock()
	if pending == nil || instance.joinWrites[pending.attachment.SessionGeneration] != pending || pending.inFlight == 0 {
		instance.mu.Unlock()
		return false
	}
	pending.inFlight--
	var followups []servercore.RelayEvent
	var lost []flow.AttachmentKey
	var closes []servercore.RelayAction
	var err error
	if !instance.terminal {
		generation := uint64(0)
		if !pending.completed && (succeeded || pending.inFlight == 0) {
			generation = pending.generation
			pending.completed = true
		} else if succeeded && instance.owner.IsAttachmentPublished(pending.attachment) {
			// A later successful duplicate still receives the current ACK snapshot.
			actions, beginErr := instance.owner.Handle(flow.FlowEvent{
				Kind:      flow.FlowLifecycle,
				Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleJoinRequested, Attachment: pending.attachment},
			})
			err = beginErr
			if action, ok := joinLifecycleAction(actions, flow.LifecycleActionSendJoinSuccess); ok {
				generation = action.Generation
			}
		}
		if generation != 0 {
			kind := flow.LifecycleJoinResultSendFailed
			if succeeded {
				kind = flow.LifecycleJoinResultSendCompleted
			}
			followups, lost, closes, err = instance.applyLifecycleLocked(flow.LifecycleEvent{
				Kind: kind, Attachment: pending.attachment, Generation: generation,
			})
		}
	}
	if pending.inFlight == 0 {
		delete(instance.joinWrites, pending.attachment.SessionGeneration)
		if !instance.owner.IsAttachmentPublished(pending.attachment) {
			session.release(instance.key.FlowID, pending.attachment)
		}
	}
	instance.mu.Unlock()
	instance.runEffects(followups, lost, closes)
	if err != nil {
		_ = instance.handle(servercore.RelayEvent{Kind: servercore.RelayResetRequested, ResetReason: protocol.ResetInternalFailure})
	}
	instance.mu.Lock()
	published := instance.owner.IsAttachmentPublished(pending.attachment)
	instance.mu.Unlock()
	return published
}
