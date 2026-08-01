package flow

import (
	"reflect"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

// JOIN_RESULT is an executor action. Retrying the same attachment must allocate
// a fresh action generation so a late result from the prior attempt cannot
// publish the replacement attempt.
func TestLifecycleJoinResultAttemptGenerationRejectsLateCompletion(t *testing.T) {
	lifecycle := NewLifecycle()
	lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})
	attachment := AttachmentKey{SessionGeneration: 7, AttachmentGeneration: 11}

	firstActions := lifecycle.Handle(LifecycleEvent{
		Kind:       LifecycleJoinRequested,
		Attachment: attachment,
	})
	if len(firstActions) != 1 || firstActions[0].Kind != LifecycleActionSendJoinSuccess {
		t.Fatalf("first JOIN actions = %#v", firstActions)
	}
	firstGeneration := firstActions[0].Generation
	if firstGeneration == 0 {
		t.Fatal("first JOIN_RESULT action has no attempt generation")
	}

	lifecycle.Handle(LifecycleEvent{
		Kind:       LifecycleJoinResultSendFailed,
		Attachment: attachment,
		Generation: firstGeneration,
	})
	secondActions := lifecycle.Handle(LifecycleEvent{
		Kind:       LifecycleJoinRequested,
		Attachment: attachment,
	})
	if len(secondActions) != 1 || secondActions[0].Kind != LifecycleActionSendJoinSuccess {
		t.Fatalf("second JOIN actions = %#v", secondActions)
	}
	secondGeneration := secondActions[0].Generation
	if secondGeneration == 0 || secondGeneration == firstGeneration {
		t.Fatalf("JOIN_RESULT generations were reused: first=%d second=%d", firstGeneration, secondGeneration)
	}

	lateActions := lifecycle.Handle(LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: attachment,
		Generation: firstGeneration,
	})
	if len(lateActions) != 0 {
		t.Fatalf("late first completion produced actions %#v", lateActions)
	}
	lateSnapshot := lifecycle.Snapshot()
	if lateSnapshot.State != AwaitingAttachment || len(lateSnapshot.Published) != 0 || len(lateSnapshot.Provisional) != 1 {
		t.Fatalf("late first completion changed current attempt: %#v", lateSnapshot)
	}

	currentActions := lifecycle.Handle(LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: attachment,
		Generation: secondGeneration,
	})
	if lifecycle.State() != Relaying || len(currentActions) < 2 || currentActions[0].Kind != LifecycleActionPublishAttachment {
		t.Fatalf("current completion did not publish: state=%d actions=%#v", lifecycle.State(), currentActions)
	}
}

func TestFlowLocalFINProgressRefreshesNoProgressDeadline(t *testing.T) {
	attachment := AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1}
	machine := newRelayingFlow(t, attachment)

	if _, err := machine.Handle(FlowEvent{Kind: FlowLocalData, Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	before := machine.Snapshot()
	if before.NoProgressGeneration == 0 {
		t.Fatal("DATA did not arm the no-progress deadline")
	}

	actions, err := machine.Handle(FlowEvent{Kind: FlowLocalEOF})
	if err != nil {
		t.Fatal(err)
	}
	after := machine.Snapshot()
	if after.NoProgressGeneration == 0 || after.NoProgressGeneration == before.NoProgressGeneration {
		t.Fatalf("FIN progress did not replace no-progress deadline: before=%d after=%d actions=%#v",
			before.NoProgressGeneration, after.NoProgressGeneration, actions)
	}
	if !hasFlowAction(actions, FlowActionCancelNoProgressDeadline) || !hasFlowAction(actions, FlowActionArmNoProgressDeadline) {
		t.Fatalf("FIN progress deadline actions = %#v", actions)
	}
}

func TestFlowLateAttemptResultCannotMutateResettingFlow(t *testing.T) {
	attachment := AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1}
	machine := newRelayingFlow(t, attachment)
	actions, err := machine.Handle(FlowEvent{Kind: FlowLocalData, Data: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	item := requireFlowTxItem(t, actions)
	if _, err := machine.Handle(FlowEvent{
		Kind:              FlowStartAttempt,
		ItemID:            item.ItemID,
		AttemptGeneration: item.AttemptGeneration,
		Attachment:        attachment,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := machine.Handle(FlowEvent{
		Kind: FlowLifecycle,
		Lifecycle: LifecycleEvent{
			Kind:   LifecycleResetRequested,
			Reason: protocol.ResetCancelled,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if machine.Snapshot().Lifecycle.State != Resetting {
		t.Fatalf("state = %d, want Resetting", machine.Snapshot().Lifecycle.State)
	}

	beforeLate := machine.Snapshot()
	if len(machine.tx.segments) != 0 {
		t.Fatalf("Resetting flow retained replay segments: %d", len(machine.tx.segments))
	}
	if _, err := machine.Handle(FlowEvent{
		Kind:              FlowAttemptResult,
		ItemID:            item.ItemID,
		AttemptGeneration: item.AttemptGeneration,
		Attachment:        attachment,
		AttemptOutcome:    AttemptSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if afterLate := machine.Snapshot(); !reflect.DeepEqual(afterLate, beforeLate) {
		t.Fatalf("late result mutated Resetting flow: before=%#v after=%#v", beforeLate, afterLate)
	}
}

func TestFlowRemoteResetReleasesOwnedPayloadBuffers(t *testing.T) {
	machine := newRelayingFlow(t, AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1})
	if _, err := machine.Handle(FlowEvent{Kind: FlowLocalData, Data: []byte("outbound-secret")}); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Handle(FlowEvent{
		Kind:   FlowRemoteData,
		Offset: 8,
		Data:   []byte("inbound-secret"),
	}); err != nil {
		t.Fatal(err)
	}
	before := machine.Snapshot()
	if before.TxReplayBytes == 0 || before.Rx.BufferedBytes == 0 {
		t.Fatalf("test did not populate both owned buffers: %#v", before)
	}

	if _, err := machine.Handle(FlowEvent{
		Kind:      FlowLifecycle,
		Lifecycle: LifecycleEvent{Kind: LifecycleRemoteReset},
	}); err != nil {
		t.Fatal(err)
	}
	after := machine.Snapshot()
	if after.Lifecycle.State != Reset {
		t.Fatalf("state = %d, want Reset", after.Lifecycle.State)
	}
	if after.TxReplayBytes != 0 || after.TxReplaySegments != 0 || after.Rx.BufferedBytes != 0 || after.Rx.RangeCount != 0 {
		t.Fatalf("terminal flow retained payload buffers: %#v", after)
	}
	if len(machine.tx.segments) != 0 || len(machine.rx.ranges) != 0 || machine.rx.pendingWrite != nil {
		t.Fatalf("terminal owner retained internal payloads: tx=%d rx=%d pending=%v",
			len(machine.tx.segments), len(machine.rx.ranges), machine.rx.pendingWrite != nil)
	}
}
