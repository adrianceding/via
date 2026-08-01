package flow

import (
	"errors"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

func TestFlowRetriesSameBytesAfterEveryAttemptFails(t *testing.T) {
	firstAttachment := AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1}
	secondAttachment := AttachmentKey{SessionGeneration: 2, AttachmentGeneration: 1}
	machine := newRelayingFlow(t, firstAttachment)
	publishAttachment(t, machine, secondAttachment)

	actions, err := machine.Handle(FlowEvent{Kind: FlowLocalData, Data: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	first := requireFlowTxItem(t, actions)
	initial := machine.Snapshot()
	if initial.RetryGeneration == 0 || initial.NoProgressGeneration == 0 {
		t.Fatalf("data deadlines were not armed: %#v", initial)
	}

	attachments := []AttachmentKey{
		firstAttachment,
		secondAttachment,
	}
	for _, attachment := range attachments {
		if _, err := machine.Handle(FlowEvent{
			Kind:              FlowStartAttempt,
			ItemID:            first.ItemID,
			AttemptGeneration: first.AttemptGeneration,
			Attachment:        attachment,
		}); err != nil {
			t.Fatalf("start attempt on %+v: %v", attachment, err)
		}
		if _, err := machine.Handle(FlowEvent{
			Kind:              FlowAttemptResult,
			ItemID:            first.ItemID,
			AttemptGeneration: first.AttemptGeneration,
			Attachment:        attachment,
			AttemptOutcome:    AttemptFailed,
		}); err != nil {
			t.Fatalf("record attempt on %+v: %v", attachment, err)
		}
	}

	actions, err = machine.Handle(FlowEvent{Kind: FlowRetryDeadline, Generation: initial.RetryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	retry := requireFlowTxItem(t, actions)
	if retry.ItemID != first.ItemID || retry.AttemptGeneration <= first.AttemptGeneration || string(retry.CopyData()) != "payload" {
		t.Fatalf("retry item = %#v, first = %#v", retry, first)
	}
	after := machine.Snapshot()
	if after.RetryGeneration == 0 || after.RetryGeneration == initial.RetryGeneration {
		t.Fatalf("retry deadline was not replaced: before=%d after=%d", initial.RetryGeneration, after.RetryGeneration)
	}
	if after.NoProgressGeneration != initial.NoProgressGeneration {
		t.Fatalf("retry refreshed absolute no-progress deadline: before=%d after=%d", initial.NoProgressGeneration, after.NoProgressGeneration)
	}
	if _, err := machine.Handle(FlowEvent{
		Kind:              FlowStartAttempt,
		ItemID:            first.ItemID,
		AttemptGeneration: first.AttemptGeneration,
		Attachment:        attachments[0],
	}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("old attempt generation error = %v", err)
	}
}

func TestFlowScalarStatusReadsDoNotAllocate(t *testing.T) {
	attachment := AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1}
	machine := newRelayingFlow(t, attachment)
	publishAttachment(t, machine, AttachmentKey{SessionGeneration: 2, AttachmentGeneration: 1})
	if got := machine.StatusSnapshot().PublishedAttachments; got != 2 {
		t.Fatalf("published attachments = %d", got)
	}
	allocations := testing.AllocsPerRun(100, func() {
		_ = machine.StatusSnapshot()
		_ = machine.LifecycleState()
		_ = machine.TxState()
		_ = machine.TxReplayBytes()
		_ = machine.TxAvailableWindow()
		_ = machine.TxAcknowledgedOffset()
		_ = machine.IsAttachmentPublished(attachment)
		_ = machine.HasPublishedAttachments()
	})
	if allocations != 0 {
		t.Fatalf("scalar status allocations = %.0f, want 0", allocations)
	}
}

func TestFlowRejectsAttemptOnUnpublishedOrLostAttachment(t *testing.T) {
	attachment := AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1}
	machine := newRelayingFlow(t, attachment)
	actions, err := machine.Handle(FlowEvent{Kind: FlowLocalData, Data: []byte("data")})
	if err != nil {
		t.Fatal(err)
	}
	item := requireFlowTxItem(t, actions)
	unknown := AttachmentKey{SessionGeneration: 2, AttachmentGeneration: 1}
	if _, err := machine.Handle(FlowEvent{
		Kind:              FlowStartAttempt,
		ItemID:            item.ItemID,
		AttemptGeneration: item.AttemptGeneration,
		Attachment:        unknown,
	}); !errors.Is(err, ErrAttachmentUnknown) {
		t.Fatalf("unpublished attachment error = %v", err)
	}
	if _, err := machine.Handle(FlowEvent{
		Kind: FlowLifecycle,
		Lifecycle: LifecycleEvent{
			Kind:       LifecycleAttachmentLost,
			Attachment: attachment,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Handle(FlowEvent{
		Kind:              FlowStartAttempt,
		ItemID:            item.ItemID,
		AttemptGeneration: item.AttemptGeneration,
		Attachment:        attachment,
	}); !errors.Is(err, ErrAttachmentUnknown) {
		t.Fatalf("lost attachment error = %v", err)
	}
}

func TestFlowCumulativeACKAndSelectiveACKDeadlineSemantics(t *testing.T) {
	machine := newRelayingFlow(t, AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1})
	if _, err := machine.Handle(FlowEvent{Kind: FlowLocalData, Data: []byte("abc")}); err != nil {
		t.Fatal(err)
	}
	before := machine.Snapshot()

	if _, err := machine.Handle(FlowEvent{
		Kind:   FlowRemoteACK,
		Offset: 0,
		Ranges: []ByteRange{{Start: 1, End: 3}},
	}); err != nil {
		t.Fatal(err)
	}
	afterSelective := machine.Snapshot()
	if afterSelective.NoProgressGeneration != before.NoProgressGeneration || afterSelective.RetryGeneration != before.RetryGeneration {
		t.Fatalf("selective ACK refreshed deadlines: before=%#v after=%#v", before, afterSelective)
	}

	actions, err := machine.Handle(FlowEvent{Kind: FlowRemoteACK, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	afterCumulative := machine.Snapshot()
	if afterCumulative.TxReplayBytes != 0 || afterCumulative.RetryGeneration != 0 || afterCumulative.NoProgressGeneration != 0 {
		t.Fatalf("cumulative ACK did not release idle flow: %#v", afterCumulative)
	}
	if !hasFlowAction(actions, FlowActionCancelRetryDeadline) || !hasFlowAction(actions, FlowActionCancelNoProgressDeadline) {
		t.Fatalf("deadline cancellation actions = %#v", actions)
	}
}

func TestFlowNoProgressDeadlineEntersResetting(t *testing.T) {
	machine := newRelayingFlow(t, AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1})
	if _, err := machine.Handle(FlowEvent{Kind: FlowLocalData, Data: []byte("blocked")}); err != nil {
		t.Fatal(err)
	}
	snapshot := machine.Snapshot()
	actions, err := machine.Handle(FlowEvent{Kind: FlowNoProgressDeadline, Generation: snapshot.NoProgressGeneration})
	if err != nil {
		t.Fatal(err)
	}
	if got := machine.Snapshot(); got.Lifecycle.State != Resetting || got.RetryGeneration != 0 || got.NoProgressGeneration != 0 {
		t.Fatalf("deadline state = %#v", got)
	}
	reset := requireLifecycleAction(t, actions, LifecycleActionSendReset)
	if reset.Reason != protocol.ResetDeadlineExceeded {
		t.Fatalf("reset reason = %v", reset.Reason)
	}
}

func TestFlowDirectionsConvergeThroughClosing(t *testing.T) {
	machine := newRelayingFlow(t, AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1})
	if _, err := machine.Handle(FlowEvent{Kind: FlowLocalEOF}); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Handle(FlowEvent{Kind: FlowRemoteFINACK, FinalOffset: 0}); err != nil {
		t.Fatal(err)
	}
	actions, err := machine.Handle(FlowEvent{Kind: FlowRemoteFIN, FinalOffset: 0})
	if err != nil {
		t.Fatal(err)
	}
	closeWrite := requireFlowRxAction(t, actions, RxCloseWriteLocal)
	actions, err = machine.Handle(FlowEvent{Kind: FlowCloseWriteResult, Generation: closeWrite.Generation})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := machine.Snapshot()
	if snapshot.TxState != TxComplete || snapshot.Rx.State != RxComplete || snapshot.Lifecycle.State != Closing {
		t.Fatalf("converged snapshot = %#v", snapshot)
	}
	if !hasLifecycleAction(actions, LifecycleActionCloseLocal) || !hasLifecycleAction(actions, LifecycleActionArmClosingDeadline) {
		t.Fatalf("closing actions = %#v", actions)
	}

	if _, err := machine.Handle(FlowEvent{Kind: FlowRemoteFINACK, FinalOffset: 0}); err != nil {
		t.Fatalf("duplicate FIN_ACK during Closing: %v", err)
	}
	actions, err = machine.Handle(FlowEvent{Kind: FlowRemoteFIN, FinalOffset: 0})
	if err != nil {
		t.Fatalf("duplicate FIN during Closing: %v", err)
	}
	if machine.Snapshot().Lifecycle.State != Closing || !hasFlowRxAction(actions, RxSendFINACK) {
		t.Fatalf("Closing duplicate FIN actions/state = %#v / %#v", actions, machine.Snapshot())
	}
}

func TestFlowRemoteResourceViolationResetsOnlyFlow(t *testing.T) {
	machine := newRelayingFlow(t, AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 1})
	actions, err := machine.Handle(FlowEvent{
		Kind:   FlowRemoteData,
		Offset: ReceiveWindowSize,
		Data:   []byte{1},
	})
	if !errors.Is(err, ErrWindowExceeded) {
		t.Fatalf("receive error = %v", err)
	}
	reset := requireLifecycleAction(t, actions, LifecycleActionSendReset)
	if reset.Reason != protocol.ResetResourceLimit || machine.Snapshot().Lifecycle.State != Resetting {
		t.Fatalf("reset action/state = %#v / %v", reset, machine.Snapshot().Lifecycle.State)
	}
}

func newRelayingFlow(t *testing.T, attachment AttachmentKey) *Flow {
	t.Helper()
	machine := NewFlow()
	if _, err := machine.Handle(FlowEvent{Kind: FlowStarted}); err != nil {
		t.Fatal(err)
	}
	publishAttachment(t, machine, attachment)
	if machine.Snapshot().Lifecycle.State != Relaying {
		t.Fatalf("flow state = %v", machine.Snapshot().Lifecycle.State)
	}
	return machine
}

func publishAttachment(t *testing.T, machine *Flow, attachment AttachmentKey) {
	t.Helper()
	actions, err := machine.Handle(FlowEvent{
		Kind: FlowLifecycle,
		Lifecycle: LifecycleEvent{
			Kind:       LifecycleJoinRequested,
			Attachment: attachment,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	join := requireLifecycleAction(t, actions, LifecycleActionSendJoinSuccess)
	if join.Generation == 0 {
		t.Fatal("JOIN result has no generation")
	}
	if _, err := machine.Handle(FlowEvent{
		Kind: FlowLifecycle,
		Lifecycle: LifecycleEvent{
			Kind:       LifecycleJoinResultSendCompleted,
			Attachment: attachment,
			Generation: join.Generation,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func requireFlowTxItem(t *testing.T, actions []FlowAction) TxItem {
	t.Helper()
	for _, action := range actions {
		if action.Kind == FlowActionTxItem {
			return action.TxItem
		}
	}
	t.Fatalf("no Tx item in actions %#v", actions)
	return TxItem{}
}

func requireFlowRxAction(t *testing.T, actions []FlowAction, kind RxActionKind) RxAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == FlowActionRx && action.Rx.Kind == kind {
			return action.Rx
		}
	}
	t.Fatalf("no Rx action %v in %#v", kind, actions)
	return RxAction{}
}

func requireLifecycleAction(t *testing.T, actions []FlowAction, kind LifecycleActionKind) LifecycleAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == FlowActionLifecycle && action.Lifecycle.Kind == kind {
			return action.Lifecycle
		}
	}
	t.Fatalf("no lifecycle action %v in %#v", kind, actions)
	return LifecycleAction{}
}

func hasFlowAction(actions []FlowAction, kind FlowActionKind) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

func hasFlowRxAction(actions []FlowAction, kind RxActionKind) bool {
	for _, action := range actions {
		if action.Kind == FlowActionRx && action.Rx.Kind == kind {
			return true
		}
	}
	return false
}

func hasLifecycleAction(actions []FlowAction, kind LifecycleActionKind) bool {
	for _, action := range actions {
		if action.Kind == FlowActionLifecycle && action.Lifecycle.Kind == kind {
			return true
		}
	}
	return false
}
