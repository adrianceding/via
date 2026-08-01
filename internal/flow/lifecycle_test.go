package flow

import (
	"reflect"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

func TestLifecyclePublishesAttachmentOnlyAfterJoinResultSendCompletes(t *testing.T) {
	lifecycle := NewLifecycle()
	startActions := lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})
	if len(startActions) != 1 || startActions[0].Kind != LifecycleActionArmRecoveryDeadline || startActions[0].Generation == 0 {
		t.Fatalf("start actions = %#v", startActions)
	}
	recoveryGeneration := startActions[0].Generation
	attachment := testAttachment(7, 11)

	joinGeneration := assertJoinResultAction(t, lifecycle, attachment, LifecycleActionSendJoinSuccess)
	snapshot := lifecycle.Snapshot()
	if len(snapshot.Published) != 0 || !reflect.DeepEqual(snapshot.Provisional, []AttachmentKey{attachment}) {
		t.Fatalf("snapshot before result send completion = %#v", snapshot)
	}

	stale := testAttachment(7, 10)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: stale,
		Generation: joinGeneration,
	}, AwaitingAttachment)
	if !reflect.DeepEqual(lifecycle.Snapshot().Provisional, []AttachmentKey{attachment}) {
		t.Fatalf("stale completion changed provisional entries: %#v", lifecycle.Snapshot())
	}

	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: attachment,
		Generation: joinGeneration,
	}, Relaying,
		LifecycleAction{Kind: LifecycleActionPublishAttachment, Attachment: attachment},
		LifecycleAction{Kind: LifecycleActionSendAcknowledgementSnapshot, Attachment: attachment},
		LifecycleAction{Kind: LifecycleActionCancelRecoveryDeadline, Generation: recoveryGeneration},
		LifecycleAction{Kind: LifecycleActionResumeLocalRead},
	)
	snapshot = lifecycle.Snapshot()
	if !reflect.DeepEqual(snapshot.Published, []AttachmentKey{attachment}) || len(snapshot.Provisional) != 0 {
		t.Fatalf("snapshot after publication = %#v", snapshot)
	}

	// Snapshots must be isolated from internal collections.
	snapshot.Published[0] = testAttachment(99, 99)
	if got := lifecycle.Snapshot().Published[0]; got != attachment {
		t.Fatalf("mutating snapshot changed lifecycle: %#v", got)
	}
}

func TestLifecycleConcurrentProvisionalJoinsAreBoundedAndRollbackIndependently(t *testing.T) {
	lifecycle := NewLifecycle()
	lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})
	first := testAttachment(2, 20)
	second := testAttachment(1, 10)

	firstAttempt := assertJoinResultAction(t, lifecycle, first, LifecycleActionSendJoinSuccess)
	assertJoinResultAction(t, lifecycle, second, LifecycleActionSendJoinSuccess)
	expected := []AttachmentKey{second, first}
	if got := lifecycle.Snapshot().Provisional; !reflect.DeepEqual(got, expected) {
		t.Fatalf("sorted provisional attachments = %#v", got)
	}
	for sessionGeneration := uint64(3); sessionGeneration <= uint64(MaxAttachments); sessionGeneration++ {
		attachment := testAttachment(sessionGeneration, sessionGeneration*10)
		assertJoinResultAction(t, lifecycle, attachment, LifecycleActionSendJoinSuccess)
		expected = append(expected, attachment)
	}
	if got := lifecycle.Snapshot().Provisional; !reflect.DeepEqual(got, expected) {
		t.Fatalf("full provisional attachments = %#v", got)
	}
	overflow := testAttachment(uint64(MaxAttachments)+1, (uint64(MaxAttachments)+1)*10)
	assertJoinResultAction(t, lifecycle, overflow, LifecycleActionSendJoinFailure)

	// A duplicate request with an in-flight result must not consume capacity or emit another send action.
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinRequested,
		Attachment: first,
	}, AwaitingAttachment)
	// One transport session cannot own another attachment generation concurrently.
	assertJoinResultAction(t, lifecycle, testAttachment(first.SessionGeneration, 21), LifecycleActionSendJoinFailure)

	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendFailed,
		Attachment: first,
		Generation: firstAttempt,
	}, AwaitingAttachment)
	retryAttempt := assertJoinResultAction(t, lifecycle, first, LifecycleActionSendJoinSuccess)
	if retryAttempt == firstAttempt {
		t.Fatal("JOIN retry reused the failed action generation")
	}
	// A stale send completing after a new provisional entry exists must neither publish nor consume the new attempt.
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: first,
		Generation: firstAttempt,
	}, AwaitingAttachment)
	if got := lifecycle.Snapshot().Provisional; !reflect.DeepEqual(got, expected) {
		t.Fatalf("late old completion changed retried provisional JOIN: %#v", got)
	}
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendFailed,
		Attachment: first,
		Generation: retryAttempt,
	}, AwaitingAttachment)
	overflowAttempt := assertJoinResultAction(t, lifecycle, overflow, LifecycleActionSendJoinSuccess)

	// Closing a transport session rolls back only provisional JOINs from the matching generation.
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind: LifecycleSessionClosed,
		Attachment: AttachmentKey{
			SessionGeneration: overflow.SessionGeneration,
		},
	}, AwaitingAttachment)
	expectedWithoutFirst := append([]AttachmentKey(nil), expected[:1]...)
	expectedWithoutFirst = append(expectedWithoutFirst, expected[2:]...)
	if got := lifecycle.Snapshot().Provisional; !reflect.DeepEqual(got, expectedWithoutFirst) {
		t.Fatalf("provisional attachments after session close = %#v", got)
	}
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: overflow,
		Generation: overflowAttempt,
	}, AwaitingAttachment)

	assertJoinResultAction(t, lifecycle, testAttachment(0, 1), LifecycleActionSendJoinFailure)
	assertJoinResultAction(t, lifecycle, testAttachment(4, 0), LifecycleActionSendJoinFailure)
}

func TestLifecyclePublishedCapacityAndDuplicateJoin(t *testing.T) {
	lifecycle := NewLifecycle()
	lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})
	first := testAttachment(1, 1)
	second := testAttachment(2, 1)
	publishTestAttachment(t, lifecycle, first)
	publishTestAttachment(t, lifecycle, second)
	expected := []AttachmentKey{first, second}
	for sessionGeneration := uint64(3); sessionGeneration <= uint64(MaxAttachments); sessionGeneration++ {
		attachment := testAttachment(sessionGeneration, 1)
		publishTestAttachment(t, lifecycle, attachment)
		expected = append(expected, attachment)
	}

	if got := lifecycle.Snapshot().Published; !reflect.DeepEqual(got, expected) {
		t.Fatalf("published attachments = %#v", got)
	}
	overflow := testAttachment(uint64(MaxAttachments)+1, 1)
	assertJoinResultAction(t, lifecycle, overflow, LifecycleActionSendJoinFailure)

	duplicateAttempt := assertJoinResultAction(t, lifecycle, first, LifecycleActionSendJoinSuccess)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: first,
		Generation: duplicateAttempt,
	}, Relaying, LifecycleAction{
		Kind:       LifecycleActionSendAcknowledgementSnapshot,
		Attachment: first,
	})
	if got := lifecycle.Snapshot().Published; len(got) != MaxAttachments {
		t.Fatalf("duplicate JOIN changed published count: %#v", got)
	}

	duplicateFailureAttempt := assertJoinResultAction(t, lifecycle, first, LifecycleActionSendJoinSuccess)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendFailed,
		Attachment: first,
		Generation: duplicateFailureAttempt,
	}, Relaying)
	retriedDuplicateAttempt := assertJoinResultAction(t, lifecycle, first, LifecycleActionSendJoinSuccess)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: first,
		Generation: duplicateFailureAttempt,
	}, Relaying)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: first,
		Generation: retriedDuplicateAttempt,
	}, Relaying, LifecycleAction{
		Kind:       LifecycleActionSendAcknowledgementSnapshot,
		Attachment: first,
	})
	if got := lifecycle.Snapshot().Published; len(got) != MaxAttachments {
		t.Fatalf("duplicate JOIN send failure withdrew publication: %#v", got)
	}
}

func TestLifecycleLastAttachmentLossRecoversSavedState(t *testing.T) {
	lifecycle := NewLifecycle()
	initialTimer := lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})[0].Generation
	first := testAttachment(1, 1)
	second := testAttachment(2, 2)
	publishTestAttachment(t, lifecycle, first)
	publishTestAttachment(t, lifecycle, second)

	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleAttachmentLost,
		Attachment: first,
	}, Relaying, LifecycleAction{
		Kind:       LifecycleActionWithdrawAttachment,
		Attachment: first,
	})
	assertLifecycleStepKind(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleAttachmentLost,
		Attachment: second,
	}, Recovering,
		LifecycleActionWithdrawAttachment,
		LifecycleActionPauseLocalRead,
		LifecycleActionArmRecoveryDeadline,
	)
	snapshot := lifecycle.Snapshot()
	if snapshot.RecoveryReturnState != Relaying || snapshot.RecoveryDeadlineGeneration == 0 || len(snapshot.Published) != 0 {
		t.Fatalf("recovering snapshot = %#v", snapshot)
	}
	recoveryTimer := snapshot.RecoveryDeadlineGeneration
	if recoveryTimer == initialTimer {
		t.Fatal("recovery reused an old timer generation")
	}

	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleRecoveryDeadline,
		Generation: initialTimer,
	}, Recovering)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind: LifecycleSessionClosed,
		Attachment: AttachmentKey{
			SessionGeneration: first.SessionGeneration,
		},
	}, Recovering)

	replacement := testAttachment(3, 3)
	replacementAttempt := assertJoinResultAction(t, lifecycle, replacement, LifecycleActionSendJoinSuccess)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: replacement,
		Generation: replacementAttempt,
	}, Relaying,
		LifecycleAction{Kind: LifecycleActionPublishAttachment, Attachment: replacement},
		LifecycleAction{Kind: LifecycleActionSendAcknowledgementSnapshot, Attachment: replacement},
		LifecycleAction{Kind: LifecycleActionCancelRecoveryDeadline, Generation: recoveryTimer},
		LifecycleAction{Kind: LifecycleActionResumeLocalRead},
	)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleAttachmentLost,
		Attachment: testAttachment(replacement.SessionGeneration, replacement.AttachmentGeneration-1),
	}, Relaying)
}

func TestLifecycleClosingUsesOnlyLingerAndNeverRecovers(t *testing.T) {
	lifecycle := NewLifecycle()
	lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})
	first := testAttachment(1, 1)
	second := testAttachment(2, 2)
	publishTestAttachment(t, lifecycle, first)
	publishTestAttachment(t, lifecycle, second)

	closingActions := lifecycle.Handle(LifecycleEvent{Kind: LifecycleDirectionsComplete})
	if lifecycle.State() != Closing || len(closingActions) != 2 ||
		closingActions[0].Kind != LifecycleActionCloseLocal ||
		closingActions[1].Kind != LifecycleActionArmClosingDeadline || closingActions[1].Generation == 0 {
		t.Fatalf("closing transition = state %d, actions %#v", lifecycle.State(), closingActions)
	}
	closingTimer := closingActions[1].Generation
	if snapshot := lifecycle.Snapshot(); snapshot.RecoveryDeadlineGeneration != 0 {
		t.Fatalf("Closing owns a recovery deadline: %#v", snapshot)
	}

	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleAttachmentLost,
		Attachment: first,
	}, Closing, LifecycleAction{
		Kind:       LifecycleActionWithdrawAttachment,
		Attachment: first,
	})
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleAttachmentLost,
		Attachment: second,
	}, Closing, LifecycleAction{
		Kind:       LifecycleActionWithdrawAttachment,
		Attachment: second,
	})
	if snapshot := lifecycle.Snapshot(); snapshot.RecoveryDeadlineGeneration != 0 {
		t.Fatalf("attachment loss armed recovery in Closing: %#v", snapshot)
	}

	replacement := testAttachment(3, 3)
	replacementAttempt := assertJoinResultAction(t, lifecycle, replacement, LifecycleActionSendJoinSuccess)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: replacement,
		Generation: replacementAttempt,
	}, Closing,
		LifecycleAction{Kind: LifecycleActionPublishAttachment, Attachment: replacement},
		LifecycleAction{Kind: LifecycleActionSendAcknowledgementSnapshot, Attachment: replacement},
	)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleClosingDeadline,
		Generation: closingTimer - 1,
	}, Closing)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleClosingDeadline,
		Generation: closingTimer,
	}, Closed, LifecycleAction{
		Kind:       LifecycleActionWithdrawAttachment,
		Attachment: replacement,
	})
	assertTerminalSnapshot(t, lifecycle, Closed)
	assertLifecycleStep(t, lifecycle, LifecycleEvent{
		Kind:       LifecycleJoinRequested,
		Attachment: testAttachment(4, 4),
	}, Closed)
}

func TestLifecycleResettingAttemptsResetOnceAndConverges(t *testing.T) {
	terminationKinds := []LifecycleEventKind{
		LifecycleResetSendCompleted,
		LifecycleResetSendFailed,
		LifecycleResetDeadline,
	}
	for _, terminationKind := range terminationKinds {
		t.Run(lifecycleEventName(terminationKind), func(t *testing.T) {
			lifecycle, attachment := newRelayingLifecycle(t)
			actions := lifecycle.Handle(LifecycleEvent{
				Kind:   LifecycleResetRequested,
				Reason: protocol.ResetProtocolConflict,
			})
			if lifecycle.State() != Resetting || len(actions) != 3 ||
				actions[0].Kind != LifecycleActionCloseLocal ||
				actions[1].Kind != LifecycleActionSendReset ||
				actions[1].Reason != protocol.ResetProtocolConflict || actions[1].Generation == 0 ||
				actions[2].Kind != LifecycleActionArmResetDeadline || actions[2].Generation == 0 {
				t.Fatalf("reset transition = state %d, actions %#v", lifecycle.State(), actions)
			}
			resetAttempt := actions[1].Generation
			resetTimer := actions[2].Generation
			if resetAttempt == resetTimer {
				t.Fatal("RESET attempt and deadline reused a generation")
			}

			assertJoinResultAction(t, lifecycle, testAttachment(9, 9), LifecycleActionSendJoinFailure)
			assertLifecycleStep(t, lifecycle, LifecycleEvent{
				Kind:   LifecycleResetRequested,
				Reason: protocol.ResetCancelled,
			}, Resetting)
			assertLifecycleStep(t, lifecycle, LifecycleEvent{
				Kind:       LifecycleResetSendCompleted,
				Generation: resetAttempt + resetTimer + 1,
			}, Resetting)

			generation := resetAttempt
			wantActions := []LifecycleAction{
				{Kind: LifecycleActionCancelResetDeadline, Generation: resetTimer},
				{Kind: LifecycleActionWithdrawAttachment, Attachment: attachment},
			}
			if terminationKind == LifecycleResetDeadline {
				generation = resetTimer
				wantActions = wantActions[1:]
			}
			assertLifecycleStep(t, lifecycle, LifecycleEvent{
				Kind:       terminationKind,
				Generation: generation,
			}, Reset, wantActions...)
			assertTerminalSnapshot(t, lifecycle, Reset)
			assertLifecycleStep(t, lifecycle, LifecycleEvent{
				Kind:       LifecycleResetSendCompleted,
				Generation: resetAttempt,
			}, Reset)
		})
	}
}

func TestLifecycleRecoveryDeadlineAndRemoteReset(t *testing.T) {
	t.Run("recovery deadline", func(t *testing.T) {
		lifecycle := NewLifecycle()
		recoveryTimer := lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})[0].Generation
		actions := lifecycle.Handle(LifecycleEvent{
			Kind:       LifecycleRecoveryDeadline,
			Generation: recoveryTimer,
		})
		if lifecycle.State() != Resetting || len(actions) != 3 ||
			actions[0].Kind != LifecycleActionCloseLocal ||
			actions[1].Kind != LifecycleActionSendReset ||
			actions[1].Reason != protocol.ResetDeadlineExceeded ||
			actions[2].Kind != LifecycleActionArmResetDeadline {
			t.Fatalf("recovery expiry = state %d, actions %#v", lifecycle.State(), actions)
		}
	})

	t.Run("remote reset", func(t *testing.T) {
		lifecycle, attachment := newRelayingLifecycle(t)
		actions := lifecycle.Handle(LifecycleEvent{Kind: LifecycleRemoteReset})
		assertLifecycleStateAndActions(t, lifecycle, Reset, actions,
			[]LifecycleAction{
				{Kind: LifecycleActionCloseLocal},
				{Kind: LifecycleActionWithdrawAttachment, Attachment: attachment},
			})
		for _, action := range actions {
			if action.Kind == LifecycleActionSendReset {
				t.Fatal("remote RESET produced a RESET response")
			}
		}
	})

	t.Run("reset interrupts closing", func(t *testing.T) {
		lifecycle, _ := newRelayingLifecycle(t)
		closingActions := lifecycle.Handle(LifecycleEvent{Kind: LifecycleDirectionsComplete})
		closingTimer := closingActions[len(closingActions)-1].Generation
		actions := lifecycle.Handle(LifecycleEvent{
			Kind:   LifecycleResetRequested,
			Reason: 99,
		})
		if lifecycle.State() != Resetting || len(actions) != 3 ||
			actions[0] != (LifecycleAction{Kind: LifecycleActionCancelClosingDeadline, Generation: closingTimer}) ||
			actions[1].Kind != LifecycleActionSendReset || actions[1].Reason != protocol.ResetInternalFailure ||
			actions[2].Kind != LifecycleActionArmResetDeadline {
			t.Fatalf("reset from Closing actions = %#v", actions)
		}
	})
}

func TestLifecycleCompleteEventStateTable(t *testing.T) {
	states := []LifecycleState{
		AwaitingAttachment,
		Relaying,
		Recovering,
		Closing,
		Closed,
		Resetting,
		Reset,
	}
	events := []LifecycleEventKind{
		LifecycleStarted,
		LifecycleJoinRequested,
		LifecycleJoinResultSendCompleted,
		LifecycleJoinResultSendFailed,
		LifecycleAttachmentLost,
		LifecycleSessionClosed,
		LifecycleRecoveryDeadline,
		LifecycleDirectionsComplete,
		LifecycleClosingDeadline,
		LifecycleResetRequested,
		LifecycleResetSendCompleted,
		LifecycleResetSendFailed,
		LifecycleResetDeadline,
		LifecycleRemoteReset,
	}

	for _, state := range states {
		for _, eventKind := range events {
			t.Run(lifecycleStateName(state)+"/"+lifecycleEventName(eventKind), func(t *testing.T) {
				lifecycle, attachment := newLifecycleAt(t, state)
				event := lifecycleMatrixEvent(lifecycle, attachment, eventKind)
				lifecycle.Handle(event)
				wantState := lifecycleMatrixState(state, eventKind)
				if lifecycle.State() != wantState {
					t.Fatalf("state = %d, want %d", lifecycle.State(), wantState)
				}
				assertLifecycleInvariants(t, lifecycle)
			})
		}
	}
}

func TestLifecycleStaleGenerationsHaveNoSideEffects(t *testing.T) {
	lifecycle, attachment := newRelayingLifecycle(t)
	before := lifecycle.Snapshot()
	staleEvents := []LifecycleEvent{
		{Kind: LifecycleJoinResultSendCompleted, Attachment: testAttachment(attachment.SessionGeneration, attachment.AttachmentGeneration-1)},
		{Kind: LifecycleJoinResultSendFailed, Attachment: testAttachment(attachment.SessionGeneration, attachment.AttachmentGeneration-1)},
		{Kind: LifecycleAttachmentLost, Attachment: testAttachment(attachment.SessionGeneration, attachment.AttachmentGeneration-1)},
		{Kind: LifecycleSessionClosed, Attachment: AttachmentKey{SessionGeneration: attachment.SessionGeneration - 1}},
		{Kind: LifecycleRecoveryDeadline, Generation: 1},
		{Kind: LifecycleClosingDeadline, Generation: 1},
		{Kind: LifecycleResetSendCompleted, Generation: 1},
		{Kind: LifecycleResetSendFailed, Generation: 1},
		{Kind: LifecycleResetDeadline, Generation: 1},
	}
	for _, event := range staleEvents {
		if actions := lifecycle.Handle(event); len(actions) != 0 {
			t.Fatalf("stale event %#v produced actions %#v", event, actions)
		}
		if got := lifecycle.Snapshot(); !reflect.DeepEqual(got, before) {
			t.Fatalf("stale event %#v changed snapshot:\n got %#v\nwant %#v", event, got, before)
		}
	}
}

func TestNilLifecycleIsTerminalAndSideEffectFree(t *testing.T) {
	var lifecycle *Lifecycle
	if lifecycle.State() != Reset || lifecycle.Snapshot().State != Reset {
		t.Fatal("nil lifecycle is not represented as Reset")
	}
	if actions := lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted}); len(actions) != 0 {
		t.Fatalf("nil lifecycle actions = %#v", actions)
	}
}

func newRelayingLifecycle(t *testing.T) (*Lifecycle, AttachmentKey) {
	t.Helper()
	lifecycle := NewLifecycle()
	lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})
	attachment := testAttachment(10, 20)
	publishTestAttachment(t, lifecycle, attachment)
	return lifecycle, attachment
}

func newLifecycleAt(t *testing.T, state LifecycleState) (*Lifecycle, AttachmentKey) {
	t.Helper()
	lifecycle := NewLifecycle()
	lifecycle.Handle(LifecycleEvent{Kind: LifecycleStarted})
	attachment := testAttachment(10, 20)
	if state == AwaitingAttachment {
		return lifecycle, attachment
	}
	publishTestAttachment(t, lifecycle, attachment)
	switch state {
	case Relaying:
	case Recovering:
		lifecycle.Handle(LifecycleEvent{Kind: LifecycleAttachmentLost, Attachment: attachment})
	case Closing:
		lifecycle.Handle(LifecycleEvent{Kind: LifecycleDirectionsComplete})
	case Resetting:
		lifecycle.Handle(LifecycleEvent{Kind: LifecycleResetRequested, Reason: protocol.ResetCancelled})
	case Closed:
		lifecycle.Handle(LifecycleEvent{Kind: LifecycleDirectionsComplete})
		deadline := lifecycle.Snapshot().ClosingDeadlineGeneration
		lifecycle.Handle(LifecycleEvent{Kind: LifecycleClosingDeadline, Generation: deadline})
	case Reset:
		lifecycle.Handle(LifecycleEvent{Kind: LifecycleRemoteReset})
	default:
		t.Fatalf("unsupported lifecycle state %d", state)
	}
	return lifecycle, attachment
}

func publishTestAttachment(t *testing.T, lifecycle *Lifecycle, attachment AttachmentKey) {
	t.Helper()
	generation := assertJoinResultAction(t, lifecycle, attachment, LifecycleActionSendJoinSuccess)
	actions := lifecycle.Handle(LifecycleEvent{
		Kind:       LifecycleJoinResultSendCompleted,
		Attachment: attachment,
		Generation: generation,
	})
	if len(actions) < 2 || actions[0].Kind != LifecycleActionPublishAttachment || actions[1].Kind != LifecycleActionSendAcknowledgementSnapshot {
		t.Fatalf("JOIN completion actions = %#v", actions)
	}
}

func assertJoinResultAction(t *testing.T, lifecycle *Lifecycle, attachment AttachmentKey, kind LifecycleActionKind) uint64 {
	t.Helper()
	wantState := lifecycle.State()
	actions := lifecycle.Handle(LifecycleEvent{
		Kind:       LifecycleJoinRequested,
		Attachment: attachment,
	})
	if lifecycle.State() != wantState || len(actions) != 1 || actions[0].Kind != kind ||
		actions[0].Attachment != attachment || actions[0].Generation == 0 {
		t.Fatalf("JOIN result action = state %d, actions %#v; want state %d, kind %d, attachment %#v, non-zero generation",
			lifecycle.State(), actions, wantState, kind, attachment)
	}
	return actions[0].Generation
}

func assertLifecycleStep(t *testing.T, lifecycle *Lifecycle, event LifecycleEvent, wantState LifecycleState, wantActions ...LifecycleAction) {
	t.Helper()
	actions := lifecycle.Handle(event)
	assertLifecycleStateAndActions(t, lifecycle, wantState, actions, wantActions)
}

func assertLifecycleStepKind(t *testing.T, lifecycle *Lifecycle, event LifecycleEvent, wantState LifecycleState, wantKinds ...LifecycleActionKind) {
	t.Helper()
	actions := lifecycle.Handle(event)
	if lifecycle.State() != wantState {
		t.Fatalf("state = %d, want %d; actions %#v", lifecycle.State(), wantState, actions)
	}
	if len(actions) != len(wantKinds) {
		t.Fatalf("action count = %d, want %d: %#v", len(actions), len(wantKinds), actions)
	}
	for index, wantKind := range wantKinds {
		if actions[index].Kind != wantKind {
			t.Fatalf("action %d kind = %d, want %d: %#v", index, actions[index].Kind, wantKind, actions)
		}
	}
}

func assertLifecycleStateAndActions(t *testing.T, lifecycle *Lifecycle, wantState LifecycleState, actions, wantActions []LifecycleAction) {
	t.Helper()
	if lifecycle.State() != wantState {
		t.Fatalf("state = %d, want %d; actions %#v", lifecycle.State(), wantState, actions)
	}
	if !reflect.DeepEqual(actions, wantActions) {
		t.Fatalf("actions = %#v, want %#v", actions, wantActions)
	}
}

func assertTerminalSnapshot(t *testing.T, lifecycle *Lifecycle, state LifecycleState) {
	t.Helper()
	snapshot := lifecycle.Snapshot()
	if snapshot.State != state || len(snapshot.Published) != 0 || len(snapshot.Provisional) != 0 ||
		snapshot.RecoveryReturnState != 0 || snapshot.RecoveryDeadlineGeneration != 0 ||
		snapshot.ClosingDeadlineGeneration != 0 || snapshot.ResetDeadlineGeneration != 0 ||
		snapshot.ResetAttemptGeneration != 0 {
		t.Fatalf("terminal snapshot = %#v", snapshot)
	}
}

func assertLifecycleInvariants(t *testing.T, lifecycle *Lifecycle) {
	t.Helper()
	snapshot := lifecycle.Snapshot()
	if len(snapshot.Published)+len(snapshot.Provisional) > MaxAttachments {
		t.Fatalf("attachment reservation overflow: %#v", snapshot)
	}
	switch snapshot.State {
	case AwaitingAttachment:
		if snapshot.RecoveryDeadlineGeneration == 0 || len(snapshot.Published) != 0 {
			t.Fatalf("AwaitingAttachment invariant: %#v", snapshot)
		}
	case Relaying:
		if snapshot.RecoveryDeadlineGeneration != 0 || len(snapshot.Published) == 0 {
			t.Fatalf("Relaying invariant: %#v", snapshot)
		}
	case Recovering:
		if snapshot.RecoveryReturnState != Relaying || snapshot.RecoveryDeadlineGeneration == 0 || len(snapshot.Published) != 0 {
			t.Fatalf("Recovering invariant: %#v", snapshot)
		}
	case Closing:
		if snapshot.RecoveryDeadlineGeneration != 0 || snapshot.ClosingDeadlineGeneration == 0 {
			t.Fatalf("Closing invariant: %#v", snapshot)
		}
	case Resetting:
		if snapshot.RecoveryDeadlineGeneration != 0 || snapshot.ClosingDeadlineGeneration != 0 ||
			snapshot.ResetDeadlineGeneration == 0 || snapshot.ResetAttemptGeneration == 0 || len(snapshot.Provisional) != 0 {
			t.Fatalf("Resetting invariant: %#v", snapshot)
		}
	case Closed, Reset:
		assertTerminalSnapshot(t, lifecycle, snapshot.State)
	}
}

func lifecycleMatrixEvent(lifecycle *Lifecycle, attachment AttachmentKey, kind LifecycleEventKind) LifecycleEvent {
	snapshot := lifecycle.Snapshot()
	event := LifecycleEvent{Kind: kind}
	switch kind {
	case LifecycleJoinRequested, LifecycleJoinResultSendCompleted, LifecycleJoinResultSendFailed:
		event.Attachment = testAttachment(99, 99)
	case LifecycleAttachmentLost:
		event.Attachment = attachment
	case LifecycleSessionClosed:
		event.Attachment.SessionGeneration = attachment.SessionGeneration
	case LifecycleRecoveryDeadline:
		event.Generation = snapshot.RecoveryDeadlineGeneration
	case LifecycleClosingDeadline:
		event.Generation = snapshot.ClosingDeadlineGeneration
	case LifecycleResetRequested:
		event.Reason = protocol.ResetCancelled
	case LifecycleResetSendCompleted, LifecycleResetSendFailed:
		event.Generation = snapshot.ResetAttemptGeneration
	case LifecycleResetDeadline:
		event.Generation = snapshot.ResetDeadlineGeneration
	}
	return event
}

func lifecycleMatrixState(state LifecycleState, kind LifecycleEventKind) LifecycleState {
	switch kind {
	case LifecycleAttachmentLost, LifecycleSessionClosed:
		if state == Relaying {
			return Recovering
		}
	case LifecycleRecoveryDeadline:
		if state == AwaitingAttachment || state == Recovering {
			return Resetting
		}
	case LifecycleDirectionsComplete:
		if state == AwaitingAttachment || state == Relaying || state == Recovering {
			return Closing
		}
	case LifecycleClosingDeadline:
		if state == Closing {
			return Closed
		}
	case LifecycleResetRequested:
		if state == AwaitingAttachment || state == Relaying || state == Recovering || state == Closing {
			return Resetting
		}
	case LifecycleResetSendCompleted, LifecycleResetSendFailed, LifecycleResetDeadline:
		if state == Resetting {
			return Reset
		}
	case LifecycleRemoteReset:
		if state != Closed && state != Reset {
			return Reset
		}
	}
	return state
}

func testAttachment(sessionGeneration, attachmentGeneration uint64) AttachmentKey {
	return AttachmentKey{
		SessionGeneration:    sessionGeneration,
		AttachmentGeneration: attachmentGeneration,
	}
}

func lifecycleStateName(state LifecycleState) string {
	names := map[LifecycleState]string{
		AwaitingAttachment: "awaiting_attachment",
		Relaying:           "relaying",
		Recovering:         "recovering",
		Closing:            "closing",
		Closed:             "closed",
		Resetting:          "resetting",
		Reset:              "reset",
	}
	return names[state]
}

func lifecycleEventName(kind LifecycleEventKind) string {
	names := map[LifecycleEventKind]string{
		LifecycleStarted:                 "started",
		LifecycleJoinRequested:           "join_requested",
		LifecycleJoinResultSendCompleted: "join_result_send_completed",
		LifecycleJoinResultSendFailed:    "join_result_send_failed",
		LifecycleAttachmentLost:          "attachment_lost",
		LifecycleSessionClosed:           "session_closed",
		LifecycleRecoveryDeadline:        "recovery_deadline",
		LifecycleDirectionsComplete:      "directions_complete",
		LifecycleClosingDeadline:         "closing_deadline",
		LifecycleResetRequested:          "reset_requested",
		LifecycleResetSendCompleted:      "reset_send_completed",
		LifecycleResetSendFailed:         "reset_send_failed",
		LifecycleResetDeadline:           "reset_deadline",
		LifecycleRemoteReset:             "remote_reset",
	}
	return names[kind]
}
