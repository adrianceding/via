package server

import (
	"crypto/subtle"
	"errors"
	"math"
	"testing"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

func TestJoinPublishesOnlyAfterResultSendCompletion(t *testing.T) {
	fixture := newJoinFixture(t, 8)
	publishedBeforeSessionIndex := false
	fixture.session.beforePublish = func() {
		publishedBeforeSessionIndex = len(fixture.machine.Snapshot().Lifecycle.Published) == 1
	}
	output, err := fixture.coordinator.Begin(fixture.session, fixture.message)
	if err != nil {
		t.Fatal(err)
	}
	send := requireJoinSessionAction(t, output.SessionActions, transport.SessionActionSendFrame)
	if got := fixture.session.Snapshot(); got.ReservedAttachments != 1 || got.AttachmentCount != 0 {
		t.Fatalf("before result completion session = %#v", got)
	}
	if state := fixture.machine.Snapshot().Lifecycle.State; state != flow.AwaitingAttachment {
		t.Fatalf("before result completion flow state = %v", state)
	}

	completed, err := fixture.coordinator.CompleteResult(send.Correlation, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fixture.session.Snapshot(); got.ReservedAttachments != 0 || got.AttachmentCount != 1 {
		t.Fatalf("after result completion session = %#v", got)
	}
	if state := fixture.machine.Snapshot().Lifecycle.State; state != flow.Relaying {
		t.Fatalf("after result completion flow state = %v", state)
	}
	if !hasJoinFlowAction(completed.FlowActions, flow.LifecycleActionPublishAttachment) {
		t.Fatalf("completion actions = %#v", completed.FlowActions)
	}
	if !publishedBeforeSessionIndex {
		t.Fatal("session index was updated before the authoritative Flow attachment")
	}
}

func TestJoinResultFailureRollsBackReservation(t *testing.T) {
	fixture := newJoinFixture(t, 8)
	output, err := fixture.coordinator.Begin(fixture.session, fixture.message)
	if err != nil {
		t.Fatal(err)
	}
	send := requireJoinSessionAction(t, output.SessionActions, transport.SessionActionSendFrame)
	if _, err := fixture.coordinator.CompleteResult(send.Correlation, false); err != nil {
		t.Fatal(err)
	}
	if got := fixture.session.Snapshot(); got.ReservedAttachments != 0 || got.AttachmentCount != 0 {
		t.Fatalf("rolled back session = %#v", got)
	}
	if got := fixture.machine.Snapshot().Lifecycle; len(got.Provisional) != 0 || len(got.Published) != 0 {
		t.Fatalf("rolled back flow = %#v", got)
	}
}

func TestJoinAuthorizationFailuresUseOneWireResult(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*joinFixture)
	}{
		{
			name: "wrong principal",
			prepare: func(fixture *joinFixture) {
				fixture.session.snapshot.PrincipalID = "client-02"
			},
		},
		{
			name: "wrong capability",
			prepare: func(fixture *joinFixture) {
				fixture.message.Capability[0] ^= 0xff
			},
		},
		{
			name: "unknown flow",
			prepare: func(fixture *joinFixture) {
				fixture.message.FlowID[0] ^= 0xff
			},
		},
		{
			name: "non joinable state",
			prepare: func(fixture *joinFixture) {
				if _, err := fixture.machine.Handle(flow.FlowEvent{
					Kind: flow.FlowLifecycle,
					Lifecycle: flow.LifecycleEvent{
						Kind:   flow.LifecycleResetRequested,
						Reason: protocol.ResetCancelled,
					},
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newJoinFixture(t, 8)
			test.prepare(&fixture)
			output, err := fixture.coordinator.Begin(fixture.session, fixture.message)
			if err != nil {
				t.Fatal(err)
			}
			result := requireJoinResult(t, output.SessionActions)
			if result.FlowID != fixture.message.FlowID || result.Result != protocol.JoinFailure {
				t.Fatalf("JOIN_RESULT = %#v", result)
			}
			if got := fixture.session.Snapshot(); got.ReservedAttachments != 0 || got.AttachmentCount != 0 {
				t.Fatalf("failed JOIN retained session state = %#v", got)
			}
			if len(fixture.coordinator.tracked) != 0 || len(fixture.coordinator.pending) != 0 {
				t.Fatalf("failed JOIN retained coordinator state: tracked=%d pending=%d", len(fixture.coordinator.tracked), len(fixture.coordinator.pending))
			}
		})
	}
}

func TestJoinDirectoryIdentityIsDefensivelyChecked(t *testing.T) {
	fixture := newJoinFixture(t, 8)
	bad := JoinAuthorization{
		PrincipalID: "other-principal",
		FlowID:      fixture.message.FlowID,
		Flow:        fixture.machine,
	}
	fixture.coordinator.directory = JoinAuthorizeFunc(func(string, protocol.FlowID, protocol.Capability) (JoinAuthorization, bool) {
		return bad, true
	})
	output, err := fixture.coordinator.Begin(fixture.session, fixture.message)
	if err != nil {
		t.Fatal(err)
	}
	if result := requireJoinResult(t, output.SessionActions); result.Result != protocol.JoinFailure {
		t.Fatalf("JOIN_RESULT = %#v", result)
	}
}

func TestJoinDuplicateIsIdempotentBeforeAndAfterPublication(t *testing.T) {
	fixture := newJoinFixture(t, 8)
	first, err := fixture.coordinator.Begin(fixture.session, fixture.message)
	if err != nil {
		t.Fatal(err)
	}
	firstSend := requireJoinSessionAction(t, first.SessionActions, transport.SessionActionSendFrame)
	inFlightDuplicate, err := fixture.coordinator.Begin(fixture.session, fixture.message)
	if err != nil {
		t.Fatal(err)
	}
	if len(inFlightDuplicate.SessionActions) != 0 || len(fixture.coordinator.tracked) != 1 || len(fixture.coordinator.pending) != 1 {
		t.Fatalf("in-flight duplicate = %#v, tracked=%d pending=%d", inFlightDuplicate, len(fixture.coordinator.tracked), len(fixture.coordinator.pending))
	}
	if _, err := fixture.coordinator.CompleteResult(firstSend.Correlation, true); err != nil {
		t.Fatal(err)
	}

	publishedDuplicate, err := fixture.coordinator.Begin(fixture.session, fixture.message)
	if err != nil {
		t.Fatal(err)
	}
	duplicateSend := requireJoinSessionAction(t, publishedDuplicate.SessionActions, transport.SessionActionSendFrame)
	completed, err := fixture.coordinator.CompleteResult(duplicateSend.Correlation, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fixture.session.Snapshot(); got.AttachmentCount != 1 || got.ReservedAttachments != 0 {
		t.Fatalf("duplicate changed session count = %#v", got)
	}
	if hasJoinFlowAction(completed.FlowActions, flow.LifecycleActionPublishAttachment) ||
		!hasJoinFlowAction(completed.FlowActions, flow.LifecycleActionSendAcknowledgementSnapshot) {
		t.Fatalf("published duplicate actions = %#v", completed.FlowActions)
	}
}

func TestJoinLateCompletionCannotPublishReplacement(t *testing.T) {
	fixture := newJoinFixture(t, 8)
	first, err := fixture.coordinator.Begin(fixture.session, fixture.message)
	if err != nil {
		t.Fatal(err)
	}
	firstCorrelation := requireJoinSessionAction(t, first.SessionActions, transport.SessionActionSendFrame).Correlation
	if _, err := fixture.coordinator.CompleteResult(firstCorrelation, false); err != nil {
		t.Fatal(err)
	}

	replacement, err := fixture.coordinator.Begin(fixture.session, fixture.message)
	if err != nil {
		t.Fatal(err)
	}
	replacementCorrelation := requireJoinSessionAction(t, replacement.SessionActions, transport.SessionActionSendFrame).Correlation
	if replacementCorrelation == firstCorrelation {
		t.Fatal("replacement reused result generation")
	}
	if _, err := fixture.coordinator.CompleteResult(firstCorrelation, true); err != nil {
		t.Fatal(err)
	}
	if got := fixture.session.Snapshot(); got.ReservedAttachments != 1 || got.AttachmentCount != 0 {
		t.Fatalf("late completion published replacement = %#v", got)
	}
	if _, err := fixture.coordinator.CompleteResult(replacementCorrelation, true); err != nil {
		t.Fatal(err)
	}
	if got := fixture.session.Snapshot(); got.ReservedAttachments != 0 || got.AttachmentCount != 1 {
		t.Fatalf("replacement completion = %#v", got)
	}
}

func TestJoinStaleLifecycleGenerationAndPublicationFailureRollBack(t *testing.T) {
	t.Run("stale lifecycle generation", func(t *testing.T) {
		fixture := newJoinFixture(t, 8)
		output := beginJoin(t, fixture.coordinator, fixture.session, fixture.message)
		correlation := requireJoinSessionAction(t, output.SessionActions, transport.SessionActionSendFrame).Correlation
		pending := fixture.coordinator.pending[correlation]
		pending.lifecycleGeneration++
		fixture.coordinator.pending[correlation] = pending

		if _, err := fixture.coordinator.CompleteResult(correlation, true); err != nil {
			t.Fatal(err)
		}
		if got := fixture.session.Snapshot(); got.ReservedAttachments != 0 || got.AttachmentCount != 0 {
			t.Fatalf("stale lifecycle result retained session state = %#v", got)
		}
		if got := fixture.machine.Snapshot().Lifecycle; len(got.Provisional) != 0 || len(got.Published) != 0 {
			t.Fatalf("stale lifecycle result retained flow state = %#v", got)
		}
		if len(fixture.coordinator.tracked) != 0 || len(fixture.coordinator.pending) != 0 {
			t.Fatal("stale lifecycle result retained coordinator state")
		}
	})

	t.Run("session publication failure", func(t *testing.T) {
		fixture := newJoinFixture(t, 8)
		output := beginJoin(t, fixture.coordinator, fixture.session, fixture.message)
		correlation := requireJoinSessionAction(t, output.SessionActions, transport.SessionActionSendFrame).Correlation
		fixture.session.publishError = transport.ErrInvalidSession
		completed, err := fixture.coordinator.CompleteResult(correlation, true)
		if !errors.Is(err, ErrJoinResultUnavailable) {
			t.Fatalf("publication error = %v", err)
		}
		if got := fixture.session.Snapshot(); got.ReservedAttachments != 0 || got.AttachmentCount != 0 {
			t.Fatalf("publication failure retained session state = %#v", got)
		}
		if got := fixture.machine.Snapshot().Lifecycle; got.State != flow.Recovering || len(got.Provisional) != 0 || len(got.Published) != 0 {
			t.Fatalf("publication failure retained flow state = %#v", got)
		}
		if !hasJoinFlowAction(completed.FlowActions, flow.LifecycleActionCancelRecoveryDeadline) ||
			!hasJoinFlowAction(completed.FlowActions, flow.LifecycleActionArmRecoveryDeadline) {
			t.Fatalf("publication rollback did not replace the recovery deadline: %#v", completed.FlowActions)
		}
		if len(fixture.coordinator.tracked) != 0 || len(fixture.coordinator.pending) != 0 {
			t.Fatal("publication failure retained coordinator state")
		}
	})
}

func TestJoinCoordinatorAndFlowCapacityUseUnifiedFailure(t *testing.T) {
	t.Run("coordinator capacity", func(t *testing.T) {
		firstID := protocol.FlowID{1}
		secondID := protocol.FlowID{2}
		capability := protocol.Capability{3}
		firstFlow := startedJoinFlow(t)
		secondFlow := startedJoinFlow(t)
		authorizations := map[protocol.FlowID]JoinAuthorization{
			firstID:  {PrincipalID: "client-01", FlowID: firstID, Flow: firstFlow},
			secondID: {PrincipalID: "client-01", FlowID: secondID, Flow: secondFlow},
		}
		coordinator := mustJoinCoordinator(t, testJoinDirectory(authorizations, capability), 1)
		session := readyJoinSession(11, "client-01")
		first := beginJoin(t, coordinator, session, protocol.Join{FlowID: firstID, Capability: capability})
		completeJoin(t, coordinator, first, true)

		second, err := coordinator.Begin(session, protocol.Join{FlowID: secondID, Capability: capability})
		if err != nil {
			t.Fatal(err)
		}
		if result := requireJoinResult(t, second.SessionActions); result.Result != protocol.JoinFailure {
			t.Fatalf("capacity result = %#v", result)
		}
		if session.Snapshot().AttachmentCount != 1 || secondFlow.Snapshot().Lifecycle.State != flow.AwaitingAttachment {
			t.Fatalf("capacity mutated existing state: session=%#v flow=%#v", session.Snapshot(), secondFlow.Snapshot().Lifecycle)
		}
	})

	t.Run("flow attachment capacity", func(t *testing.T) {
		fixture := newJoinFixture(t, flow.MaxAttachments+1)
		for generation := uint64(7); generation < uint64(7+flow.MaxAttachments); generation++ {
			session := readyJoinSession(generation, "client-01")
			output := beginJoin(t, fixture.coordinator, session, fixture.message)
			completeJoin(t, fixture.coordinator, output, true)
		}
		overflow := readyJoinSession(uint64(7+flow.MaxAttachments), "client-01")
		output, err := fixture.coordinator.Begin(overflow, fixture.message)
		if err != nil {
			t.Fatal(err)
		}
		if result := requireJoinResult(t, output.SessionActions); result.Result != protocol.JoinFailure {
			t.Fatalf("overflow result = %#v", result)
		}
		if got := overflow.Snapshot(); got.ReservedAttachments != 0 || got.AttachmentCount != 0 {
			t.Fatalf("overflow session retained attachment = %#v", got)
		}
		if len(fixture.machine.Snapshot().Lifecycle.Published) != flow.MaxAttachments {
			t.Fatalf("published count = %d", len(fixture.machine.Snapshot().Lifecycle.Published))
		}
	})
}

func TestJoinSessionCloseWithdrawsPublishedAndProvisional(t *testing.T) {
	firstID := protocol.FlowID{1}
	secondID := protocol.FlowID{2}
	capability := protocol.Capability{3}
	firstFlow := startedJoinFlow(t)
	secondFlow := startedJoinFlow(t)
	authorizations := map[protocol.FlowID]JoinAuthorization{
		firstID:  {PrincipalID: "client-01", FlowID: firstID, Flow: firstFlow},
		secondID: {PrincipalID: "client-01", FlowID: secondID, Flow: secondFlow},
	}
	coordinator := mustJoinCoordinator(t, testJoinDirectory(authorizations, capability), 8)
	session := readyJoinSession(21, "client-01")
	first := beginJoin(t, coordinator, session, protocol.Join{FlowID: firstID, Capability: capability})
	completeJoin(t, coordinator, first, true)
	second := beginJoin(t, coordinator, session, protocol.Join{FlowID: secondID, Capability: capability})
	lateCorrelation := requireJoinSessionAction(t, second.SessionActions, transport.SessionActionSendFrame).Correlation
	session.close()

	closed, err := coordinator.SessionClosed(21)
	if err != nil {
		t.Fatal(err)
	}
	if !hasJoinFlowAction(closed.FlowActions, flow.LifecycleActionWithdrawAttachment) {
		t.Fatalf("session close actions = %#v", closed.FlowActions)
	}
	if len(coordinator.tracked) != 0 || len(coordinator.pending) != 0 {
		t.Fatalf("session close retained coordinator state: tracked=%d pending=%d", len(coordinator.tracked), len(coordinator.pending))
	}
	if firstFlow.Snapshot().Lifecycle.State != flow.Recovering || len(firstFlow.Snapshot().Lifecycle.Published) != 0 {
		t.Fatalf("published flow after close = %#v", firstFlow.Snapshot().Lifecycle)
	}
	if secondFlow.Snapshot().Lifecycle.State != flow.AwaitingAttachment || len(secondFlow.Snapshot().Lifecycle.Provisional) != 0 {
		t.Fatalf("provisional flow after close = %#v", secondFlow.Snapshot().Lifecycle)
	}
	if _, err := coordinator.CompleteResult(lateCorrelation, true); err != nil {
		t.Fatal(err)
	}
	if len(secondFlow.Snapshot().Lifecycle.Published) != 0 {
		t.Fatal("late completion published closed session")
	}
}

func TestJoinAttachmentLostAndGenerationExhaustionAreBounded(t *testing.T) {
	fixture := newJoinFixture(t, 8)
	output := beginJoin(t, fixture.coordinator, fixture.session, fixture.message)
	completeJoin(t, fixture.coordinator, output, true)
	attachment := fixture.machine.Snapshot().Lifecycle.Published[0]
	lost, err := fixture.coordinator.AttachmentLost(attachment)
	if err != nil {
		t.Fatal(err)
	}
	if !hasJoinFlowAction(lost.FlowActions, flow.LifecycleActionWithdrawAttachment) || len(fixture.coordinator.tracked) != 0 {
		t.Fatalf("lost output = %#v tracked=%d", lost, len(fixture.coordinator.tracked))
	}
	if _, err := fixture.coordinator.AttachmentLost(attachment); err != nil {
		t.Fatal(err)
	}

	exhausted := newJoinFixture(t, 8)
	exhausted.coordinator.nextGeneration = math.MaxUint64
	failure, err := exhausted.coordinator.Begin(exhausted.session, exhausted.message)
	if err != nil {
		t.Fatal(err)
	}
	if result := requireJoinResult(t, failure.SessionActions); result.Result != protocol.JoinFailure {
		t.Fatalf("exhaustion result = %#v", result)
	}
	if len(exhausted.coordinator.tracked) != 0 || len(exhausted.coordinator.pending) != 0 {
		t.Fatal("generation exhaustion retained state")
	}
}

func TestJoinExplicitWithdrawalCleansPublishedIndex(t *testing.T) {
	fixture := newJoinFixture(t, 8)
	output := beginJoin(t, fixture.coordinator, fixture.session, fixture.message)
	completeJoin(t, fixture.coordinator, output, true)
	attachment := fixture.machine.Snapshot().Lifecycle.Published[0]
	if err := fixture.coordinator.WithdrawAttachment(attachment); err != nil {
		t.Fatal(err)
	}
	if got := fixture.session.Snapshot(); got.AttachmentCount != 0 || got.ReservedAttachments != 0 {
		t.Fatalf("published withdrawal session = %#v", got)
	}
	if len(fixture.coordinator.tracked) != 0 || len(fixture.coordinator.pending) != 0 {
		t.Fatal("published withdrawal retained coordinator state")
	}
	if err := fixture.coordinator.WithdrawAttachment(attachment); err != nil {
		t.Fatal(err)
	}
}

func TestJoinFlowTerminalCleansEverySessionAndLateResult(t *testing.T) {
	fixture := newJoinFixture(t, 8)
	first := beginJoin(t, fixture.coordinator, fixture.session, fixture.message)
	completeJoin(t, fixture.coordinator, first, true)
	secondSession := readyJoinSession(8, "client-01")
	second := beginJoin(t, fixture.coordinator, secondSession, fixture.message)
	lateCorrelation := requireJoinSessionAction(t, second.SessionActions, transport.SessionActionSendFrame).Correlation

	resetActions, err := fixture.machine.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind:   flow.LifecycleResetRequested,
			Reason: protocol.ResetCancelled,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resetGeneration := requireJoinLifecycleGeneration(t, resetActions, flow.LifecycleActionSendReset)
	if _, err := fixture.machine.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind:       flow.LifecycleResetSendCompleted,
			Generation: resetGeneration,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if fixture.machine.Snapshot().Lifecycle.State != flow.Reset {
		t.Fatalf("terminal state = %v", fixture.machine.Snapshot().Lifecycle.State)
	}

	if err := fixture.coordinator.FlowTerminal("client-01", fixture.message.FlowID); err != nil {
		t.Fatal(err)
	}
	if fixture.session.Snapshot().AttachmentCount != 0 || secondSession.Snapshot().ReservedAttachments != 0 {
		t.Fatalf("terminal indexes: first=%#v second=%#v", fixture.session.Snapshot(), secondSession.Snapshot())
	}
	if len(fixture.coordinator.tracked) != 0 || len(fixture.coordinator.pending) != 0 {
		t.Fatal("terminal flow retained coordinator state")
	}
	if _, err := fixture.coordinator.CompleteResult(lateCorrelation, true); err != nil {
		t.Fatal(err)
	}
	if secondSession.Snapshot().AttachmentCount != 0 {
		t.Fatal("late result republished terminal flow")
	}
	if err := fixture.coordinator.FlowTerminal("client-01", fixture.message.FlowID); err != nil {
		t.Fatal(err)
	}
}

func TestJoinConstructorAndResultQueueFailureAreStable(t *testing.T) {
	if _, err := NewJoinCoordinator(nil, 1); !errors.Is(err, ErrInvalidJoinCoordinator) {
		t.Fatalf("nil directory error = %v", err)
	}
	fixture := newJoinFixture(t, 8)
	fixture.session.sendError = transport.ErrSessionCapacity
	if _, err := fixture.coordinator.Begin(fixture.session, fixture.message); !errors.Is(err, ErrJoinResultUnavailable) {
		t.Fatalf("send error = %v", err)
	}
	if len(fixture.coordinator.tracked) != 0 || len(fixture.coordinator.pending) != 0 || fixture.session.Snapshot().ReservedAttachments != 0 {
		t.Fatal("send error did not roll back")
	}
}

type joinFixture struct {
	coordinator *JoinCoordinator
	session     *fakeJoinSession
	machine     *flow.Flow
	message     protocol.Join
}

func newJoinFixture(t *testing.T, maximum int) joinFixture {
	t.Helper()
	flowID := protocol.FlowID{1}
	capability := protocol.Capability{2}
	machine := flow.NewFlow()
	if _, err := machine.Handle(flow.FlowEvent{Kind: flow.FlowStarted}); err != nil {
		t.Fatal(err)
	}
	authorization := JoinAuthorization{
		PrincipalID: "client-01",
		FlowID:      flowID,
		Flow:        machine,
	}
	directory := testJoinDirectory(map[protocol.FlowID]JoinAuthorization{flowID: authorization}, capability)
	coordinator, err := NewJoinCoordinator(directory, maximum)
	if err != nil {
		t.Fatal(err)
	}
	return joinFixture{
		coordinator: coordinator,
		session: &fakeJoinSession{snapshot: transport.SessionSnapshot{
			Generation:  7,
			State:       transport.SessionReady,
			PrincipalID: "client-01",
		}},
		machine: machine,
		message: protocol.Join{FlowID: flowID, Capability: capability},
	}
}

func startedJoinFlow(t *testing.T) *flow.Flow {
	t.Helper()
	machine := flow.NewFlow()
	if _, err := machine.Handle(flow.FlowEvent{Kind: flow.FlowStarted}); err != nil {
		t.Fatal(err)
	}
	return machine
}

func mustJoinCoordinator(t *testing.T, directory JoinDirectory, maximum int) *JoinCoordinator {
	t.Helper()
	coordinator, err := NewJoinCoordinator(directory, maximum)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func testJoinDirectory(authorizations map[protocol.FlowID]JoinAuthorization, capability protocol.Capability) JoinDirectory {
	dummy := protocol.Capability{0xa5}
	return JoinAuthorizeFunc(func(principalID string, flowID protocol.FlowID, supplied protocol.Capability) (JoinAuthorization, bool) {
		authorization, found := authorizations[flowID]
		expected := dummy
		if found {
			expected = capability
		}
		capabilityMatches := subtle.ConstantTimeCompare(supplied[:], expected[:]) == 1
		return authorization, found && capabilityMatches && principalID == authorization.PrincipalID
	})
}

func readyJoinSession(generation uint64, principalID string) *fakeJoinSession {
	return &fakeJoinSession{snapshot: transport.SessionSnapshot{
		Generation:  generation,
		State:       transport.SessionReady,
		PrincipalID: principalID,
	}}
}

func beginJoin(t *testing.T, coordinator *JoinCoordinator, session JoinSession, message protocol.Join) JoinOutput {
	t.Helper()
	output, err := coordinator.Begin(session, message)
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func completeJoin(t *testing.T, coordinator *JoinCoordinator, output JoinOutput, succeeded bool) JoinOutput {
	t.Helper()
	send := requireJoinSessionAction(t, output.SessionActions, transport.SessionActionSendFrame)
	completed, err := coordinator.CompleteResult(send.Correlation, succeeded)
	if err != nil {
		t.Fatal(err)
	}
	return completed
}

type fakeJoinSession struct {
	snapshot      transport.SessionSnapshot
	reservations  map[protocol.FlowID]uint64
	attachments   map[protocol.FlowID]uint64
	nextSend      uint64
	sendError     error
	publishError  error
	beforePublish func()
}

func (session *fakeJoinSession) Snapshot() transport.SessionSnapshot {
	snapshot := session.snapshot
	snapshot.ReservedAttachments = len(session.reservations)
	snapshot.AttachmentCount = len(session.attachments)
	return snapshot
}

func (session *fakeJoinSession) Handle(event transport.SessionEvent) ([]transport.SessionAction, error) {
	if session.reservations == nil {
		session.reservations = make(map[protocol.FlowID]uint64)
	}
	if session.attachments == nil {
		session.attachments = make(map[protocol.FlowID]uint64)
	}
	switch event.Kind {
	case transport.SessionReserveAttachment:
		session.reservations[event.FlowID] = event.AttachmentGeneration
	case transport.SessionCancelAttachment:
		if session.reservations[event.FlowID] == event.AttachmentGeneration {
			delete(session.reservations, event.FlowID)
		}
	case transport.SessionPublishAttachment:
		if session.beforePublish != nil {
			session.beforePublish()
		}
		if session.publishError != nil {
			return nil, session.publishError
		}
		if session.attachments[event.FlowID] == event.AttachmentGeneration {
			return nil, nil
		}
		if session.reservations[event.FlowID] != event.AttachmentGeneration {
			return nil, transport.ErrInvalidSession
		}
		delete(session.reservations, event.FlowID)
		session.attachments[event.FlowID] = event.AttachmentGeneration
	case transport.SessionWithdrawAttachment:
		if session.attachments[event.FlowID] == event.AttachmentGeneration {
			delete(session.attachments, event.FlowID)
		}
	case transport.SessionSendMessage:
		if session.sendError != nil {
			return nil, session.sendError
		}
		session.nextSend++
		return []transport.SessionAction{{
			Kind:        transport.SessionActionSendFrame,
			Generation:  session.nextSend,
			Correlation: event.Correlation,
			Message:     event.Message,
		}}, nil
	}
	return nil, nil
}

func (session *fakeJoinSession) close() {
	session.snapshot.State = transport.SessionClosed
	clear(session.reservations)
	clear(session.attachments)
}

func requireJoinSessionAction(t *testing.T, actions []transport.SessionAction, kind transport.SessionActionKind) transport.SessionAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("missing session action %v in %#v", kind, actions)
	return transport.SessionAction{}
}

func hasJoinFlowAction(actions []flow.FlowAction, kind flow.LifecycleActionKind) bool {
	for _, action := range actions {
		if action.Kind == flow.FlowActionLifecycle && action.Lifecycle.Kind == kind {
			return true
		}
	}
	return false
}

func requireJoinLifecycleGeneration(t *testing.T, actions []flow.FlowAction, kind flow.LifecycleActionKind) uint64 {
	t.Helper()
	for _, action := range actions {
		if action.Kind == flow.FlowActionLifecycle && action.Lifecycle.Kind == kind {
			if action.Lifecycle.Generation == 0 {
				t.Fatalf("lifecycle action %v has no generation", kind)
			}
			return action.Lifecycle.Generation
		}
	}
	t.Fatalf("missing lifecycle action %v in %#v", kind, actions)
	return 0
}

func requireJoinResult(t *testing.T, actions []transport.SessionAction) protocol.JoinResult {
	t.Helper()
	action := requireJoinSessionAction(t, actions, transport.SessionActionSendFrame)
	result, ok := action.Message.(protocol.JoinResult)
	if !ok {
		t.Fatalf("session message = %T", action.Message)
	}
	return result
}
