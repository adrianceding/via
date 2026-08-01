package transport

import (
	"bytes"
	"errors"
	"net/netip"
	"testing"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/protocol"
)

func TestSessionsCompleteAuthenticationWithStrictReadBarriers(t *testing.T) {
	client, server := authenticateSessions(t)
	clientSnapshot := client.Snapshot()
	serverSnapshot := server.Snapshot()
	if clientSnapshot.State != SessionReady || serverSnapshot.State != SessionReady {
		t.Fatalf("session states = %v / %v", clientSnapshot.State, serverSnapshot.State)
	}
	if clientSnapshot.PrincipalID != "client-01" || serverSnapshot.PrincipalID != "client-01" {
		t.Fatalf("principals = %q / %q", clientSnapshot.PrincipalID, serverSnapshot.PrincipalID)
	}
	if !clientSnapshot.ReadAllowed || !serverSnapshot.ReadAllowed {
		t.Fatalf("ready read barriers = %t / %t", clientSnapshot.ReadAllowed, serverSnapshot.ReadAllowed)
	}
}

func TestSessionRejectsFrameBeforeAuthenticationSendCompletes(t *testing.T) {
	server := newServerSessionForTest(t)
	actions, err := server.Handle(SessionEvent{Kind: SessionStarted})
	if err != nil {
		t.Fatal(err)
	}
	if requireSessionAction(t, actions, SessionActionSendFrame).Generation == 0 {
		t.Fatal("challenge send has no generation")
	}
	encoded, err := protocol.EncodeMessage(protocol.AuthProof{PrincipalID: "client-01"})
	if err != nil {
		t.Fatal(err)
	}
	actions, err = server.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: encoded})
	if !errors.Is(err, ErrSessionBusy) || server.Snapshot().State != SessionClosed || !hasSessionAction(actions, SessionActionCloseTransport) {
		t.Fatalf("early frame result: state=%v actions=%#v err=%v", server.Snapshot().State, actions, err)
	}
}

func TestSessionAttachmentAuthorizationAndDispatchBarrier(t *testing.T) {
	_, server := authenticateSessions(t)
	flowID := protocol.FlowID{1}
	encoded := mustEncodeSessionMessage(t, protocol.Data{FlowID: flowID, Bytes: []byte("x")})

	actions, err := server.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: encoded})
	if err != nil {
		t.Fatal(err)
	}
	if !hasSessionAction(actions, SessionActionDroppedUnknownAttachment) || !hasSessionAction(actions, SessionActionAllowRead) {
		t.Fatalf("unknown attachment actions = %#v", actions)
	}
	if err := publishSessionAttachment(server, flowID, 7); err != nil {
		t.Fatal(err)
	}
	actions, err = server.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: encoded})
	if err != nil {
		t.Fatal(err)
	}
	dispatch := requireSessionAction(t, actions, SessionActionDispatch)
	if dispatch.Generation == 0 || dispatch.FlowID != flowID || dispatch.AttachmentGeneration != 7 {
		t.Fatalf("dispatch = %#v", dispatch)
	}
	if server.Snapshot().ReadAllowed {
		t.Fatal("dispatch did not hold read barrier")
	}
	if actions, err := server.Handle(SessionEvent{Kind: SessionDispatchCompleted, Generation: dispatch.Generation + 1}); err != nil || len(actions) != 0 {
		t.Fatalf("stale dispatch result: actions=%#v err=%v", actions, err)
	}
	actions, err = server.Handle(SessionEvent{Kind: SessionDispatchCompleted, Generation: dispatch.Generation})
	if err != nil || !hasSessionAction(actions, SessionActionAllowRead) || !server.Snapshot().ReadAllowed {
		t.Fatalf("dispatch completion: actions=%#v err=%v snapshot=%#v", actions, err, server.Snapshot())
	}
}

func TestSessionAttachmentGenerationAndCapacity(t *testing.T) {
	_, server := authenticateSessions(t)
	unreserved := protocol.FlowID{9}
	if _, err := server.Handle(SessionEvent{Kind: SessionPublishAttachment, FlowID: unreserved, AttachmentGeneration: 1}); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("unreserved publish error = %v", err)
	}
	reserved := protocol.FlowID{8}
	if _, err := server.Handle(SessionEvent{Kind: SessionReserveAttachment, FlowID: reserved, AttachmentGeneration: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Handle(SessionEvent{Kind: SessionCancelAttachment, FlowID: reserved, AttachmentGeneration: 6}); err != nil {
		t.Fatal(err)
	}
	if server.Snapshot().ReservedAttachments != 1 {
		t.Fatal("stale cancellation removed reservation")
	}
	if _, err := server.Handle(SessionEvent{Kind: SessionCancelAttachment, FlowID: reserved, AttachmentGeneration: 5}); err != nil {
		t.Fatal(err)
	}
	if server.Snapshot().ReservedAttachments != 0 {
		t.Fatal("matching cancellation retained reservation")
	}
	first := protocol.FlowID{1}
	if err := publishSessionAttachment(server, first, 1); err != nil {
		t.Fatal(err)
	}
	if err := publishSessionAttachment(server, first, 1); err != nil {
		t.Fatalf("idempotent publish: %v", err)
	}
	if err := publishSessionAttachment(server, first, 2); !errors.Is(err, ErrAttachmentConflict) {
		t.Fatalf("conflicting publish error = %v", err)
	}
	if _, err := server.Handle(SessionEvent{Kind: SessionWithdrawAttachment, FlowID: first, AttachmentGeneration: 2}); err != nil {
		t.Fatal(err)
	}
	if server.Snapshot().AttachmentCount != 1 {
		t.Fatal("stale withdrawal removed current attachment")
	}
	if _, err := server.Handle(SessionEvent{Kind: SessionWithdrawAttachment, FlowID: first, AttachmentGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < MaxSessionAttachments; index++ {
		flowID := protocol.FlowID{byte(index >> 8), byte(index), 2}
		if err := publishSessionAttachment(server, flowID, uint64(index+1)); err != nil {
			t.Fatalf("publish %d: %v", index, err)
		}
	}
	if err := publishSessionAttachment(server, protocol.FlowID{9, 9, 9}, 1); !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
}

func TestSessionFailedSendReportsCurrentAndQueuedOwners(t *testing.T) {
	_, server := authenticateSessions(t)
	firstActions, err := server.Handle(SessionEvent{
		Kind:        SessionSendMessage,
		Message:     &protocol.OpenResult{Result: protocol.OpenConnectFailed},
		Class:       FrameControl,
		Correlation: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondActions, err := server.Handle(SessionEvent{
		Kind:        SessionSendMessage,
		Message:     protocol.OpenResult{Result: protocol.OpenConnectFailed},
		Class:       FrameControl,
		Correlation: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireSessionAction(t, firstActions, SessionActionSendFrame)
	second := requireSessionAction(t, secondActions, SessionActionSendFrame)
	actions, err := server.Handle(SessionEvent{Kind: SessionSendFailed, Generation: first.Generation})
	if err != nil {
		t.Fatal(err)
	}
	var results []SessionAction
	for _, action := range actions {
		if action.Kind == SessionActionSendResult {
			results = append(results, action)
		}
	}
	if len(results) != 2 || results[0].Generation != first.Generation || results[0].Correlation != 10 || results[0].SendSucceeded ||
		results[1].Generation != second.Generation || results[1].Correlation != 20 || results[1].SendSucceeded {
		t.Fatalf("send failure results = %#v", results)
	}
	if !hasSessionAction(actions, SessionActionCloseTransport) || server.Snapshot().State != SessionClosed {
		t.Fatalf("send failure did not close session: %#v / %#v", actions, server.Snapshot())
	}
}

func TestSessionCloseFailsSendsAndWithdrawsAttachmentsDeterministically(t *testing.T) {
	_, server := authenticateSessions(t)
	flowA := protocol.FlowID{2}
	flowB := protocol.FlowID{1}
	if err := publishSessionAttachment(server, flowA, 20); err != nil {
		t.Fatal(err)
	}
	if err := publishSessionAttachment(server, flowB, 10); err != nil {
		t.Fatal(err)
	}
	actions, err := server.Handle(SessionEvent{
		Kind:        SessionSendMessage,
		Message:     protocol.OpenResult{FlowID: flowA, Result: protocol.OpenConnectFailed},
		Class:       FrameControl,
		Correlation: 99,
	})
	if err != nil {
		t.Fatal(err)
	}
	send := requireSessionAction(t, actions, SessionActionSendFrame)
	actions, err = server.Handle(SessionEvent{Kind: SessionTransportClosed})
	if err != nil {
		t.Fatal(err)
	}
	if server.Snapshot().State != SessionClosed || hasSessionAction(actions, SessionActionCloseTransport) {
		t.Fatalf("transport-close result = %#v / %#v", server.Snapshot(), actions)
	}
	result := requireSessionAction(t, actions, SessionActionSendResult)
	if result.Generation != send.Generation || result.Correlation != 99 || result.SendSucceeded {
		t.Fatalf("send failure result = %#v", result)
	}
	var lost []SessionAction
	for _, action := range actions {
		if action.Kind == SessionActionAttachmentLost {
			lost = append(lost, action)
		}
	}
	if len(lost) != 2 || lost[0].FlowID != flowB || lost[0].Attachment.AttachmentGeneration != 10 ||
		lost[1].FlowID != flowA || lost[1].Attachment.AttachmentGeneration != 20 {
		t.Fatalf("attachment loss actions = %#v", lost)
	}
}

func TestSessionCloseCancelsProvisionalAttachment(t *testing.T) {
	_, server := authenticateSessions(t)
	flowID := protocol.FlowID{3}
	if _, err := server.Handle(SessionEvent{
		Kind:                 SessionReserveAttachment,
		FlowID:               flowID,
		AttachmentGeneration: 30,
	}); err != nil {
		t.Fatal(err)
	}
	actions, err := server.Handle(SessionEvent{Kind: SessionTransportClosed})
	if err != nil {
		t.Fatal(err)
	}
	lost := requireSessionAction(t, actions, SessionActionAttachmentLost)
	if lost.FlowID != flowID || lost.Attachment.SessionGeneration != 2 || lost.Attachment.AttachmentGeneration != 30 {
		t.Fatalf("provisional loss action = %#v", lost)
	}
	if snapshot := server.Snapshot(); snapshot.AttachmentCount != 0 || snapshot.ReservedAttachments != 0 {
		t.Fatalf("closed attachment counts = %#v", snapshot)
	}
}

func TestSessionProbeEchoAndDirectionValidation(t *testing.T) {
	client, server := authenticateSessions(t)
	probe := mustEncodeSessionMessage(t, protocol.Probe{Token: 123})
	actions, err := server.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: probe})
	if err != nil {
		t.Fatal(err)
	}
	send := requireSessionAction(t, actions, SessionActionSendFrame)
	_, message, err := protocol.DecodeEncodedFrame(send.Encoded)
	if err != nil || message != (protocol.ProbeACK{Token: 123}) || !hasSessionAction(actions, SessionActionAllowRead) {
		t.Fatalf("probe actions/message = %#v / %#v / %v", actions, message, err)
	}

	wrongDirection := mustEncodeSessionMessage(t, protocol.Open{
		DeliveryMode:  protocol.DeliveryAdaptive,
		PathSelection: protocol.PathFastest,
		Target: protocol.Target{
			Address: netip.MustParseAddr("192.0.2.1"),
			Port:    443,
		},
	})
	actions, err = client.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: wrongDirection})
	if !errors.Is(err, ErrInvalidSession) || client.Snapshot().State != SessionClosed || !hasSessionAction(actions, SessionActionCloseTransport) {
		t.Fatalf("wrong-direction result: state=%v actions=%#v err=%v", client.Snapshot().State, actions, err)
	}
}

func TestSessionMessageDirectionAndFlowIDMatrix(t *testing.T) {
	client, server := authenticateSessions(t)
	flowID := protocol.FlowID{4}
	if err := publishSessionAttachment(client, flowID, 11); err != nil {
		t.Fatal(err)
	}
	if err := publishSessionAttachment(server, flowID, 12); err != nil {
		t.Fatal(err)
	}

	serverInbound := []protocol.Message{
		protocol.Open{
			FlowID:        flowID,
			DeliveryMode:  protocol.DeliveryAdaptive,
			PathSelection: protocol.PathFastest,
			Target:        protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 443},
		},
		protocol.Join{FlowID: flowID},
	}
	clientInbound := []protocol.Message{
		protocol.OpenResult{FlowID: flowID, Result: protocol.OpenConnectFailed},
		protocol.JoinResult{FlowID: flowID, Result: protocol.JoinFailure},
	}
	shared := []protocol.Message{
		protocol.Data{FlowID: flowID, Bytes: []byte{1}},
		protocol.ACK{FlowID: flowID},
		protocol.FIN{FlowID: flowID},
		protocol.FINACK{FlowID: flowID},
		protocol.Reset{FlowID: flowID, Reason: protocol.ResetCancelled},
	}
	for _, message := range serverInbound {
		dispatchSessionMessage(t, server, message, flowID)
		sendSessionMessage(t, client, message)
	}
	for _, message := range clientInbound {
		dispatchSessionMessage(t, client, message, flowID)
		sendSessionMessage(t, server, message)
	}
	for _, message := range shared {
		dispatchSessionMessage(t, client, message, flowID)
		dispatchSessionMessage(t, server, message, flowID)
		sendSessionMessage(t, client, message)
		sendSessionMessage(t, server, message)
	}
}

func TestSessionPendingSendLimitIsHard(t *testing.T) {
	_, server := authenticateSessions(t)
	for index := 0; index < int(V1OutputQueueFrameLimit); index++ {
		if _, err := server.Handle(SessionEvent{
			Kind:        SessionSendMessage,
			Message:     protocol.OpenResult{Result: protocol.OpenConnectFailed},
			Class:       FrameControl,
			Correlation: uint64(index + 1),
		}); err != nil {
			t.Fatalf("send %d: %v", index, err)
		}
	}
	if _, err := server.Handle(SessionEvent{
		Kind:    SessionSendMessage,
		Message: protocol.OpenResult{Result: protocol.OpenConnectFailed},
		Class:   FrameControl,
	}); !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("pending send capacity error = %v", err)
	}
	actions, err := server.Handle(SessionEvent{Kind: SessionTransportClosed})
	if err != nil {
		t.Fatal(err)
	}
	results := 0
	for _, action := range actions {
		if action.Kind == SessionActionSendResult {
			results++
		}
	}
	if results != int(V1OutputQueueFrameLimit) {
		t.Fatalf("failed send results = %d", results)
	}
}

func authenticateSessions(t *testing.T) (*Session, *Session) {
	t.Helper()
	client := newClientSessionForTest(t)
	server := newServerSessionForTest(t)

	clientStart, err := client.Handle(SessionEvent{Kind: SessionStarted})
	if err != nil || !hasSessionAction(clientStart, SessionActionAllowRead) {
		t.Fatalf("client start = %#v, %v", clientStart, err)
	}
	serverStart, err := server.Handle(SessionEvent{Kind: SessionStarted})
	if err != nil {
		t.Fatal(err)
	}
	challenge := requireSessionAction(t, serverStart, SessionActionSendFrame)
	if _, err := server.Handle(SessionEvent{Kind: SessionSendCompleted, Generation: challenge.Generation}); err != nil {
		t.Fatal(err)
	}
	clientProofActions, err := client.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: challenge.Encoded})
	if err != nil {
		t.Fatal(err)
	}
	proof := requireSessionAction(t, clientProofActions, SessionActionSendFrame)
	if _, err := client.Handle(SessionEvent{Kind: SessionSendCompleted, Generation: proof.Generation}); err != nil {
		t.Fatal(err)
	}
	serverResultActions, err := server.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: proof.Encoded})
	if err != nil {
		t.Fatal(err)
	}
	result := requireSessionAction(t, serverResultActions, SessionActionSendFrame)
	serverReadyActions, err := server.Handle(SessionEvent{Kind: SessionSendCompleted, Generation: result.Generation})
	if err != nil || !hasSessionAction(serverReadyActions, SessionActionAuthenticated) || !hasSessionAction(serverReadyActions, SessionActionAllowRead) {
		t.Fatalf("server ready = %#v, %v", serverReadyActions, err)
	}
	clientReadyActions, err := client.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: result.Encoded})
	if err != nil || !hasSessionAction(clientReadyActions, SessionActionAuthenticated) || !hasSessionAction(clientReadyActions, SessionActionAllowRead) {
		t.Fatalf("client ready = %#v, %v", clientReadyActions, err)
	}
	return client, server
}

func newClientSessionForTest(t *testing.T) *Session {
	t.Helper()
	machine, err := auth.NewClientMachine("client-01", auth.Key{1}, bytes.NewReader(make([]byte, auth.NonceSize)))
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewClientSession(1, machine)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func newServerSessionForTest(t *testing.T) *Session {
	t.Helper()
	generator, err := auth.NewChallengeGenerator(bytes.NewReader(make([]byte, auth.KeySize+16)))
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(map[string]auth.Key{"client-01": {1}}, auth.Key{2})
	machine, err := auth.NewServerMachine(generator, verifier)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewServerSession(2, machine)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func publishSessionAttachment(session *Session, flowID protocol.FlowID, generation uint64) error {
	if _, err := session.Handle(SessionEvent{
		Kind:                 SessionReserveAttachment,
		FlowID:               flowID,
		AttachmentGeneration: generation,
	}); err != nil {
		return err
	}
	_, err := session.Handle(SessionEvent{
		Kind:                 SessionPublishAttachment,
		FlowID:               flowID,
		AttachmentGeneration: generation,
	})
	return err
}

func dispatchSessionMessage(t *testing.T, session *Session, message protocol.Message, wantFlowID protocol.FlowID) {
	t.Helper()
	actions, err := session.Handle(SessionEvent{Kind: SessionFrameReceived, Encoded: mustEncodeSessionMessage(t, message)})
	if err != nil {
		t.Fatal(err)
	}
	dispatch := requireSessionAction(t, actions, SessionActionDispatch)
	if dispatch.FlowID != wantFlowID || dispatch.Generation == 0 {
		t.Fatalf("dispatch = %#v", dispatch)
	}
	if _, err := session.Handle(SessionEvent{Kind: SessionDispatchCompleted, Generation: dispatch.Generation}); err != nil {
		t.Fatal(err)
	}
}

func sendSessionMessage(t *testing.T, session *Session, message protocol.Message) {
	t.Helper()
	class := FrameControl
	encoded := mustEncodeSessionMessage(t, message)
	frameType, _, err := protocol.ParseHeader(encoded[:protocol.HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	if frameType == protocol.TypeData {
		class = FrameData
	}
	actions, err := session.Handle(SessionEvent{Kind: SessionSendMessage, Message: message, Class: class, Correlation: 1})
	if err != nil {
		t.Fatal(err)
	}
	send := requireSessionAction(t, actions, SessionActionSendFrame)
	if _, err := session.Handle(SessionEvent{Kind: SessionSendCompleted, Generation: send.Generation}); err != nil {
		t.Fatal(err)
	}
}

func mustEncodeSessionMessage(t *testing.T, message protocol.Message) []byte {
	t.Helper()
	encoded, err := protocol.EncodeMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func requireSessionAction(t *testing.T, actions []SessionAction, kind SessionActionKind) SessionAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("no session action %v in %#v", kind, actions)
	return SessionAction{}
}

func hasSessionAction(actions []SessionAction, kind SessionActionKind) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}
