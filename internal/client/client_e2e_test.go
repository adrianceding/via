package client

import (
	"bytes"
	"context"
	"net/netip"
	"reflect"
	"testing"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

func TestClientGate8OpenJoinDataAndRecoveryChain(t *testing.T) {
	manager := newSessionManagerForTest(t, 2, 2)
	ethernet := clientCandidate(1, "eth0", "192.0.2.10")
	wireless := clientCandidate(2, "wlan0", "192.0.2.20")
	dials := sessionActions(mustChangeClientPaths(t, manager, wireless, ethernet), SessionActionDial)
	if len(dials) != 2 || dials[0].Candidate != ethernet || dials[1].Candidate != wireless {
		t.Fatalf("initial path dials = %#v", dials)
	}

	sessions := make(map[uint64]*transport.Session, 3)
	readyGenerations := make([]uint64, 0, 2)
	for _, dial := range dials {
		connection := &gate8Connection{
			localEndpoint:  netip.AddrPortFrom(dial.Candidate.LocalAddress, 40000+uint16(dial.Generation)).String(),
			remoteEndpoint: "127.0.0.1:9443",
		}
		actions, err := manager.Handle(SessionManagerEvent{
			Kind: SessionDialCompleted, Generation: dial.Generation, Connection: connection,
		})
		if err != nil || len(sessionActions(actions, SessionActionStartAuthentication)) != 1 {
			t.Fatalf("dial completion generation %d = %#v, %v", dial.Generation, actions, err)
		}
		sessions[dial.Generation] = gate8ReadyClientSession(t, dial.Generation)
		actions, err = manager.Handle(SessionManagerEvent{
			Kind: SessionAuthenticationCompleted, Generation: dial.Generation, Succeeded: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		ready := requireSessionManagerAction(t, actions, SessionActionReady)
		if ready.Generation != dial.Generation || ready.Candidate != dial.Candidate || ready.Connection != connection {
			t.Fatalf("ready action = %#v", ready)
		}
		readyGenerations = append(readyGenerations, ready.Generation)
	}
	if snapshot := manager.Snapshot(); len(snapshot.Sessions) != 2 ||
		snapshot.Sessions[0].ActualLocalAddress == snapshot.Sessions[1].ActualLocalAddress ||
		snapshot.Sessions[0].State != ManagedSessionReady || snapshot.Sessions[1].State != ManagedSessionReady {
		t.Fatalf("two-interface session snapshot = %#v", snapshot)
	}

	target := protocol.Target{DNSName: "example.test", Port: 443}
	socks := newSOCKSCoordinatorForTest(t)
	socksStart, err := socks.Handle(SOCKSEvent{Kind: SOCKSStart})
	if err != nil {
		t.Fatal(err)
	}
	greeting := requireSOCKSAction(t, socksStart, SOCKSActionReadGreeting)
	methodActions, err := socks.Handle(SOCKSEvent{
		Kind: SOCKSGreetingResult, Generation: greeting.Generation, Accepted: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	method := requireSOCKSAction(t, methodActions, SOCKSActionWrite)
	authenticationActions, err := socks.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: method.Generation})
	if err != nil {
		t.Fatal(err)
	}
	authentication := requireSOCKSAction(t, authenticationActions, SOCKSActionReadAuthentication)
	authenticationReplyActions, err := socks.Handle(SOCKSEvent{
		Kind: SOCKSAuthenticationResult, Generation: authentication.Generation, Accepted: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticationReply := requireSOCKSAction(t, authenticationReplyActions, SOCKSActionWrite)
	requestActions, err := socks.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: authenticationReply.Generation})
	if err != nil {
		t.Fatal(err)
	}
	request := requireSOCKSAction(t, requestActions, SOCKSActionReadRequest)
	flowStartActions, err := socks.Handle(SOCKSEvent{
		Kind: SOCKSRequestResult, Generation: request.Generation, Target: target,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startFlow := requireSOCKSAction(t, flowStartActions, SOCKSActionStartFlow); startFlow.Target != target {
		t.Fatalf("SOCKS target = %#v", startFlow.Target)
	}
	if socks.Snapshot().ApplicationEnabled {
		t.Fatal("application enabled before OPEN/JOIN")
	}

	openJoin, err := NewOpenJoinCoordinator(OpenJoinSpec{
		Target: target, DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, generation := range readyGenerations {
		gate8HandleOpenJoin(t, openJoin, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: generation})
	}
	start := gate8HandleOpenJoin(t, openJoin, OpenJoinEvent{Kind: OpenJoinStart})
	identity := requireOpenJoinAction(t, start, OpenJoinActionGenerateIdentity)
	flowID := protocol.FlowID{0x81}
	openToken := protocol.OpenToken{0x82}
	opening := gate8HandleOpenJoin(t, openJoin, OpenJoinEvent{
		Kind: OpenJoinIdentityGenerated, Generation: identity.Generation, FlowID: flowID, OpenToken: openToken,
	})
	open := requireOpenJoinAction(t, opening, OpenJoinActionSendOpen)
	if open.Open.Target != target || open.Open.FlowID != flowID || open.Open.OpenToken != openToken {
		t.Fatalf("OPEN = %#v", open)
	}
	gate8SendMessage(t, sessions[open.SessionGeneration], open.Open, transport.FrameControl, open.Generation)

	machine := flow.NewFlow()
	relay, err := NewApplicationRelay(flowID, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, machine)
	if err != nil {
		t.Fatal(err)
	}
	relayStart, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayStart})
	if err != nil {
		t.Fatal(err)
	}
	if gate8CountRelayActions(relayStart, ApplicationRelayActionRead) != 0 ||
		!relay.Snapshot().ApplicationReadPaused || relay.Snapshot().Flow.Lifecycle.State != flow.AwaitingAttachment {
		t.Fatalf("relay before first JOIN = actions %#v, snapshot %#v", relayStart, relay.Snapshot())
	}

	capability := protocol.Capability{0x83}
	openResult := protocol.OpenResult{
		FlowID: flowID, Result: protocol.OpenSuccess, Capability: capability,
		DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest,
	}
	dispatchedOpen := gate8DispatchMessage(t, sessions[open.SessionGeneration], openResult).(protocol.OpenResult)
	joinActions := gate8HandleOpenJoin(t, openJoin, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: open.Generation,
		SessionGeneration: open.SessionGeneration, OpenResult: dispatchedOpen,
	})
	if socks.Snapshot().ApplicationEnabled || !relay.Snapshot().ApplicationReadPaused {
		t.Fatal("application became readable after OPEN but before JOIN publication")
	}

	first := gate8PublishClientJoin(t, openJoin, relay, machine, sessions, joinActions)
	if first.attachment.SessionGeneration != readyGenerations[0] {
		t.Fatalf("first attachment = %#v", first.attachment)
	}
	if gate8CountRelayActions(first.relayActions, ApplicationRelayActionRead) != 0 {
		t.Fatalf("first publication relay actions = %#v", first.relayActions)
	}
	if actionIndex(first.openJoinActions, OpenJoinActionReplyApplicationSuccess) < 0 {
		t.Fatalf("first publication OPEN/JOIN actions = %#v", first.openJoinActions)
	}
	if socks.Snapshot().ApplicationEnabled {
		t.Fatal("application enabled before SOCKS success reply completed")
	}
	socksReply, err := socks.Handle(SOCKSEvent{Kind: SOCKSFlowResult, OpenResult: protocol.OpenSuccess})
	if err != nil {
		t.Fatal(err)
	}
	replyWrite := requireSOCKSAction(t, socksReply, SOCKSActionWrite)
	socksEnabled, err := socks.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: replyWrite.Generation})
	if err != nil || countSOCKSActions(socksEnabled, SOCKSActionEnableApplication) != 1 || !socks.Snapshot().ApplicationEnabled {
		t.Fatalf("SOCKS success completion = %#v, %#v, %v", socksEnabled, socks.Snapshot(), err)
	}
	relayEnabled, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayEnableApplication})
	if err != nil || gate8CountRelayActions(relayEnabled, ApplicationRelayActionRead) != 1 {
		t.Fatalf("relay application enable = %#v, %v", relayEnabled, err)
	}
	read := gate8RequireRelayAction(t, relayEnabled, ApplicationRelayActionRead)

	second := gate8PublishClientJoin(t, openJoin, relay, machine, sessions, first.openJoinActions)
	if second.attachment.SessionGeneration != readyGenerations[1] ||
		len(openJoin.Snapshot().Attachments) != 2 || machine.Snapshot().Lifecycle.State != flow.Relaying {
		t.Fatalf("second attachment = %#v, OPEN/JOIN %#v, flow %#v", second.attachment, openJoin.Snapshot(), machine.Snapshot())
	}

	payload := []byte("gate-8-payload")
	dataActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: read.Generation, Data: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	dataSend := gate8RequireMessageAction[protocol.Data](t, dataActions)
	data := applicationRelayActionMessage(t, dataSend).(protocol.Data)
	if data.FlowID != flowID || data.Offset != 0 || !bytes.Equal(data.Bytes, payload) {
		t.Fatalf("application DATA = %#v", data)
	}
	gate8SendMessage(t, sessions[dataSend.Attachment.SessionGeneration], data, dataSend.Class, dataSend.Generation)
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: dataSend.Generation, AttemptOutcome: flow.AttemptSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if relay.Snapshot().Flow.TxReplayBytes != uint64(len(payload)) {
		t.Fatalf("unacknowledged replay bytes = %d", relay.Snapshot().Flow.TxReplayBytes)
	}

	var firstBackoff SessionManagerAction
	for index, generation := range readyGenerations {
		managerActions, managerErr := manager.Handle(SessionManagerEvent{Kind: SessionConnectionLost, Generation: generation})
		if managerErr != nil {
			t.Fatal(managerErr)
		}
		requireSessionManagerAction(t, managerActions, SessionActionLost)
		backoff := requireSessionManagerAction(t, managerActions, SessionActionArmBackoff)
		if index == 0 {
			firstBackoff = backoff
		}
		lossActions := gate8HandleOpenJoin(t, openJoin, OpenJoinEvent{
			Kind: OpenJoinSessionLost, SessionGeneration: generation,
		})
		release := requireOpenJoinAction(t, lossActions, OpenJoinActionReleaseAttachment)
		transportLoss, transportErr := sessions[generation].Handle(transport.SessionEvent{Kind: transport.SessionTransportClosed})
		if transportErr != nil {
			t.Fatal(transportErr)
		}
		lost := gate8RequireSessionAction(t, transportLoss, transport.SessionActionAttachmentLost)
		if lost.Attachment != release.Attachment {
			t.Fatalf("loss attachment mismatch: transport %#v, OPEN/JOIN %#v", lost.Attachment, release.Attachment)
		}
		relayLoss, relayErr := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySessionClosed, Generation: generation,
		})
		if relayErr != nil {
			t.Fatal(relayErr)
		}
		if gate8CountRelayActions(relayLoss, ApplicationRelayActionAttachmentWithdrawn) != 1 {
			t.Fatalf("relay loss actions = %#v", relayLoss)
		}
		if index == 0 && relay.Snapshot().ApplicationReadPaused {
			t.Fatal("first attachment loss paused while one attachment remained")
		}
	}
	if snapshot := relay.Snapshot(); !snapshot.ApplicationReadPaused ||
		snapshot.Flow.Lifecycle.State != flow.Recovering || snapshot.Flow.TxReplayBytes != uint64(len(payload)) {
		t.Fatalf("all attachments lost snapshot = %#v", snapshot)
	}
	if snapshot := openJoin.Snapshot(); len(snapshot.Attachments) != 0 || snapshot.State != OpenJoinWaitingJoinSession {
		t.Fatalf("OPEN/JOIN waiting recovery = %#v", snapshot)
	}

	replacementDials, err := manager.Handle(SessionManagerEvent{
		Kind: SessionBackoffExpired, Generation: firstBackoff.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	replacementDial := requireSessionManagerAction(t, replacementDials, SessionActionDial)
	if replacementDial.Generation == firstBackoff.Generation || replacementDial.Candidate != firstBackoff.Candidate {
		t.Fatalf("replacement dial = %#v, previous %#v", replacementDial, firstBackoff)
	}
	replacementConnection := &gate8Connection{
		localEndpoint: netip.AddrPortFrom(replacementDial.Candidate.LocalAddress, 41000).String(), remoteEndpoint: "127.0.0.1:9443",
	}
	dialCompleted, err := manager.Handle(SessionManagerEvent{
		Kind: SessionDialCompleted, Generation: replacementDial.Generation, Connection: replacementConnection,
	})
	if err != nil || len(sessionActions(dialCompleted, SessionActionStartAuthentication)) != 1 {
		t.Fatalf("replacement dial completion = %#v, %v", dialCompleted, err)
	}
	sessions[replacementDial.Generation] = gate8ReadyClientSession(t, replacementDial.Generation)
	replacementReady, err := manager.Handle(SessionManagerEvent{
		Kind: SessionAuthenticationCompleted, Generation: replacementDial.Generation, Succeeded: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireSessionManagerAction(t, replacementReady, SessionActionReady)
	recoveryJoin := gate8HandleOpenJoin(t, openJoin, OpenJoinEvent{
		Kind: OpenJoinSessionReady, SessionGeneration: replacementDial.Generation,
	})
	if hasOpenJoinAction(recoveryJoin, OpenJoinActionSendOpen) {
		t.Fatalf("recovery repeated OPEN = %#v", recoveryJoin)
	}
	join := requireOpenJoinAction(t, recoveryJoin, OpenJoinActionSendJoin)
	if join.Join.FlowID != flowID || join.Join.Capability != capability {
		t.Fatalf("recovery JOIN lost identity/capability = %#v", join)
	}
	recovered := gate8PublishClientJoin(t, openJoin, relay, machine, sessions, recoveryJoin)
	replayed := gate8RequireMessageAction[protocol.Data](t, recovered.relayActions)
	if replayed.Attachment.SessionGeneration != replacementDial.Generation ||
		!bytes.Equal(applicationRelayActionMessage(t, replayed).(protocol.Data).Bytes, payload) {
		t.Fatalf("recovery DATA = %#v", replayed)
	}
	if snapshot := relay.Snapshot(); snapshot.ApplicationReadPaused || snapshot.Flow.Lifecycle.State != flow.Relaying {
		t.Fatalf("recovered relay snapshot = %#v", snapshot)
	}
	if snapshot := openJoin.Snapshot(); !snapshot.ApplicationAccepted || len(snapshot.Attachments) != 1 || snapshot.OpenAttempts != 1 {
		t.Fatalf("recovered OPEN/JOIN snapshot = %#v", snapshot)
	}
	if hasOpenJoinAction(recovered.openJoinActions, OpenJoinActionReplyApplicationSuccess) {
		t.Fatalf("recovery application actions = %#v", recovered.openJoinActions)
	}
}

type gate8Publication struct {
	attachment      flow.AttachmentKey
	relayActions    []ApplicationRelayAction
	openJoinActions []OpenJoinAction
}

func gate8PublishClientJoin(
	t *testing.T,
	coordinator *OpenJoinCoordinator,
	relay *ApplicationRelay,
	machine *flow.Flow,
	sessions map[uint64]*transport.Session,
	joinActions []OpenJoinAction,
) gate8Publication {
	t.Helper()
	reserve := requireOpenJoinAction(t, joinActions, OpenJoinActionReserveAttachment)
	join := requireOpenJoinAction(t, joinActions, OpenJoinActionSendJoin)
	if reserve.Attachment != join.Attachment || reserve.SessionGeneration != join.SessionGeneration {
		t.Fatalf("JOIN reservation/send mismatch: %#v / %#v", reserve, join)
	}
	session := sessions[join.SessionGeneration]
	if session == nil {
		t.Fatalf("missing transport session generation %d", join.SessionGeneration)
	}
	reserved, err := relay.Handle(ApplicationRelayEvent{
		Kind:       ApplicationRelayReserveAttachment,
		Generation: reserve.Generation,
		Attachment: reserve.Attachment,
	})
	if err != nil || len(reserved) != 0 {
		t.Fatalf("flow attachment reservation = %#v, %v", reserved, err)
	}
	if _, err := session.Handle(transport.SessionEvent{
		Kind: transport.SessionReserveAttachment, FlowID: join.Join.FlowID,
		AttachmentGeneration: join.Attachment.AttachmentGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	gate8SendMessage(t, session, join.Join, transport.FrameControl, join.Generation)
	dispatched := gate8DispatchMessage(t, session, protocol.JoinResult{
		FlowID: join.Join.FlowID, Result: protocol.JoinSuccess,
	}).(protocol.JoinResult)
	publishActions := gate8HandleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinJoinResultReceived, Generation: join.Generation,
		SessionGeneration: join.SessionGeneration, JoinResult: dispatched,
	})
	publish := requireOpenJoinAction(t, publishActions, OpenJoinActionPublishAttachment)
	if publish.Attachment != join.Attachment {
		t.Fatalf("JOIN publication changed attachment: %#v / %#v", publish, join)
	}
	relayActions, err := relay.Handle(ApplicationRelayEvent{
		Kind:       ApplicationRelayPublishAttachment,
		Generation: publish.Generation,
		Attachment: publish.Attachment,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !gate8HasAttachment(machine.Snapshot().Lifecycle.Published, publish.Attachment) ||
		session.Snapshot().AttachmentCount != 0 || session.Snapshot().ReservedAttachments != 1 {
		t.Fatalf("authority order before session index: flow %#v, session %#v", machine.Snapshot(), session.Snapshot())
	}
	if _, err := session.Handle(transport.SessionEvent{
		Kind: transport.SessionPublishAttachment, FlowID: join.Join.FlowID,
		AttachmentGeneration: publish.Attachment.AttachmentGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	if session.Snapshot().AttachmentCount != 1 || session.Snapshot().ReservedAttachments != 0 {
		t.Fatalf("published session index = %#v", session.Snapshot())
	}
	openJoinActions := gate8HandleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: publish.Generation,
		SessionGeneration: publish.SessionGeneration,
	})
	return gate8Publication{
		attachment: publish.Attachment, relayActions: relayActions, openJoinActions: openJoinActions,
	}
}

func gate8ReadyClientSession(t *testing.T, generation uint64) *transport.Session {
	t.Helper()
	machine, err := auth.NewClientMachine("client-01", protocol.PathGroupID{1}, auth.Key{1}, bytes.NewReader(make([]byte, auth.NonceSize)))
	if err != nil {
		t.Fatal(err)
	}
	session, err := transport.NewClientSession(generation, machine)
	if err != nil {
		t.Fatal(err)
	}
	start, err := session.Handle(transport.SessionEvent{Kind: transport.SessionStarted})
	if err != nil || !gate8HasSessionAction(start, transport.SessionActionAllowRead) {
		t.Fatalf("client session start = %#v, %v", start, err)
	}
	challenge := [32]byte{0x91}
	proofActions := gate8ReceiveEncoded(t, session, protocol.AuthChallenge{Challenge: challenge})
	proof := gate8RequireSessionAction(t, proofActions, transport.SessionActionSendFrame)
	proofCompleted, err := session.Handle(transport.SessionEvent{
		Kind: transport.SessionSendCompleted, Generation: proof.Generation,
	})
	if err != nil || !gate8HasSessionAction(proofCompleted, transport.SessionActionAllowRead) {
		t.Fatalf("client proof completion = %#v, %v", proofCompleted, err)
	}
	ready := gate8ReceiveEncoded(t, session, protocol.AuthResult{Result: protocol.AuthSuccess})
	if !gate8HasSessionAction(ready, transport.SessionActionAuthenticated) || session.Snapshot().State != transport.SessionReady {
		t.Fatalf("client session ready = %#v, snapshot %#v", ready, session.Snapshot())
	}
	return session
}

func gate8SendMessage(t *testing.T, session *transport.Session, message protocol.Message, class transport.FrameClass, correlation uint64) {
	t.Helper()
	actions, err := session.Handle(transport.SessionEvent{
		Kind: transport.SessionSendMessage, Message: message, Class: class, Correlation: correlation,
	})
	if err != nil {
		t.Fatal(err)
	}
	send := gate8RequireSessionAction(t, actions, transport.SessionActionSendFrame)
	_, decoded, err := protocol.DecodeEncodedFrame(send.Encoded)
	if err != nil || !reflect.DeepEqual(decoded, message) {
		t.Fatalf("encoded message = %#v, want %#v, error %v", decoded, message, err)
	}
	completed, err := session.Handle(transport.SessionEvent{
		Kind: transport.SessionSendCompleted, Generation: send.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := gate8RequireSessionAction(t, completed, transport.SessionActionSendResult)
	if !result.SendSucceeded || result.Correlation != correlation {
		t.Fatalf("session send result = %#v", result)
	}
}

func gate8DispatchMessage(t *testing.T, session *transport.Session, message protocol.Message) protocol.Message {
	t.Helper()
	actions := gate8ReceiveEncoded(t, session, message)
	dispatch := gate8RequireSessionAction(t, actions, transport.SessionActionDispatch)
	completed, err := session.Handle(transport.SessionEvent{
		Kind: transport.SessionDispatchCompleted, Generation: dispatch.Generation,
	})
	if err != nil || !gate8HasSessionAction(completed, transport.SessionActionAllowRead) {
		t.Fatalf("dispatch completion = %#v, %v", completed, err)
	}
	return dispatch.Message
}

func gate8ReceiveEncoded(t *testing.T, session *transport.Session, message protocol.Message) []transport.SessionAction {
	t.Helper()
	encoded, err := protocol.EncodeMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := session.Handle(transport.SessionEvent{Kind: transport.SessionFrameReceived, Encoded: encoded})
	if err != nil {
		t.Fatal(err)
	}
	return actions
}

func gate8HandleOpenJoin(t *testing.T, coordinator *OpenJoinCoordinator, event OpenJoinEvent) []OpenJoinAction {
	t.Helper()
	actions, err := coordinator.Handle(event)
	if err != nil {
		t.Fatalf("OPEN/JOIN event %#v: %v", event, err)
	}
	if len(actions) > MaxOpenJoinActions {
		t.Fatalf("OPEN/JOIN action count = %d", len(actions))
	}
	return actions
}

func gate8RequireSessionAction(t *testing.T, actions []transport.SessionAction, kind transport.SessionActionKind) transport.SessionAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("missing transport session action %d in %#v", kind, actions)
	return transport.SessionAction{}
}

func gate8HasSessionAction(actions []transport.SessionAction, kind transport.SessionActionKind) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

func gate8RequireRelayAction(t *testing.T, actions []ApplicationRelayAction, kind ApplicationRelayActionKind) ApplicationRelayAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("missing application relay action %d in %#v", kind, actions)
	return ApplicationRelayAction{}
}

func gate8RequireMessageAction[T protocol.Message](t *testing.T, actions []ApplicationRelayAction) ApplicationRelayAction {
	t.Helper()
	for _, action := range actions {
		if _, ok := applicationRelayActionMessage(t, action).(T); ok {
			return action
		}
	}
	t.Fatalf("missing application relay message %T in %#v", *new(T), actions)
	return ApplicationRelayAction{}
}

func gate8CountRelayActions(actions []ApplicationRelayAction, kind ApplicationRelayActionKind) int {
	count := 0
	for _, action := range actions {
		if action.Kind == kind {
			count++
		}
	}
	return count
}

func gate8HasAttachment(attachments []flow.AttachmentKey, want flow.AttachmentKey) bool {
	for _, attachment := range attachments {
		if attachment == want {
			return true
		}
	}
	return false
}

type gate8Connection struct {
	localEndpoint  string
	remoteEndpoint string
}

func (*gate8Connection) Capabilities() transport.Capabilities {
	capabilities, _ := transport.NewCapabilities(transport.CapabilitySpec{
		MaxEncodedFrame: protocol.MaxPayloadSize, Reliable: true, Ordered: true, HalfClose: true,
	})
	return capabilities
}

func (*gate8Connection) QueueLimits() transport.QueueLimits { return transport.V1QueueLimits() }
func (connection *gate8Connection) LocalEndpoint() string   { return connection.localEndpoint }
func (connection *gate8Connection) RemoteEndpoint() string  { return connection.remoteEndpoint }
func (*gate8Connection) ReadFrame(context.Context) ([]byte, error) {
	return nil, transport.ErrClosed
}
func (*gate8Connection) WriteFrame(context.Context, transport.WriteRequest) error { return nil }
func (*gate8Connection) CloseWrite() error                                        { return nil }
func (*gate8Connection) Close() error                                             { return nil }
