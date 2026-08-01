package auth

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

type challengeSourceFunc func() ([ChallengeSize]byte, error)

func (function challengeSourceFunc) Next() ([ChallengeSize]byte, error) {
	return function()
}

type proofVerifierFunc func(protocol.AuthProof, [ChallengeSize]byte) bool

func (function proofVerifierFunc) Verify(proof protocol.AuthProof, challenge [ChallengeSize]byte) bool {
	return function(proof, challenge)
}

func TestMachinesCompleteHandshakeWithStrictReadBarriers(t *testing.T) {
	principalID := "client.example"
	key := testKey(0x31)
	challenge := testChallenge(0x51)
	nonce := testNonce(0x71)

	server, err := NewServerMachine(
		challengeSourceFunc(func() ([ChallengeSize]byte, error) { return challenge, nil }),
		NewVerifier(map[string]Key{principalID: key}, testKey(0x91)),
	)
	if err != nil {
		t.Fatalf("NewServerMachine() error = %v", err)
	}
	client, err := NewClientMachine(principalID, key, bytes.NewReader(nonce[:]))
	if err != nil {
		t.Fatalf("NewClientMachine() error = %v", err)
	}

	assertClientStep(t, client, Event{Kind: EventStart}, ClientConnected,
		Action{Kind: ActionAllowRead})
	assertServerStep(t, server, Event{Kind: EventStart}, ServerSendingChallenge,
		Action{Kind: ActionSend, Message: protocol.AuthChallenge{Challenge: challenge}})
	assertServerStep(t, server, Event{Kind: EventSendCompleted}, ServerAwaitingProof,
		Action{Kind: ActionAllowRead})

	clientActions := client.Handle(Event{
		Kind:    EventFrameReceived,
		Message: protocol.AuthChallenge{Challenge: challenge},
	})
	if client.State() != ClientSendingProof {
		t.Fatalf("client state = %v, want %v", client.State(), ClientSendingProof)
	}
	if len(clientActions) != 1 || clientActions[0].Kind != ActionSend {
		t.Fatalf("client actions = %#v, want one send", clientActions)
	}
	proof, ok := clientActions[0].Message.(protocol.AuthProof)
	if !ok {
		t.Fatalf("client send message = %T, want protocol.AuthProof", clientActions[0].Message)
	}
	wantProof, err := ComputeProof(key, principalID, challenge, nonce)
	if err != nil {
		t.Fatalf("ComputeProof() error = %v", err)
	}
	if proof.PrincipalID != principalID || proof.ClientNonce != nonce || proof.Proof != wantProof {
		t.Fatalf("proof = %#v, want injected nonce and computed proof", proof)
	}
	assertNoAllowRead(t, clientActions)

	assertClientStep(t, client, Event{Kind: EventSendCompleted}, ClientAwaitingResult,
		Action{Kind: ActionAllowRead})
	serverActions := server.Handle(Event{Kind: EventFrameReceived, Message: proof})
	assertStateAndActions(t, server.State(), ServerSendingResult, serverActions, []Action{
		{Kind: ActionSend, Message: protocol.AuthResult{Result: protocol.AuthSuccess}},
	})
	if principal, published := server.PrincipalID(); published || principal != "" {
		t.Fatalf("principal before result send completion = %q, %v", principal, published)
	}
	assertNoAllowRead(t, serverActions)

	assertServerStep(t, server, Event{Kind: EventSendCompleted}, ServerAuthenticated,
		Action{Kind: ActionAuthenticationComplete, PrincipalID: principalID},
		Action{Kind: ActionAllowRead})
	if principal, published := server.PrincipalID(); !published || principal != principalID {
		t.Fatalf("published principal = %q, %v, want %q, true", principal, published, principalID)
	}

	assertClientStep(t, client, Event{
		Kind:    EventFrameReceived,
		Message: protocol.AuthResult{Result: protocol.AuthSuccess},
	}, ClientAuthenticated,
		Action{Kind: ActionAuthenticationComplete, PrincipalID: principalID},
		Action{Kind: ActionAllowRead})
	if principal, authenticated := client.PrincipalID(); !authenticated || principal != principalID {
		t.Fatalf("client principal = %q, %v, want %q, true", principal, authenticated, principalID)
	}
}

func TestServerFailureResponsesAreUniform(t *testing.T) {
	tests := []struct {
		name        string
		failure     Event
		closeReason CloseReason
	}{
		{
			name: "invalid proof",
			failure: Event{
				Kind: EventFrameReceived,
				Message: protocol.AuthProof{
					PrincipalID: "unknown",
				},
			},
			closeReason: CloseAuthenticationFailed,
		},
		{
			name:        "deadline",
			failure:     Event{Kind: EventDeadline},
			closeReason: CloseDeadlineExceeded,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServerAwaitingProof(t, test.name != "invalid proof")
			actions := server.Handle(test.failure)
			assertStateAndActions(t, server.State(), ServerSendingFailure, actions, []Action{
				{Kind: ActionSend, Message: protocol.AuthResult{Result: protocol.AuthFailure}},
			})
			if principal, published := server.PrincipalID(); published || principal != "" {
				t.Fatalf("principal after authentication failure = %q, %v", principal, published)
			}
			assertNoAllowRead(t, actions)

			assertServerStep(t, server, Event{Kind: EventSendCompleted}, ServerClosed,
				Action{Kind: ActionClose, Reason: test.closeReason})
		})
	}
}

func TestServerDoesNotPublishPrincipalWhenSuccessResultSendFails(t *testing.T) {
	server := newServerAt(t, ServerSendingResult)
	if principal, published := server.PrincipalID(); published || principal != "" {
		t.Fatalf("principal before send result = %q, %v", principal, published)
	}

	assertServerStep(t, server, Event{Kind: EventSendFailed}, ServerClosed,
		Action{Kind: ActionClose, Reason: CloseIOFailure})
	if principal, published := server.PrincipalID(); published || principal != "" {
		t.Fatalf("principal after send failure = %q, %v", principal, published)
	}
}

func TestEntropyFailuresCloseWithoutSending(t *testing.T) {
	t.Run("server challenge", func(t *testing.T) {
		server, err := NewServerMachine(
			challengeSourceFunc(func() ([ChallengeSize]byte, error) {
				return [ChallengeSize]byte{}, io.ErrUnexpectedEOF
			}),
			proofVerifierFunc(func(protocol.AuthProof, [ChallengeSize]byte) bool { return true }),
		)
		if err != nil {
			t.Fatalf("NewServerMachine() error = %v", err)
		}
		assertServerStep(t, server, Event{Kind: EventStart}, ServerClosed,
			Action{Kind: ActionClose, Reason: CloseLocalFailure})
	})

	t.Run("client nonce", func(t *testing.T) {
		client, err := NewClientMachine("client", testKey(0x22), errorReader{})
		if err != nil {
			t.Fatalf("NewClientMachine() error = %v", err)
		}
		assertClientStep(t, client, Event{Kind: EventStart}, ClientConnected,
			Action{Kind: ActionAllowRead})
		assertClientStep(t, client, Event{
			Kind:    EventFrameReceived,
			Message: protocol.AuthChallenge{Challenge: testChallenge(0x33)},
		}, ClientClosed,
			Action{Kind: ActionClose, Reason: CloseLocalFailure})
	})
}

func TestFramesDuringSendBarrierCloseMachines(t *testing.T) {
	serverStates := []ServerState{
		ServerSendingChallenge,
		ServerSendingResult,
		ServerSendingFailure,
	}
	for _, state := range serverStates {
		t.Run("server_"+state.String(), func(t *testing.T) {
			server := newServerAt(t, state)
			assertServerStep(t, server, Event{
				Kind:    EventFrameReceived,
				Message: protocol.AuthProof{PrincipalID: "client"},
			}, ServerClosed,
				Action{Kind: ActionClose, Reason: CloseProtocolViolation})
		})
	}

	t.Run("client_sending_proof", func(t *testing.T) {
		client := newClientAt(t, ClientSendingProof)
		assertClientStep(t, client, Event{
			Kind:    EventFrameReceived,
			Message: protocol.AuthResult{Result: protocol.AuthSuccess},
		}, ClientClosed,
			Action{Kind: ActionClose, Reason: CloseProtocolViolation})
	})
}

func TestWrongAndRepeatedAuthenticationFramesCloseMachines(t *testing.T) {
	serverTests := []struct {
		name    string
		state   ServerState
		message protocol.Message
	}{
		{"before start", ServerAccepted, protocol.AuthProof{PrincipalID: "client"}},
		{"invalid principal", ServerAwaitingProof, protocol.AuthProof{PrincipalID: "bad principal!"}},
		{"wrong proof frame", ServerAwaitingProof, protocol.AuthChallenge{}},
		{"repeated frame", ServerAuthenticated, protocol.AuthProof{PrincipalID: "client"}},
	}
	for _, test := range serverTests {
		t.Run("server_"+test.name, func(t *testing.T) {
			server := newServerAt(t, test.state)
			assertServerStep(t, server, Event{Kind: EventFrameReceived, Message: test.message}, ServerClosed,
				Action{Kind: ActionClose, Reason: CloseProtocolViolation})
		})
	}

	clientTests := []struct {
		name    string
		state   ClientState
		message protocol.Message
	}{
		{"before start", ClientCreated, protocol.AuthChallenge{}},
		{"wrong challenge frame", ClientConnected, protocol.AuthResult{Result: protocol.AuthSuccess}},
		{"wrong result frame", ClientAwaitingResult, protocol.AuthChallenge{}},
		{"repeated frame", ClientAuthenticated, protocol.AuthChallenge{}},
	}
	for _, test := range clientTests {
		t.Run("client_"+test.name, func(t *testing.T) {
			client := newClientAt(t, test.state)
			assertClientStep(t, client, Event{Kind: EventFrameReceived, Message: test.message}, ClientClosed,
				Action{Kind: ActionClose, Reason: CloseProtocolViolation})
		})
	}
}

func TestDeadlineMatrix(t *testing.T) {
	serverTests := []struct {
		state       ServerState
		wantState   ServerState
		wantActions []Action
	}{
		{ServerAccepted, ServerClosed, []Action{{Kind: ActionClose, Reason: CloseDeadlineExceeded}}},
		{ServerSendingChallenge, ServerClosed, []Action{{Kind: ActionClose, Reason: CloseDeadlineExceeded}}},
		{ServerAwaitingProof, ServerSendingFailure, []Action{{Kind: ActionSend, Message: protocol.AuthResult{Result: protocol.AuthFailure}}}},
		{ServerSendingResult, ServerClosed, []Action{{Kind: ActionClose, Reason: CloseDeadlineExceeded}}},
		{ServerSendingFailure, ServerClosed, []Action{{Kind: ActionClose, Reason: CloseDeadlineExceeded}}},
		{ServerAuthenticated, ServerAuthenticated, nil},
		{ServerClosed, ServerClosed, nil},
	}
	for _, test := range serverTests {
		t.Run("server_"+test.state.String(), func(t *testing.T) {
			server := newServerAt(t, test.state)
			actions := server.Handle(Event{Kind: EventDeadline})
			assertStateAndActions(t, server.State(), test.wantState, actions, test.wantActions)
		})
	}

	clientTests := []struct {
		state       ClientState
		wantState   ClientState
		wantActions []Action
	}{
		{ClientCreated, ClientClosed, []Action{{Kind: ActionClose, Reason: CloseDeadlineExceeded}}},
		{ClientConnected, ClientClosed, []Action{{Kind: ActionClose, Reason: CloseDeadlineExceeded}}},
		{ClientSendingProof, ClientClosed, []Action{{Kind: ActionClose, Reason: CloseDeadlineExceeded}}},
		{ClientAwaitingResult, ClientClosed, []Action{{Kind: ActionClose, Reason: CloseDeadlineExceeded}}},
		{ClientAuthenticated, ClientAuthenticated, nil},
		{ClientClosed, ClientClosed, nil},
	}
	for _, test := range clientTests {
		t.Run("client_"+test.state.String(), func(t *testing.T) {
			client := newClientAt(t, test.state)
			actions := client.Handle(Event{Kind: EventDeadline})
			assertStateAndActions(t, client.State(), test.wantState, actions, test.wantActions)
		})
	}
}

func TestClosedEventMatrix(t *testing.T) {
	for _, state := range []ServerState{
		ServerAccepted,
		ServerSendingChallenge,
		ServerAwaitingProof,
		ServerSendingResult,
		ServerSendingFailure,
		ServerAuthenticated,
		ServerClosed,
	} {
		t.Run("server_"+state.String(), func(t *testing.T) {
			server := newServerAt(t, state)
			actions := server.Handle(Event{Kind: EventClosed})
			assertStateAndActions(t, server.State(), ServerClosed, actions, nil)
			if principal, published := server.PrincipalID(); published || principal != "" {
				t.Fatalf("principal after closed = %q, %v", principal, published)
			}
			if server.challenges != nil || server.verifier != nil || server.challenge != [ChallengeSize]byte{} || server.candidatePrincipal != "" {
				t.Fatalf("server retained temporary authentication state after close: %#v", server)
			}
		})
	}

	for _, state := range []ClientState{
		ClientCreated,
		ClientConnected,
		ClientSendingProof,
		ClientAwaitingResult,
		ClientAuthenticated,
		ClientClosed,
	} {
		t.Run("client_"+state.String(), func(t *testing.T) {
			client := newClientAt(t, state)
			actions := client.Handle(Event{Kind: EventClosed})
			assertStateAndActions(t, client.State(), ClientClosed, actions, nil)
			if principal, authenticated := client.PrincipalID(); authenticated || principal != "" {
				t.Fatalf("principal after closed = %q, %v", principal, authenticated)
			}
			if client.key != (Key{}) || client.random != nil {
				t.Fatalf("client retained temporary authentication state after close: %#v", client)
			}
		})
	}
}

func TestSendFailureAndUnexpectedEventMatrix(t *testing.T) {
	for _, state := range []ServerState{ServerSendingChallenge, ServerSendingResult, ServerSendingFailure} {
		t.Run("server_send_failure_"+state.String(), func(t *testing.T) {
			server := newServerAt(t, state)
			assertServerStep(t, server, Event{Kind: EventSendFailed}, ServerClosed,
				Action{Kind: ActionClose, Reason: CloseIOFailure})
		})
	}
	for _, state := range []ClientState{ClientSendingProof} {
		t.Run("client_send_failure_"+state.String(), func(t *testing.T) {
			client := newClientAt(t, state)
			assertClientStep(t, client, Event{Kind: EventSendFailed}, ClientClosed,
				Action{Kind: ActionClose, Reason: CloseIOFailure})
		})
	}

	t.Run("unexpected server send completion", func(t *testing.T) {
		server := newServerAt(t, ServerAwaitingProof)
		assertServerStep(t, server, Event{Kind: EventSendCompleted}, ServerClosed,
			Action{Kind: ActionClose, Reason: CloseProtocolViolation})
	})
	t.Run("unexpected client send completion", func(t *testing.T) {
		client := newClientAt(t, ClientConnected)
		assertClientStep(t, client, Event{Kind: EventSendCompleted}, ClientClosed,
			Action{Kind: ActionClose, Reason: CloseProtocolViolation})
	})
	t.Run("unknown event", func(t *testing.T) {
		server := newServerAt(t, ServerAccepted)
		assertServerStep(t, server, Event{Kind: EventKind(255)}, ServerClosed,
			Action{Kind: ActionClose, Reason: CloseProtocolViolation})
	})
}

func TestClientFailureResultCloses(t *testing.T) {
	client := newClientAt(t, ClientAwaitingResult)
	assertClientStep(t, client, Event{
		Kind:    EventFrameReceived,
		Message: protocol.AuthResult{Result: protocol.AuthFailure},
	}, ClientClosed,
		Action{Kind: ActionClose, Reason: CloseAuthenticationFailed})
}

func TestMachineConstructorsRejectInvalidDependencies(t *testing.T) {
	validChallengeSource := challengeSourceFunc(func() ([ChallengeSize]byte, error) {
		return testChallenge(0x11), nil
	})
	validVerifier := proofVerifierFunc(func(protocol.AuthProof, [ChallengeSize]byte) bool { return true })

	if _, err := NewServerMachine(nil, validVerifier); !errors.Is(err, ErrInvalidMachineConfiguration) {
		t.Fatalf("NewServerMachine(nil source) error = %v", err)
	}
	if _, err := NewServerMachine(validChallengeSource, nil); !errors.Is(err, ErrInvalidMachineConfiguration) {
		t.Fatalf("NewServerMachine(nil verifier) error = %v", err)
	}
	if _, err := NewClientMachine("bad principal!", testKey(0x12), bytes.NewReader(make([]byte, NonceSize))); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("NewClientMachine(invalid principal) error = %v", err)
	}
	if _, err := NewClientMachine("client", testKey(0x12), nil); !errors.Is(err, ErrInvalidMachineConfiguration) {
		t.Fatalf("NewClientMachine(nil random) error = %v", err)
	}
}

func newServerAt(t *testing.T, want ServerState) *ServerMachine {
	t.Helper()
	challenge := testChallenge(0x41)
	server, err := NewServerMachine(
		challengeSourceFunc(func() ([ChallengeSize]byte, error) { return challenge, nil }),
		proofVerifierFunc(func(protocol.AuthProof, [ChallengeSize]byte) bool { return true }),
	)
	if err != nil {
		t.Fatalf("NewServerMachine() error = %v", err)
	}
	if want == ServerAccepted {
		return server
	}
	server.Handle(Event{Kind: EventStart})
	if want == ServerSendingChallenge {
		return server
	}
	server.Handle(Event{Kind: EventSendCompleted})
	if want == ServerAwaitingProof {
		return server
	}
	server.Handle(Event{
		Kind: EventFrameReceived,
		Message: protocol.AuthProof{
			PrincipalID: "client",
		},
	})
	if want == ServerSendingResult {
		return server
	}
	if want == ServerSendingFailure {
		server = newServerWithInvalidProof(t)
		return server
	}
	server.Handle(Event{Kind: EventSendCompleted})
	if want == ServerAuthenticated {
		return server
	}
	server.Handle(Event{Kind: EventClosed})
	if want == ServerClosed {
		return server
	}
	t.Fatalf("unsupported server state %v", want)
	return nil
}

func newServerAwaitingProof(t *testing.T, accept bool) *ServerMachine {
	t.Helper()
	server, err := NewServerMachine(
		challengeSourceFunc(func() ([ChallengeSize]byte, error) { return testChallenge(0x41), nil }),
		proofVerifierFunc(func(protocol.AuthProof, [ChallengeSize]byte) bool { return accept }),
	)
	if err != nil {
		t.Fatalf("NewServerMachine() error = %v", err)
	}
	server.Handle(Event{Kind: EventStart})
	server.Handle(Event{Kind: EventSendCompleted})
	return server
}

func newServerWithInvalidProof(t *testing.T) *ServerMachine {
	t.Helper()
	server := newServerAwaitingProof(t, false)
	server.Handle(Event{Kind: EventFrameReceived, Message: protocol.AuthProof{PrincipalID: "client"}})
	return server
}

func newClientAt(t *testing.T, want ClientState) *ClientMachine {
	t.Helper()
	client, err := NewClientMachine("client", testKey(0x21), bytes.NewReader(make([]byte, NonceSize)))
	if err != nil {
		t.Fatalf("NewClientMachine() error = %v", err)
	}
	if want == ClientCreated {
		return client
	}
	client.Handle(Event{Kind: EventStart})
	if want == ClientConnected {
		return client
	}
	client.Handle(Event{Kind: EventFrameReceived, Message: protocol.AuthChallenge{Challenge: testChallenge(0x43)}})
	if want == ClientSendingProof {
		return client
	}
	client.Handle(Event{Kind: EventSendCompleted})
	if want == ClientAwaitingResult {
		return client
	}
	client.Handle(Event{Kind: EventFrameReceived, Message: protocol.AuthResult{Result: protocol.AuthSuccess}})
	if want == ClientAuthenticated {
		return client
	}
	client.Handle(Event{Kind: EventClosed})
	if want == ClientClosed {
		return client
	}
	t.Fatalf("unsupported client state %v", want)
	return nil
}

func assertServerStep(t *testing.T, machine *ServerMachine, event Event, wantState ServerState, wantActions ...Action) {
	t.Helper()
	actions := machine.Handle(event)
	assertStateAndActions(t, machine.State(), wantState, actions, wantActions)
}

func assertClientStep(t *testing.T, machine *ClientMachine, event Event, wantState ClientState, wantActions ...Action) {
	t.Helper()
	actions := machine.Handle(event)
	assertStateAndActions(t, machine.State(), wantState, actions, wantActions)
}

func assertStateAndActions[S comparable](t *testing.T, gotState, wantState S, gotActions, wantActions []Action) {
	t.Helper()
	if gotState != wantState {
		t.Fatalf("state = %v, want %v", gotState, wantState)
	}
	if !reflect.DeepEqual(gotActions, wantActions) {
		t.Fatalf("actions = %#v, want %#v", gotActions, wantActions)
	}
}

func assertNoAllowRead(t *testing.T, actions []Action) {
	t.Helper()
	for _, action := range actions {
		if action.Kind == ActionAllowRead {
			t.Fatalf("actions unexpectedly allow a read: %#v", actions)
		}
	}
}

func testKey(seed byte) Key {
	var key Key
	for index := range key {
		key[index] = seed + byte(index)
	}
	return key
}

func testChallenge(seed byte) [ChallengeSize]byte {
	var challenge [ChallengeSize]byte
	for index := range challenge {
		challenge[index] = seed + byte(index)
	}
	return challenge
}

func testNonce(seed byte) [NonceSize]byte {
	var nonce [NonceSize]byte
	for index := range nonce {
		nonce[index] = seed + byte(index)
	}
	return nonce
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}
