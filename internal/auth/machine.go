package auth

import (
	"errors"
	"fmt"
	"io"

	"github.com/adrianceding/via/internal/protocol"
)

var ErrInvalidMachineConfiguration = errors.New("auth: invalid machine configuration")

// EventKind identifies an external event accepted by the authentication state owner.
type EventKind uint8

const (
	EventStart EventKind = iota + 1
	EventFrameReceived
	EventSendCompleted
	EventSendFailed
	EventDeadline
	EventClosed
)

// Event is one authentication state machine input. Message is used only for EventFrameReceived.
type Event struct {
	Kind    EventKind
	Message protocol.Message
}

// ActionKind identifies an action the coordinator must execute in return order.
type ActionKind uint8

const (
	ActionSend ActionKind = iota + 1
	ActionAllowRead
	ActionAuthenticationComplete
	ActionClose
)

// CloseReason is a fixed, low-cardinality authentication closure reason.
type CloseReason uint8

const (
	CloseProtocolViolation CloseReason = iota + 1
	CloseAuthenticationFailed
	CloseDeadlineExceeded
	CloseIOFailure
	CloseLocalFailure
)

// Action is an external effect emitted by the authentication state machine.
//
// ActionAllowRead authorizes reading exactly one complete protocol frame.
// ActionAuthenticationComplete must be processed before ActionAllowRead so the
// server principal is published before flow frames can be received.
type Action struct {
	Kind        ActionKind
	Message     protocol.Message
	PrincipalID string
	PathGroupID protocol.PathGroupID
	Reason      CloseReason
}

// ChallengeSource injects a unique challenge source into server authentication.
type ChallengeSource interface {
	Next() ([ChallengeSize]byte, error)
}

// ProofVerifier injects a proof verifier into server authentication.
type ProofVerifier interface {
	Verify(protocol.AuthProof, [ChallengeSize]byte) bool
}

// ServerState is the server authentication state.
type ServerState uint8

const (
	ServerAccepted ServerState = iota + 1
	ServerSendingChallenge
	ServerAwaitingProof
	ServerSendingResult
	ServerSendingFailure
	ServerAuthenticated
	ServerClosed
)

func (state ServerState) String() string {
	switch state {
	case ServerAccepted:
		return "accepted"
	case ServerSendingChallenge:
		return "sending_challenge"
	case ServerAwaitingProof:
		return "awaiting_proof"
	case ServerSendingResult:
		return "sending_result"
	case ServerSendingFailure:
		return "sending_failure"
	case ServerAuthenticated:
		return "authenticated"
	case ServerClosed:
		return "closed"
	default:
		return fmt.Sprintf("server_state_%d", uint8(state))
	}
}

// ServerMachine is the pure server authentication state machine for one
// transport session. A single state owner must call Handle serially.
type ServerMachine struct {
	state              ServerState
	challenges         ChallengeSource
	verifier           ProofVerifier
	challenge          [ChallengeSize]byte
	candidatePrincipal string
	candidatePathGroup protocol.PathGroupID
	principalID        string
	pathGroupID        protocol.PathGroupID
	pendingCloseReason CloseReason
}

func NewServerMachine(challenges ChallengeSource, verifier ProofVerifier) (*ServerMachine, error) {
	if challenges == nil {
		return nil, fmt.Errorf("%w: nil challenge source", ErrInvalidMachineConfiguration)
	}
	if verifier == nil {
		return nil, fmt.Errorf("%w: nil proof verifier", ErrInvalidMachineConfiguration)
	}
	return &ServerMachine{
		state:      ServerAccepted,
		challenges: challenges,
		verifier:   verifier,
	}, nil
}

func (machine *ServerMachine) State() ServerState {
	if machine == nil {
		return ServerClosed
	}
	return machine.state
}

// PrincipalID returns the published principal only after the successful authentication result has been sent.
func (machine *ServerMachine) PrincipalID() (string, bool) {
	if machine == nil || machine.state != ServerAuthenticated || machine.principalID == "" {
		return "", false
	}
	return machine.principalID, true
}

func (machine *ServerMachine) PathGroupID() (protocol.PathGroupID, bool) {
	if machine == nil || machine.state != ServerAuthenticated || !protocol.ValidPathGroupID(machine.pathGroupID) {
		return protocol.PathGroupID{}, false
	}
	return machine.pathGroupID, true
}

func (machine *ServerMachine) Handle(event Event) []Action {
	if machine == nil || machine.state == ServerClosed {
		return nil
	}
	if event.Kind == EventClosed {
		machine.close()
		return nil
	}

	switch machine.state {
	case ServerAccepted:
		return machine.handleAccepted(event)
	case ServerSendingChallenge:
		return machine.handleSendingChallenge(event)
	case ServerAwaitingProof:
		return machine.handleAwaitingProof(event)
	case ServerSendingResult:
		return machine.handleSendingResult(event)
	case ServerSendingFailure:
		return machine.handleSendingFailure(event)
	case ServerAuthenticated:
		if event.Kind == EventDeadline {
			return nil
		}
		return machine.fail(CloseProtocolViolation)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ServerMachine) handleAccepted(event Event) []Action {
	switch event.Kind {
	case EventStart:
		challenge, err := machine.challenges.Next()
		if err != nil {
			return machine.fail(CloseLocalFailure)
		}
		machine.challenge = challenge
		machine.state = ServerSendingChallenge
		return []Action{{
			Kind:    ActionSend,
			Message: protocol.AuthChallenge{Challenge: challenge},
		}}
	case EventDeadline:
		return machine.fail(CloseDeadlineExceeded)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ServerMachine) handleSendingChallenge(event Event) []Action {
	switch event.Kind {
	case EventSendCompleted:
		machine.state = ServerAwaitingProof
		return []Action{{Kind: ActionAllowRead}}
	case EventSendFailed:
		return machine.fail(CloseIOFailure)
	case EventDeadline:
		return machine.fail(CloseDeadlineExceeded)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ServerMachine) handleAwaitingProof(event Event) []Action {
	switch event.Kind {
	case EventFrameReceived:
		proof, ok := authProof(event.Message)
		if !ok {
			return machine.fail(CloseProtocolViolation)
		}
		if !protocol.ValidPrincipalID(proof.PrincipalID) || !protocol.ValidPathGroupID(proof.PathGroupID) {
			return machine.fail(CloseProtocolViolation)
		}
		valid := machine.verifier.Verify(proof, machine.challenge)
		machine.challenge = [ChallengeSize]byte{}
		if valid {
			machine.candidatePrincipal = proof.PrincipalID
			machine.candidatePathGroup = proof.PathGroupID
			machine.state = ServerSendingResult
			return []Action{{
				Kind:    ActionSend,
				Message: protocol.AuthResult{Result: protocol.AuthSuccess},
			}}
		}
		return machine.sendFailure(CloseAuthenticationFailed)
	case EventDeadline:
		machine.challenge = [ChallengeSize]byte{}
		return machine.sendFailure(CloseDeadlineExceeded)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ServerMachine) handleSendingResult(event Event) []Action {
	switch event.Kind {
	case EventSendCompleted:
		machine.principalID = machine.candidatePrincipal
		machine.pathGroupID = machine.candidatePathGroup
		machine.candidatePrincipal = ""
		machine.candidatePathGroup = protocol.PathGroupID{}
		machine.state = ServerAuthenticated
		return []Action{
			{Kind: ActionAuthenticationComplete, PrincipalID: machine.principalID, PathGroupID: machine.pathGroupID},
			{Kind: ActionAllowRead},
		}
	case EventSendFailed:
		return machine.fail(CloseIOFailure)
	case EventDeadline:
		return machine.fail(CloseDeadlineExceeded)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ServerMachine) handleSendingFailure(event Event) []Action {
	switch event.Kind {
	case EventSendCompleted:
		reason := machine.pendingCloseReason
		if reason == 0 {
			reason = CloseAuthenticationFailed
		}
		return machine.fail(reason)
	case EventSendFailed:
		return machine.fail(CloseIOFailure)
	case EventDeadline:
		return machine.fail(CloseDeadlineExceeded)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ServerMachine) sendFailure(reason CloseReason) []Action {
	machine.candidatePrincipal = ""
	machine.candidatePathGroup = protocol.PathGroupID{}
	machine.pendingCloseReason = reason
	machine.state = ServerSendingFailure
	return []Action{{
		Kind:    ActionSend,
		Message: protocol.AuthResult{Result: protocol.AuthFailure},
	}}
}

func (machine *ServerMachine) fail(reason CloseReason) []Action {
	machine.close()
	return []Action{{Kind: ActionClose, Reason: reason}}
}

func (machine *ServerMachine) close() {
	machine.state = ServerClosed
	machine.challenges = nil
	machine.verifier = nil
	machine.challenge = [ChallengeSize]byte{}
	machine.candidatePrincipal = ""
	machine.candidatePathGroup = protocol.PathGroupID{}
	machine.principalID = ""
	machine.pathGroupID = protocol.PathGroupID{}
	machine.pendingCloseReason = 0
}

// ClientState is the client authentication state. Created is the initial state before the connection reaches its owner.
type ClientState uint8

const (
	ClientCreated ClientState = iota + 1
	ClientConnected
	ClientSendingProof
	ClientAwaitingResult
	ClientAuthenticated
	ClientClosed
)

func (state ClientState) String() string {
	switch state {
	case ClientCreated:
		return "created"
	case ClientConnected:
		return "connected"
	case ClientSendingProof:
		return "sending_proof"
	case ClientAwaitingResult:
		return "awaiting_result"
	case ClientAuthenticated:
		return "authenticated"
	case ClientClosed:
		return "closed"
	default:
		return fmt.Sprintf("client_state_%d", uint8(state))
	}
}

// ClientMachine is the pure client authentication state machine for one
// transport session. A single state owner must call Handle serially.
type ClientMachine struct {
	state       ClientState
	principalID string
	pathGroupID protocol.PathGroupID
	key         Key
	random      io.Reader
}

func NewClientMachine(principalID string, pathGroupID protocol.PathGroupID, key Key, random io.Reader) (*ClientMachine, error) {
	if !protocol.ValidPrincipalID(principalID) || !protocol.ValidPathGroupID(pathGroupID) {
		return nil, ErrInvalidPrincipal
	}
	if random == nil {
		return nil, fmt.Errorf("%w: nil random reader", ErrInvalidMachineConfiguration)
	}
	return &ClientMachine{
		state:       ClientCreated,
		principalID: principalID,
		pathGroupID: pathGroupID,
		key:         key,
		random:      random,
	}, nil
}

func (machine *ClientMachine) State() ClientState {
	if machine == nil {
		return ClientClosed
	}
	return machine.state
}

func (machine *ClientMachine) PrincipalID() (string, bool) {
	if machine == nil || machine.state != ClientAuthenticated {
		return "", false
	}
	return machine.principalID, true
}

func (machine *ClientMachine) PathGroupID() (protocol.PathGroupID, bool) {
	if machine == nil || machine.state != ClientAuthenticated || !protocol.ValidPathGroupID(machine.pathGroupID) {
		return protocol.PathGroupID{}, false
	}
	return machine.pathGroupID, true
}

func (machine *ClientMachine) Handle(event Event) []Action {
	if machine == nil || machine.state == ClientClosed {
		return nil
	}
	if event.Kind == EventClosed {
		machine.close()
		return nil
	}

	switch machine.state {
	case ClientCreated:
		switch event.Kind {
		case EventStart:
			machine.state = ClientConnected
			return []Action{{Kind: ActionAllowRead}}
		case EventDeadline:
			return machine.fail(CloseDeadlineExceeded)
		default:
			return machine.fail(CloseProtocolViolation)
		}
	case ClientConnected:
		return machine.handleConnected(event)
	case ClientSendingProof:
		return machine.handleSendingProof(event)
	case ClientAwaitingResult:
		return machine.handleAwaitingResult(event)
	case ClientAuthenticated:
		if event.Kind == EventDeadline {
			return nil
		}
		return machine.fail(CloseProtocolViolation)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ClientMachine) handleConnected(event Event) []Action {
	switch event.Kind {
	case EventFrameReceived:
		challenge, ok := authChallenge(event.Message)
		if !ok {
			return machine.fail(CloseProtocolViolation)
		}
		var nonce [NonceSize]byte
		if _, err := io.ReadFull(machine.random, nonce[:]); err != nil {
			return machine.fail(CloseLocalFailure)
		}
		proof, err := ComputeProof(machine.key, machine.principalID, machine.pathGroupID, challenge.Challenge, nonce)
		if err != nil {
			return machine.fail(CloseLocalFailure)
		}
		machine.key = Key{}
		machine.random = nil
		machine.state = ClientSendingProof
		return []Action{{
			Kind: ActionSend,
			Message: protocol.AuthProof{
				PrincipalID: machine.principalID,
				PathGroupID: machine.pathGroupID,
				ClientNonce: nonce,
				Proof:       proof,
			},
		}}
	case EventDeadline:
		return machine.fail(CloseDeadlineExceeded)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ClientMachine) handleSendingProof(event Event) []Action {
	switch event.Kind {
	case EventSendCompleted:
		machine.state = ClientAwaitingResult
		return []Action{{Kind: ActionAllowRead}}
	case EventSendFailed:
		return machine.fail(CloseIOFailure)
	case EventDeadline:
		return machine.fail(CloseDeadlineExceeded)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ClientMachine) handleAwaitingResult(event Event) []Action {
	switch event.Kind {
	case EventFrameReceived:
		result, ok := authResult(event.Message)
		if !ok {
			return machine.fail(CloseProtocolViolation)
		}
		switch result.Result {
		case protocol.AuthSuccess:
			machine.state = ClientAuthenticated
			return []Action{
				{Kind: ActionAuthenticationComplete, PrincipalID: machine.principalID, PathGroupID: machine.pathGroupID},
				{Kind: ActionAllowRead},
			}
		case protocol.AuthFailure:
			return machine.fail(CloseAuthenticationFailed)
		default:
			return machine.fail(CloseProtocolViolation)
		}
	case EventDeadline:
		return machine.fail(CloseDeadlineExceeded)
	default:
		return machine.fail(CloseProtocolViolation)
	}
}

func (machine *ClientMachine) fail(reason CloseReason) []Action {
	machine.close()
	return []Action{{Kind: ActionClose, Reason: reason}}
}

func (machine *ClientMachine) close() {
	machine.state = ClientClosed
	machine.principalID = ""
	machine.pathGroupID = protocol.PathGroupID{}
	machine.key = Key{}
	machine.random = nil
}

func authChallenge(message protocol.Message) (protocol.AuthChallenge, bool) {
	switch typed := message.(type) {
	case protocol.AuthChallenge:
		return typed, true
	case *protocol.AuthChallenge:
		if typed != nil {
			return *typed, true
		}
	}
	return protocol.AuthChallenge{}, false
}

func authProof(message protocol.Message) (protocol.AuthProof, bool) {
	switch typed := message.(type) {
	case protocol.AuthProof:
		return typed, true
	case *protocol.AuthProof:
		if typed != nil {
			return *typed, true
		}
	}
	return protocol.AuthProof{}, false
}

func authResult(message protocol.Message) (protocol.AuthResult, bool) {
	switch typed := message.(type) {
	case protocol.AuthResult:
		return typed, true
	case *protocol.AuthResult:
		if typed != nil {
			return *typed, true
		}
	}
	return protocol.AuthResult{}, false
}
