package transport

import (
	"bytes"
	"errors"
	"sort"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
)

const MaxSessionAttachments = 256

var (
	ErrInvalidSession     = errors.New("transport: invalid session state")
	ErrSessionBusy        = errors.New("transport: session read barrier is busy")
	ErrSessionCapacity    = errors.New("transport: session capacity exceeded")
	ErrAttachmentConflict = errors.New("transport: attachment generation conflict")
)

type SessionRole uint8

const (
	SessionClient SessionRole = iota + 1
	SessionServer
)

type SessionState uint8

const (
	SessionAuthenticating SessionState = iota + 1
	SessionReady
	SessionClosed
)

type SessionEventKind uint8

const (
	SessionStarted SessionEventKind = iota + 1
	SessionFrameReceived
	SessionSendMessage
	SessionSendCompleted
	SessionSendFailed
	SessionAuthDeadline
	SessionDispatchCompleted
	SessionReserveAttachment
	SessionCancelAttachment
	SessionPublishAttachment
	SessionWithdrawAttachment
	SessionTransportClosed
)

type SessionEvent struct {
	Kind                 SessionEventKind
	Encoded              []byte
	Message              protocol.Message
	Class                FrameClass
	Generation           uint64
	Correlation          uint64
	FlowID               protocol.FlowID
	AttachmentGeneration uint64
}

type SessionActionKind uint8

const (
	SessionActionSendFrame SessionActionKind = iota + 1
	SessionActionAllowRead
	SessionActionAuthenticated
	SessionActionDispatch
	SessionActionSendResult
	SessionActionProbeResult
	SessionActionCloseTransport
	SessionActionAttachmentLost
	SessionActionDroppedUnknownAttachment
)

type SessionCloseReason uint8

const (
	SessionCloseProtocol SessionCloseReason = iota + 1
	SessionCloseAuthentication
	SessionCloseDeadline
	SessionCloseIO
	SessionCloseLocal
)

type SessionAction struct {
	Kind                 SessionActionKind
	Generation           uint64
	Correlation          uint64
	Encoded              []byte
	Class                FrameClass
	PrincipalID          string
	Message              protocol.Message
	FlowID               protocol.FlowID
	AttachmentGeneration uint64
	Attachment           flow.AttachmentKey
	ProbeToken           uint64
	SendSucceeded        bool
	CloseReason          SessionCloseReason
}

type SessionSnapshot struct {
	Generation          uint64
	Role                SessionRole
	State               SessionState
	PrincipalID         string
	ReadAllowed         bool
	PendingDispatch     uint64
	PendingSends        int
	ReservedAttachments int
	AttachmentCount     int
}

type authenticationMachine interface {
	Handle(auth.Event) []auth.Action
}

type pendingSendKind uint8

const (
	pendingAuth pendingSendKind = iota + 1
	pendingGeneric
	pendingProbe
)

type pendingSend struct {
	kind        pendingSendKind
	correlation uint64
}

// Session is the single owner of authentication barriers and the derived
// attachment index for one transport connection. It performs no network I/O.
type Session struct {
	generation      uint64
	role            SessionRole
	state           SessionState
	auth            authenticationMachine
	principalID     string
	readAllowed     bool
	nextGeneration  uint64
	pendingSends    map[uint64]pendingSend
	pendingDispatch uint64
	dispatchEncoded []byte
	attachments     map[protocol.FlowID]uint64
	reservations    map[protocol.FlowID]uint64
}

func NewClientSession(generation uint64, machine *auth.ClientMachine) (*Session, error) {
	return newSession(generation, SessionClient, machine)
}

func NewServerSession(generation uint64, machine *auth.ServerMachine) (*Session, error) {
	return newSession(generation, SessionServer, machine)
}

func newSession(generation uint64, role SessionRole, machine authenticationMachine) (*Session, error) {
	if generation == 0 || machine == nil || (role != SessionClient && role != SessionServer) {
		return nil, ErrInvalidSession
	}
	return &Session{
		generation:   generation,
		role:         role,
		state:        SessionAuthenticating,
		auth:         machine,
		pendingSends: make(map[uint64]pendingSend, MaxSessionAttachments),
		attachments:  make(map[protocol.FlowID]uint64, MaxSessionAttachments),
		reservations: make(map[protocol.FlowID]uint64, MaxSessionAttachments),
	}, nil
}

func (session *Session) Snapshot() SessionSnapshot {
	if session == nil {
		return SessionSnapshot{State: SessionClosed}
	}
	return SessionSnapshot{
		Generation:          session.generation,
		Role:                session.role,
		State:               session.state,
		PrincipalID:         session.principalID,
		ReadAllowed:         session.readAllowed,
		PendingDispatch:     session.pendingDispatch,
		PendingSends:        len(session.pendingSends),
		ReservedAttachments: len(session.reservations),
		AttachmentCount:     len(session.attachments),
	}
}

func (session *Session) Handle(event SessionEvent) ([]SessionAction, error) {
	if session == nil || session.state == SessionClosed {
		return nil, nil
	}
	switch event.Kind {
	case SessionStarted:
		if session.state != SessionAuthenticating || session.nextGeneration != 0 || session.readAllowed {
			return session.close(SessionCloseProtocol, true), ErrInvalidSession
		}
		return session.applyAuth(session.auth.Handle(auth.Event{Kind: auth.EventStart})), nil
	case SessionFrameReceived:
		return session.receiveFrame(event.Encoded)
	case SessionSendMessage:
		if session.state != SessionReady {
			return nil, ErrInvalidSession
		}
		return session.sendMessage(event.Message, event.Class, pendingGeneric, event.Correlation)
	case SessionSendCompleted:
		return session.completeSend(event.Generation, true)
	case SessionSendFailed:
		return session.completeSend(event.Generation, false)
	case SessionAuthDeadline:
		if session.state != SessionAuthenticating {
			return nil, nil
		}
		return session.applyAuth(session.auth.Handle(auth.Event{Kind: auth.EventDeadline})), nil
	case SessionDispatchCompleted:
		return session.completeDispatch(event.Generation), nil
	case SessionReserveAttachment:
		return nil, session.reserveAttachment(event.FlowID, event.AttachmentGeneration)
	case SessionCancelAttachment:
		session.cancelAttachment(event.FlowID, event.AttachmentGeneration)
		return nil, nil
	case SessionPublishAttachment:
		return nil, session.publishAttachment(event.FlowID, event.AttachmentGeneration)
	case SessionWithdrawAttachment:
		session.withdrawAttachment(event.FlowID, event.AttachmentGeneration)
		return nil, nil
	case SessionTransportClosed:
		return session.close(SessionCloseIO, false), nil
	default:
		return session.close(SessionCloseProtocol, true), ErrInvalidSession
	}
}

func (session *Session) receiveFrame(encoded []byte) ([]SessionAction, error) {
	if !session.readAllowed || session.pendingDispatch != 0 {
		return session.close(SessionCloseProtocol, true), ErrSessionBusy
	}
	session.readAllowed = false
	_, message, err := protocol.DecodeEncodedFrame(encoded)
	if err != nil {
		return session.close(SessionCloseProtocol, true), err
	}
	if session.state == SessionAuthenticating {
		return session.applyAuth(session.auth.Handle(auth.Event{Kind: auth.EventFrameReceived, Message: message})), nil
	}
	if session.state != SessionReady {
		return nil, ErrInvalidSession
	}
	return session.routeReadyFrame(encoded, message)
}

func (session *Session) routeReadyFrame(encoded []byte, message protocol.Message) ([]SessionAction, error) {
	switch typed := message.(type) {
	case protocol.Probe:
		actions, err := session.sendMessage(protocol.ProbeACK{Token: typed.Token}, FrameControl, pendingProbe, 0)
		if err != nil {
			return session.close(SessionCloseLocal, true), err
		}
		return append(actions, session.allowRead()), nil
	case protocol.ProbeACK:
		return []SessionAction{{Kind: SessionActionProbeResult, ProbeToken: typed.Token}, session.allowRead()}, nil
	case protocol.AuthChallenge, protocol.AuthProof, protocol.AuthResult:
		return session.close(SessionCloseProtocol, true), ErrInvalidSession
	}

	flowID, hasFlowID := messageFlowID(message)
	if !hasFlowID || !session.validInboundDirection(message) {
		return session.close(SessionCloseProtocol, true), ErrInvalidSession
	}
	attachmentGeneration := uint64(0)
	if requiresAttachment(message) {
		var ok bool
		attachmentGeneration, ok = session.attachments[flowID]
		if !ok {
			return []SessionAction{
				{Kind: SessionActionDroppedUnknownAttachment, FlowID: flowID},
				session.allowRead(),
			}, nil
		}
	}
	generation, err := session.allocateGeneration()
	if err != nil {
		return session.close(SessionCloseLocal, true), err
	}
	session.pendingDispatch = generation
	session.dispatchEncoded = encoded
	return []SessionAction{{
		Kind:                 SessionActionDispatch,
		Generation:           generation,
		Message:              message,
		FlowID:               flowID,
		AttachmentGeneration: attachmentGeneration,
	}}, nil
}

func (session *Session) applyAuth(authActions []auth.Action) []SessionAction {
	var actions []SessionAction
	for _, action := range authActions {
		switch action.Kind {
		case auth.ActionSend:
			sendActions, err := session.sendMessage(action.Message, FrameControl, pendingAuth, 0)
			if err != nil {
				return append(actions, session.close(SessionCloseLocal, true)...)
			}
			actions = append(actions, sendActions...)
		case auth.ActionAllowRead:
			actions = append(actions, session.allowRead())
		case auth.ActionAuthenticationComplete:
			session.state = SessionReady
			session.principalID = action.PrincipalID
			actions = append(actions, SessionAction{Kind: SessionActionAuthenticated, PrincipalID: action.PrincipalID})
		case auth.ActionClose:
			actions = append(actions, session.close(mapAuthCloseReason(action.Reason), true)...)
		}
	}
	return actions
}

func (session *Session) sendMessage(message protocol.Message, class FrameClass, kind pendingSendKind, correlation uint64) ([]SessionAction, error) {
	if message == nil {
		return nil, ErrInvalidFrame
	}
	if len(session.pendingSends) >= int(V1OutputQueueFrameLimit) {
		return nil, ErrSessionCapacity
	}
	encoded, err := protocol.EncodeMessage(message)
	if err != nil {
		return nil, err
	}
	frameType, _, err := protocol.ParseHeader(encoded[:protocol.HeaderSize])
	if err != nil {
		return nil, err
	}
	if kind != pendingAuth && !session.validOutboundType(frameType) {
		return nil, ErrInvalidSession
	}
	wantClass := FrameControl
	if frameType == protocol.TypeData {
		wantClass = FrameData
	}
	if class != wantClass {
		return nil, ErrInvalidFrame
	}
	generation, err := session.allocateGeneration()
	if err != nil {
		return nil, err
	}
	session.pendingSends[generation] = pendingSend{kind: kind, correlation: correlation}
	return []SessionAction{{
		Kind:        SessionActionSendFrame,
		Generation:  generation,
		Correlation: correlation,
		Encoded:     encoded,
		Class:       class,
	}}, nil
}

func (session *Session) completeSend(generation uint64, succeeded bool) ([]SessionAction, error) {
	pending, ok := session.pendingSends[generation]
	if !ok || generation == 0 {
		return nil, nil
	}
	delete(session.pendingSends, generation)
	if pending.kind == pendingAuth {
		eventKind := auth.EventSendFailed
		if succeeded {
			eventKind = auth.EventSendCompleted
		}
		return session.applyAuth(session.auth.Handle(auth.Event{Kind: eventKind})), nil
	}
	if !succeeded {
		var actions []SessionAction
		if pending.kind == pendingGeneric {
			actions = append(actions, SessionAction{
				Kind:          SessionActionSendResult,
				Generation:    generation,
				Correlation:   pending.correlation,
				SendSucceeded: false,
			})
		}
		return append(actions, session.close(SessionCloseIO, true)...), nil
	}
	if pending.kind == pendingProbe {
		return nil, nil
	}
	return []SessionAction{{
		Kind:          SessionActionSendResult,
		Generation:    generation,
		Correlation:   pending.correlation,
		SendSucceeded: true,
	}}, nil
}

func (session *Session) completeDispatch(generation uint64) []SessionAction {
	if generation == 0 || generation != session.pendingDispatch {
		return nil
	}
	session.pendingDispatch = 0
	session.dispatchEncoded = nil
	return []SessionAction{session.allowRead()}
}

func (session *Session) publishAttachment(flowID protocol.FlowID, generation uint64) error {
	if session.state != SessionReady || generation == 0 || flowID == (protocol.FlowID{}) {
		return ErrInvalidSession
	}
	if existing, ok := session.attachments[flowID]; ok {
		if existing == generation {
			return nil
		}
		return ErrAttachmentConflict
	}
	reserved, ok := session.reservations[flowID]
	if !ok || reserved != generation {
		return ErrInvalidSession
	}
	delete(session.reservations, flowID)
	session.attachments[flowID] = generation
	return nil
}

func (session *Session) reserveAttachment(flowID protocol.FlowID, generation uint64) error {
	if session.state != SessionReady || generation == 0 || flowID == (protocol.FlowID{}) {
		return ErrInvalidSession
	}
	if existing, ok := session.attachments[flowID]; ok {
		if existing == generation {
			return nil
		}
		return ErrAttachmentConflict
	}
	if existing, ok := session.reservations[flowID]; ok {
		if existing == generation {
			return nil
		}
		return ErrAttachmentConflict
	}
	if len(session.attachments)+len(session.reservations) >= MaxSessionAttachments {
		return ErrSessionCapacity
	}
	session.reservations[flowID] = generation
	return nil
}

func (session *Session) cancelAttachment(flowID protocol.FlowID, generation uint64) {
	if current, ok := session.reservations[flowID]; ok && current == generation {
		delete(session.reservations, flowID)
	}
}

func (session *Session) withdrawAttachment(flowID protocol.FlowID, generation uint64) {
	if current, ok := session.attachments[flowID]; ok && current == generation {
		delete(session.attachments, flowID)
	}
}

func (session *Session) close(reason SessionCloseReason, closeTransport bool) []SessionAction {
	if session.state == SessionClosed {
		return nil
	}
	session.state = SessionClosed
	session.readAllowed = false
	session.pendingDispatch = 0
	session.dispatchEncoded = nil
	if session.auth != nil {
		session.auth.Handle(auth.Event{Kind: auth.EventClosed})
		session.auth = nil
	}

	actions := make([]SessionAction, 0, len(session.pendingSends)+len(session.attachments)+len(session.reservations)+1)
	generations := make([]uint64, 0, len(session.pendingSends))
	for generation, pending := range session.pendingSends {
		if pending.kind == pendingGeneric {
			generations = append(generations, generation)
		}
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i] < generations[j] })
	for _, generation := range generations {
		pending := session.pendingSends[generation]
		actions = append(actions, SessionAction{
			Kind:          SessionActionSendResult,
			Generation:    generation,
			Correlation:   pending.correlation,
			SendSucceeded: false,
		})
	}
	clear(session.pendingSends)

	flowIDs := make([]protocol.FlowID, 0, len(session.attachments)+len(session.reservations))
	for flowID := range session.attachments {
		flowIDs = append(flowIDs, flowID)
	}
	for flowID := range session.reservations {
		flowIDs = append(flowIDs, flowID)
	}
	sort.Slice(flowIDs, func(i, j int) bool { return bytes.Compare(flowIDs[i][:], flowIDs[j][:]) < 0 })
	for _, flowID := range flowIDs {
		attachmentGeneration := session.attachments[flowID]
		if attachmentGeneration == 0 {
			attachmentGeneration = session.reservations[flowID]
		}
		actions = append(actions, SessionAction{
			Kind:       SessionActionAttachmentLost,
			FlowID:     flowID,
			Attachment: flow.AttachmentKey{SessionGeneration: session.generation, AttachmentGeneration: attachmentGeneration},
		})
	}
	clear(session.attachments)
	clear(session.reservations)
	if closeTransport {
		actions = append(actions, SessionAction{Kind: SessionActionCloseTransport, CloseReason: reason})
	}
	return actions
}

func (session *Session) allowRead() SessionAction {
	session.readAllowed = true
	return SessionAction{Kind: SessionActionAllowRead}
}

func (session *Session) allocateGeneration() (uint64, error) {
	if session.nextGeneration == ^uint64(0) {
		return 0, ErrSessionCapacity
	}
	session.nextGeneration++
	return session.nextGeneration, nil
}

func (session *Session) validInboundDirection(message protocol.Message) bool {
	switch message.(type) {
	case protocol.Open, protocol.Join:
		return session.role == SessionServer
	case protocol.OpenResult, protocol.JoinResult:
		return session.role == SessionClient
	case protocol.Data, protocol.ACK, protocol.FIN, protocol.FINACK, protocol.Reset:
		return true
	default:
		return false
	}
}

func (session *Session) validOutboundType(frameType protocol.Type) bool {
	switch frameType {
	case protocol.TypeOpen, protocol.TypeJoin:
		return session.role == SessionClient
	case protocol.TypeOpenResult, protocol.TypeJoinResult:
		return session.role == SessionServer
	case protocol.TypeData, protocol.TypeACK, protocol.TypeFIN, protocol.TypeFINACK, protocol.TypeReset, protocol.TypeProbe, protocol.TypeProbeACK:
		return true
	default:
		return false
	}
}

func messageFlowID(message protocol.Message) (protocol.FlowID, bool) {
	switch typed := message.(type) {
	case protocol.Open:
		return typed.FlowID, true
	case protocol.OpenResult:
		return typed.FlowID, true
	case protocol.Join:
		return typed.FlowID, true
	case protocol.JoinResult:
		return typed.FlowID, true
	case protocol.Data:
		return typed.FlowID, true
	case protocol.ACK:
		return typed.FlowID, true
	case protocol.FIN:
		return typed.FlowID, true
	case protocol.FINACK:
		return typed.FlowID, true
	case protocol.Reset:
		return typed.FlowID, true
	default:
		return protocol.FlowID{}, false
	}
}

func requiresAttachment(message protocol.Message) bool {
	switch message.(type) {
	case protocol.Data, protocol.ACK, protocol.FIN, protocol.FINACK, protocol.Reset:
		return true
	default:
		return false
	}
}

func mapAuthCloseReason(reason auth.CloseReason) SessionCloseReason {
	switch reason {
	case auth.CloseAuthenticationFailed:
		return SessionCloseAuthentication
	case auth.CloseDeadlineExceeded:
		return SessionCloseDeadline
	case auth.CloseIOFailure:
		return SessionCloseIO
	case auth.CloseLocalFailure:
		return SessionCloseLocal
	default:
		return SessionCloseProtocol
	}
}
