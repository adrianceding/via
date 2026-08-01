package client

import (
	"errors"
	"io"
	"math"
	"net"
	"time"

	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/socks5"
)

const SOCKSWriteTimeout = 10 * time.Second

var (
	ErrInvalidSOCKSCoordinator = errors.New("client: invalid SOCKS coordinator")
	ErrInvalidSOCKSEvent       = errors.New("client: invalid SOCKS event")
	ErrSOCKSGeneration         = errors.New("client: SOCKS generation exhausted")
)

type SOCKSState uint8

const (
	SOCKSAccepted SOCKSState = iota + 1
	SOCKSReadingGreeting
	SOCKSWritingMethod
	SOCKSReadingAuthentication
	SOCKSWritingAuthentication
	SOCKSReadingRequest
	SOCKSWaitingFlow
	SOCKSWritingReply
	SOCKSRelaying
	SOCKSClosed
)

type SOCKSEventKind uint8

const (
	SOCKSStart SOCKSEventKind = iota + 1
	SOCKSGreetingResult
	SOCKSAuthenticationResult
	SOCKSRequestResult
	SOCKSWriteResult
	SOCKSFlowResult
	SOCKSDeadline
	SOCKSPeerClosed
)

type SOCKSEvent struct {
	Kind       SOCKSEventKind
	Generation uint64
	Accepted   bool
	Target     protocol.Target
	OpenResult protocol.OpenResultCode
	Err        error
}

type SOCKSActionKind uint8

const (
	SOCKSActionReadGreeting SOCKSActionKind = iota + 1
	SOCKSActionReadAuthentication
	SOCKSActionReadRequest
	SOCKSActionWrite
	SOCKSActionStartFlow
	SOCKSActionEnableApplication
	SOCKSActionCloseConnection
	SOCKSActionArmDeadline
	SOCKSActionCancelDeadline
	SOCKSActionCancelFlow
)

type SOCKSAction struct {
	Kind       SOCKSActionKind
	Generation uint64
	After      time.Duration
	Target     protocol.Target
	data       []byte
}

func (action SOCKSAction) DataLen() int     { return len(action.data) }
func (action SOCKSAction) CopyData() []byte { return append([]byte(nil), action.data...) }

type socksWritePurpose uint8

const (
	socksWriteMethodSuccess socksWritePurpose = iota + 1
	socksWriteMethodFailure
	socksWriteAuthenticationSuccess
	socksWriteAuthenticationFailure
	socksWriteReplySuccess
	socksWriteReplyFailure
)

type SOCKSSnapshot struct {
	State              SOCKSState
	PendingGeneration  uint64
	DeadlineGeneration uint64
	ApplicationEnabled bool
}

// SOCKSCoordinator owns only the handshake state for one local SOCKS connection
// and performs no socket I/O. It emits no application payload read action before
// SOCKSActionEnableApplication.
type SOCKSCoordinator struct {
	state                  SOCKSState
	authenticationRequired bool
	greetingTimeout        time.Duration
	requestTimeout         time.Duration
	nextGeneration         uint64
	pendingGeneration      uint64
	deadlineGeneration     uint64
	writePurpose           socksWritePurpose
	closeIssued            bool
	applicationEnabled     bool
	flowStarted            bool
}

func NewSOCKSCoordinator(greetingTimeout, requestTimeout time.Duration, authenticationRequired bool) (*SOCKSCoordinator, error) {
	if !validSOCKSTimeout(greetingTimeout) || !validSOCKSTimeout(requestTimeout) {
		return nil, ErrInvalidSOCKSCoordinator
	}
	return &SOCKSCoordinator{
		state: SOCKSAccepted, authenticationRequired: authenticationRequired,
		greetingTimeout: greetingTimeout, requestTimeout: requestTimeout,
	}, nil
}

func (coordinator *SOCKSCoordinator) Snapshot() SOCKSSnapshot {
	if coordinator == nil {
		return SOCKSSnapshot{State: SOCKSClosed}
	}
	return SOCKSSnapshot{
		State: coordinator.state, PendingGeneration: coordinator.pendingGeneration,
		DeadlineGeneration: coordinator.deadlineGeneration, ApplicationEnabled: coordinator.applicationEnabled,
	}
}

func (coordinator *SOCKSCoordinator) Handle(event SOCKSEvent) ([]SOCKSAction, error) {
	if coordinator == nil {
		return nil, ErrInvalidSOCKSCoordinator
	}
	if coordinator.state == SOCKSClosed {
		return nil, nil
	}
	if event.Kind == SOCKSPeerClosed {
		actions := coordinator.finishRead()
		if coordinator.flowStarted {
			actions = append(actions, SOCKSAction{Kind: SOCKSActionCancelFlow})
			coordinator.flowStarted = false
		}
		coordinator.enterClosed(false)
		return actions, nil
	}
	switch event.Kind {
	case SOCKSStart:
		if coordinator.state != SOCKSAccepted {
			return coordinator.closeWithError(ErrInvalidSOCKSEvent)
		}
		coordinator.state = SOCKSReadingGreeting
		return coordinator.beginRead(SOCKSActionReadGreeting, coordinator.greetingTimeout)
	case SOCKSGreetingResult:
		return coordinator.greetingResult(event)
	case SOCKSAuthenticationResult:
		return coordinator.authenticationResult(event)
	case SOCKSRequestResult:
		return coordinator.requestResult(event)
	case SOCKSWriteResult:
		return coordinator.writeResult(event)
	case SOCKSFlowResult:
		return coordinator.flowResult(event)
	case SOCKSDeadline:
		if event.Generation == 0 || event.Generation != coordinator.deadlineGeneration {
			return nil, nil
		}
		coordinator.deadlineGeneration = 0
		return coordinator.closeWithError(nil)
	default:
		return coordinator.closeWithError(ErrInvalidSOCKSEvent)
	}
}

func (coordinator *SOCKSCoordinator) authenticationResult(event SOCKSEvent) ([]SOCKSAction, error) {
	if coordinator.state != SOCKSReadingAuthentication || event.Generation == 0 || event.Generation != coordinator.pendingGeneration {
		return nil, nil
	}
	actions := coordinator.finishRead()
	accepted := event.Err == nil && event.Accepted
	reply := socks5.AuthenticationReply(accepted)
	purpose := socksWriteAuthenticationFailure
	if accepted {
		purpose = socksWriteAuthenticationSuccess
	}
	write, err := coordinator.beginWrite(reply[:], purpose)
	return append(actions, write...), err
}

func (coordinator *SOCKSCoordinator) greetingResult(event SOCKSEvent) ([]SOCKSAction, error) {
	if coordinator.state != SOCKSReadingGreeting || event.Generation == 0 || event.Generation != coordinator.pendingGeneration {
		return nil, nil
	}
	actions := coordinator.finishRead()
	if event.Err != nil || !event.Accepted {
		if errors.Is(event.Err, socks5.ErrNoAcceptableMethod) || event.Err == nil {
			method := socks5.MethodReply(coordinator.requiredMethod(), false)
			write, err := coordinator.beginWrite(method[:], socksWriteMethodFailure)
			return append(actions, write...), err
		}
		closed, err := coordinator.closeWithError(nil)
		return append(actions, closed...), err
	}
	method := socks5.MethodReply(coordinator.requiredMethod(), true)
	write, err := coordinator.beginWrite(method[:], socksWriteMethodSuccess)
	return append(actions, write...), err
}

func (coordinator *SOCKSCoordinator) requestResult(event SOCKSEvent) ([]SOCKSAction, error) {
	if coordinator.state != SOCKSReadingRequest || event.Generation == 0 || event.Generation != coordinator.pendingGeneration {
		return nil, nil
	}
	actions := coordinator.finishRead()
	if event.Err != nil {
		code := socks5.ReplyGeneralFailure
		switch {
		case errors.Is(event.Err, socks5.ErrCommandUnsupported):
			code = socks5.ReplyCommandNotSupported
		case errors.Is(event.Err, socks5.ErrAddressTypeUnsupported):
			code = socks5.ReplyAddressTypeNotSupported
		}
		reply, _ := socks5.Reply(code)
		write, err := coordinator.beginWrite(reply[:], socksWriteReplyFailure)
		return append(actions, write...), err
	}
	if err := protocol.ValidateTarget(event.Target); err != nil {
		reply, _ := socks5.Reply(socks5.ReplyAddressTypeNotSupported)
		write, writeErr := coordinator.beginWrite(reply[:], socksWriteReplyFailure)
		return append(actions, write...), writeErr
	}
	coordinator.state = SOCKSWaitingFlow
	coordinator.flowStarted = true
	return append(actions, SOCKSAction{Kind: SOCKSActionStartFlow, Target: event.Target}), nil
}

func (coordinator *SOCKSCoordinator) writeResult(event SOCKSEvent) ([]SOCKSAction, error) {
	if (coordinator.state != SOCKSWritingMethod && coordinator.state != SOCKSWritingAuthentication && coordinator.state != SOCKSWritingReply) ||
		event.Generation == 0 || event.Generation != coordinator.pendingGeneration {
		return nil, nil
	}
	actions := coordinator.finishRead()
	purpose := coordinator.writePurpose
	coordinator.writePurpose = 0
	if event.Err != nil {
		closed, err := coordinator.closeWithError(nil)
		return append(actions, closed...), err
	}
	switch purpose {
	case socksWriteMethodSuccess:
		if coordinator.authenticationRequired {
			coordinator.state = SOCKSReadingAuthentication
			next, err := coordinator.beginRead(SOCKSActionReadAuthentication, coordinator.greetingTimeout)
			return append(actions, next...), err
		}
		coordinator.state = SOCKSReadingRequest
		next, err := coordinator.beginRead(SOCKSActionReadRequest, coordinator.requestTimeout)
		return append(actions, next...), err
	case socksWriteAuthenticationSuccess:
		coordinator.state = SOCKSReadingRequest
		next, err := coordinator.beginRead(SOCKSActionReadRequest, coordinator.requestTimeout)
		return append(actions, next...), err
	case socksWriteMethodFailure, socksWriteAuthenticationFailure, socksWriteReplyFailure:
		closed, err := coordinator.closeWithError(nil)
		return append(actions, closed...), err
	case socksWriteReplySuccess:
		coordinator.state = SOCKSRelaying
		coordinator.applicationEnabled = true
		return append(actions, SOCKSAction{Kind: SOCKSActionEnableApplication}), nil
	default:
		closed, err := coordinator.closeWithError(ErrInvalidSOCKSEvent)
		return append(actions, closed...), err
	}
}

func (coordinator *SOCKSCoordinator) requiredMethod() byte {
	if coordinator.authenticationRequired {
		return socks5.MethodUsernamePassword
	}
	return socks5.MethodNoAuth
}

func (coordinator *SOCKSCoordinator) flowResult(event SOCKSEvent) ([]SOCKSAction, error) {
	if coordinator.state != SOCKSWaitingFlow {
		return nil, nil
	}
	code := socks5.ReplyForOpenResult(event.OpenResult)
	reply, err := socks5.Reply(code)
	if err != nil {
		return coordinator.closeWithError(err)
	}
	purpose := socksWriteReplyFailure
	if code == socks5.ReplySucceeded {
		purpose = socksWriteReplySuccess
	} else {
		coordinator.flowStarted = false
	}
	return coordinator.beginWrite(reply[:], purpose)
}

func (coordinator *SOCKSCoordinator) beginRead(kind SOCKSActionKind, timeout time.Duration) ([]SOCKSAction, error) {
	operation, ok := coordinator.allocateGeneration()
	if !ok {
		return coordinator.closeWithError(ErrSOCKSGeneration)
	}
	deadline, ok := coordinator.allocateGeneration()
	if !ok {
		return coordinator.closeWithError(ErrSOCKSGeneration)
	}
	coordinator.pendingGeneration = operation
	coordinator.deadlineGeneration = deadline
	return []SOCKSAction{
		{Kind: SOCKSActionArmDeadline, Generation: deadline, After: timeout},
		{Kind: kind, Generation: operation},
	}, nil
}

func (coordinator *SOCKSCoordinator) finishRead() []SOCKSAction {
	coordinator.pendingGeneration = 0
	if coordinator.deadlineGeneration == 0 {
		return nil
	}
	generation := coordinator.deadlineGeneration
	coordinator.deadlineGeneration = 0
	return []SOCKSAction{{Kind: SOCKSActionCancelDeadline, Generation: generation}}
}

func (coordinator *SOCKSCoordinator) beginWrite(data []byte, purpose socksWritePurpose) ([]SOCKSAction, error) {
	if len(data) == 0 {
		return coordinator.closeWithError(ErrInvalidSOCKSEvent)
	}
	generation, ok := coordinator.allocateGeneration()
	if !ok {
		return coordinator.closeWithError(ErrSOCKSGeneration)
	}
	deadline, ok := coordinator.allocateGeneration()
	if !ok {
		return coordinator.closeWithError(ErrSOCKSGeneration)
	}
	coordinator.pendingGeneration = generation
	coordinator.deadlineGeneration = deadline
	coordinator.writePurpose = purpose
	if purpose == socksWriteMethodSuccess || purpose == socksWriteMethodFailure {
		coordinator.state = SOCKSWritingMethod
	} else if purpose == socksWriteAuthenticationSuccess || purpose == socksWriteAuthenticationFailure {
		coordinator.state = SOCKSWritingAuthentication
	} else {
		coordinator.state = SOCKSWritingReply
	}
	return []SOCKSAction{
		{Kind: SOCKSActionArmDeadline, Generation: deadline, After: SOCKSWriteTimeout},
		{Kind: SOCKSActionWrite, Generation: generation, data: append([]byte(nil), data...)},
	}, nil
}

func (coordinator *SOCKSCoordinator) closeWithError(cause error) ([]SOCKSAction, error) {
	actions := coordinator.finishRead()
	if coordinator.flowStarted {
		actions = append(actions, SOCKSAction{Kind: SOCKSActionCancelFlow})
		coordinator.flowStarted = false
	}
	coordinator.enterClosed(true)
	if coordinator.closeIssued {
		actions = append(actions, SOCKSAction{Kind: SOCKSActionCloseConnection})
		coordinator.closeIssued = false
	}
	return actions, cause
}

func (coordinator *SOCKSCoordinator) enterClosed(issueClose bool) {
	coordinator.state = SOCKSClosed
	coordinator.pendingGeneration = 0
	coordinator.deadlineGeneration = 0
	coordinator.writePurpose = 0
	coordinator.applicationEnabled = false
	if issueClose && !coordinator.closeIssued {
		coordinator.closeIssued = true
	}
}

func (coordinator *SOCKSCoordinator) allocateGeneration() (uint64, bool) {
	if coordinator.nextGeneration == math.MaxUint64 {
		return 0, false
	}
	coordinator.nextGeneration++
	return coordinator.nextGeneration, true
}

func validSOCKSTimeout(timeout time.Duration) bool {
	return timeout >= time.Second && timeout <= 30*time.Second
}

// SOCKSExecutor performs bounded handshake reads, fixed reply writes, and closure outside the state owner.
type SOCKSExecutor struct {
	connection             net.Conn
	authenticationRequired bool
	username               string
	password               string
}

func NewSOCKSExecutor(connection net.Conn, username, password string) (*SOCKSExecutor, error) {
	authenticationRequired := username != "" || password != ""
	if connection == nil || authenticationRequired && (!socks5.ValidCredential(username) || !socks5.ValidCredential(password)) {
		return nil, ErrInvalidSOCKSCoordinator
	}
	return &SOCKSExecutor{
		connection: connection, authenticationRequired: authenticationRequired,
		username: username, password: password,
	}, nil
}

func (executor *SOCKSExecutor) Execute(action SOCKSAction) (SOCKSEvent, bool, error) {
	if executor == nil || executor.connection == nil {
		return SOCKSEvent{}, false, ErrInvalidSOCKSCoordinator
	}
	switch action.Kind {
	case SOCKSActionReadGreeting:
		if action.Generation == 0 {
			return SOCKSEvent{}, false, ErrInvalidSOCKSEvent
		}
		requiredMethod := byte(socks5.MethodNoAuth)
		if executor.authenticationRequired {
			requiredMethod = socks5.MethodUsernamePassword
		}
		accepted, err := socks5.ReadGreeting(executor.connection, requiredMethod)
		return SOCKSEvent{Kind: SOCKSGreetingResult, Generation: action.Generation, Accepted: accepted, Err: err}, true, nil
	case SOCKSActionReadAuthentication:
		if action.Generation == 0 || !executor.authenticationRequired {
			return SOCKSEvent{}, false, ErrInvalidSOCKSEvent
		}
		accepted, err := socks5.Authenticate(executor.connection, executor.username, executor.password)
		return SOCKSEvent{Kind: SOCKSAuthenticationResult, Generation: action.Generation, Accepted: accepted, Err: err}, true, nil
	case SOCKSActionReadRequest:
		if action.Generation == 0 {
			return SOCKSEvent{}, false, ErrInvalidSOCKSEvent
		}
		target, err := socks5.ReadRequest(executor.connection)
		return SOCKSEvent{Kind: SOCKSRequestResult, Generation: action.Generation, Target: target, Err: err}, true, nil
	case SOCKSActionWrite:
		if action.Generation == 0 || action.DataLen() == 0 || action.DataLen() > 10 {
			return SOCKSEvent{}, false, ErrInvalidSOCKSEvent
		}
		err := writeSOCKSBytes(executor.connection, action.data)
		return SOCKSEvent{Kind: SOCKSWriteResult, Generation: action.Generation, Err: err}, true, nil
	case SOCKSActionCloseConnection:
		return SOCKSEvent{}, false, executor.connection.Close()
	default:
		return SOCKSEvent{}, false, ErrInvalidSOCKSEvent
	}
}

func writeSOCKSBytes(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
