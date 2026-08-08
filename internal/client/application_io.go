package client

import (
	"errors"
	"net"
	"sync"
)

var (
	ErrInvalidApplicationIOAction = errors.New("client: invalid application I/O action")
	ErrApplicationIOContract      = errors.New("client: application connection violated I/O contract")
	ErrApplicationHalfClose       = errors.New("client: application connection does not support close-write")
)

type applicationCloseWriter interface {
	CloseWrite() error
}

// ApplicationIOExecutor performs potentially blocking application connection
// operations outside the flow state owner. One read and one write may run in
// parallel on an executor; ApplicationRelay keeps at most one action in flight
// per direction. Close does not wait for in-flight operations and relies on
// net.Conn concurrent-close semantics to unblock them.
type ApplicationIOExecutor struct {
	connection    net.Conn
	readBuffer    []byte
	maxWriteBytes int
	closeOnce     sync.Once
	closeErr      error
}

func NewApplicationIOExecutor(connection net.Conn) (*ApplicationIOExecutor, error) {
	return NewApplicationIOExecutorWithLimit(connection, MaxApplicationWriteBytes)
}

func NewApplicationIOExecutorWithLimit(connection net.Conn, maxWriteBytes int) (*ApplicationIOExecutor, error) {
	if maxWriteBytes == 0 {
		maxWriteBytes = MaxApplicationWriteBytes
	}
	if connection == nil || maxWriteBytes < 1 {
		return nil, ErrInvalidApplicationIOAction
	}
	return &ApplicationIOExecutor{connection: connection, maxWriteBytes: maxWriteBytes}, nil
}

// Execute returns a result to feed back into ApplicationRelay. hasResult=false
// applies only to close actions. Network read and write errors are carried in
// ApplicationRelayEvent.Err; a method error indicates an invalid action or
// executor contract.
func (executor *ApplicationIOExecutor) Execute(action ApplicationRelayAction) (event ApplicationRelayEvent, hasResult bool, err error) {
	if executor == nil || executor.connection == nil || action.Generation == 0 {
		return ApplicationRelayEvent{}, false, ErrInvalidApplicationIOAction
	}

	switch action.Kind {
	case ApplicationRelayActionRead:
		if action.MaxBytes < 1 || action.MaxBytes > MaxApplicationReadBytes {
			return ApplicationRelayEvent{}, false, ErrInvalidApplicationIOAction
		}
		if cap(executor.readBuffer) < action.MaxBytes {
			executor.readBuffer = make([]byte, action.MaxBytes)
		}
		buffer := executor.readBuffer[:action.MaxBytes]
		n, readErr := executor.connection.Read(buffer)
		if n < 0 || n > len(buffer) {
			return ApplicationRelayEvent{
				Kind:       ApplicationRelayReadResult,
				Generation: action.Generation,
				Err:        errors.Join(ErrApplicationIOContract, readErr),
			}, true, nil
		}
		return ApplicationRelayEvent{
			Kind:       ApplicationRelayReadResult,
			Generation: action.Generation,
			Data:       buffer[:n:n],
			Err:        readErr,
		}, true, nil

	case ApplicationRelayActionWrite:
		if action.DataLen() < 1 || action.DataLen() > executor.maxWriteBytes {
			return ApplicationRelayEvent{}, false, ErrInvalidApplicationIOAction
		}
		data := action.BorrowData()
		n, writeErr := executor.connection.Write(data)
		if n < 0 || n > len(data) {
			writeErr = errors.Join(ErrApplicationIOContract, writeErr)
		}
		return ApplicationRelayEvent{
			Kind:       ApplicationRelayWriteResult,
			Generation: action.Generation,
			N:          n,
			Err:        writeErr,
		}, true, nil

	case ApplicationRelayActionCloseWrite:
		connection, ok := executor.connection.(applicationCloseWriter)
		closeErr := ErrApplicationHalfClose
		if ok {
			closeErr = connection.CloseWrite()
		}
		return ApplicationRelayEvent{
			Kind:       ApplicationRelayCloseWriteResult,
			Generation: action.Generation,
			Err:        closeErr,
		}, true, nil

	case ApplicationRelayActionClose:
		executor.closeOnce.Do(func() {
			executor.closeErr = executor.connection.Close()
		})
		return ApplicationRelayEvent{}, false, executor.closeErr

	default:
		return ApplicationRelayEvent{}, false, ErrInvalidApplicationIOAction
	}
}
