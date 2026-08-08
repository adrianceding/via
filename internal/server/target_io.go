package server

import (
	"errors"
	"net"
	"sync"
)

var (
	ErrInvalidTargetIOAction = errors.New("server: invalid target I/O action")
	ErrTargetIOContract      = errors.New("server: target connection violated I/O contract")
	ErrTargetHalfClose       = errors.New("server: target connection does not support close-write")
)

type closeWriter interface {
	CloseWrite() error
}

// TargetIOExecutor performs potentially blocking target connection operations
// outside the flow state owner. One read and one write may run in parallel on an
// executor; Relay keeps at most one action in flight per direction. Close does not
// wait for in-flight operations and relies on net.Conn concurrent-close semantics to unblock them.
type TargetIOExecutor struct {
	connection    net.Conn
	closeOnce     sync.Once
	closeErr      error
	readBuffer    []byte
	maxWriteBytes int
}

func NewTargetIOExecutor(connection net.Conn) (*TargetIOExecutor, error) {
	return NewTargetIOExecutorWithLimit(connection, MaxTargetWriteBytes)
}

func NewTargetIOExecutorWithLimit(connection net.Conn, maxWriteBytes int) (*TargetIOExecutor, error) {
	if maxWriteBytes == 0 {
		maxWriteBytes = MaxTargetWriteBytes
	}
	if connection == nil || maxWriteBytes < 1 {
		return nil, ErrInvalidTargetIOAction
	}
	return &TargetIOExecutor{connection: connection, maxWriteBytes: maxWriteBytes}, nil
}

// Execute returns a result to feed back into Relay. hasResult=false applies only
// to close actions. Network read and write errors are carried in RelayEvent.Err;
// a method error indicates an invalid action or executor contract.
func (executor *TargetIOExecutor) Execute(action RelayAction) (event RelayEvent, hasResult bool, err error) {
	if executor == nil || executor.connection == nil || action.Generation == 0 {
		return RelayEvent{}, false, ErrInvalidTargetIOAction
	}

	switch action.Kind {
	case RelayActionReadTarget:
		if action.MaxBytes < 1 || action.MaxBytes > MaxTargetReadBytes {
			return RelayEvent{}, false, ErrInvalidTargetIOAction
		}
		if cap(executor.readBuffer) < action.MaxBytes {
			executor.readBuffer = make([]byte, action.MaxBytes)
		}
		buffer := executor.readBuffer[:action.MaxBytes]
		n, readErr := executor.connection.Read(buffer)
		if n < 0 || n > len(buffer) {
			return RelayEvent{
				Kind:       RelayTargetReadResult,
				Generation: action.Generation,
				Err:        errors.Join(ErrTargetIOContract, readErr),
			}, true, nil
		}
		return RelayEvent{
			Kind:       RelayTargetReadResult,
			Generation: action.Generation,
			Data:       buffer[:n:n],
			Err:        readErr,
		}, true, nil

	case RelayActionWriteTarget:
		if action.DataLen() < 1 || action.DataLen() > executor.maxWriteBytes {
			return RelayEvent{}, false, ErrInvalidTargetIOAction
		}
		data := action.BorrowData()
		n, writeErr := executor.connection.Write(data)
		if n < 0 || n > len(data) {
			writeErr = errors.Join(ErrTargetIOContract, writeErr)
		}
		return RelayEvent{
			Kind:       RelayTargetWriteResult,
			Generation: action.Generation,
			N:          n,
			Err:        writeErr,
		}, true, nil

	case RelayActionCloseWriteTarget:
		connection, ok := executor.connection.(closeWriter)
		closeErr := ErrTargetHalfClose
		if ok {
			closeErr = connection.CloseWrite()
		}
		return RelayEvent{
			Kind:       RelayTargetCloseWriteResult,
			Generation: action.Generation,
			Err:        closeErr,
		}, true, nil

	case RelayActionCloseTarget:
		executor.closeOnce.Do(func() {
			executor.closeErr = executor.connection.Close()
		})
		return RelayEvent{}, false, executor.closeErr

	default:
		return RelayEvent{}, false, ErrInvalidTargetIOAction
	}
}
