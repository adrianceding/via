package status

import "context"

// ControlKind reserves stable types for a future in-process control plane.
// The v1 HTTP handlers neither accept these commands nor register write routes.
type ControlKind uint8

const (
	ControlEnableInterface ControlKind = iota + 1
	ControlDisableInterface
	ControlReconnectSession
	ControlResetFlow
)

type ControlCommand struct {
	Kind          ControlKind
	InterfaceName string
	SessionIDHash string
	FlowIDHash    string
}

type ControlResult struct {
	Accepted bool
	Reason   TransitionReason
}

type ControlService interface {
	Execute(context.Context, ControlCommand) ControlResult
}
