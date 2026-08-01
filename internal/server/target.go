package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"

	"github.com/adrianceding/via/internal/protocol"
)

const MaxResolvedTargetAddresses = 64

var (
	ErrInvalidTargetExecutor = errors.New("server: invalid target executor")
	ErrInvalidDialTarget     = errors.New("server: invalid dial target")
	ErrTargetResolution      = errors.New("server: target resolution failed")
	ErrTargetAddressLimit    = errors.New("server: target address limit exceeded")
	ErrTargetConnect         = errors.New("server: target connection failed")
	ErrTargetDialState       = errors.New("server: invalid target dial state")
	ErrTargetDialGeneration  = errors.New("server: invalid target dial generation")
)

// TargetResolver is the DNS boundary used by the target connection executor.
// net.Resolver implements this interface.
type TargetResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// TargetConnector accepts only resolved literal TCP addresses so the underlying
// dialer cannot perform its own parallel resolution. net.Dialer implements this interface.
type TargetConnector interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

// TargetDialExecutor performs potentially blocking DNS and TCP operations. The
// caller must invoke it outside the flow state owner and use a context to bound
// the entire resolution and serial dialing operation.
type TargetDialExecutor struct {
	resolver  TargetResolver
	connector TargetConnector
}

func NewTargetDialExecutor(resolver TargetResolver, connector TargetConnector) (*TargetDialExecutor, error) {
	if resolver == nil || connector == nil {
		return nil, ErrInvalidTargetExecutor
	}
	return &TargetDialExecutor{resolver: resolver, connector: connector}, nil
}

// Execute performs an Execute action in a worker and returns the same generation to the state owner.
func (executor *TargetDialExecutor) Execute(ctx context.Context, action TargetDialAction) TargetDialResult {
	if action.Kind != TargetDialActionExecute || action.Generation == 0 {
		return TargetDialResult{Generation: action.Generation, Err: ErrTargetDialState}
	}
	connection, err := executor.Dial(ctx, action.Target)
	return TargetDialResult{Generation: action.Generation, Connection: connection, Err: err}
}

// ExecuteClose releases a connection carried by a late, conflicting, or erroneous result in a worker.
func (executor *TargetDialExecutor) ExecuteClose(action TargetDialAction) error {
	if action.Kind != TargetDialActionClose || action.Connection == nil {
		return ErrTargetDialState
	}
	return action.Connection.Close()
}

// Dial resolves the target and dials serially in the resolver's normalized order.
// It passes only one literal IP at a time to the connector, preventing name
// resolution or Happy Eyeballs. The first successful connection returns immediately.
func (executor *TargetDialExecutor) Dial(ctx context.Context, target protocol.Target) (net.Conn, error) {
	if executor == nil || executor.resolver == nil || executor.connector == nil || ctx == nil {
		return nil, ErrInvalidTargetExecutor
	}
	if err := protocol.ValidateTarget(target); err != nil {
		return nil, ErrInvalidDialTarget
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	addresses, err := executor.resolve(ctx, target)
	if err != nil {
		return nil, err
	}
	port := strconv.FormatUint(uint64(target.Port), 10)
	for _, address := range addresses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		connection, dialErr := executor.connector.DialContext(
			ctx,
			"tcp",
			net.JoinHostPort(address.String(), port),
		)
		if contextErr := ctx.Err(); contextErr != nil {
			closeTargetConnection(connection)
			return nil, contextErr
		}
		if dialErr != nil {
			closeTargetConnection(connection)
			continue
		}
		if connection == nil {
			continue
		}
		return connection, nil
	}
	return nil, ErrTargetConnect
}

func (executor *TargetDialExecutor) resolve(ctx context.Context, target protocol.Target) ([]netip.Addr, error) {
	if target.Address.IsValid() {
		return []netip.Addr{target.Address.Unmap()}, nil
	}

	addresses, err := executor.resolver.LookupNetIP(ctx, "ip", target.DNSName)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, ErrTargetResolution
	}
	if len(addresses) > MaxResolvedTargetAddresses {
		return nil, ErrTargetAddressLimit
	}

	normalized := make([]netip.Addr, 0, len(addresses))
	seen := make(map[netip.Addr]struct{}, len(addresses))
	for _, address := range addresses {
		if !address.IsValid() || address.Zone() != "" {
			return nil, ErrTargetResolution
		}
		address = address.Unmap()
		if !address.Is4() && !address.Is6() {
			return nil, ErrTargetResolution
		}
		if _, duplicate := seen[address]; duplicate {
			continue
		}
		seen[address] = struct{}{}
		normalized = append(normalized, address)
	}
	if len(normalized) == 0 {
		return nil, ErrTargetResolution
	}
	return normalized, nil
}

// TargetDialState is the pure coordination state for the unique target connection action.
type TargetDialState uint8

const (
	TargetDialNew TargetDialState = iota + 1
	TargetDialing
	TargetDialReady
	TargetDialFailed
)

// TargetDialActionKind distinguishes worker execution, connection publication,
// and resource closure. Only Execute and Close require an executor outside the
// flow state owner.
type TargetDialActionKind uint8

const (
	TargetDialActionNone TargetDialActionKind = iota
	TargetDialActionExecute
	TargetDialActionPublish
	TargetDialActionClose
)

type TargetDialAction struct {
	Kind       TargetDialActionKind
	Generation uint64
	Target     protocol.Target
	Connection net.Conn
}

type TargetDialResult struct {
	Generation uint64
	Connection net.Conn
	Err        error
}

// TargetDialCoordinator owns only one DialTarget generation and its publication
// fact; it performs no network I/O. A deadline, reset, or cancellation invalidates
// the current generation through Invalidate; a late connection becomes a Close action.
type TargetDialCoordinator struct {
	target     protocol.Target
	state      TargetDialState
	generation uint64
}

func NewTargetDialCoordinator(target protocol.Target) (*TargetDialCoordinator, error) {
	if err := protocol.ValidateTarget(target); err != nil {
		return nil, ErrInvalidDialTarget
	}
	return &TargetDialCoordinator{target: target, state: TargetDialNew}, nil
}

func (coordinator *TargetDialCoordinator) State() TargetDialState {
	if coordinator == nil {
		return TargetDialFailed
	}
	return coordinator.state
}

// Begin can succeed only once. A higher-level state owner allocates generation and associates its deadline.
func (coordinator *TargetDialCoordinator) Begin(generation uint64) (TargetDialAction, error) {
	if coordinator == nil || coordinator.state != TargetDialNew {
		return TargetDialAction{}, ErrTargetDialState
	}
	if generation == 0 {
		return TargetDialAction{}, ErrTargetDialGeneration
	}
	coordinator.state = TargetDialing
	coordinator.generation = generation
	return TargetDialAction{
		Kind:       TargetDialActionExecute,
		Generation: generation,
		Target:     coordinator.target,
	}, nil
}

// Invalidate moves the matching active generation to the failed state. The caller must also cancel the action context.
func (coordinator *TargetDialCoordinator) Invalidate(generation uint64) bool {
	if coordinator == nil || generation == 0 || coordinator.state != TargetDialing || generation != coordinator.generation {
		return false
	}
	coordinator.state = TargetDialFailed
	return true
}

// Complete accepts the sole error-free connection for the current generation.
// An erroneous result, invalidated generation, or second successful connection
// is never published; the caller must execute the returned Close action for any
// connection it carries.
func (coordinator *TargetDialCoordinator) Complete(result TargetDialResult) TargetDialAction {
	if coordinator == nil || coordinator.state != TargetDialing || result.Generation == 0 || result.Generation != coordinator.generation {
		return closeTargetAction(result.Generation, result.Connection)
	}
	if result.Err != nil || result.Connection == nil {
		coordinator.state = TargetDialFailed
		return closeTargetAction(result.Generation, result.Connection)
	}
	coordinator.state = TargetDialReady
	return TargetDialAction{
		Kind:       TargetDialActionPublish,
		Generation: result.Generation,
		Connection: result.Connection,
	}
}

func closeTargetAction(generation uint64, connection net.Conn) TargetDialAction {
	if connection == nil {
		return TargetDialAction{}
	}
	return TargetDialAction{
		Kind:       TargetDialActionClose,
		Generation: generation,
		Connection: connection,
	}
}

func closeTargetConnection(connection net.Conn) {
	if connection != nil {
		_ = connection.Close()
	}
}
