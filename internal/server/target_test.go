package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
)

func TestTargetDialExecutorResolvesAndDialsSerially(t *testing.T) {
	resolver := &recordingResolver{addresses: []netip.Addr{
		netip.MustParseAddr("2001:db8::2"),
		netip.MustParseAddr("::ffff:192.0.2.10"),
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.11"),
	}}
	connector := &recordingConnector{results: []connectResult{
		{err: errors.New("first failed")},
		{err: errors.New("second failed")},
		{connection: newTrackedConnection()},
	}}
	executor, err := NewTargetDialExecutor(resolver, connector)
	if err != nil {
		t.Fatalf("NewTargetDialExecutor failed: %v", err)
	}

	connection, err := executor.Dial(context.Background(), protocol.Target{DNSName: "relay.example", Port: 443})
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	if connection != connector.results[2].connection {
		t.Fatal("Dial returned an unexpected connection")
	}
	if resolver.calls != 1 || resolver.network != "ip" || resolver.host != "relay.example" {
		t.Fatalf("unexpected resolver call: calls=%d network=%q host=%q", resolver.calls, resolver.network, resolver.host)
	}
	wantAddresses := []string{"[2001:db8::2]:443", "192.0.2.10:443", "192.0.2.11:443"}
	if !reflect.DeepEqual(connector.addresses, wantAddresses) {
		t.Fatalf("dial order = %v, want %v", connector.addresses, wantAddresses)
	}
	if connector.maxActive != 1 {
		t.Fatalf("maximum concurrent candidate dials = %d, want 1", connector.maxActive)
	}
}

func TestTargetDialExecutorLiteralSkipsResolverAndStopsAfterSuccess(t *testing.T) {
	resolver := &recordingResolver{err: errors.New("must not be called")}
	first := newTrackedConnection()
	connector := &recordingConnector{results: []connectResult{
		{connection: first},
		{connection: newTrackedConnection()},
	}}
	executor, err := NewTargetDialExecutor(resolver, connector)
	if err != nil {
		t.Fatalf("NewTargetDialExecutor failed: %v", err)
	}

	connection, err := executor.Dial(context.Background(), protocol.Target{
		Address: netip.MustParseAddr("192.0.2.25"),
		Port:    8443,
	})
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	if connection != first || resolver.calls != 0 {
		t.Fatalf("literal result=%v resolver calls=%d", connection, resolver.calls)
	}
	if !reflect.DeepEqual(connector.addresses, []string{"192.0.2.25:8443"}) {
		t.Fatalf("dial addresses = %v", connector.addresses)
	}
}

func TestTargetDialExecutorRejectsResolutionFailuresAndLimits(t *testing.T) {
	tests := []struct {
		name      string
		resolver  *recordingResolver
		wantError error
	}{
		{
			name:      "resolver error",
			resolver:  &recordingResolver{err: errors.New("lookup failed")},
			wantError: ErrTargetResolution,
		},
		{
			name:      "empty result",
			resolver:  &recordingResolver{},
			wantError: ErrTargetResolution,
		},
		{
			name: "too many results",
			resolver: &recordingResolver{addresses: func() []netip.Addr {
				addresses := make([]netip.Addr, MaxResolvedTargetAddresses+1)
				for index := range addresses {
					addresses[index] = netip.AddrFrom4([4]byte{192, 0, 2, byte(index + 1)})
				}
				return addresses
			}()},
			wantError: ErrTargetAddressLimit,
		},
		{
			name:      "invalid result",
			resolver:  &recordingResolver{addresses: []netip.Addr{{}}},
			wantError: ErrTargetResolution,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connector := &recordingConnector{}
			executor, err := NewTargetDialExecutor(test.resolver, connector)
			if err != nil {
				t.Fatalf("NewTargetDialExecutor failed: %v", err)
			}
			connection, err := executor.Dial(context.Background(), protocol.Target{DNSName: "relay.example", Port: 443})
			if connection != nil || !errors.Is(err, test.wantError) {
				t.Fatalf("Dial = (%v, %v), want nil and %v", connection, err, test.wantError)
			}
			if len(connector.addresses) != 0 {
				t.Fatalf("dialed after resolution failure: %v", connector.addresses)
			}
		})
	}
}

func TestTargetDialExecutorClosesConnectionsReturnedWithErrorOrCancellation(t *testing.T) {
	t.Run("connection with error", func(t *testing.T) {
		late := newTrackedConnection()
		connector := &recordingConnector{results: []connectResult{{connection: late, err: errors.New("late failure")}}}
		executor, _ := NewTargetDialExecutor(&recordingResolver{}, connector)

		connection, err := executor.Dial(context.Background(), protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 80})
		if connection != nil || !errors.Is(err, ErrTargetConnect) || !late.isClosed() {
			t.Fatalf("Dial = (%v, %v), closed=%v", connection, err, late.isClosed())
		}
	})

	t.Run("connector ignores canceled context", func(t *testing.T) {
		late := newTrackedConnection()
		ctx, cancel := context.WithCancel(context.Background())
		connector := &recordingConnector{beforeReturn: cancel, results: []connectResult{{connection: late}}}
		executor, _ := NewTargetDialExecutor(&recordingResolver{}, connector)

		connection, err := executor.Dial(ctx, protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 80})
		if connection != nil || !errors.Is(err, context.Canceled) || !late.isClosed() {
			t.Fatalf("Dial = (%v, %v), closed=%v", connection, err, late.isClosed())
		}
	})
}

func TestTargetDialCoordinatorAcceptsOnlyCurrentGeneration(t *testing.T) {
	target := protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 80}
	coordinator, err := NewTargetDialCoordinator(target)
	if err != nil {
		t.Fatalf("NewTargetDialCoordinator failed: %v", err)
	}
	action, err := coordinator.Begin(41)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if action.Kind != TargetDialActionExecute || action.Generation != 41 || action.Target != target {
		t.Fatalf("unexpected begin action: %+v", action)
	}
	if _, err := coordinator.Begin(42); !errors.Is(err, ErrTargetDialState) {
		t.Fatalf("second Begin error = %v", err)
	}

	late := newTrackedConnection()
	closeAction := coordinator.Complete(TargetDialResult{Generation: 40, Connection: late})
	if closeAction.Kind != TargetDialActionClose || closeAction.Connection != late {
		t.Fatalf("late completion action = %+v", closeAction)
	}
	if coordinator.State() != TargetDialing {
		t.Fatalf("late completion changed state to %v", coordinator.State())
	}

	accepted := newTrackedConnection()
	publishAction := coordinator.Complete(TargetDialResult{Generation: 41, Connection: accepted})
	if publishAction.Kind != TargetDialActionPublish || publishAction.Connection != accepted {
		t.Fatalf("current completion action = %+v", publishAction)
	}
	if coordinator.State() != TargetDialReady {
		t.Fatalf("state = %v, want ready", coordinator.State())
	}

	second := newTrackedConnection()
	closeAction = coordinator.Complete(TargetDialResult{Generation: 41, Connection: second})
	if closeAction.Kind != TargetDialActionClose || closeAction.Connection != second {
		t.Fatalf("second success action = %+v", closeAction)
	}
}

func TestTargetDialCoordinatorDeadlineInvalidatesLateSuccess(t *testing.T) {
	coordinator, err := NewTargetDialCoordinator(protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 80})
	if err != nil {
		t.Fatalf("NewTargetDialCoordinator failed: %v", err)
	}
	if _, err := coordinator.Begin(9); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if !coordinator.Invalidate(9) || coordinator.State() != TargetDialFailed {
		t.Fatalf("Invalidate did not fail active generation: state=%v", coordinator.State())
	}
	if coordinator.Invalidate(9) || coordinator.Invalidate(8) {
		t.Fatal("stale invalidation unexpectedly changed state")
	}

	late := newTrackedConnection()
	action := coordinator.Complete(TargetDialResult{Generation: 9, Connection: late})
	if action.Kind != TargetDialActionClose || action.Connection != late {
		t.Fatalf("late completion action = %+v", action)
	}
	executor, err := NewTargetDialExecutor(&recordingResolver{}, &recordingConnector{})
	if err != nil {
		t.Fatalf("NewTargetDialExecutor failed: %v", err)
	}
	if err := executor.ExecuteClose(action); err != nil || !late.isClosed() {
		t.Fatalf("ExecuteClose error=%v closed=%v", err, late.isClosed())
	}
}

func TestTargetDialActionRoundTrip(t *testing.T) {
	connection := newTrackedConnection()
	executor, err := NewTargetDialExecutor(&recordingResolver{}, &recordingConnector{
		results: []connectResult{{connection: connection}},
	})
	if err != nil {
		t.Fatalf("NewTargetDialExecutor failed: %v", err)
	}
	coordinator, err := NewTargetDialCoordinator(protocol.Target{
		Address: netip.MustParseAddr("2001:db8::8"),
		Port:    443,
	})
	if err != nil {
		t.Fatalf("NewTargetDialCoordinator failed: %v", err)
	}
	action, err := coordinator.Begin(17)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	result := executor.Execute(context.Background(), action)
	if result.Generation != 17 || result.Connection != connection || result.Err != nil {
		t.Fatalf("Execute result = %+v", result)
	}
	publish := coordinator.Complete(result)
	if publish.Kind != TargetDialActionPublish || publish.Generation != 17 || publish.Connection != connection {
		t.Fatalf("Complete action = %+v", publish)
	}
}

func TestTargetDialCoordinatorFailureAndInvalidInputs(t *testing.T) {
	if _, err := NewTargetDialExecutor(nil, &recordingConnector{}); !errors.Is(err, ErrInvalidTargetExecutor) {
		t.Fatalf("nil resolver error = %v", err)
	}
	if _, err := NewTargetDialExecutor(&recordingResolver{}, nil); !errors.Is(err, ErrInvalidTargetExecutor) {
		t.Fatalf("nil connector error = %v", err)
	}
	if _, err := NewTargetDialCoordinator(protocol.Target{}); err == nil {
		t.Fatal("invalid target was accepted")
	}

	coordinator, _ := NewTargetDialCoordinator(protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 80})
	if _, err := coordinator.Begin(0); !errors.Is(err, ErrTargetDialGeneration) {
		t.Fatalf("zero generation error = %v", err)
	}
	if action := coordinator.Complete(TargetDialResult{Generation: 1, Err: ErrTargetConnect}); action.Kind != TargetDialActionNone {
		t.Fatalf("completion before begin = %+v", action)
	}
	if _, err := coordinator.Begin(3); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if action := coordinator.Complete(TargetDialResult{Generation: 3, Err: ErrTargetConnect}); action.Kind != TargetDialActionNone {
		t.Fatalf("failed completion action = %+v", action)
	}
	if coordinator.State() != TargetDialFailed {
		t.Fatalf("state = %v, want failed", coordinator.State())
	}
}

type recordingResolver struct {
	addresses []netip.Addr
	err       error
	calls     int
	network   string
	host      string
}

func (resolver *recordingResolver) LookupNetIP(_ context.Context, network, host string) ([]netip.Addr, error) {
	resolver.calls++
	resolver.network = network
	resolver.host = host
	return append([]netip.Addr(nil), resolver.addresses...), resolver.err
}

type connectResult struct {
	connection net.Conn
	err        error
}

type recordingConnector struct {
	mu           sync.Mutex
	results      []connectResult
	addresses    []string
	active       int
	maxActive    int
	beforeReturn func()
}

func (connector *recordingConnector) DialContext(_ context.Context, network, address string) (net.Conn, error) {
	connector.mu.Lock()
	if network != "tcp" {
		connector.mu.Unlock()
		return nil, errors.New("unexpected network")
	}
	connector.addresses = append(connector.addresses, address)
	connector.active++
	if connector.active > connector.maxActive {
		connector.maxActive = connector.active
	}
	index := len(connector.addresses) - 1
	var result connectResult
	if index < len(connector.results) {
		result = connector.results[index]
	} else {
		result.err = errors.New("no scripted result")
	}
	beforeReturn := connector.beforeReturn
	connector.mu.Unlock()

	if beforeReturn != nil {
		beforeReturn()
	}

	connector.mu.Lock()
	connector.active--
	connector.mu.Unlock()
	return result.connection, result.err
}

type trackedConnection struct {
	mu     sync.Mutex
	closed bool
}

func newTrackedConnection() *trackedConnection { return &trackedConnection{} }

func (connection *trackedConnection) Read([]byte) (int, error)          { return 0, io.EOF }
func (connection *trackedConnection) Write(buffer []byte) (int, error)  { return len(buffer), nil }
func (connection *trackedConnection) LocalAddr() net.Addr               { return testAddress("local") }
func (connection *trackedConnection) RemoteAddr() net.Addr              { return testAddress("remote") }
func (connection *trackedConnection) SetDeadline(_ time.Time) error     { return nil }
func (connection *trackedConnection) SetReadDeadline(_ time.Time) error { return nil }
func (connection *trackedConnection) SetWriteDeadline(_ time.Time) error {
	return nil
}
func (connection *trackedConnection) Close() error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.closed = true
	return nil
}
func (connection *trackedConnection) isClosed() bool {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.closed
}

type testAddress string

func (address testAddress) Network() string { return string(address) }
func (address testAddress) String() string  { return string(address) }
