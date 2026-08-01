package client

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"

	networkpath "github.com/adrianceding/via/internal/path"
	"github.com/adrianceding/via/internal/transport"
)

func TestSessionManagerSelectsDistinctInterfacesAndBindsAddresses(t *testing.T) {
	manager := newSessionManagerForTest(t, 2, 2)
	ethFirst := clientCandidate(1, "eth0", "192.0.2.10")
	ethSecond := clientCandidate(1, "eth0", "192.0.2.11")
	wlan := clientCandidate(2, "wlan0", "192.0.2.20")
	actions, err := manager.Handle(SessionManagerEvent{
		Kind: SessionPathsChanged, Candidates: []networkpath.Candidate{ethSecond, wlan, ethFirst},
	})
	if err != nil {
		t.Fatal(err)
	}
	dials := sessionActions(actions, SessionActionDial)
	if len(dials) != 2 || dials[0].Candidate != ethFirst || dials[1].Candidate != wlan {
		t.Fatalf("dial candidates = %#v", dials)
	}
	if dials[0].Options.LocalEndpoint != "192.0.2.10:0" || dials[1].Options.LocalEndpoint != "192.0.2.20:0" {
		t.Fatalf("local bindings = %q / %q", dials[0].Options.LocalEndpoint, dials[1].Options.LocalEndpoint)
	}
	if dials[0].Options.InterfaceName != "eth0" || dials[1].Options.InterfaceName != "wlan0" {
		t.Fatalf("interface bindings = %q / %q", dials[0].Options.InterfaceName, dials[1].Options.InterfaceName)
	}
	if dials[0].Options.RemoteEndpoint != "127.0.0.1:9443" || dials[0].Options.QueueLimits != transport.V1QueueLimits() {
		t.Fatalf("dial options = %#v", dials[0].Options)
	}
}

func TestSessionManagerUsesEveryEligibleInterfaceByDefault(t *testing.T) {
	manager := newSessionManagerForTest(t, DefaultSessions, DefaultAuthInProgress)
	candidates := make([]networkpath.Candidate, 0, 32)
	for index := 1; index <= 16; index++ {
		name := fmt.Sprintf("eth%02d", index)
		candidates = append(candidates,
			clientCandidate(index, name, fmt.Sprintf("192.0.2.%d", index)),
			clientCandidate(index, name, fmt.Sprintf("198.51.100.%d", index)),
		)
	}
	actions := mustChangeClientPaths(t, manager, candidates...)
	dials := sessionActions(actions, SessionActionDial)
	if len(dials) != 16 || len(manager.Snapshot().Sessions) != 16 {
		t.Fatalf("sessions = %d, dials = %d; want one per interface", len(manager.Snapshot().Sessions), len(dials))
	}
	for index, dial := range dials {
		if dial.Candidate.InterfaceName != fmt.Sprintf("eth%02d", index+1) ||
			dial.Candidate.LocalAddress != netip.MustParseAddr(fmt.Sprintf("192.0.2.%d", index+1)) {
			t.Fatalf("dial %d = %#v", index, dial.Candidate)
		}
	}
}

func TestSessionManagerAuthenticationLimitPumpsNextCandidate(t *testing.T) {
	manager := newSessionManagerForTest(t, 2, 1)
	first := clientCandidate(1, "eth0", "192.0.2.10")
	second := clientCandidate(2, "wlan0", "192.0.2.20")
	actions, err := manager.Handle(SessionManagerEvent{Kind: SessionPathsChanged, Candidates: []networkpath.Candidate{first, second}})
	if err != nil {
		t.Fatal(err)
	}
	firstDial := requireSessionManagerAction(t, actions, SessionActionDial)
	if len(sessionActions(actions, SessionActionDial)) != 1 {
		t.Fatalf("initial dials = %#v", actions)
	}
	connection := newFakeClientConnection()
	actions, err = manager.Handle(SessionManagerEvent{
		Kind: SessionDialCompleted, Generation: firstDial.Generation, Connection: connection,
	})
	if err != nil || len(sessionActions(actions, SessionActionStartAuthentication)) != 1 {
		t.Fatalf("dial completion = %#v, %v", actions, err)
	}
	actions, err = manager.Handle(SessionManagerEvent{
		Kind: SessionAuthenticationCompleted, Generation: firstDial.Generation, Succeeded: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessionActions(actions, SessionActionReady)) != 1 || len(sessionActions(actions, SessionActionDial)) != 1 {
		t.Fatalf("authentication completion = %#v", actions)
	}
	if snapshot := manager.Snapshot(); len(snapshot.Sessions) != 2 || snapshot.Sessions[0].State != ManagedSessionReady || snapshot.Sessions[1].State != ManagedSessionDialing {
		t.Fatalf("snapshot = %#v", snapshot)
	} else if snapshot.Sessions[0].ActualLocalAddress != first.LocalAddress {
		t.Fatalf("actual local address = %v, want %v", snapshot.Sessions[0].ActualLocalAddress, first.LocalAddress)
	} else if snapshot.Sessions[0].LocalEndpoint != connection.LocalEndpoint() || snapshot.Sessions[0].RemoteEndpoint != connection.RemoteEndpoint() {
		t.Fatalf("session endpoints = %q / %q", snapshot.Sessions[0].LocalEndpoint, snapshot.Sessions[0].RemoteEndpoint)
	}
}

func TestSessionManagerRejectsConnectionBoundToWrongAddress(t *testing.T) {
	manager := newSessionManagerForTest(t, 1, 1)
	candidate := clientCandidate(1, "eth0", "192.0.2.10")
	dial := requireSessionManagerAction(t, mustChangeClientPaths(t, manager, candidate), SessionActionDial)
	wrong := newFakeClientConnection()
	wrong.localEndpoint = "192.0.2.99:12345"
	actions, err := manager.Handle(SessionManagerEvent{
		Kind: SessionDialCompleted, Generation: dial.Generation, Connection: wrong,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessionActions(actions, SessionActionCloseConnection)) != 1 ||
		len(sessionActions(actions, SessionActionArmBackoff)) != 1 ||
		len(sessionActions(actions, SessionActionStartAuthentication)) != 0 {
		t.Fatalf("wrong binding actions = %#v", actions)
	}
	if snapshot := manager.Snapshot().Sessions[0]; snapshot.LocalEndpoint != "" || snapshot.RemoteEndpoint != "" {
		t.Fatalf("failed dial retained endpoints = %#v", snapshot)
	}
}

func TestSessionManagerPathRemovalClosesReadyAndRejectsLateDial(t *testing.T) {
	manager := newSessionManagerForTest(t, 1, 1)
	first := clientCandidate(1, "eth0", "192.0.2.10")
	second := clientCandidate(2, "wlan0", "192.0.2.20")
	firstDial := requireSessionManagerAction(t, mustChangeClientPaths(t, manager, first), SessionActionDial)
	firstConnection := newFakeClientConnection()
	_, _ = manager.Handle(SessionManagerEvent{Kind: SessionDialCompleted, Generation: firstDial.Generation, Connection: firstConnection})
	_, _ = manager.Handle(SessionManagerEvent{Kind: SessionAuthenticationCompleted, Generation: firstDial.Generation, Succeeded: true})

	actions := mustChangeClientPaths(t, manager, second)
	if len(sessionActions(actions, SessionActionLost)) != 1 || len(sessionActions(actions, SessionActionCloseConnection)) != 1 {
		t.Fatalf("removal actions = %#v", actions)
	}
	secondDial := requireSessionManagerAction(t, actions, SessionActionDial)
	if secondDial.Candidate != second {
		t.Fatalf("replacement dial = %#v", secondDial)
	}

	late := newFakeClientConnection()
	actions, err := manager.Handle(SessionManagerEvent{
		Kind: SessionDialCompleted, Generation: firstDial.Generation, Connection: late,
	})
	if err != nil || len(sessionActions(actions, SessionActionCloseConnection)) != 1 || sessionActions(actions, SessionActionCloseConnection)[0].Connection != late {
		t.Fatalf("late dial actions = %#v, %v", actions, err)
	}
}

func TestSessionManagerBackoffIsBoundedAndGenerationSafe(t *testing.T) {
	manager := newSessionManagerForTest(t, 1, 1)
	candidate := clientCandidate(1, "eth0", "192.0.2.10")
	dial := requireSessionManagerAction(t, mustChangeClientPaths(t, manager, candidate), SessionActionDial)
	actions, err := manager.Handle(SessionManagerEvent{
		Kind: SessionDialCompleted, Generation: dial.Generation, Err: errors.New("dial failed"),
	})
	if err != nil {
		t.Fatal(err)
	}
	backoff := requireSessionManagerAction(t, actions, SessionActionArmBackoff)
	if backoff.After != SessionInitialBackoff {
		t.Fatalf("first backoff = %v", backoff.After)
	}
	if actions, err := manager.Handle(SessionManagerEvent{Kind: SessionBackoffExpired, Generation: dial.Generation + 1}); err != nil || len(actions) != 0 {
		t.Fatalf("stale backoff = %#v, %v", actions, err)
	}
	actions, err = manager.Handle(SessionManagerEvent{Kind: SessionBackoffExpired, Generation: dial.Generation})
	if err != nil {
		t.Fatal(err)
	}
	secondDial := requireSessionManagerAction(t, actions, SessionActionDial)
	actions, _ = manager.Handle(SessionManagerEvent{
		Kind: SessionDialCompleted, Generation: secondDial.Generation, Err: errors.New("again"),
	})
	if got := requireSessionManagerAction(t, actions, SessionActionArmBackoff).After; got != SessionMaximumBackoff {
		t.Fatalf("second backoff = %v", got)
	}
}

func TestSessionManagerCountsOnlyReadyConnectionLosses(t *testing.T) {
	manager := newSessionManagerForTest(t, 1, 1)
	candidate := clientCandidate(1, "eth0", "192.0.2.10")
	firstDial := requireSessionManagerAction(t, mustChangeClientPaths(t, manager, candidate), SessionActionDial)
	failedConnection := newFakeClientConnection()
	_, _ = manager.Handle(SessionManagerEvent{Kind: SessionDialCompleted, Generation: firstDial.Generation, Connection: failedConnection})
	actions, err := manager.Handle(SessionManagerEvent{
		Kind: SessionAuthenticationCompleted, Generation: firstDial.Generation, Err: errors.New("authentication failed"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := manager.Snapshot().Sessions[0].Reconnects; got != 0 {
		t.Fatalf("reconnects after authentication failure = %d", got)
	}
	actions, err = manager.Handle(SessionManagerEvent{Kind: SessionBackoffExpired, Generation: firstDial.Generation})
	if err != nil {
		t.Fatal(err)
	}
	secondDial := requireSessionManagerAction(t, actions, SessionActionDial)
	readyConnection := newFakeClientConnection()
	_, _ = manager.Handle(SessionManagerEvent{Kind: SessionDialCompleted, Generation: secondDial.Generation, Connection: readyConnection})
	_, _ = manager.Handle(SessionManagerEvent{Kind: SessionAuthenticationCompleted, Generation: secondDial.Generation, Succeeded: true})
	actions, err = manager.Handle(SessionManagerEvent{Kind: SessionConnectionLost, Generation: secondDial.Generation})
	if err != nil || len(sessionActions(actions, SessionActionLost)) != 1 {
		t.Fatalf("ready loss actions = %#v, %v", actions, err)
	}
	if got := manager.Snapshot().Sessions[0].Reconnects; got != 1 {
		t.Fatalf("reconnects after ready loss = %d", got)
	}
}

func TestSessionManagerShutdownIsIdempotentAndClosesLateResults(t *testing.T) {
	manager := newSessionManagerForTest(t, 1, 1)
	candidate := clientCandidate(1, "eth0", "192.0.2.10")
	dial := requireSessionManagerAction(t, mustChangeClientPaths(t, manager, candidate), SessionActionDial)
	if actions, err := manager.Handle(SessionManagerEvent{Kind: SessionShutdown}); err != nil || len(sessionActions(actions, SessionActionCancelDial)) != 1 {
		t.Fatalf("shutdown dialing actions = %#v, %v", actions, err)
	}
	if actions, err := manager.Handle(SessionManagerEvent{Kind: SessionShutdown}); err != nil || len(actions) != 0 {
		t.Fatalf("second shutdown = %#v, %v", actions, err)
	}
	late := newFakeClientConnection()
	actions, err := manager.Handle(SessionManagerEvent{
		Kind: SessionDialCompleted, Generation: dial.Generation, Connection: late,
	})
	if err != nil || len(sessionActions(actions, SessionActionCloseConnection)) != 1 {
		t.Fatalf("late result after shutdown = %#v, %v", actions, err)
	}
	if !manager.Snapshot().ShuttingDown || len(manager.Snapshot().Sessions) != 0 {
		t.Fatalf("shutdown snapshot = %#v", manager.Snapshot())
	}
}

func TestSessionManagerValidatesLimitsCandidatesAndExhaustion(t *testing.T) {
	valid := SessionManagerConfig{
		DesiredSessions: 2, AuthInProgress: 2, RemoteEndpoint: "127.0.0.1:9443", QueueLimits: transport.V1QueueLimits(),
	}
	invalid := []SessionManagerConfig{
		{},
		withSessionConfig(valid, func(config *SessionManagerConfig) { config.DesiredSessions = MaxSessions + 1 }),
		withSessionConfig(valid, func(config *SessionManagerConfig) { config.AuthInProgress = 3 }),
		withSessionConfig(valid, func(config *SessionManagerConfig) { config.RemoteEndpoint = "" }),
		withSessionConfig(valid, func(config *SessionManagerConfig) { config.QueueLimits.MaxFrames-- }),
	}
	for _, config := range invalid {
		if _, err := NewSessionManager(config); !errors.Is(err, ErrInvalidSessionManager) {
			t.Fatalf("config %#v error = %v", config, err)
		}
	}
	manager := newSessionManagerForTest(t, 1, 1)
	if _, err := manager.Handle(SessionManagerEvent{
		Kind: SessionPathsChanged, Candidates: []networkpath.Candidate{{InterfaceIndex: 1, InterfaceName: "eth0"}},
	}); !errors.Is(err, ErrInvalidSessionEvent) {
		t.Fatalf("invalid candidate error = %v", err)
	}
	manager.nextGeneration = ^uint64(0)
	actions, err := manager.Handle(SessionManagerEvent{
		Kind: SessionPathsChanged, Candidates: []networkpath.Candidate{clientCandidate(1, "eth0", "192.0.2.1")},
	})
	if !errors.Is(err, ErrSessionGeneration) || len(actions) != 0 || manager.Snapshot().Sessions[0].State != ManagedSessionWaiting {
		t.Fatalf("generation exhaustion = %#v / %#v / %v", actions, manager.Snapshot(), err)
	}

	manager = newSessionManagerForTest(t, 2, 2)
	manager.nextGeneration = ^uint64(0) - 1
	actions, err = manager.Handle(SessionManagerEvent{
		Kind: SessionPathsChanged,
		Candidates: []networkpath.Candidate{
			clientCandidate(1, "eth0", "192.0.2.1"),
			clientCandidate(2, "wlan0", "192.0.2.2"),
		},
	})
	if !errors.Is(err, ErrSessionGeneration) || len(actions) != 0 {
		t.Fatalf("partial generation exhaustion = %#v / %v", actions, err)
	}
	for _, session := range manager.Snapshot().Sessions {
		if session.State != ManagedSessionWaiting || session.Generation != 0 {
			t.Fatalf("partial exhaustion mutated session = %#v", manager.Snapshot())
		}
	}
}

func newSessionManagerForTest(t *testing.T, desired, authenticating int) *SessionManager {
	t.Helper()
	manager, err := NewSessionManager(SessionManagerConfig{
		DesiredSessions: desired, AuthInProgress: authenticating,
		RemoteEndpoint: "127.0.0.1:9443", QueueLimits: transport.V1QueueLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func clientCandidate(index int, name, address string) networkpath.Candidate {
	return networkpath.Candidate{InterfaceIndex: index, InterfaceName: name, LocalAddress: netip.MustParseAddr(address)}
}

func mustChangeClientPaths(t *testing.T, manager *SessionManager, candidates ...networkpath.Candidate) []SessionManagerAction {
	t.Helper()
	actions, err := manager.Handle(SessionManagerEvent{Kind: SessionPathsChanged, Candidates: candidates})
	if err != nil {
		t.Fatal(err)
	}
	return actions
}

func requireSessionManagerAction(t *testing.T, actions []SessionManagerAction, kind SessionManagerActionKind) SessionManagerAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("missing session action %v in %#v", kind, actions)
	return SessionManagerAction{}
}

func sessionActions(actions []SessionManagerAction, kind SessionManagerActionKind) []SessionManagerAction {
	var result []SessionManagerAction
	for _, action := range actions {
		if action.Kind == kind {
			result = append(result, action)
		}
	}
	return result
}

func withSessionConfig(config SessionManagerConfig, change func(*SessionManagerConfig)) SessionManagerConfig {
	change(&config)
	return config
}

func newFakeClientConnection() *fakeClientConnection {
	return &fakeClientConnection{localEndpoint: "192.0.2.10:12345", remoteEndpoint: "127.0.0.1:9443"}
}

type fakeClientConnection struct {
	localEndpoint  string
	remoteEndpoint string
}

func (*fakeClientConnection) Capabilities() transport.Capabilities {
	capabilities, _ := transport.NewCapabilities(transport.CapabilitySpec{MaxEncodedFrame: 65536, Reliable: true, Ordered: true, HalfClose: true})
	return capabilities
}
func (*fakeClientConnection) QueueLimits() transport.QueueLimits { return transport.V1QueueLimits() }
func (connection *fakeClientConnection) LocalEndpoint() string   { return connection.localEndpoint }
func (connection *fakeClientConnection) RemoteEndpoint() string  { return connection.remoteEndpoint }
func (*fakeClientConnection) ReadFrame(context.Context) ([]byte, error) {
	return nil, transport.ErrClosed
}
func (*fakeClientConnection) WriteFrame(context.Context, transport.WriteRequest) error { return nil }
func (*fakeClientConnection) CloseWrite() error                                        { return nil }
func (*fakeClientConnection) Close() error                                             { return nil }
