package server

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

func TestServerGate7OpenJoinRelayAndTerminalChain(t *testing.T) {
	clock := newRegistryTestClock()
	limiter := mustRateLimiter(t, 64)
	request := testOpen(0x61)
	if !limiter.AllowOpen(clock.Now(), "client-a") {
		t.Fatal("first OPEN was rate limited")
	}
	registry := newTestRegistry(t, testRegistryLimits(), &countingCapabilityReader{value: 0x5a}, clock.Now)

	opening := registry.HandleOpen("client-a", request)
	requireCapabilityAction(t, opening)
	duplicateOpening := registry.HandleOpen("client-a", request)
	if duplicateOpening.Operation != opening.Operation || duplicateOpening.GenerateCapability != nil {
		t.Fatalf("pending duplicate OPEN = %#v", duplicateOpening)
	}
	generated := registry.GenerateCapability(*opening.GenerateCapability)
	if generated.Dial == nil {
		t.Fatalf("capability result = %#v", generated)
	}

	targetConnection := &scriptedTargetConnection{
		readData: []byte("down"),
		writeN:   2,
	}
	resolver := &recordingResolver{addresses: []netip.Addr{netip.MustParseAddr("192.0.2.20")}}
	connector := &recordingConnector{results: []connectResult{{connection: targetConnection}}}
	dialer, err := NewTargetDialExecutor(resolver, connector)
	if err != nil {
		t.Fatal(err)
	}
	connection, dialErr := dialer.Dial(context.Background(), generated.Dial.Target)
	opened := registry.CompleteDial(*generated.Dial, dialErr == nil && connection != nil)
	if dialErr != nil || !opened.Ready || opened.Result.Result != protocol.OpenSuccess || opened.Owner == nil {
		t.Fatalf("dial/open result = connection %v, dial error %v, outcome %#v", connection, dialErr, opened)
	}
	if len(connector.addresses) != 1 {
		t.Fatalf("target dial count = %d, want 1", len(connector.addresses))
	}
	replayedOpen := registry.HandleOpen("client-a", request)
	if !replayedOpen.Ready || replayedOpen.Result != opened.Result || len(connector.addresses) != 1 {
		t.Fatalf("completed duplicate OPEN = %#v, dials %d", replayedOpen, len(connector.addresses))
	}

	relay, err := NewRelay(request.FlowID, policy.Config{
		Mode: opened.Result.DeliveryMode, Selection: opened.Result.PathSelection,
		Constraints: opened.Result.Constraints,
	}, opened.Owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{Kind: RelayStart}); err != nil {
		t.Fatal(err)
	}
	targetIO, err := NewTargetIOExecutor(connection)
	if err != nil {
		t.Fatal(err)
	}

	joins, err := NewJoinCoordinator(registry, 8)
	if err != nil {
		t.Fatal(err)
	}
	session := readyJoinSession(17, "client-a")
	if !limiter.AllowJoin(clock.Now(), "client-a", request.FlowID, true) {
		t.Fatal("first JOIN was rate limited")
	}
	joinOutput, err := joins.Begin(session, protocol.Join{
		FlowID: request.FlowID, Capability: opened.Result.Capability,
	})
	if err != nil {
		t.Fatal(err)
	}
	joinSend := requireJoinSessionAction(t, joinOutput.SessionActions, transport.SessionActionSendFrame)
	joinOutput, err = joins.CompleteResult(joinSend.Correlation, true)
	if err != nil {
		t.Fatal(err)
	}
	relayActions, err := relay.Handle(RelayEvent{Kind: RelayApplyFlowActions, FlowActions: joinOutput.FlowActions})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(relayActions, RelayActionAttachmentPublished) != 1 {
		t.Fatalf("JOIN relay actions = %#v", relayActions)
	}
	completeControlSends(t, relay, relayActions)
	readTarget := requireRelayAction(t, relayActions, RelayActionReadTarget)
	if !registry.UpdateLifecycle(FlowKey{PrincipalID: "client-a", FlowID: request.FlowID}, opened.Owner, flow.Relaying) {
		t.Fatal("registry rejected Relaying lifecycle")
	}

	readEvent, hasResult, err := targetIO.Execute(readTarget)
	if err != nil || !hasResult {
		t.Fatalf("target read execution = %#v, %v, %v", readEvent, hasResult, err)
	}
	relayActions, err = relay.Handle(readEvent)
	if err != nil {
		t.Fatal(err)
	}
	downlink := requireMessageAction[protocol.Data](t, relayActions)
	if got := relayActionMessage(t, downlink).(protocol.Data); got.Offset != 0 || string(got.Bytes) != "down" {
		t.Fatalf("downlink DATA = %#v", got)
	}

	attachment := relay.Snapshot().Flow.Lifecycle.Published[0]
	uplink := protocol.Data{FlowID: request.FlowID, Offset: 0, Bytes: []byte("up")}
	relayActions, err = relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: attachment, Message: uplink,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeTarget := requireRelayAction(t, relayActions, RelayActionWriteTarget)
	completeControlSends(t, relay, relayActions)
	writeEvent, hasResult, err := targetIO.Execute(writeTarget)
	if err != nil || !hasResult {
		t.Fatalf("target write execution = %#v, %v, %v", writeEvent, hasResult, err)
	}
	relayActions, err = relay.Handle(writeEvent)
	if err != nil {
		t.Fatal(err)
	}
	assertACKOffset(t, relayActions, 2)
	completeControlSends(t, relay, relayActions)
	if got := targetConnection.writtenBytes(); string(got) != "up" {
		t.Fatalf("target bytes = %q", got)
	}

	relayActions, err = relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: attachment, Message: uplink,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(relayActions, RelayActionWriteTarget) != 0 {
		t.Fatalf("duplicate DATA wrote target again: %#v", relayActions)
	}
	assertACKOffset(t, relayActions, 2)

	relayActions, err = relay.Handle(RelayEvent{
		Kind: RelayResetRequested, ResetReason: protocol.ResetCancelled,
	})
	if err != nil {
		t.Fatal(err)
	}
	closeTarget := requireRelayAction(t, relayActions, RelayActionCloseTarget)
	if _, hasResult, err := targetIO.Execute(closeTarget); err != nil || hasResult {
		t.Fatalf("target close = hasResult %v, error %v", hasResult, err)
	}
	resetSend := requireMessageAction[protocol.Reset](t, relayActions)
	if !registry.UpdateLifecycle(FlowKey{PrincipalID: "client-a", FlowID: request.FlowID}, opened.Owner, flow.Resetting) {
		t.Fatal("registry rejected Resetting lifecycle")
	}
	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: resetSend.Generation, AttemptOutcome: flow.AttemptSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if relay.Snapshot().Flow.Lifecycle.State != flow.Reset {
		t.Fatalf("relay terminal state = %v", relay.Snapshot().Flow.Lifecycle.State)
	}
	if !registry.MarkTerminal(FlowKey{PrincipalID: "client-a", FlowID: request.FlowID}, opened.Owner, TerminalReset) {
		t.Fatal("registry rejected terminal flow")
	}
	requireOpenFailure(t, registry.HandleOpen("client-a", request), protocol.OpenInternalFailure)
	if len(connector.addresses) != 1 {
		t.Fatalf("terminal replay created %d target dials", len(connector.addresses))
	}
	targetConnection.mu.Lock()
	closeCalls := targetConnection.closes
	targetConnection.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("target close calls = %d, want 1", closeCalls)
	}
	if got := registry.Snapshot(); got.Flows != 0 || got.Tombstones != 1 || got.TargetDialReservations != 0 {
		t.Fatalf("terminal registry usage = %#v", got)
	}
}

func TestServerGate7FixedDeadlinesRemainConsistent(t *testing.T) {
	if OpenDeadline != 15*time.Second || TombstoneLifetime != 60*time.Second {
		t.Fatalf("server deadlines changed: OPEN=%v tombstone=%v", OpenDeadline, TombstoneLifetime)
	}
}
