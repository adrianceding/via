package daemon

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"testing"
	"time"

	clientcore "github.com/adrianceding/via/internal/client"
	"github.com/adrianceding/via/internal/config"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	servercore "github.com/adrianceding/via/internal/server"
	statusapi "github.com/adrianceding/via/internal/status"
)

func TestRuntimeStatusMapsLocalIOFailuresWithoutTreatingEOFAsFailure(t *testing.T) {
	localFailure := errors.New("local I/O failure")
	serverCases := []struct {
		name  string
		event servercore.RelayEvent
		err   error
		want  statusapi.TransitionReason
	}{
		{name: "read", event: servercore.RelayEvent{Kind: servercore.RelayTargetReadResult, Err: localFailure}, want: statusapi.ReasonLocalIOFailure},
		{name: "invalid read result", event: servercore.RelayEvent{Kind: servercore.RelayTargetReadResult}, err: localFailure, want: statusapi.ReasonLocalIOFailure},
		{name: "read eof", event: servercore.RelayEvent{Kind: servercore.RelayTargetReadResult, Err: io.EOF}, want: statusapi.ReasonNone},
		{name: "write", event: servercore.RelayEvent{Kind: servercore.RelayTargetWriteResult, Err: localFailure}, want: statusapi.ReasonLocalIOFailure},
		{name: "close write", event: servercore.RelayEvent{Kind: servercore.RelayTargetCloseWriteResult, Err: localFailure}, want: statusapi.ReasonLocalIOFailure},
	}
	for _, testCase := range serverCases {
		t.Run("server-"+testCase.name, func(t *testing.T) {
			if got := statusReasonForRelayEvent(testCase.event, testCase.err); got != testCase.want {
				t.Fatalf("reason = %d, want %d", got, testCase.want)
			}
		})
	}

	clientCases := []struct {
		name  string
		event clientcore.ApplicationRelayEvent
		want  statusapi.TransitionReason
	}{
		{name: "read", event: clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayReadResult, Err: localFailure}, want: statusapi.ReasonLocalIOFailure},
		{name: "read eof", event: clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayReadResult, Err: io.EOF}, want: statusapi.ReasonNone},
		{name: "write", event: clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayWriteResult, Err: localFailure}, want: statusapi.ReasonLocalIOFailure},
		{name: "close write", event: clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayCloseWriteResult, Err: localFailure}, want: statusapi.ReasonLocalIOFailure},
		{name: "remote reset", event: clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayRemoteMessage, Message: protocol.Reset{}}, want: statusapi.ReasonRemoteReset},
		{name: "normal close", event: clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayClosingDeadline}, want: statusapi.ReasonCompleted},
	}
	for _, testCase := range clientCases {
		t.Run("client-"+testCase.name, func(t *testing.T) {
			if got := clientRelayReason(testCase.event); got != testCase.want {
				t.Fatalf("reason = %d, want %d", got, testCase.want)
			}
		})
	}
	if got := clientStatusReasonForReset(protocol.ResetInternalFailure); got != statusapi.ReasonInternalFailure {
		t.Fatalf("client internal reset reason = %d, want %d", got, statusapi.ReasonInternalFailure)
	}
	if got := statusReasonForReset(protocol.ResetInternalFailure); got != statusapi.ReasonInternalFailure {
		t.Fatalf("server internal reset reason = %d, want %d", got, statusapi.ReasonInternalFailure)
	}
}

func TestClientStatusReasonUsesResetActionCause(t *testing.T) {
	actions := []clientcore.ApplicationRelayAction{{Message: protocol.Reset{Reason: protocol.ResetResourceLimit}}}
	if got := clientStatusReasonFromRelayActions(actions); got != statusapi.ReasonResourceLimit {
		t.Fatalf("reset action reason = %d, want %d", got, statusapi.ReasonResourceLimit)
	}
	if got := clientStatusReasonFromRelayActions(nil); got != statusapi.ReasonNone {
		t.Fatalf("empty actions reason = %d", got)
	}
}

func TestServerStatusReasonUsesResetActionCause(t *testing.T) {
	actions := []servercore.RelayAction{{Message: protocol.Reset{Reason: protocol.ResetResourceLimit}}}
	if got := serverStatusReasonFromRelayActions(actions); got != statusapi.ReasonResourceLimit {
		t.Fatalf("reset action reason = %d, want %d", got, statusapi.ReasonResourceLimit)
	}
	if got := serverStatusReasonFromRelayActions(nil); got != statusapi.ReasonNone {
		t.Fatalf("empty actions reason = %d", got)
	}
}

func TestRemoteProtocolDiagnosticCategoriesAreFixed(t *testing.T) {
	if got := remoteMessageCategory(protocol.ACK{}); got != "acknowledgement" {
		t.Fatalf("ACK category = %q", got)
	}
	if got := flowErrorCategory(errors.Join(flow.ErrACKNotCanonical, errors.New("detail"))); got != "non-canonical acknowledgement ranges" {
		t.Fatalf("ACK error category = %q", got)
	}
	if got := flowErrorCategory(errors.Join(servercore.ErrRelaySendLimit, errors.New("detail"))); got != "pending send action limit exceeded" {
		t.Fatalf("send limit category = %q", got)
	}
	if got := flowErrorCategory(policy.ErrInvalidSample); got != "invalid path quality sample" {
		t.Fatalf("quality error category = %q", got)
	}
	if got := flowErrorCategory(errors.New("unclassified")); got != "internal error" {
		t.Fatalf("fallback error category = %q", got)
	}
}

func TestClientRelayDiagnosticCategoriesAreFixed(t *testing.T) {
	event := clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayRemoteMessage, Message: protocol.FINACK{}}
	if got := clientRelayEventCategory(event); got != "remote finish acknowledgement message" {
		t.Fatalf("client event category = %q", got)
	}
	if got := clientRelayErrorCategory(errors.Join(clientcore.ErrApplicationRelaySendLimit, errors.New("detail"))); got != "pending send action limit exceeded" {
		t.Fatalf("client limit category = %q", got)
	}
	if got := clientRelayErrorCategory(flow.ErrDataConflict); got != "overlapping data conflict" {
		t.Fatalf("client flow category = %q", got)
	}
}

func TestServerFlowResettingPreservesOriginalTransitionReason(t *testing.T) {
	if reason := stableServerRelayReason(flow.Resetting, statusapi.ReasonProtocolConflict); reason != statusapi.ReasonNone {
		t.Fatalf("resetting late reason = %d", reason)
	}
	if reason := stableServerRelayReason(flow.Relaying, statusapi.ReasonProtocolConflict); reason != statusapi.ReasonProtocolConflict {
		t.Fatalf("relaying protocol reason = %d", reason)
	}
}

func TestClientFlowTerminalStatusPreservesFirstFailureReason(t *testing.T) {
	instance, repository := newClientStatusReasonFixture(t, 0x91)
	event := clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelayResetRequested, ResetReason: protocol.ResetLocalIOFailure,
	}
	if _, err := instance.relay.Handle(event); err != nil {
		t.Fatal(err)
	}
	instance.publishStatus(clientRelayReason(event))
	if instance.lastReason != statusapi.ReasonLocalIOFailure {
		t.Fatalf("first terminal reason = %d, want %d", instance.lastReason, statusapi.ReasonLocalIOFailure)
	}

	resetGeneration := instance.relay.Snapshot().Flow.Lifecycle.ResetDeadlineGeneration
	if _, err := instance.relay.Handle(clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelayResetDeadline, Generation: resetGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	instance.publishStatus(statusapi.ReasonNone)
	waitFor(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Terminals) == 1 && snapshot.Terminals[0].State == statusapi.FlowReset &&
			snapshot.Terminals[0].Reason == statusapi.ReasonLocalIOFailure
	}, "client terminal reason preservation")
}

func TestClientFlowNormalCloseStatusIsCompleted(t *testing.T) {
	instance, repository := newClientStatusReasonFixture(t, 0x92)
	if _, err := instance.machine.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle, Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleDirectionsComplete},
	}); err != nil {
		t.Fatal(err)
	}
	closingGeneration := instance.relay.Snapshot().Flow.Lifecycle.ClosingDeadlineGeneration
	event := clientcore.ApplicationRelayEvent{
		Kind: clientcore.ApplicationRelayClosingDeadline, Generation: closingGeneration,
	}
	if _, err := instance.relay.Handle(event); err != nil {
		t.Fatal(err)
	}
	instance.publishStatus(statusapi.ReasonNone)
	waitFor(t, time.Second, func() bool {
		snapshot := repository.Snapshot()
		return len(snapshot.Terminals) == 1 && snapshot.Terminals[0].State == statusapi.FlowClosed &&
			snapshot.Terminals[0].Reason == statusapi.ReasonCompleted
	}, "client normal close status")
}

func newClientStatusReasonFixture(t *testing.T, flowByte byte) (*clientFlow, *statusapi.Repository) {
	t.Helper()
	repository, err := statusapi.NewRepository(statusapi.Limits{Interfaces: 1, Sessions: 1, Flows: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- repository.Run(ctx, nil) }()
	t.Cleanup(func() {
		cancel()
		if runErr := <-done; runErr != nil {
			t.Errorf("status repository: %v", runErr)
		}
	})

	var hashKey [32]byte
	hashKey[0] = flowByte
	observer, err := newRuntimeStatusWithKey(repository, 1, 1, hashKey)
	if err != nil {
		t.Fatal(err)
	}
	flowID := protocol.FlowID{flowByte}
	machine := flow.NewFlow()
	relay, err := clientcore.NewApplicationRelay(flowID, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, machine)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(clientcore.ApplicationRelayEvent{Kind: clientcore.ApplicationRelayStart}); err != nil {
		t.Fatal(err)
	}
	instance := &clientFlow{
		host: &clientDaemon{
			configuration: config.Client{Delivery: config.Delivery{
				Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
			}},
			statusObserver: observer,
		},
		machine: machine, relay: relay, flowID: flowID,
		target: protocol.Target{Address: netip.MustParseAddr("192.0.2.91"), Port: 443},
	}
	instance.publishStatus(statusapi.ReasonStarted)
	return instance, repository
}
