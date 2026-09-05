package daemon

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
	servercore "github.com/adrianceding/via/internal/server"
	"github.com/adrianceding/via/internal/transport"
)

func TestServerConcurrentAttachmentRepliesSharePathGroup(t *testing.T) {
	for _, firstOpen := range []bool{true, false} {
		for _, secondOpen := range []bool{true, false} {
			for _, failFirst := range []bool{false, true} {
				name := "join"
				if firstOpen {
					name = "open"
				}
				if secondOpen {
					name += "-open"
				} else {
					name += "-join"
				}
				if failFirst {
					name += "-late-failure"
				}
				t.Run(name, func(t *testing.T) {
					harness := newServerRuntimeHarness(t)
					defer harness.close()
					blocked := &blockedAttachmentReplyConnection{
						recordingTransportConnection: harness.connection,
						started:                      make(chan struct{}), release: make(chan struct{}), fail: failFirst,
					}
					defer blocked.unblock()
					harness.session.connection = blocked
					secondConnection := newRecordingTransportConnection(t)
					second, err := newWireSession(harness.daemon.runtimeCtx, 2, secondConnection)
					if err != nil {
						t.Fatal(err)
					}
					defer second.close()
					second.principal, second.pathGroupID = harness.session.principal, harness.session.pathGroupID
					if !harness.daemon.addSession(second) {
						t.Fatal("second lane was not admitted")
					}
					request := protocol.Open{
						FlowID: protocol.FlowID{2}, OpenToken: protocol.OpenToken{3},
						DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest,
						Target: protocol.Target{Address: netip.MustParseAddr("127.0.0.1"), Port: 9},
					}
					instance, result := installServerRuntimeFlow(t, harness, second.principal, request)
					send := func(session *wireSession, open bool) error {
						if open {
							return harness.daemon.handleOpen(session, request)
						}
						return harness.daemon.handleJoin(session, protocol.Join{FlowID: request.FlowID, Capability: result.Capability})
					}
					firstDone := make(chan error, 1)
					go func() { firstDone <- send(harness.session, firstOpen) }()
					select {
					case <-blocked.started:
					case <-time.After(time.Second):
						t.Fatal("first reply did not start")
					}
					secondDone := make(chan error, 1)
					go func() { secondDone <- send(second, secondOpen) }()
					select {
					case err := <-secondDone:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(time.Second):
						t.Fatal("healthy lane waited for stalled reply")
					}
					if secondOpen {
						got, ok := secondConnection.lastOpenResult()
						if !ok || got.Result != protocol.OpenSuccess || !got.ImplicitAttachment {
							t.Fatalf("duplicate OPEN returned %#v, present=%t", got, ok)
						}
					} else if secondConnection.joinSuccesses() != 1 {
						t.Fatal("duplicate JOIN was rejected")
					}
					if got := instance.snapshot().Flow.Lifecycle; len(got.Published) != 1 || len(got.Provisional) != 0 {
						t.Fatalf("healthy lane did not publish exactly one attachment: %#v", got)
					}
					blocked.unblock()
					if err := <-firstDone; failFirst != errors.Is(err, errTestJoinWrite) || !failFirst && err != nil {
						t.Fatalf("first reply error = %v, failFirst=%t", err, failFirst)
					}
					if got := instance.snapshot().Flow.Lifecycle; len(got.Published) != 1 || len(got.Provisional) != 0 {
						t.Fatalf("late result changed published attachment: %#v", got)
					}
					if harness.daemon.nextAttachment.Load() != 1 || harness.daemon.registry.Snapshot().Flows != 2 {
						t.Fatal("duplicate reply allocated an extra attachment or target flow")
					}
				})
			}
		}
	}
}

func TestServerFlowStatusSeparatesPrincipals(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	harness.daemon.principalKeys["client-02"] = auth.Key{2}
	other, _ := installServerRuntimeFlow(t, harness, "client-02", protocol.Open{
		FlowID: harness.flowID, OpenToken: protocol.OpenToken{3},
		DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest,
		Target: protocol.Target{Address: netip.MustParseAddr("198.51.100.9"), Port: 443},
	})
	observer := harness.daemon.statusObserver
	firstKey := runtimeFlowKey{principal: harness.key.PrincipalID, id: harness.flowID}
	secondKey := runtimeFlowKey{principal: other.key.PrincipalID, id: other.key.FlowID}
	observer.recordFlowTraffic(firstKey, 11, 12)
	observer.recordFlowTraffic(secondKey, 21, 22)
	observer.mu.Lock()
	first, second := observer.flows[firstKey], observer.flows[secondKey]
	count, resources := len(observer.flows), observer.resources.Flows
	traffic := observer.flowTraffic[secondKey]
	observer.mu.Unlock()
	if count != 2 || resources != 2 || first.entry.IDHash == second.entry.IDHash || first.entry.FlowID == second.entry.FlowID {
		t.Fatalf("principal flow identities collided: count=%d resources=%d first=%#v second=%#v", count, resources, first, second)
	}
	harness.instance.close()
	observer.mu.Lock()
	remaining, exists := observer.flows[secondKey]
	remainingTraffic := observer.flowTraffic[secondKey]
	count, resources = len(observer.flows), observer.resources.Flows
	observer.mu.Unlock()
	if !exists || count != 1 || resources != 1 || remaining != second || remainingTraffic != traffic {
		t.Fatal("terminating one principal changed the other principal's flow")
	}
}

func TestServerJoinFailureRetainsOtherInFlightReply(t *testing.T) {
	for _, finalSuccess := range []bool{false, true} {
		name := "all-failed"
		if finalSuccess {
			name = "failure-before-success"
		}
		t.Run(name, func(t *testing.T) {
			harness := newServerRuntimeHarness(t)
			defer harness.close()
			first, ok := harness.instance.beginJoin(harness.session)
			if !ok {
				t.Fatal("first reply rejected")
			}
			second, ok := harness.instance.beginJoin(harness.session)
			if !ok || first != second {
				t.Fatal("second reply did not share attachment")
			}
			if harness.instance.completeJoin(harness.session, first, false) {
				t.Fatal("failed provisional write published an attachment")
			}
			harness.session.attachmentsMu.RLock()
			reserved := harness.session.reservations[harness.flowID] == first.attachment
			harness.session.attachmentsMu.RUnlock()
			if !reserved || len(harness.instance.snapshot().Flow.Lifecycle.Provisional) != 1 {
				t.Fatal("failure withdrew another in-flight reply")
			}
			if published := harness.instance.completeJoin(harness.session, second, finalSuccess); published != finalSuccess {
				t.Fatalf("last result publication=%t, success=%t", published, finalSuccess)
			}
			harness.instance.mu.Lock()
			pendingCount := len(harness.instance.joinWrites)
			harness.instance.mu.Unlock()
			harness.session.attachmentsMu.RLock()
			reservationCount := len(harness.session.reservations)
			harness.session.attachmentsMu.RUnlock()
			if pendingCount != 0 || reservationCount != 0 {
				t.Fatalf("completed replies retained records=%d reservations=%d", pendingCount, reservationCount)
			}
		})
	}
}

func TestServerJoinWritesAreBoundedAndLateResultsIgnored(t *testing.T) {
	harness := newServerRuntimeHarness(t)
	defer harness.close()
	var pending *serverJoinWrites
	for range maxConcurrentJoinWrites {
		var ok bool
		pending, ok = harness.instance.beginJoin(harness.session)
		if !ok {
			t.Fatal("reply rejected below the I/O limit")
		}
	}
	if _, ok := harness.instance.beginJoin(harness.session); ok {
		t.Fatal("reply count exceeded its hard limit")
	}
	for range maxConcurrentJoinWrites {
		harness.instance.completeJoin(harness.session, pending, false)
	}
	replacement, ok := harness.instance.beginJoin(harness.session)
	if !ok || replacement.attachment == pending.attachment || replacement.generation == pending.generation {
		t.Fatal("failed transaction could not start a fresh generation")
	}
	if harness.instance.completeJoin(harness.session, pending, true) || replacement.inFlight != 1 {
		t.Fatal("late result affected the replacement transaction")
	}
	harness.instance.close()
	if _, ok := harness.instance.beginJoin(harness.session); ok {
		t.Fatal("terminal flow admitted a reply")
	}
	if harness.instance.completeJoin(harness.session, replacement, true) {
		t.Fatal("terminal flow published a late successful reply")
	}
	if got := harness.instance.snapshot().Flow.Lifecycle; got.State != flow.Reset || len(got.Published) != 0 || len(got.Provisional) != 0 {
		t.Fatalf("terminal join state = %#v", got)
	}
}

func installServerRuntimeFlow(t *testing.T, harness *serverRuntimeHarness, principal string, request protocol.Open) (*serverFlow, protocol.OpenResult) {
	t.Helper()
	outcome := harness.daemon.registry.HandleOpen(principal, request)
	if outcome.GenerateCapability == nil {
		t.Fatal("missing capability generation")
	}
	outcome = harness.daemon.registry.GenerateCapability(*outcome.GenerateCapability)
	if outcome.Dial == nil {
		t.Fatal("missing target dial")
	}
	outcome = harness.daemon.registry.CompleteDial(*outcome.Dial, true)
	if !outcome.Ready || outcome.Owner == nil || outcome.Result.Result != protocol.OpenSuccess {
		t.Fatal("registry flow did not open")
	}
	target, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	key := servercore.FlowKey{PrincipalID: principal, FlowID: request.FlowID}
	instance, err := newServerFlow(harness.daemon, key, request.FlowID, request.Target, request.DeliveryMode, request.PathSelection, request.Constraints, outcome.Owner, target)
	if err != nil {
		t.Fatal(err)
	}
	if !harness.daemon.addFlow(key, instance) {
		t.Fatal("server flow was not installed")
	}
	if err := instance.start(); err != nil {
		t.Fatal(err)
	}
	instance.activateStatus()
	return instance, outcome.Result
}

type blockedAttachmentReplyConnection struct {
	*recordingTransportConnection
	started     chan struct{}
	release     chan struct{}
	blockOnce   sync.Once
	releaseOnce sync.Once
	fail        bool
}

func (connection *blockedAttachmentReplyConnection) unblock() {
	connection.releaseOnce.Do(func() { close(connection.release) })
}

func (connection *blockedAttachmentReplyConnection) WriteFrame(ctx context.Context, request transport.WriteRequest) error {
	_, message, err := protocol.DecodeEncodedFrame(request.Encoded)
	if err != nil {
		return err
	}
	switch message.(type) {
	case protocol.OpenResult, protocol.JoinResult:
		connection.blockOnce.Do(func() {
			close(connection.started)
			select {
			case <-connection.release:
			case <-ctx.Done():
			}
		})
		if connection.fail {
			return errTestJoinWrite
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return connection.recordingTransportConnection.WriteFrame(ctx, request)
}
