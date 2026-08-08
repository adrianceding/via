package client

import (
	"errors"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/server"
)

var (
	testClientFlowID     = protocol.FlowID{0x11}
	testClientOpenToken  = protocol.OpenToken{0x22}
	testClientCapability = protocol.Capability{0x33}
)

func TestOpenJoinConfigurationAndStartBoundary(t *testing.T) {
	valid := []OpenJoinSpec{
		{Target: testClientTarget(), DeliveryMode: protocol.DeliveryRedundant, PathSelection: protocol.PathNone},
		{Target: testClientTarget(), DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest},
		{
			Target: testClientTarget(), DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathDistributed,
			Constraints: protocol.DeliveryConstraints{Fallback: protocol.DeliveryFallbackFastest},
		},
	}
	for _, spec := range valid {
		if _, err := NewOpenJoinCoordinator(spec); err != nil {
			t.Fatalf("valid configuration %#v: %v", spec, err)
		}
	}
	invalid := []OpenJoinSpec{
		{},
		{Target: protocol.Target{Address: netip.MustParseAddr("192.0.2.1")}, DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest},
		{Target: testClientTarget(), DeliveryMode: protocol.DeliveryRedundant, PathSelection: protocol.PathFastest},
		{Target: testClientTarget(), DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathNone},
		{
			Target: testClientTarget(), DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest,
			Constraints: protocol.DeliveryConstraints{MaxDelayGap: time.Second},
		},
		{Target: testClientTarget(), DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathDistributed},
	}
	for _, spec := range invalid {
		if _, err := NewOpenJoinCoordinator(spec); !errors.Is(err, ErrInvalidOpenJoin) {
			t.Fatalf("invalid configuration %#v error = %v", spec, err)
		}
	}

	coordinator := newTestOpenJoin(t)
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
	identity := requireOpenJoinAction(t, actions, OpenJoinActionGenerateIdentity)
	deadline := requireOpenJoinAction(t, actions, OpenJoinActionArmOpenDeadline)
	if identity.Generation == 0 || deadline.Generation == 0 || identity.Generation == deadline.Generation || deadline.Duration != OpenDeadline {
		t.Fatalf("start actions = %#v", actions)
	}
	snapshot := coordinator.Snapshot()
	if snapshot.State != OpenJoinGeneratingIdentity || snapshot.IdentityGeneration != identity.Generation || snapshot.OpenDeadlineGeneration != deadline.Generation {
		t.Fatalf("start snapshot = %#v", snapshot)
	}
	if repeated := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart}); len(repeated) != 0 {
		t.Fatalf("duplicate start actions = %#v", repeated)
	}
}

func TestOpenJoinSendsConstraintsAndRejectsAlteredEcho(t *testing.T) {
	constraints := protocol.DeliveryConstraints{
		MaxDeliveryDelay: 80 * time.Millisecond,
		MaxDelayGap:      30 * time.Millisecond,
		Fallback:         protocol.DeliveryFallbackPause,
	}
	coordinator, err := NewOpenJoinCoordinator(OpenJoinSpec{
		Target:        testClientTarget(),
		DeliveryMode:  protocol.DeliveryAdaptive,
		PathSelection: protocol.PathDistributed,
		Constraints:   constraints,
	})
	if err != nil {
		t.Fatal(err)
	}
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	open, _ := beginTestOpen(t, coordinator)
	if open.Open.Constraints != constraints {
		t.Fatalf("OPEN constraints = %#v, want %#v", open.Open.Constraints, constraints)
	}

	result := protocol.OpenResult{
		FlowID:        testClientFlowID,
		Result:        protocol.OpenSuccess,
		Capability:    testClientCapability,
		DeliveryMode:  protocol.DeliveryAdaptive,
		PathSelection: protocol.PathDistributed,
		Constraints:   constraints,
	}
	result.Constraints.MaxDelayGap += protocol.MinimumDeliveryConstraint
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind:              OpenJoinOpenResultReceived,
		Generation:        open.Generation,
		SessionGeneration: open.SessionGeneration,
		OpenResult:        result,
	})
	failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow)
	if failure.Failure != OpenJoinFailureProtocol || coordinator.Snapshot().State != OpenJoinFailed {
		t.Fatalf("altered constraint echo = %#v / %#v", failure, coordinator.Snapshot())
	}
}

func TestOpenJoinRacesFasterReconnectedSession(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinSessionReady, SessionGeneration: 1, SessionRTT: 350 * time.Millisecond,
	})
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinSessionReady, SessionGeneration: 3, SessionRTT: 170 * time.Millisecond,
	})

	_, opens := beginTestOpens(t, coordinator)
	if got := openJoinSessionGenerations(opens); !slices.Equal(got, []uint64{1, 3}) {
		t.Fatalf("OPEN sessions = %#v, want all ready sessions", got)
	}
}

func TestOpenJoinSessionQualityDoesNotExcludeAttachmentEstablishment(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinSessionReady, SessionGeneration: 1, SessionRTT: 170 * time.Millisecond,
	})
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinSessionReady, SessionGeneration: 2, SessionRTT: 350 * time.Millisecond,
	})
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinSessionQuality, SessionGeneration: 2, SessionRTT: 120 * time.Millisecond,
	})

	_, opens := beginTestOpens(t, coordinator)
	if got := openJoinSessionGenerations(opens); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("OPEN sessions = %#v, want all ready sessions", got)
	}
}

func TestOpenJoinRacesStalledSessionAsBoundedFallback(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinSessionReady, SessionGeneration: 1,
		SessionRTT: 100 * time.Millisecond, SessionStall: 3 * time.Second,
	})
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinSessionReady, SessionGeneration: 2, SessionRTT: 170 * time.Millisecond,
	})

	_, opens := beginTestOpens(t, coordinator)
	if got := openJoinSessionGenerations(opens); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("OPEN sessions = %#v, want stalled path retained as establishment fallback", got)
	}
}

func TestOpenJoinRejectsZeroOrFailedIdentityAndIgnoresLateIdentity(t *testing.T) {
	tests := []struct {
		name  string
		event func(uint64) OpenJoinEvent
	}{
		{
			name: "zero FlowID",
			event: func(generation uint64) OpenJoinEvent {
				return OpenJoinEvent{Kind: OpenJoinIdentityGenerated, Generation: generation, OpenToken: testClientOpenToken}
			},
		},
		{
			name: "zero OpenToken",
			event: func(generation uint64) OpenJoinEvent {
				return OpenJoinEvent{Kind: OpenJoinIdentityGenerated, Generation: generation, FlowID: testClientFlowID}
			},
		},
		{
			name: "random source failure",
			event: func(generation uint64) OpenJoinEvent {
				return OpenJoinEvent{Kind: OpenJoinIdentityFailed, Generation: generation}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator := newTestOpenJoin(t)
			start := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
			identity := requireOpenJoinAction(t, start, OpenJoinActionGenerateIdentity)
			if stale := handleOpenJoin(t, coordinator, test.event(identity.Generation+100)); len(stale) != 0 {
				t.Fatalf("late random result actions = %#v", stale)
			}
			actions := handleOpenJoin(t, coordinator, test.event(identity.Generation))
			requireOpenJoinAction(t, actions, OpenJoinActionCancelOpenDeadline)
			failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow)
			if failure.Failure != OpenJoinFailureEntropy || coordinator.Snapshot().State != OpenJoinFailed {
				t.Fatalf("random failure = %#v / %#v", failure, coordinator.Snapshot())
			}
			if late := handleOpenJoin(t, coordinator, validIdentityEvent(identity.Generation)); len(late) != 0 {
				t.Fatalf("late result actions in terminal state = %#v", late)
			}
		})
	}
}

func TestOpenJoinRetriesAllPendingSessionsAndSurvivesOneLoss(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 20})
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 10})
	openDeadline, opens := beginTestOpens(t, coordinator)
	if got := openJoinSessionGenerations(opens); !slices.Equal(got, []uint64{10, 20}) {
		t.Fatalf("initial concurrent OPEN sessions = %#v", got)
	}
	ready := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 5})
	newOpen := requireOpenJoinAction(t, ready, OpenJoinActionSendOpen)
	if newOpen.SessionGeneration != 5 || hasOpenJoinAction(ready, OpenJoinActionArmOpenRetry) {
		t.Fatalf("new session OPEN = %#v", ready)
	}
	retryGeneration := coordinator.Snapshot().OpenRetryGeneration
	retried := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinOpenRetryDue, Generation: retryGeneration})
	retries := openJoinActions(retried, OpenJoinActionSendOpen)
	if got := openJoinSessionGenerations(retries); !slices.Equal(got, []uint64{5, 10, 20}) {
		t.Fatalf("full OPEN retry session set = %#v", got)
	}
	if coordinator.Snapshot().OpenDeadlineGeneration != openDeadline {
		t.Fatal("OPEN retry refreshed the absolute deadline")
	}

	lostAttempt, ok := coordinator.PendingAttempt(5, OpenJoinAttemptOpen)
	if !ok {
		t.Fatal("session 5 OPEN attempt missing after retry")
	}
	if actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionLost, SessionGeneration: 5}); len(actions) != 0 {
		t.Fatalf("single-session loss affected other concurrent OPEN attempts: %#v", actions)
	}
	if actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: lostAttempt.Generation, SessionGeneration: 5,
		OpenResult: successfulTestOpenResult(),
	}); len(actions) != 0 {
		t.Fatalf("stale OPEN result actions = %#v", actions)
	}
	if _, ok := coordinator.PendingAttempt(10, OpenJoinAttemptOpen); !ok {
		t.Fatal("single-session loss cleared another OPEN attempt")
	}
}

func TestOpenJoinJoinRetryRotatesToOtherReadySession(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 2})
	first := completeTestOpen(t, coordinator)
	if first.SessionGeneration != 1 {
		t.Fatalf("first JOIN session = %d", first.SessionGeneration)
	}
	retryGeneration := coordinator.Snapshot().JoinRetryGeneration
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinJoinRetryDue, Generation: retryGeneration})
	requireOpenJoinAction(t, actions, OpenJoinActionReleaseAttachment)
	replacement := requireOpenJoinAction(t, actions, OpenJoinActionSendJoin)
	if replacement.SessionGeneration != 2 || replacement.Join != first.Join {
		t.Fatalf("rotated JOIN = %#v, first = %#v", replacement, first)
	}
}

func TestOpenJoinOpenRetriesAreBoundedWithoutRefreshingDeadline(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 7})
	_, deadline := beginTestOpen(t, coordinator)
	for coordinator.Snapshot().OpenAttempts < MaxOpenAttempts {
		snapshot := coordinator.Snapshot()
		if snapshot.OpenRetryGeneration == 0 {
			t.Fatalf("retry timer missing after OPEN attempt %d", snapshot.OpenAttempts)
		}
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinOpenRetryDue, Generation: snapshot.OpenRetryGeneration})
		requireOpenJoinAction(t, actions, OpenJoinActionSendOpen)
		if coordinator.Snapshot().OpenDeadlineGeneration != deadline {
			t.Fatal("OPEN retry changed the absolute deadline generation")
		}
	}
	snapshot := coordinator.Snapshot()
	if snapshot.OpenAttempts != MaxOpenAttempts || snapshot.OpenRetryGeneration == 0 || snapshot.State != OpenJoinOpening {
		t.Fatalf("OPEN retry-limit snapshot = %#v", snapshot)
	}
	final := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenRetryDue, Generation: snapshot.OpenRetryGeneration,
	})
	if hasOpenJoinAction(final, OpenJoinActionSendOpen) {
		t.Fatalf("network action emitted after OPEN retry limit = %#v", final)
	}
	if snapshot = coordinator.Snapshot(); snapshot.OpenRetryGeneration != 0 ||
		snapshot.OpenAttempts != MaxOpenAttempts || snapshot.State != OpenJoinOpening {
		t.Fatalf("OPEN tail-wait snapshot = %#v", snapshot)
	}
	if actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinOpenRetryDue, Generation: 999}); len(actions) != 0 {
		t.Fatalf("out-of-limit retry actions = %#v", actions)
	}
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinOpenDeadlineReached, Generation: deadline})
	failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow)
	if failure.Failure != OpenJoinFailureOpenDeadline || coordinator.Snapshot().State != OpenJoinFailed {
		t.Fatalf("OPEN deadline failure = %#v / %#v", failure, coordinator.Snapshot())
	}
}

func TestOpenJoinStrictlyValidatesCurrentOpenResult(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.OpenResult)
	}{
		{name: "FlowID mismatch", mutate: func(result *protocol.OpenResult) { result.FlowID[0] ^= 0xff }},
		{name: "zero capability", mutate: func(result *protocol.OpenResult) { result.Capability = protocol.Capability{} }},
		{name: "delivery mode mismatch", mutate: func(result *protocol.OpenResult) { result.DeliveryMode = protocol.DeliveryRedundant }},
		{name: "path selection mismatch", mutate: func(result *protocol.OpenResult) { result.PathSelection = protocol.PathDistributed }},
		{name: "invalid result enum", mutate: func(result *protocol.OpenResult) { result.Result = 99 }},
		{name: "failed result carries delivery constraints", mutate: func(result *protocol.OpenResult) {
			result.Result = protocol.OpenConnectFailed
			result.Capability = protocol.Capability{}
			result.DeliveryMode = 0
			result.PathSelection = 0
			result.Constraints = protocol.DeliveryConstraints{Fallback: protocol.DeliveryFallbackFastest}
		}},
		{name: "failed result claims implicit attachment", mutate: func(result *protocol.OpenResult) {
			result.Result = protocol.OpenConnectFailed
			result.Capability = protocol.Capability{}
			result.DeliveryMode = 0
			result.PathSelection = 0
			result.ImplicitAttachment = true
		}},
		{name: "failed result carries capability", mutate: func(result *protocol.OpenResult) {
			result.Result = protocol.OpenConnectFailed
			result.DeliveryMode = 0
			result.PathSelection = 0
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator := newTestOpenJoin(t)
			handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
			open, _ := beginTestOpen(t, coordinator)
			result := successfulTestOpenResult()
			test.mutate(&result)
			actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
				Kind: OpenJoinOpenResultReceived, Generation: open.Generation, SessionGeneration: open.SessionGeneration, OpenResult: result,
			})
			failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow)
			if failure.Failure != OpenJoinFailureProtocol || coordinator.Snapshot().State != OpenJoinFailed {
				t.Fatalf("strict validation failure = %#v / %#v", failure, coordinator.Snapshot())
			}
		})
	}

	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	open, _ := beginTestOpen(t, coordinator)
	rejected := protocol.OpenResult{FlowID: testClientFlowID, Result: protocol.OpenConnectFailed}
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: open.Generation, SessionGeneration: 1, OpenResult: rejected,
	})
	failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow)
	if failure.Failure != OpenJoinFailureOpenRejected || failure.OpenResult != protocol.OpenConnectFailed {
		t.Fatalf("OPEN application rejection = %#v", failure)
	}
}

func TestAdaptiveOpenImplicitAttachmentSkipsJoinCycle(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	open, _ := beginTestOpen(t, coordinator)
	result := successfulTestOpenResult()
	result.ImplicitAttachment = true
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: open.Generation, SessionGeneration: open.SessionGeneration, OpenResult: result,
	})
	if hasOpenJoinAction(actions, OpenJoinActionFailFlow) {
		t.Fatalf("adaptive implicit attachment rejected = %#v", actions)
	}
	reserve := requireOpenJoinAction(t, actions, OpenJoinActionReserveAttachment)
	publish := requireOpenJoinAction(t, actions, OpenJoinActionPublishAttachment)
	if reserve.SessionGeneration != 1 || publish.SessionGeneration != 1 {
		t.Fatalf("implicit attachment used wrong session = %#v / %#v", reserve, publish)
	}
	if hasOpenJoinAction(actions, OpenJoinActionSendJoin) {
		t.Fatalf("implicit attachment unexpectedly sent JOIN = %#v", actions)
	}
	snapshot := coordinator.Snapshot()
	if snapshot.State != OpenJoinPublishingJoin {
		t.Fatalf("state after implicit attachment = %d, want OpenJoinPublishingJoin", snapshot.State)
	}
}

func TestAdaptiveOpenRacesAllReadySessionsAndPublishesEachAttachment(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	for generation := uint64(1); generation <= 3; generation++ {
		handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinSessionReady, SessionGeneration: generation,
		})
	}
	start := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
	identity := requireOpenJoinAction(t, start, OpenJoinActionGenerateIdentity)
	opening := handleOpenJoin(t, coordinator, validIdentityEvent(identity.Generation))
	opens := openJoinActions(opening, OpenJoinActionSendOpen)
	retry := requireOpenJoinAction(t, opening, OpenJoinActionArmOpenRetry)
	if len(opens) != 3 || retry.Duration != OpenRetryInterval {
		t.Fatalf("adaptive concurrent OPEN actions = %#v", opening)
	}

	for _, open := range opens {
		result := successfulTestOpenResult()
		result.ImplicitAttachment = true
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinOpenResultReceived, Generation: open.Generation,
			SessionGeneration: open.SessionGeneration, OpenResult: result,
		})
		if hasOpenJoinAction(actions, OpenJoinActionSendJoin) {
			t.Fatalf("implicit attachment for session %d sent JOIN: %#v", open.SessionGeneration, actions)
		}
		publish := requireOpenJoinAction(t, actions, OpenJoinActionPublishAttachment)
		completed := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinAttachmentPublished, Generation: publish.Generation,
			SessionGeneration: publish.SessionGeneration,
		})
		if open.SessionGeneration == opens[len(opens)-1].SessionGeneration {
			if !hasOpenJoinAction(completed, OpenJoinActionReplyApplicationSuccess) {
				t.Fatalf("last adaptive attachment did not enable the application: %#v", completed)
			}
		} else if hasOpenJoinAction(completed, OpenJoinActionReplyApplicationSuccess) {
			t.Fatalf("session %d enabled the application before all paths were ready: %#v", open.SessionGeneration, completed)
		}
	}

	if snapshot := coordinator.Snapshot(); snapshot.State != OpenJoinActive ||
		len(snapshot.Attachments) != len(opens) || !snapshot.ApplicationAccepted {
		t.Fatalf("snapshot after all adaptive paths attached = %#v", snapshot)
	}
}

func TestAdaptiveOpenFailureDoesNotBlockHealthyImplicitAttachment(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	for generation := uint64(1); generation <= 2; generation++ {
		handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinSessionReady, SessionGeneration: generation,
		})
	}
	_, opens := beginTestOpens(t, coordinator)
	if len(opens) != 2 {
		t.Fatalf("adaptive concurrent OPEN attempts = %#v", opens)
	}

	failed := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: opens[0].Generation,
		SessionGeneration: opens[0].SessionGeneration,
		OpenResult:        protocol.OpenResult{FlowID: testClientFlowID, Result: protocol.OpenInternalFailure},
	})
	if len(failed) != 0 || coordinator.Snapshot().State != OpenJoinOpening {
		t.Fatalf("single OPEN failure blocked concurrent establishment = %#v / %#v", failed, coordinator.Snapshot())
	}

	result := successfulTestOpenResult()
	result.ImplicitAttachment = true
	healthy := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: opens[1].Generation,
		SessionGeneration: opens[1].SessionGeneration, OpenResult: result,
	})
	if hasOpenJoinAction(healthy, OpenJoinActionFailFlow) {
		t.Fatalf("healthy OPEN terminated by failed path = %#v", healthy)
	}
	if joins := openJoinActions(healthy, OpenJoinActionSendJoin); len(joins) != 1 ||
		joins[0].SessionGeneration != opens[0].SessionGeneration {
		t.Fatalf("failed path did not send JOIN after capability became available = %#v", healthy)
	}
	publish := requireOpenJoinAction(t, healthy, OpenJoinActionPublishAttachment)
	if publish.SessionGeneration != opens[1].SessionGeneration {
		t.Fatalf("healthy implicit attachment = %#v", publish)
	}
	completed := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: publish.Generation,
		SessionGeneration: publish.SessionGeneration,
	})
	if !hasOpenJoinAction(completed, OpenJoinActionReplyApplicationSuccess) ||
		!coordinator.Snapshot().ApplicationAccepted {
		t.Fatalf("healthy attachment did not enable the application = %#v / %#v", completed, coordinator.Snapshot())
	}
}

func TestAdaptivePendingOpenStopsAfterRetryLimitWithoutLeavingActive(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	for generation := uint64(1); generation <= 2; generation++ {
		handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinSessionReady, SessionGeneration: generation,
		})
	}
	_, opens := beginTestOpens(t, coordinator)
	result := successfulTestOpenResult()
	result.ImplicitAttachment = true
	healthy := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: opens[0].Generation,
		SessionGeneration: opens[0].SessionGeneration, OpenResult: result,
	})
	publish := requireOpenJoinAction(t, healthy, OpenJoinActionPublishAttachment)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: publish.Generation,
		SessionGeneration: publish.SessionGeneration,
	})

	for coordinator.Snapshot().OpenAttempts < MaxOpenAttempts {
		snapshot := coordinator.Snapshot()
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinOpenRetryDue, Generation: snapshot.OpenRetryGeneration,
		})
		retries := openJoinActions(actions, OpenJoinActionSendOpen)
		if len(retries) != 1 || retries[0].SessionGeneration != opens[1].SessionGeneration {
			t.Fatalf("unresponsive path retry = %#v", actions)
		}
		if coordinator.Snapshot().State != OpenJoinActive {
			t.Fatalf("optional OPEN retry left Active = %#v", coordinator.Snapshot())
		}
	}
	finalGeneration := coordinator.Snapshot().OpenRetryGeneration
	final := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenRetryDue, Generation: finalGeneration,
	})
	if hasOpenJoinAction(final, OpenJoinActionSendOpen) {
		t.Fatalf("OPEN send after retry limit = %#v", final)
	}
	if _, ok := coordinator.PendingAttempt(opens[1].SessionGeneration, OpenJoinAttemptOpen); ok {
		t.Fatal("pending OPEN attempt remains after retry limit")
	}
	if snapshot := coordinator.Snapshot(); snapshot.State != OpenJoinActive || snapshot.OpenRetryGeneration != 0 {
		t.Fatalf("snapshot after OPEN tail wait = %#v", snapshot)
	}

	ready := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 3})
	join := requireOpenJoinAction(t, ready, OpenJoinActionSendJoin)
	if join.SessionGeneration != 3 {
		t.Fatalf("new session JOIN = %#v", join)
	}
}

func TestOpenJoinPublishesBeforeApplicationSuccessAndAddsEveryReadySession(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 2})
	join := completeTestOpen(t, coordinator)
	if join.SessionGeneration != 1 || join.Join.FlowID != testClientFlowID || join.Join.Capability != testClientCapability {
		t.Fatalf("first JOIN = %#v", join)
	}
	if snapshot := coordinator.Snapshot(); snapshot.ApplicationAccepted {
		t.Fatalf("application enabled before JOIN = %#v", snapshot)
	}

	publish := acceptCurrentJoinResult(t, coordinator)
	if hasOpenJoinAction(publish.actions, OpenJoinActionReplyApplicationSuccess) {
		t.Fatalf("application enabled before publication completed = %#v", publish.actions)
	}
	completed := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: publish.action.Generation, SessionGeneration: publish.action.SessionGeneration,
	})
	if actionIndex(completed, OpenJoinActionReplyApplicationSuccess) < 0 {
		t.Fatalf("first publication completion did not report SOCKS success = %#v", completed)
	}
	secondJoin := requireOpenJoinAction(t, completed, OpenJoinActionSendJoin)
	if secondJoin.SessionGeneration != 2 {
		t.Fatalf("second JOIN = %#v", secondJoin)
	}
	snapshot := coordinator.Snapshot()
	if !snapshot.ApplicationAccepted || len(snapshot.Attachments) != 1 || snapshot.State != OpenJoinJoining {
		t.Fatalf("first publication snapshot = %#v", snapshot)
	}

	secondPublish := acceptCurrentJoinResult(t, coordinator)
	secondCompleted := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: secondPublish.action.Generation, SessionGeneration: secondPublish.action.SessionGeneration,
	})
	if hasOpenJoinAction(secondCompleted, OpenJoinActionReplyApplicationSuccess) {
		t.Fatalf("second attachment enabled the application again = %#v", secondCompleted)
	}
	snapshot = coordinator.Snapshot()
	if snapshot.State != OpenJoinActive || len(snapshot.Attachments) != 2 {
		t.Fatalf("two-attachment snapshot = %#v", snapshot)
	}
	thirdJoinActions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 3})
	requireOpenJoinAction(t, thirdJoinActions, OpenJoinActionSendJoin)
	thirdPublish := acceptCurrentJoinResult(t, coordinator)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: thirdPublish.action.Generation, SessionGeneration: thirdPublish.action.SessionGeneration,
	})
	if snapshot := coordinator.Snapshot(); snapshot.State != OpenJoinActive || len(snapshot.Attachments) != 3 {
		t.Fatalf("new session attachment snapshot = %#v", snapshot)
	}
}

func TestOpenJoinJoinRetriesAreBoundedAndLateResultsHaveNoEffect(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	completeTestOpen(t, coordinator)
	deadline := coordinator.Snapshot().JoinDeadlineGeneration
	var staleAttempt OpenJoinAttempt
	for coordinator.Snapshot().JoinAttempts < MaxJoinAttempts {
		snapshot := coordinator.Snapshot()
		staleAttempt = snapshot.Attempt
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinJoinRetryDue, Generation: snapshot.JoinRetryGeneration})
		requireOpenJoinAction(t, actions, OpenJoinActionReleaseAttachment)
		requireOpenJoinAction(t, actions, OpenJoinActionSendJoin)
		if coordinator.Snapshot().JoinDeadlineGeneration != deadline {
			t.Fatal("JOIN retry refreshed the absolute deadline")
		}
		late := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinJoinResultReceived, Generation: staleAttempt.Generation, SessionGeneration: staleAttempt.SessionGeneration,
			JoinResult: protocol.JoinResult{FlowID: testClientFlowID, Result: protocol.JoinSuccess},
		})
		if len(late) != 0 {
			t.Fatalf("stale JOIN result actions = %#v", late)
		}
	}
	current := coordinator.Snapshot().Attempt
	if snapshot := coordinator.Snapshot(); snapshot.JoinRetryGeneration != 0 || snapshot.JoinAttempts != MaxJoinAttempts {
		t.Fatalf("JOIN retry-limit snapshot = %#v", snapshot)
	}
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinJoinDeadlineReached, Generation: deadline})
	failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow)
	if failure.Failure != OpenJoinFailureJoinDeadline {
		t.Fatalf("JOIN deadline failure = %#v", failure)
	}
	late := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinJoinResultReceived, Generation: current.Generation, SessionGeneration: current.SessionGeneration,
		JoinResult: protocol.JoinResult{FlowID: testClientFlowID, Result: protocol.JoinSuccess},
	})
	if len(late) != 0 || coordinator.Snapshot().State != OpenJoinFailed {
		t.Fatalf("JOIN result after deadline = %#v / %#v", late, coordinator.Snapshot())
	}
}

func TestOpenJoinStrictlyValidatesJoinAndSwitchesFailedReservation(t *testing.T) {
	for _, result := range []protocol.JoinResult{
		{FlowID: protocol.FlowID{0xff}, Result: protocol.JoinSuccess},
		{FlowID: testClientFlowID, Result: 99},
	} {
		coordinator := newTestOpenJoin(t)
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
		join := completeTestOpen(t, coordinator)
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinJoinResultReceived, Generation: join.Generation, SessionGeneration: join.SessionGeneration, JoinResult: result,
		})
		if failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow); failure.Failure != OpenJoinFailureProtocol {
			t.Fatalf("JOIN strict validation = %#v", failure)
		}
	}

	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 2})
	first := completeTestOpen(t, coordinator)
	deadline := coordinator.Snapshot().JoinDeadlineGeneration
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinJoinReservationFailed, Generation: first.Generation, SessionGeneration: first.SessionGeneration,
	})
	requireOpenJoinAction(t, actions, OpenJoinActionReleaseAttachment)
	replacement := requireOpenJoinAction(t, actions, OpenJoinActionSendJoin)
	if replacement.SessionGeneration != 2 || coordinator.Snapshot().JoinDeadlineGeneration != deadline {
		t.Fatalf("reservation failure switch = %#v / %#v", replacement, coordinator.Snapshot())
	}
	if late := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinJoinResultReceived, Generation: first.Generation, SessionGeneration: first.SessionGeneration,
		JoinResult: protocol.JoinResult{FlowID: testClientFlowID, Result: protocol.JoinSuccess},
	}); len(late) != 0 {
		t.Fatalf("late result after reservation failure = %#v", late)
	}
}

func TestOpenJoinSessionLossInvalidatesJoinAndPublicationGenerations(t *testing.T) {
	t.Run("JOIN in flight", func(t *testing.T) {
		coordinator := newTestOpenJoin(t)
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 2})
		oldJoin := completeTestOpen(t, coordinator)
		deadline := coordinator.Snapshot().JoinDeadlineGeneration
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionLost, SessionGeneration: 1})
		requireOpenJoinAction(t, actions, OpenJoinActionReleaseAttachment)
		replacement := requireOpenJoinAction(t, actions, OpenJoinActionSendJoin)
		if replacement.SessionGeneration != 2 || replacement.Generation == oldJoin.Generation || coordinator.Snapshot().JoinDeadlineGeneration != deadline {
			t.Fatalf("JOIN session-loss switch = %#v / %#v", replacement, coordinator.Snapshot())
		}
		if late := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinJoinResultReceived, Generation: oldJoin.Generation, SessionGeneration: oldJoin.SessionGeneration,
			JoinResult: protocol.JoinResult{FlowID: testClientFlowID, Result: protocol.JoinSuccess},
		}); len(late) != 0 {
			t.Fatalf("stale JOIN success actions = %#v", late)
		}
	})

	t.Run("publication in flight", func(t *testing.T) {
		coordinator := newTestOpenJoin(t)
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 2})
		completeTestOpen(t, coordinator)
		oldPublish := acceptCurrentJoinResult(t, coordinator)
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionLost, SessionGeneration: 1})
		requireOpenJoinAction(t, actions, OpenJoinActionReleaseAttachment)
		replacement := requireOpenJoinAction(t, actions, OpenJoinActionSendJoin)
		if replacement.SessionGeneration != 2 {
			t.Fatalf("publication session-loss switch = %#v", replacement)
		}
		if late := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind:       OpenJoinAttachmentPublished,
			Generation: oldPublish.action.Generation, SessionGeneration: oldPublish.action.SessionGeneration,
		}); len(late) != 0 {
			t.Fatalf("stale publication completion actions = %#v", late)
		}
		if snapshot := coordinator.Snapshot(); snapshot.ApplicationAccepted {
			t.Fatalf("stale publication completion enabled the application = %#v", snapshot)
		}
	})
}

func TestOpenJoinJoinAndPublicationFailuresAreBounded(t *testing.T) {
	t.Run("first JOIN application rejection", func(t *testing.T) {
		coordinator := newTestOpenJoin(t)
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
		join := completeTestOpen(t, coordinator)
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinJoinResultReceived, Generation: join.Generation, SessionGeneration: join.SessionGeneration,
			JoinResult: protocol.JoinResult{FlowID: testClientFlowID, Result: protocol.JoinFailure},
		})
		requireOpenJoinAction(t, actions, OpenJoinActionReleaseAttachment)
		if hasOpenJoinAction(actions, OpenJoinActionFailFlow) || coordinator.Snapshot().JoinRetryGeneration == 0 {
			t.Fatalf("JOIN application rejection did not retain bounded retry = %#v / %#v", actions, coordinator.Snapshot())
		}
	})

	t.Run("publication failure switches session without refreshing deadline", func(t *testing.T) {
		coordinator := newTestOpenJoin(t)
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 2})
		completeTestOpen(t, coordinator)
		deadline := coordinator.Snapshot().JoinDeadlineGeneration
		publish := acceptCurrentJoinResult(t, coordinator)
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinAttachmentPublishFailed, Generation: publish.action.Generation, SessionGeneration: publish.action.SessionGeneration,
		})
		requireOpenJoinAction(t, actions, OpenJoinActionReleaseAttachment)
		replacement := requireOpenJoinAction(t, actions, OpenJoinActionSendJoin)
		if replacement.SessionGeneration != 2 || coordinator.Snapshot().JoinDeadlineGeneration != deadline {
			t.Fatalf("publication failure switch = %#v / %#v", replacement, coordinator.Snapshot())
		}
		if late := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinAttachmentPublished, Generation: publish.action.Generation, SessionGeneration: publish.action.SessionGeneration,
		}); len(late) != 0 {
			t.Fatalf("late publication completion actions = %#v", late)
		}
	})
}

func TestOpenJoinDeadlinesAlsoCoverWaitingForReadySession(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	start := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
	identity := requireOpenJoinAction(t, start, OpenJoinActionGenerateIdentity)
	deadline := requireOpenJoinAction(t, start, OpenJoinActionArmOpenDeadline)
	if actions := handleOpenJoin(t, coordinator, validIdentityEvent(identity.Generation)); len(actions) != 0 {
		t.Fatalf("OPEN sent without a ready session: %#v", actions)
	}
	if snapshot := coordinator.Snapshot(); snapshot.State != OpenJoinWaitingOpenSession || snapshot.OpenDeadlineGeneration != deadline.Generation {
		t.Fatalf("waiting-for-ready-session snapshot = %#v", snapshot)
	}
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinOpenDeadlineReached, Generation: deadline.Generation})
	if failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow); failure.Failure != OpenJoinFailureOpenDeadline {
		t.Fatalf("waiting-for-ready-session deadline = %#v", failure)
	}
}

func TestOpenJoinSessionLossRecoversWithoutRepeatingApplicationSuccess(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 2})
	completeTestOpen(t, coordinator)
	firstPublish := acceptCurrentJoinResult(t, coordinator)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: firstPublish.action.Generation, SessionGeneration: firstPublish.action.SessionGeneration,
	})
	secondPublish := acceptCurrentJoinResult(t, coordinator)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: secondPublish.action.Generation, SessionGeneration: secondPublish.action.SessionGeneration,
	})
	thirdReady := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 3})
	requireOpenJoinAction(t, thirdReady, OpenJoinActionSendJoin)
	thirdPublish := acceptCurrentJoinResult(t, coordinator)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: thirdPublish.action.Generation, SessionGeneration: thirdPublish.action.SessionGeneration,
	})

	firstLoss := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionLost, SessionGeneration: 1})
	requireOpenJoinAction(t, firstLoss, OpenJoinActionReleaseAttachment)

	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionLost, SessionGeneration: 2})
	lastLoss := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionLost, SessionGeneration: 3})
	deadline := requireOpenJoinAction(t, lastLoss, OpenJoinActionArmJoinDeadline)
	if snapshot := coordinator.Snapshot(); snapshot.State != OpenJoinWaitingJoinSession || len(snapshot.Attachments) != 0 {
		t.Fatalf("all-sessions-lost snapshot = %#v", snapshot)
	}

	ready := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 4})
	join := requireOpenJoinAction(t, ready, OpenJoinActionSendJoin)
	if coordinator.Snapshot().JoinDeadlineGeneration != deadline.Generation {
		t.Fatal("recovered session refreshed the JOIN deadline")
	}
	publish := acceptCurrentJoinResult(t, coordinator)
	recovered := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: publish.action.Generation, SessionGeneration: publish.action.SessionGeneration,
	})
	if hasOpenJoinAction(recovered, OpenJoinActionReplyApplicationSuccess) {
		t.Fatalf("recovery application actions = %#v; JOIN = %#v", recovered, join)
	}
}

func TestOpenJoinReadySessionAndActionCollectionsAreHardBounded(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	for generation := uint64(MaxOpenJoinSessions); generation > 0; generation-- {
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: generation})
		if len(actions) > MaxOpenJoinActions {
			t.Fatalf("ready actions exceed limit: %d", len(actions))
		}
	}
	if _, err := coordinator.Handle(OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: MaxOpenJoinSessions + 1}); !errors.Is(err, ErrOpenJoinSessions) {
		t.Fatalf("ninth ready session error = %v", err)
	}
	want := make([]uint64, MaxOpenJoinSessions)
	for index := range want {
		want[index] = uint64(index + 1)
	}
	snapshot := coordinator.Snapshot()
	if !slices.Equal(snapshot.ReadySessions, want) {
		t.Fatalf("bounded ready sessions = %#v", snapshot.ReadySessions)
	}
	snapshot.ReadySessions[0] = 99
	if coordinator.Snapshot().ReadySessions[0] != 1 {
		t.Fatal("ready session snapshot leaked a mutable internal collection")
	}
	if actions, err := coordinator.Handle(OpenJoinEvent{Kind: OpenJoinSessionReady}); !errors.Is(err, ErrInvalidOpenJoin) || len(actions) != 0 {
		t.Fatalf("zero session generation = %#v, %v", actions, err)
	}
}

func TestOpenJoinCancellationReleasesAttachmentAndMakesLateEventsHarmless(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	completeTestOpen(t, coordinator)
	publish := acceptCurrentJoinResult(t, coordinator)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: publish.action.Generation, SessionGeneration: publish.action.SessionGeneration,
	})
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinCancelled})
	requireOpenJoinAction(t, actions, OpenJoinActionReleaseAttachment)
	if failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow); failure.Failure != OpenJoinFailureCancelled {
		t.Fatalf("cancellation failure action = %#v", failure)
	}
	for _, late := range []OpenJoinEvent{
		{Kind: OpenJoinOpenDeadlineReached, Generation: 1},
		{Kind: OpenJoinJoinDeadlineReached, Generation: 1},
		{Kind: OpenJoinSessionReady, SessionGeneration: 2},
		{Kind: OpenJoinAttachmentPublished, Generation: publish.action.Generation, SessionGeneration: publish.action.SessionGeneration},
	} {
		if got := handleOpenJoin(t, coordinator, late); len(got) != 0 {
			t.Fatalf("actions for late terminal event %#v = %#v", late, got)
		}
	}
}

func TestOpenJoinFailureNeverCarriesSuccessResult(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	start := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
	deadline := requireOpenJoinAction(t, start, OpenJoinActionArmOpenDeadline)
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenDeadlineReached, Generation: deadline.Generation,
	})
	failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow)
	if failure.OpenResult == protocol.OpenSuccess || failure.OpenResult != protocol.OpenInternalFailure {
		t.Fatalf("deadline failure result = %#v", failure)
	}
}

func TestOpenJoinRejectedAttemptsUseFullLimiterBurstAndFailOnlyAtDeadline(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	join := completeTestOpen(t, coordinator)
	deadline := coordinator.Snapshot().JoinDeadlineGeneration
	limiter, err := server.NewRateLimiter(server.DefaultRateLimitKeys)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_000, 0)
	for attempt := 1; attempt <= MaxJoinAttempts; attempt++ {
		if !limiter.AllowJoin(now, "client-01", testClientFlowID, true) {
			t.Fatalf("valid JOIN attempt %d was rate limited", attempt)
		}
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinJoinResultReceived, Generation: join.Generation,
			SessionGeneration: join.SessionGeneration,
			JoinResult:        protocol.JoinResult{FlowID: testClientFlowID, Result: protocol.JoinFailure},
		})
		if hasOpenJoinAction(actions, OpenJoinActionFailFlow) {
			t.Fatalf("JOIN attempt %d terminated early: %#v", attempt, actions)
		}
		if attempt == MaxJoinAttempts {
			break
		}
		retry := coordinator.Snapshot().JoinRetryGeneration
		join = requireOpenJoinAction(t, handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinJoinRetryDue, Generation: retry,
		}), OpenJoinActionSendJoin)
	}
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinJoinDeadlineReached, Generation: deadline,
	})
	failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow)
	if failure.Failure != OpenJoinFailureJoinDeadline || failure.OpenResult != protocol.OpenInternalFailure {
		t.Fatalf("JOIN deadline failure = %#v", failure)
	}
}

func TestOpenJoinRecoveryContinuesPastJoinDeadlineUntilFlowRecoveryDeadline(t *testing.T) {
	coordinator := newTestOpenJoin(t)
	handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
	completeTestOpen(t, coordinator)
	publish := acceptCurrentJoinResult(t, coordinator)
	handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: publish.action.Generation,
		SessionGeneration: publish.action.SessionGeneration,
	})
	lost := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionLost, SessionGeneration: 1})
	firstDeadline := requireOpenJoinAction(t, lost, OpenJoinActionArmJoinDeadline)
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinJoinDeadlineReached, Generation: firstDeadline.Generation,
	})
	if hasOpenJoinAction(actions, OpenJoinActionFailFlow) {
		t.Fatalf("5-second JOIN deadline terminated recovery early: %#v", actions)
	}
	secondDeadline := requireOpenJoinAction(t, actions, OpenJoinActionArmJoinDeadline)
	if secondDeadline.Generation == firstDeadline.Generation || coordinator.Snapshot().State != OpenJoinWaitingJoinSession {
		t.Fatalf("recovery cycle did not continue: %#v / %#v", actions, coordinator.Snapshot())
	}
}

func TestOpenJoinGenerationExhaustionReturnsNoPartialNetworkActions(t *testing.T) {
	t.Run("OPEN", func(t *testing.T) {
		coordinator := newTestOpenJoin(t)
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
		start := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
		identity := requireOpenJoinAction(t, start, OpenJoinActionGenerateIdentity)
		coordinator.nextGeneration = ^uint64(0) - 1
		actions := handleOpenJoin(t, coordinator, validIdentityEvent(identity.Generation))
		if hasOpenJoinAction(actions, OpenJoinActionSendOpen) {
			t.Fatalf("OPEN returned after generation exhaustion: %#v", actions)
		}
		if failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow); failure.OpenResult != protocol.OpenInternalFailure {
			t.Fatalf("OPEN generation exhaustion failure = %#v", failure)
		}
	})

	t.Run("JOIN", func(t *testing.T) {
		coordinator := newTestOpenJoin(t)
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: 1})
		open, _ := beginTestOpen(t, coordinator)
		coordinator.nextGeneration = ^uint64(0) - 3
		actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinOpenResultReceived, Generation: open.Generation,
			SessionGeneration: open.SessionGeneration, OpenResult: successfulTestOpenResult(),
		})
		if hasOpenJoinAction(actions, OpenJoinActionReserveAttachment) || hasOpenJoinAction(actions, OpenJoinActionSendJoin) {
			t.Fatalf("JOIN actions returned after generation exhaustion: %#v", actions)
		}
		if failure := requireOpenJoinAction(t, actions, OpenJoinActionFailFlow); failure.OpenResult != protocol.OpenInternalFailure {
			t.Fatalf("JOIN generation exhaustion failure = %#v", failure)
		}
	})
}

func TestRedundantOpenAndJoinRaceAllReadySessions(t *testing.T) {
	coordinator := newRedundantTestOpenJoin(t)
	for generation := uint64(1); generation <= 3; generation++ {
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: generation})
	}
	start := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
	identity := requireOpenJoinAction(t, start, OpenJoinActionGenerateIdentity)
	opening := handleOpenJoin(t, coordinator, validIdentityEvent(identity.Generation))
	opens := openJoinActions(opening, OpenJoinActionSendOpen)
	if len(opens) != 3 || hasOpenJoinAction(opening, OpenJoinActionArmOpenRetry) {
		t.Fatalf("redundant OPEN actions = %#v", opening)
	}
	for index, action := range opens {
		wantSession := uint64(index + 1)
		if action.SessionGeneration != wantSession || action.Open.DeliveryMode != protocol.DeliveryRedundant ||
			action.Open.PathSelection != protocol.PathNone {
			t.Fatalf("redundant OPEN %d = %#v", index, action)
		}
		if pending, ok := coordinator.PendingAttempt(wantSession, OpenJoinAttemptOpen); !ok || pending != (OpenJoinAttempt{
			Kind: OpenJoinAttemptOpen, Generation: action.Generation, SessionGeneration: wantSession,
		}) {
			t.Fatalf("session %d pending OPEN state = %#v, %t", wantSession, pending, ok)
		}
	}

	joining := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: opens[2].Generation, SessionGeneration: 3,
		OpenResult: successfulRedundantOpenResult(),
	})
	if len(openJoinActions(joining, OpenJoinActionSendJoin)) != 0 || len(openJoinActions(joining, OpenJoinActionReserveAttachment)) != 1 ||
		hasOpenJoinAction(joining, OpenJoinActionReplyApplicationSuccess) {
		t.Fatalf("first OPEN direct-attachment actions = %#v", joining)
	}
	firstPublish := requireOpenJoinAction(t, joining, OpenJoinActionPublishAttachment)
	firstCompleted := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinAttachmentPublished, Generation: firstPublish.Generation, SessionGeneration: firstPublish.SessionGeneration,
	})
	if hasOpenJoinAction(firstCompleted, OpenJoinActionReplyApplicationSuccess) ||
		coordinator.Snapshot().State != OpenJoinJoining {
		t.Fatalf("first OPEN attachment published before all paths were ready = %#v / %#v", firstCompleted, coordinator.Snapshot())
	}

	for _, open := range opens {
		if open.SessionGeneration == 3 {
			continue
		}
		result := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinOpenResultReceived, Generation: open.Generation, SessionGeneration: open.SessionGeneration,
			OpenResult: successfulRedundantOpenResult(),
		})
		if len(openJoinActions(result, OpenJoinActionSendJoin)) != 0 {
			t.Fatalf("successful OPEN for session %d still sent JOIN: %#v", open.SessionGeneration, result)
		}
		publish := requireOpenJoinAction(t, result, OpenJoinActionPublishAttachment)
		completed := handleOpenJoin(t, coordinator, OpenJoinEvent{
			Kind: OpenJoinAttachmentPublished, Generation: publish.Generation, SessionGeneration: open.SessionGeneration,
		})
		if open.SessionGeneration == opens[1].SessionGeneration {
			if !hasOpenJoinAction(completed, OpenJoinActionReplyApplicationSuccess) {
				t.Fatalf("last OPEN attachment did not enable the application: %#v", completed)
			}
		} else if hasOpenJoinAction(completed, OpenJoinActionReplyApplicationSuccess) {
			t.Fatalf("session %d enabled the application before all paths were ready: %#v", open.SessionGeneration, completed)
		}
	}
	if snapshot := coordinator.Snapshot(); snapshot.State != OpenJoinActive || len(snapshot.Attachments) != 3 ||
		!snapshot.ApplicationAccepted || snapshot.JoinDeadlineGeneration != 0 {
		t.Fatalf("snapshot after all redundant paths attached = %#v", snapshot)
	}
	if late := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: opens[1].Generation, SessionGeneration: opens[1].SessionGeneration,
		OpenResult: successfulRedundantOpenResult(),
	}); len(late) != 0 {
		t.Fatalf("late OPEN result emitted actions = %#v", late)
	}
}

func TestRedundantOpenJoinMaximumSessionBatchIsBounded(t *testing.T) {
	coordinator := newRedundantTestOpenJoin(t)
	for generation := uint64(1); generation <= MaxOpenJoinSessions; generation++ {
		handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinSessionReady, SessionGeneration: generation})
	}
	start := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
	identity := requireOpenJoinAction(t, start, OpenJoinActionGenerateIdentity)
	opening := handleOpenJoin(t, coordinator, validIdentityEvent(identity.Generation))
	opens := openJoinActions(opening, OpenJoinActionSendOpen)
	if len(opens) != MaxOpenJoinSessions {
		t.Fatalf("OPEN actions at maximum session count = %d", len(opens))
	}
	joining := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: opens[len(opens)-1].Generation,
		SessionGeneration: opens[len(opens)-1].SessionGeneration, OpenResult: successfulRedundantOpenResult(),
	})
	if len(openJoinActions(joining, OpenJoinActionSendJoin)) != 0 ||
		len(joining) > MaxOpenJoinActions {
		t.Fatalf("OPEN direct-attachment actions at maximum session count = %d: %#v", len(joining), joining)
	}
}

type publishStep struct {
	action  OpenJoinAction
	actions []OpenJoinAction
}

func newTestOpenJoin(t *testing.T) *OpenJoinCoordinator {
	t.Helper()
	coordinator, err := NewOpenJoinCoordinator(OpenJoinSpec{
		Target:        testClientTarget(),
		DeliveryMode:  protocol.DeliveryAdaptive,
		PathSelection: protocol.PathFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func newRedundantTestOpenJoin(t *testing.T) *OpenJoinCoordinator {
	t.Helper()
	coordinator, err := NewOpenJoinCoordinator(OpenJoinSpec{
		Target: testClientTarget(), DeliveryMode: protocol.DeliveryRedundant, PathSelection: protocol.PathNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func testClientTarget() protocol.Target {
	return protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 443}
}

func validIdentityEvent(generation uint64) OpenJoinEvent {
	return OpenJoinEvent{
		Kind: OpenJoinIdentityGenerated, Generation: generation,
		FlowID: testClientFlowID, OpenToken: testClientOpenToken,
	}
}

func successfulTestOpenResult() protocol.OpenResult {
	return protocol.OpenResult{
		FlowID: testClientFlowID, Result: protocol.OpenSuccess, Capability: testClientCapability,
		DeliveryMode: protocol.DeliveryAdaptive, PathSelection: protocol.PathFastest,
	}
}

func successfulRedundantOpenResult() protocol.OpenResult {
	return protocol.OpenResult{
		FlowID: testClientFlowID, Result: protocol.OpenSuccess, Capability: testClientCapability,
		DeliveryMode: protocol.DeliveryRedundant, PathSelection: protocol.PathNone, ImplicitAttachment: true,
	}
}

func beginTestOpen(t *testing.T, coordinator *OpenJoinCoordinator) (OpenJoinAction, uint64) {
	t.Helper()
	deadline, opens := beginTestOpens(t, coordinator)
	return opens[0], deadline
}

func beginTestOpens(t *testing.T, coordinator *OpenJoinCoordinator) (uint64, []OpenJoinAction) {
	t.Helper()
	start := handleOpenJoin(t, coordinator, OpenJoinEvent{Kind: OpenJoinStart})
	identity := requireOpenJoinAction(t, start, OpenJoinActionGenerateIdentity)
	deadline := requireOpenJoinAction(t, start, OpenJoinActionArmOpenDeadline)
	actions := handleOpenJoin(t, coordinator, validIdentityEvent(identity.Generation))
	retry := requireOpenJoinAction(t, actions, OpenJoinActionArmOpenRetry)
	if retry.Duration != OpenRetryInterval {
		t.Fatalf("OPEN retry interval = %v, want %v", retry.Duration, OpenRetryInterval)
	}
	opens := openJoinActions(actions, OpenJoinActionSendOpen)
	if len(opens) == 0 {
		t.Fatalf("OPEN actions = %#v", actions)
	}
	return deadline.Generation, opens
}

func completeTestOpen(t *testing.T, coordinator *OpenJoinCoordinator) OpenJoinAction {
	t.Helper()
	open, _ := beginTestOpen(t, coordinator)
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinOpenResultReceived, Generation: open.Generation, SessionGeneration: open.SessionGeneration,
		OpenResult: successfulTestOpenResult(),
	})
	if hasOpenJoinAction(actions, OpenJoinActionReplyApplicationSuccess) {
		t.Fatalf("application enabled after OPEN but before JOIN = %#v", actions)
	}
	deadline := requireOpenJoinAction(t, actions, OpenJoinActionArmJoinDeadline)
	retry := requireOpenJoinAction(t, actions, OpenJoinActionArmJoinRetry)
	if deadline.Duration != JoinDeadline || retry.Duration != JoinRetryInterval {
		t.Fatalf("JOIN deadline/retry interval = %v/%v", deadline.Duration, retry.Duration)
	}
	return requireOpenJoinAction(t, actions, OpenJoinActionSendJoin)
}

func acceptCurrentJoinResult(t *testing.T, coordinator *OpenJoinCoordinator) publishStep {
	t.Helper()
	attempt := coordinator.Snapshot().Attempt
	actions := handleOpenJoin(t, coordinator, OpenJoinEvent{
		Kind: OpenJoinJoinResultReceived, Generation: attempt.Generation, SessionGeneration: attempt.SessionGeneration,
		JoinResult: protocol.JoinResult{FlowID: testClientFlowID, Result: protocol.JoinSuccess},
	})
	return publishStep{action: requireOpenJoinAction(t, actions, OpenJoinActionPublishAttachment), actions: actions}
}

func handleOpenJoin(t *testing.T, coordinator *OpenJoinCoordinator, event OpenJoinEvent) []OpenJoinAction {
	t.Helper()
	actions, err := coordinator.Handle(event)
	if err != nil {
		t.Fatalf("Handle(%#v): %v", event, err)
	}
	if len(actions) > MaxOpenJoinActions {
		t.Fatalf("Handle(%#v) action count = %d, exceeds %d", event, len(actions), MaxOpenJoinActions)
	}
	return actions
}

func requireOpenJoinAction(t *testing.T, actions []OpenJoinAction, kind OpenJoinActionKind) OpenJoinAction {
	t.Helper()
	index := actionIndex(actions, kind)
	if index < 0 {
		t.Fatalf("actions %#v missing kind=%d", actions, kind)
	}
	return actions[index]
}

func hasOpenJoinAction(actions []OpenJoinAction, kind OpenJoinActionKind) bool {
	return actionIndex(actions, kind) >= 0
}

func openJoinActions(actions []OpenJoinAction, kind OpenJoinActionKind) []OpenJoinAction {
	var matches []OpenJoinAction
	for _, action := range actions {
		if action.Kind == kind {
			matches = append(matches, action)
		}
	}
	return matches
}

func openJoinSessionGenerations(actions []OpenJoinAction) []uint64 {
	generations := make([]uint64, 0, len(actions))
	for _, action := range actions {
		generations = append(generations, action.SessionGeneration)
	}
	return generations
}

func actionIndex(actions []OpenJoinAction, kind OpenJoinActionKind) int {
	for index, action := range actions {
		if action.Kind == kind {
			return index
		}
	}
	return -1
}
