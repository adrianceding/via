package client

import (
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

var (
	testRelayFlowID = protocol.FlowID{0x71}
	testRelayA      = flow.AttachmentKey{SessionGeneration: 1, AttachmentGeneration: 11}
	testRelayB      = flow.AttachmentKey{SessionGeneration: 2, AttachmentGeneration: 22}
)

func TestApplicationRelayDoesNotReadBeforeFirstAttachmentPublished(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	if snapshot := relay.Snapshot(); !snapshot.ApplicationReadPaused || snapshot.ApplicationReadGeneration != 0 ||
		snapshot.Flow.Lifecycle.State != flow.AwaitingAttachment {
		t.Fatalf("snapshot before JOIN publication = %#v", snapshot)
	}

	actions := publishRelayAttachmentWithoutEnable(t, relay, machine, testRelayA)
	if countRelayActions(actions, ApplicationRelayActionAttachmentPublished) != 1 ||
		countRelayActions(actions, ApplicationRelayActionRead) != 0 {
		t.Fatalf("first publication actions = %#v", actions)
	}
	publishedIndex, acknowledgementIndex := -1, -1
	for index, action := range actions {
		switch {
		case action.Kind == ApplicationRelayActionAttachmentPublished:
			publishedIndex = index
		case action.Kind == ApplicationRelayActionSendMessage:
			if _, ok := action.Message.(protocol.ACK); ok && acknowledgementIndex == -1 {
				acknowledgementIndex = index
			}
		}
	}
	if publishedIndex < 0 || acknowledgementIndex <= publishedIndex {
		t.Fatalf("publication order = published %d, ACK %d; actions=%#v", publishedIndex, acknowledgementIndex, actions)
	}
	if snapshot := relay.Snapshot(); snapshot.ApplicationReadPaused || snapshot.ApplicationReadGeneration != 0 || snapshot.ApplicationEnabled ||
		snapshot.Flow.Lifecycle.State != flow.Relaying {
		t.Fatalf("snapshot after JOIN publication = %#v", snapshot)
	}
	actions, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayEnableApplication})
	if err != nil || countRelayActions(actions, ApplicationRelayActionRead) != 1 {
		t.Fatalf("application enable actions = %#v, %v", actions, err)
	}
	if snapshot := relay.Snapshot(); snapshot.ApplicationReadPaused || snapshot.ApplicationReadGeneration == 0 || !snapshot.ApplicationEnabled {
		t.Fatalf("snapshot after SOCKS success = %#v", snapshot)
	}
}

func TestApplicationRelayDefersRemoteDataWriteUntilSOCKSSuccess(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone})
	publishRelayAttachmentWithoutEnable(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Bytes: []byte("early target data")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionWrite) != 0 {
		t.Fatalf("application write escaped SOCKS barrier: %#v", actions)
	}
	actions, err = relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayEnableApplication})
	if err != nil {
		t.Fatal(err)
	}
	write := requireRelayAction(t, actions, ApplicationRelayActionWrite)
	if string(write.CopyData()) != "early target data" {
		t.Fatalf("deferred application write = %q", write.CopyData())
	}
}

func TestApplicationRelayDefersRemoteFINUntilSOCKSSuccess(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone})
	publishRelayAttachmentWithoutEnable(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.FIN{FlowID: testRelayFlowID, FinalOffset: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionCloseWrite) != 0 {
		t.Fatalf("application half-close escaped SOCKS barrier: %#v", actions)
	}
	actions, err = relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayEnableApplication})
	if err != nil || countRelayActions(actions, ApplicationRelayActionCloseWrite) != 1 {
		t.Fatalf("deferred application half-close = %#v, %v", actions, err)
	}
}

func TestApplicationRelayAttachmentPublicationIsGenerationSafeAndBounded(t *testing.T) {
	relay, _ := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	reserve := func(attachment flow.AttachmentKey, generation uint64) error {
		actions, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelayReserveAttachment, Attachment: attachment, Generation: generation,
		})
		if len(actions) != 0 {
			t.Fatalf("reservation actions = %#v", actions)
		}
		return err
	}
	if err := reserve(testRelayA, 101); err != nil {
		t.Fatal(err)
	}
	if err := reserve(testRelayA, 101); err != nil {
		t.Fatalf("idempotent reservation error = %v", err)
	}
	if err := reserve(testRelayA, 102); !errors.Is(err, ErrInvalidApplicationRelayEvent) {
		t.Fatalf("conflicting reservation error = %v", err)
	}
	if err := reserve(testRelayB, 201); err != nil {
		t.Fatal(err)
	}
	for sessionGeneration := uint64(3); sessionGeneration <= uint64(flow.MaxAttachments); sessionGeneration++ {
		attachment := flow.AttachmentKey{SessionGeneration: sessionGeneration, AttachmentGeneration: sessionGeneration * 11}
		if err := reserve(attachment, sessionGeneration*100+1); err != nil {
			t.Fatalf("reservation %d error = %v", sessionGeneration, err)
		}
	}
	if got := len(relay.reservations); got != flow.MaxAttachments {
		t.Fatalf("reservation count = %d, want %d", got, flow.MaxAttachments)
	}
	overflow := flow.AttachmentKey{
		SessionGeneration:    uint64(flow.MaxAttachments) + 1,
		AttachmentGeneration: (uint64(flow.MaxAttachments) + 1) * 11,
	}
	if err := reserve(overflow, (uint64(flow.MaxAttachments)+1)*100+1); !errors.Is(err, ErrApplicationRelayAttachment) {
		t.Fatalf("overflow reservation error = %v", err)
	}

	stale, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayPublishAttachment, Attachment: testRelayA, Generation: 100,
	})
	if err != nil || len(stale) != 0 || relay.Snapshot().Flow.Lifecycle.State != flow.AwaitingAttachment {
		t.Fatalf("stale publication actions=%#v err=%v snapshot=%#v", stale, err, relay.Snapshot())
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReleaseAttachment, Attachment: testRelayA,
	}); err != nil {
		t.Fatal(err)
	}
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayPublishAttachment, Attachment: testRelayB, Generation: 201,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionAttachmentPublished) != 1 ||
		countRelayActions(actions, ApplicationRelayActionRead) != 0 {
		t.Fatalf("matching publication actions = %#v", actions)
	}
	actions, err = relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayEnableApplication})
	if err != nil || countRelayActions(actions, ApplicationRelayActionRead) != 1 {
		t.Fatalf("matching application enable actions = %#v, %v", actions, err)
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReleaseAttachment, Attachment: testRelayB,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionAttachmentWithdrawn) != 1 ||
		countRelayActions(actions, ApplicationRelayActionArmRecoveryDeadline) != 1 {
		t.Fatalf("published release actions = %#v", actions)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Recovering || !snapshot.ApplicationReadPaused {
		t.Fatalf("release snapshot = %#v", snapshot)
	}
}

func TestApplicationRelayRedundantApplicationReadMapsDataAndEOFToBothAttachments(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)

	readGeneration := relay.Snapshot().ApplicationReadGeneration
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: readGeneration, Data: []byte("reply"), Err: io.EOF,
	})
	if err != nil {
		t.Fatal(err)
	}
	dataAttachments := messageAttachments[protocol.Data](t, actions)
	finAttachments := messageAttachments[protocol.FIN](t, actions)
	assertAttachmentSet(t, dataAttachments, testRelayA, testRelayB)
	assertAttachmentSet(t, finAttachments, testRelayA, testRelayB)
	var dataFrames [][]byte
	for _, action := range actions {
		if message, ok := applicationRelayActionMessage(t, action).(protocol.Data); ok {
			if string(message.Bytes) != "reply" {
				t.Fatalf("DATA bytes = %q", message.Bytes)
			}
			dataFrames = append(dataFrames, action.Encoded)
		}
		if action.Kind == ApplicationRelayActionRead {
			t.Fatal("read scheduled after local EOF")
		}
		if action.Kind == ApplicationRelayActionSendMessage && action.Generation == 0 {
			t.Fatal("send action has zero generation")
		}
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.TxState != flow.TxFinPending || snapshot.Flow.TxReplayBytes != 5 {
		t.Fatalf("snapshot after EOF = %#v", snapshot)
	}
	if len(dataFrames) != 2 || &dataFrames[0][0] != &dataFrames[1][0] {
		t.Fatal("redundant DATA attempts did not share immutable encoded frame")
	}
}

func TestApplicationRelayDataSendOwnsEncodedFrameAfterReadBufferChanges(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	input := []byte("owned-frame")
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: input,
	})
	if err != nil {
		t.Fatal(err)
	}
	action := requireMessageAction[protocol.Data](t, actions)
	if action.Message != nil || action.Class != transport.FrameData || action.SendDataBytes != len(input) || len(action.Encoded) == 0 {
		t.Fatalf("DATA action ownership = %#v", action)
	}
	input[0] = 'X'
	message := applicationRelayActionMessage(t, action).(protocol.Data)
	if string(message.Bytes) != "owned-frame" {
		t.Fatalf("encoded DATA changed with read buffer = %q", message.Bytes)
	}
}

func TestApplicationRelayAdaptiveFastestPlacesNewDataOnceAcrossTwoAttachments(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)

	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("one-copy"),
	})
	if err != nil {
		t.Fatal(err)
	}
	attachments := messageAttachments[protocol.Data](t, actions)
	if len(attachments) != 1 || attachments[0] != testRelayA && attachments[0] != testRelayB {
		t.Fatalf("fastest DATA attachments = %#v", attachments)
	}
	if countRelayActions(actions, ApplicationRelayActionRead) != 1 {
		t.Fatalf("next application reads = %d, want 1", countRelayActions(actions, ApplicationRelayActionRead))
	}
}

func TestApplicationRelayAdaptiveFirstRetryFansOutThenEscalates(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	third := flow.AttachmentKey{SessionGeneration: 3, AttachmentGeneration: 33}
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	publishRelayAttachment(t, relay, machine, third)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("gap"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := messageAttachments[protocol.Data](t, actions)
	if len(first) != 1 {
		t.Fatalf("initial placements = %#v", first)
	}

	retryGeneration := relay.Snapshot().Flow.RetryGeneration
	actions, err = relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	second := messageAttachments[protocol.Data](t, actions)
	wantSecond := []flow.AttachmentKey{testRelayA, testRelayB, third}
	for index, attachment := range wantSecond {
		if attachment == first[0] {
			wantSecond = append(wantSecond[:index], wantSecond[index+1:]...)
			break
		}
	}
	assertAttachmentSet(t, second, wantSecond...)
	if state := relay.Snapshot().Policy.State; state != policy.AdaptiveTargeted {
		t.Fatalf("policy state after applicationed retry = %v", state)
	}

	retryGeneration = relay.Snapshot().Flow.RetryGeneration
	actions, err = relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	assertAttachmentSet(t, messageAttachments[protocol.Data](t, actions), testRelayA, testRelayB, third)
	if state := relay.Snapshot().Policy.State; state != policy.AdaptiveFull {
		t.Fatalf("policy state after full retry = %v", state)
	}
}

func TestApplicationRelayRedundantRetryAfterAllCopiesUnconfirmedUsesNewGeneration(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("lost-everywhere"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := dataMessageActions(t, actions)
	if len(first) != 2 || first[0].AttemptGeneration != 1 || first[1].AttemptGeneration != 1 {
		t.Fatalf("initial DATA sends = %#v", first)
	}
	for _, action := range first {
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendResult, Generation: action.Generation, AttemptOutcome: flow.AttemptFailed,
		}); err != nil {
			t.Fatal(err)
		}
	}

	retryGeneration := relay.Snapshot().Flow.RetryGeneration
	actions, err = relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	retried := dataMessageActions(t, actions)
	if len(retried) != 2 || retried[0].AttemptGeneration != 2 || retried[1].AttemptGeneration != 2 ||
		retried[0].ItemID != first[0].ItemID || retried[1].ItemID != first[0].ItemID {
		t.Fatalf("retried DATA sends = %#v", retried)
	}
	assertAttachmentSet(t, messageAttachments[protocol.Data](t, actions), testRelayA, testRelayB)
}

func TestApplicationRelayDistributedPlacementUsesBothAttachments(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)

	seen := make(map[flow.AttachmentKey]bool, 2)
	for index := 0; index < 4; index++ {
		actions, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("same-size"),
		})
		if err != nil {
			t.Fatal(err)
		}
		attachments := messageAttachments[protocol.Data](t, actions)
		if len(attachments) != 1 {
			t.Fatalf("distributed placement %d = %#v", index, attachments)
		}
		seen[attachments[0]] = true
	}
	if !seen[testRelayA] || !seen[testRelayB] {
		t.Fatalf("distributed attachments = %#v", seen)
	}
}

func TestApplicationRelayQualityEventsDriveFastestPlacementAndRetryEstimate(t *testing.T) {
	now := time.Unix(300, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	for _, event := range []ApplicationRelayEvent{
		{Kind: ApplicationRelayObserveDataQuality, Attachment: testRelayA, RTT: 20 * time.Millisecond, Bytes: 1 << 20, Interval: time.Second},
		{Kind: ApplicationRelayObserveDataQuality, Attachment: testRelayB, RTT: 80 * time.Millisecond, Bytes: 1 << 20, Interval: time.Second},
		{Kind: ApplicationRelaySetAttachmentLoad, Attachment: testRelayA, QueuedBytes: 256 << 10},
		{Kind: ApplicationRelaySetAttachmentLoad, Attachment: testRelayB, QueuedBytes: 256 << 10},
	} {
		if _, err := relay.Handle(event); err != nil {
			t.Fatal(err)
		}
	}
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("quality"),
	})
	if err != nil {
		t.Fatal(err)
	}
	attachments := messageAttachments[protocol.Data](t, actions)
	if len(attachments) != 1 || attachments[0] != testRelayA {
		t.Fatalf("quality placements = %#v", attachments)
	}
	retry := requireRelayAction(t, actions, ApplicationRelayActionArmRetryDeadline)
	wantRetry := 20*time.Millisecond + time.Duration((256<<10)+len("quality"))*time.Second/time.Duration(1<<20)
	if retry.After != wantRetry {
		t.Fatalf("retry estimate = %s, want delivery estimate %s", retry.After, wantRetry)
	}

	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySetAttachmentLoad, Attachment: testRelayA, QueuedBytes: 2 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySetStallPenalty, Attachment: testRelayA, StallPenalty: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	attachments = messageAttachments[protocol.Data](t, actions)
	if len(attachments) != 1 || attachments[0] != testRelayB {
		t.Fatalf("stall did not immediately recover outstanding DATA = %#v", attachments)
	}
}

func TestApplicationRelayLateTelemetryForWithdrawnAttachmentIsHarmless(t *testing.T) {
	tests := []struct {
		name  string
		event ApplicationRelayEvent
	}{
		{name: "data quality", event: ApplicationRelayEvent{Kind: ApplicationRelayObserveDataQuality, Attachment: testRelayA, RTT: 20 * time.Millisecond, Bytes: 1024, Interval: time.Second}},
		{name: "probe quality", event: ApplicationRelayEvent{Kind: ApplicationRelayObserveProbeQuality, Attachment: testRelayA, RTT: 20 * time.Millisecond}},
		{name: "attachment load", event: ApplicationRelayEvent{Kind: ApplicationRelaySetAttachmentLoad, Attachment: testRelayA, QueuedBytes: 1024, InFlightBytes: 512}},
		{name: "stall penalty", event: ApplicationRelayEvent{Kind: ApplicationRelaySetStallPenalty, Attachment: testRelayA, StallPenalty: time.Second}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			relay, machine := newRelayFixture(t, policy.Config{
				Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
			})
			publishRelayAttachment(t, relay, machine, testRelayA)
			publishRelayAttachment(t, relay, machine, testRelayB)
			if _, err := relay.Handle(ApplicationRelayEvent{
				Kind: ApplicationRelayReleaseAttachment, Attachment: testRelayA,
			}); err != nil {
				t.Fatal(err)
			}

			before := relay.Snapshot()
			actions, err := relay.Handle(test.event)
			if err != nil || len(actions) != 0 {
				t.Fatalf("late telemetry actions=%#v err=%v", actions, err)
			}
			if after := relay.Snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("late telemetry mutated relay: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestApplicationRelayWithdrawnAttachmentStillRejectsNonTelemetryAndPreservesOtherErrors(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReleaseAttachment, Attachment: testRelayA,
	}); err != nil {
		t.Fatal(err)
	}
	before := relay.Snapshot()
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.ACK{FlowID: testRelayFlowID},
	}); !errors.Is(err, ErrInvalidApplicationRelayEvent) {
		t.Fatalf("withdrawn non-telemetry error = %v", err)
	}
	if after := relay.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("withdrawn non-telemetry mutated relay: before=%#v after=%#v", before, after)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySetStallPenalty, Attachment: testRelayB, StallPenalty: -1,
	}); !errors.Is(err, policy.ErrInvalidSample) {
		t.Fatalf("known attachment telemetry error = %v", err)
	}
}

func TestApplicationRelayRemoteDataSerializesApplicationShortWritesAndACKsActualProgress(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	incoming := []byte("abcd")
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Offset: 0, Bytes: incoming},
	})
	if err != nil {
		t.Fatal(err)
	}
	write := requireRelayAction(t, actions, ApplicationRelayActionWrite)
	completeControlSends(t, relay, actions)
	if write.Offset != 0 || string(write.CopyData()) != "abcd" {
		t.Fatalf("initial write = offset %d data %q", write.Offset, write.CopyData())
	}
	incoming[0] = 'X'
	if string(write.CopyData()) != "abcd" {
		t.Fatalf("application write aliases inbound DATA: %q", write.CopyData())
	}

	staleActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayWriteResult, Generation: write.Generation + 1, N: 4,
	})
	if err != nil || len(staleActions) != 0 || relay.Snapshot().ApplicationWriteGeneration != write.Generation {
		t.Fatalf("stale write result actions=%#v err=%v snapshot=%#v", staleActions, err, relay.Snapshot())
	}

	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayWriteResult, Generation: write.Generation, N: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertACKOffset(t, actions, 2)
	write = requireRelayAction(t, actions, ApplicationRelayActionWrite)
	completeControlSends(t, relay, actions)
	if write.Offset != 2 || string(write.CopyData()) != "cd" {
		t.Fatalf("short-write continuation = offset %d data %q", write.Offset, write.CopyData())
	}
	if countRelayActions(actions, ApplicationRelayActionWrite) != 1 {
		t.Fatalf("concurrent application writes = %d", countRelayActions(actions, ApplicationRelayActionWrite))
	}

	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayWriteResult, Generation: write.Generation, N: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertACKOffset(t, actions, 4)
	if snapshot := relay.Snapshot(); snapshot.ApplicationWriteGeneration != 0 || snapshot.Flow.Rx.WrittenOffset != 4 {
		t.Fatalf("completed application write snapshot = %#v", snapshot)
	}
}

func TestApplicationRelayAdaptiveFastestSendsAcknowledgementOnce(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	attachments := make([]flow.AttachmentKey, 8)
	for index := range attachments {
		attachments[index] = flow.AttachmentKey{
			SessionGeneration:    uint64(index + 1),
			AttachmentGeneration: uint64(index + 1),
		}
		publishRelayAttachment(t, relay, machine, attachments[index])
	}

	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: attachments[0],
		Message: protocol.Data{FlowID: testRelayFlowID, Bytes: []byte("data")},
	})
	if err != nil {
		t.Fatal(err)
	}
	acknowledgements := messageAttachments[protocol.ACK](t, actions)
	if len(acknowledgements) != 1 {
		t.Fatalf("adaptive fastest ACK copies = %d, want 1: %#v", len(acknowledgements), actions)
	}
	if countRelayActions(actions, ApplicationRelayActionWrite) != 1 {
		t.Fatalf("adaptive fastest write actions = %#v", actions)
	}
}

func TestApplicationRelayOutOfOrderAndDuplicateDataWritesApplicationOnce(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)

	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayB,
		Message: protocol.Data{FlowID: testRelayFlowID, Offset: 4, Bytes: []byte("ef")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionWrite) != 0 {
		t.Fatalf("out-of-order suffix was written early: %#v", actions)
	}
	completeControlSends(t, relay, actions)

	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Offset: 0, Bytes: []byte("abcd")},
	})
	if err != nil {
		t.Fatal(err)
	}
	write := requireRelayAction(t, actions, ApplicationRelayActionWrite)
	completeControlSends(t, relay, actions)
	if write.Offset != 0 || string(write.CopyData()) != "abcdef" {
		t.Fatalf("reassembled write = offset %d data %q", write.Offset, write.CopyData())
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayWriteResult, Generation: write.Generation, N: write.DataLen(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertACKOffset(t, actions, 6)
	completeControlSends(t, relay, actions)

	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayB,
		Message: protocol.Data{FlowID: testRelayFlowID, Offset: 0, Bytes: []byte("abcdef")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionWrite) != 0 {
		t.Fatalf("duplicate DATA wrote application twice: %#v", actions)
	}
	assertACKOffset(t, actions, 6)
}

func TestApplicationRelayApplicationWriteProgressWithErrorACKsPrefixThenResets(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Bytes: []byte("abcd")},
	})
	if err != nil {
		t.Fatal(err)
	}
	write := requireRelayAction(t, actions, ApplicationRelayActionWrite)
	writeFailure := errors.New("application write failed")
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayWriteResult, Generation: write.Generation, N: 2, Err: writeFailure,
	})
	if !errors.Is(err, writeFailure) {
		t.Fatalf("write error = %v, want %v", err, writeFailure)
	}
	assertACKOffset(t, actions, 2)
	if countRelayActions(actions, ApplicationRelayActionClose) != 1 {
		t.Fatalf("close application actions = %d", countRelayActions(actions, ApplicationRelayActionClose))
	}
	resetSend := requireMessageAction[protocol.Reset](t, actions)
	if resetSend.Message.(protocol.Reset).Reason != protocol.ResetLocalIOFailure {
		t.Fatalf("RESET = %#v", resetSend.Message)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Resetting || !snapshot.ApplicationCloseIssued {
		t.Fatalf("resetting snapshot = %#v", snapshot)
	}

	terminalActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: resetSend.Generation, AttemptOutcome: flow.AttemptSucceeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(terminalActions, ApplicationRelayActionClose) != 0 {
		t.Fatal("application closed twice")
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Reset || snapshot.PendingSends != 0 {
		t.Fatalf("terminal snapshot = %#v", snapshot)
	}
}

func TestApplicationRelayFINCloseWriteFINACKAndClosing(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)

	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.FIN{FlowID: testRelayFlowID, FinalOffset: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	closeWrite := requireRelayAction(t, actions, ApplicationRelayActionCloseWrite)
	completeControlSends(t, relay, actions)
	if closeWrite.FinalOffset != 0 {
		t.Fatalf("close-write final offset = %d", closeWrite.FinalOffset)
	}
	stale, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayCloseWriteResult, Generation: closeWrite.Generation + 1,
	})
	if err != nil || len(stale) != 0 {
		t.Fatalf("stale close-write actions=%#v err=%v", stale, err)
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayCloseWriteResult, Generation: closeWrite.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireMessageAction[protocol.FINACK](t, actions)

	readGeneration := relay.Snapshot().ApplicationReadGeneration
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: readGeneration, Err: io.EOF,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireMessageAction[protocol.FIN](t, actions)
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.FINACK{FlowID: testRelayFlowID, FinalOffset: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionClose) != 1 || countRelayActions(actions, ApplicationRelayActionArmClosingDeadline) != 1 {
		t.Fatalf("closing actions = %#v", actions)
	}
	if state := relay.Snapshot().Flow.Lifecycle.State; state != flow.Closing {
		t.Fatalf("flow state = %v, want Closing", state)
	}
	closingGeneration := relay.Snapshot().Flow.Lifecycle.ClosingDeadlineGeneration
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayClosingDeadline, Generation: closingGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionAttachmentWithdrawn) != 1 ||
		countRelayActions(actions, ApplicationRelayActionClose) != 0 {
		t.Fatalf("closing deadline actions = %#v", actions)
	}
	if state := relay.Snapshot().Flow.Lifecycle.State; state != flow.Closed {
		t.Fatalf("flow state after closing deadline = %v, want Closed", state)
	}
}

func TestApplicationRelayACKReleasesReverseReplayAndLateSendResultIsHarmless(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("response"),
	})
	if err != nil {
		t.Fatal(err)
	}
	dataSend := requireMessageAction[protocol.Data](t, actions)
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.TxReplayBytes != 0 || snapshot.Flow.TxAcknowledgedOffset != 8 {
		t.Fatalf("ACK snapshot = %#v", snapshot)
	}
	if countRelayActions(actions, ApplicationRelayActionCancelRetryDeadline) != 1 ||
		countRelayActions(actions, ApplicationRelayActionCancelNoProgressDeadline) != 1 {
		t.Fatalf("ACK deadline actions = %#v", actions)
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: dataSend.Generation, AttemptOutcome: flow.AttemptSucceeded,
		CapacityEligible: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if relay.Snapshot().Flow.TxReplayBytes != 0 {
		t.Fatal("late local send completion recreated replay")
	}
	credit := requireRelayAction(t, actions, ApplicationRelayActionDataCredit)
	if credit.Attachment != dataSend.Attachment || credit.DataCreditBytes != 8 ||
		credit.WriteCompletedAt.IsZero() || credit.AcknowledgedAt.Before(credit.WriteCompletedAt) || !credit.CapacityEligible {
		t.Fatalf("late send completion DATA credit = %#v", credit)
	}
}

func TestApplicationRelayCreditsUniqueAttemptToSendingAttachment(t *testing.T) {
	now := time.Unix(900, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("credited"),
	})
	if err != nil {
		t.Fatal(err)
	}
	send := requireMessageAction[protocol.Data](t, actions)
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendAdmitted, Generation: send.Generation, LaneGeneration: 77,
	}); err != nil {
		t.Fatal(err)
	}
	writeCompletedAt := now.Add(10 * time.Millisecond)
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: send.Generation,
		AttemptOutcome: flow.AttemptSucceeded, WriteCompletedAt: writeCompletedAt, CapacityEligible: true,
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(20 * time.Millisecond)
	ackAttachment := testRelayA
	if send.Attachment == ackAttachment {
		ackAttachment = testRelayB
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: ackAttachment,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	credit := requireRelayAction(t, actions, ApplicationRelayActionDataCredit)
	if credit.Attachment != send.Attachment || credit.LaneGeneration != 77 || credit.DataCreditBytes != 8 ||
		credit.WriteCompletedAt != writeCompletedAt || credit.AcknowledgedAt != now || !credit.CapacityEligible {
		t.Fatalf("DATA credit = %#v, send attachment = %#v", credit, send.Attachment)
	}
}

func TestApplicationRelayCreditsEveryRedundantCopyWithoutCapacitySample(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("copies"),
	})
	if err != nil {
		t.Fatal(err)
	}
	sends := dataMessageActions(t, actions)
	if len(sends) != 2 {
		t.Fatalf("redundant sends = %#v", sends)
	}
	for index, send := range sends {
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendAdmitted, Generation: send.Generation, LaneGeneration: uint64(71 + index),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendResult, Generation: send.Generation, AttemptOutcome: flow.AttemptSucceeded,
			WriteCompletedAt: time.Unix(1000, int64(index+1)), CapacityEligible: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 6},
	})
	if err != nil {
		t.Fatal(err)
	}
	credits := make(map[uint64]uint64, 2)
	for _, action := range actions {
		if action.Kind != ApplicationRelayActionDataCredit {
			continue
		}
		if action.CapacityEligible {
			t.Fatalf("redundant DATA credit was capacity eligible: %#v", action)
		}
		credits[action.LaneGeneration] += action.DataCreditBytes
	}
	if credits[71] != 6 || credits[72] != 6 || len(credits) != 2 {
		t.Fatalf("redundant DATA credits = %#v", credits)
	}
}

func TestApplicationRelayCreditsOldGenerationWhenACKPrecedesWriteCompletion(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("retry"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := dataMessageActions(t, actions)
	if len(first) != 2 {
		t.Fatalf("initial sends = %#v", first)
	}
	for index, send := range first {
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendAdmitted, Generation: send.Generation, LaneGeneration: uint64(81 + index),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: first[0].Generation, AttemptOutcome: flow.AttemptSucceeded,
		WriteCompletedAt: time.Unix(1100, 1), CapacityEligible: true,
	}); err != nil {
		t.Fatal(err)
	}
	retryGeneration := relay.Snapshot().Flow.RetryGeneration
	actions, err = relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	retried := dataMessageActions(t, actions)
	if len(retried) != 2 {
		t.Fatalf("retried sends = %#v", retried)
	}
	for index, send := range retried {
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendAdmitted, Generation: send.Generation, LaneGeneration: uint64(91 + index),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendResult, Generation: send.Generation, AttemptOutcome: flow.AttemptSucceeded,
			WriteCompletedAt: time.Unix(1100, int64(index+2)), CapacityEligible: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	credits := make(map[uint64]uint64, 4)
	for _, action := range actions {
		if action.Kind == ApplicationRelayActionDataCredit {
			credits[action.LaneGeneration] += action.DataCreditBytes
		}
	}
	lateActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: first[1].Generation, AttemptOutcome: flow.AttemptSucceeded,
		WriteCompletedAt: time.Unix(1100, 4), CapacityEligible: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range lateActions {
		if action.Kind == ApplicationRelayActionDataCredit {
			credits[action.LaneGeneration] += action.DataCreditBytes
		}
	}
	if credits[81] != 5 || credits[82] != 5 || credits[91] != 5 || credits[92] != 5 || len(credits) != 4 {
		t.Fatalf("cross-generation DATA credits = %#v", credits)
	}
}

func TestApplicationRelayResolvesDistributedAssignmentExactlyOnceAtAdmission(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	firstActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("first"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireMessageAction[protocol.Data](t, firstActions)
	if assigned := applicationRelayAssignedBytes(relay.Snapshot()); assigned != 5 {
		t.Fatalf("assigned bytes before admission = %d, want 5", assigned)
	}
	admitted := ApplicationRelayEvent{
		Kind: ApplicationRelaySendAdmitted, Generation: first.Generation,
		Quality: policy.QualitySnapshot{SRTT: time.Millisecond, CapacityBytesSec: 1 << 20, QueuedBytes: 5},
	}
	if _, err := relay.Handle(admitted); err != nil {
		t.Fatal(err)
	}
	if assigned := applicationRelayAssignedBytes(relay.Snapshot()); assigned != 0 {
		t.Fatalf("assigned bytes after admission = %d, want 0", assigned)
	}
	secondActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("next"),
	})
	if err != nil {
		t.Fatal(err)
	}
	second := requireMessageAction[protocol.Data](t, secondActions)
	beforeDuplicate := applicationRelayAssignedBytes(relay.Snapshot())
	if beforeDuplicate != 4 {
		t.Fatalf("second assigned bytes = %d, want 4", beforeDuplicate)
	}
	if _, err := relay.Handle(admitted); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: first.Generation, AttemptOutcome: flow.AttemptSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if assigned := applicationRelayAssignedBytes(relay.Snapshot()); assigned != beforeDuplicate {
		t.Fatalf("duplicate admission or completion changed assigned bytes: got %d, want %d", assigned, beforeDuplicate)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: second.Generation, AttemptOutcome: flow.AttemptFailed,
	}); err != nil {
		t.Fatal(err)
	}
	if assigned := applicationRelayAssignedBytes(relay.Snapshot()); assigned != 0 {
		t.Fatalf("assigned bytes after admission failure = %d, want 0", assigned)
	}
}

func TestApplicationRelayCreditsSuccessfulAttemptAfterOverlappingRetry(t *testing.T) {
	now := time.Unix(950, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("retry"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireMessageAction[protocol.Data](t, actions)
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: first.Generation,
		AttemptOutcome: flow.AttemptSucceeded, WriteCompletedAt: now.Add(time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	retryGeneration := relay.Snapshot().Flow.RetryGeneration
	actions, err = relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	if retried := dataMessageActions(t, actions); len(retried) != 0 {
		t.Fatalf("retry without alternate attachment sent DATA = %#v", actions)
	}
	now = now.Add(20 * time.Millisecond)
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: first.Attachment,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	credit := requireRelayAction(t, actions, ApplicationRelayActionDataCredit)
	if credit.Attachment != first.Attachment || credit.DataCreditBytes != 5 || credit.CapacityEligible {
		t.Fatalf("overlapping retry DATA credit = %#v", credit)
	}
}

func TestApplicationRelayCreditsSuccessfulRecoveryAttempt(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	firstActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("retry"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireMessageAction[protocol.Data](t, firstActions)
	retryGeneration := relay.Snapshot().Flow.RetryGeneration
	retryActions, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	retried := requireMessageAction[protocol.Data](t, retryActions)
	if retried.Attachment == first.Attachment || retried.AttemptGeneration != first.AttemptGeneration+1 {
		t.Fatalf("recovery attempt = %#v, first = %#v", retried, first)
	}
	writeCompletedAt := time.Unix(500, 10*int64(time.Millisecond))
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: retried.Generation,
		AttemptOutcome: flow.AttemptSucceeded, WriteCompletedAt: writeCompletedAt,
	}); err != nil {
		t.Fatal(err)
	}
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: retried.Attachment,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	credit := requireRelayAction(t, actions, ApplicationRelayActionDataCredit)
	if credit.Attachment != retried.Attachment || credit.DataCreditBytes != 5 || credit.WriteCompletedAt != writeCompletedAt {
		t.Fatalf("recovery DATA credit = %#v, retry = %#v", credit, retried)
	}
}

func applicationRelayAssignedBytes(snapshot ApplicationRelaySnapshot) uint64 {
	var assigned uint64
	for _, attachment := range snapshot.Policy.Attachments {
		assigned += attachment.Assigned
	}
	return assigned
}

func TestApplicationRelayACKRearmsRetryWithLearnedPathEstimate(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayObserveDataQuality, Attachment: testRelayA,
		RTT: 20 * time.Millisecond, Bytes: 1 << 20, Interval: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySetAttachmentLoad, Attachment: testRelayA, QueuedBytes: 256 << 10,
	}); err != nil {
		t.Fatal(err)
	}
	wantRetry := 20*time.Millisecond + time.Duration((256<<10)+len("abcdef"))*time.Second/time.Duration(1<<20)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	initial := requireRelayAction(t, actions, ApplicationRelayActionArmRetryDeadline)
	if initial.After != wantRetry {
		t.Fatalf("initial learned retry = %v, want %v", initial.After, wantRetry)
	}

	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	rearmed := requireRelayAction(t, actions, ApplicationRelayActionArmRetryDeadline)
	if rearmed.After != wantRetry {
		t.Fatalf("rearmed learned retry = %v, want %v", rearmed.After, wantRetry)
	}
}

func TestApplicationRelayCapacityShiftPreservesPendingData(t *testing.T) {
	now := time.Unix(340, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	qualities := map[flow.AttachmentKey]policy.QualitySnapshot{
		testRelayA: {DataSamples: 1, DataSampleFresh: true, SRTT: 4 * time.Millisecond, CapacityBytesSec: 1_250_000},
		testRelayB: {DataSamples: 1, DataSampleFresh: true, SRTT: 24 * time.Millisecond, CapacityBytesSec: 625_000},
	}
	for index, want := range []flow.AttachmentKey{testRelayA, testRelayA, testRelayB} {
		if index == 1 {
			a := qualities[testRelayA]
			a.CapacityBytesSec, a.DataSamples = 250_000, 2
			qualities[testRelayA] = a
		}
		if _, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelaySetSessionQualities, SessionQualities: qualities}); err != nil {
			t.Fatal(err)
		}
		actions, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: make([]byte, 32<<10),
		})
		if err != nil {
			t.Fatal(err)
		}
		send := requireMessageAction[protocol.Data](t, actions)
		if send.Attachment != want || countRelayActions(actions, ApplicationRelayActionSendMessage) != 1 {
			t.Fatalf("step %d DATA placement = %+v, want %+v", index, actions, want)
		}
		if machine.TxReplayBytes() != uint64(index+1)*(32<<10) {
			t.Fatalf("capacity switch changed unacknowledged DATA: %d", machine.TxReplayBytes())
		}
		now = now.Add(300 * time.Millisecond)
	}
}

func TestApplicationRelayRetryDeadlineFollowsEarliestUnacknowledgedItem(t *testing.T) {
	now := time.Unix(350, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	if _, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelaySetSessionQualities, SessionQualities: map[flow.AttachmentKey]policy.QualitySnapshot{
		testRelayA: {SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20, QueuedBytes: 2 << 20, RetryEstimate: policy.MaximumRetryEstimate},
		testRelayB: {SRTT: 3 * time.Second, CapacityBytesSec: 1 << 20, RetryEstimate: policy.MaximumRetryEstimate},
	}}); err != nil {
		t.Fatal(err)
	}
	firstActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("first"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireMessageAction[protocol.Data](t, firstActions)
	if first.Attachment != testRelayA {
		t.Fatalf("first DATA attachment = %#v, want %#v", first.Attachment, testRelayA)
	}
	firstRetry := requireRelayAction(t, firstActions, ApplicationRelayActionArmRetryDeadline).After
	if firstRetry <= policy.MaximumRetryEstimate {
		t.Fatalf("first retry estimate = %s, want queued delivery estimate above the RTT bound", firstRetry)
	}

	if _, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelaySetSessionQuality, Attachment: testRelayA, Quality: policy.QualitySnapshot{
		SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20, StallPenalty: 5 * time.Second, RetryEstimate: policy.MaximumRetryEstimate,
	}}); err != nil {
		t.Fatal(err)
	}
	bRetryRTT := firstRetry - time.Duration(len("first"))*time.Second/time.Duration(1<<20)
	if _, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelaySetSessionQuality, Attachment: testRelayB, Quality: policy.QualitySnapshot{
		SRTT: bRetryRTT, CapacityBytesSec: 1 << 20, RetryEstimate: policy.MaximumRetryEstimate,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayReleaseAttachment, Attachment: testRelayA}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelaySetSessionQuality, Attachment: testRelayB, Quality: policy.QualitySnapshot{
		SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20, RetryEstimate: policy.MinimumRetryEstimate,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("second"),
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(300 * time.Millisecond)
	thirdActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("third"),
	})
	if err != nil {
		t.Fatal(err)
	}
	third := requireMessageAction[protocol.Data](t, thirdActions)
	if third.Attachment != testRelayB {
		t.Fatalf("third DATA attachment = %#v, want %#v", third.Attachment, testRelayB)
	}

	ackActions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayB,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 0, Ranges: []protocol.ACKRange{{Start: 11, End: 16}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rearmed := requireRelayAction(t, ackActions, ApplicationRelayActionArmRetryDeadline)
	if rearmed.After != firstRetry {
		t.Fatalf("retry after selective ACK = %s, want earliest gap estimate %s", rearmed.After, firstRetry)
	}
}

func TestApplicationRelayAttachmentLossPausesReadsAndRecoveryReplaysPendingData(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	readGeneration := relay.Snapshot().ApplicationReadGeneration
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySessionClosed, Generation: testRelayA.SessionGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionAttachmentWithdrawn) != 1 {
		t.Fatalf("withdraw actions = %#v", actions)
	}
	if snapshot := relay.Snapshot(); !snapshot.ApplicationReadPaused || snapshot.ApplicationReadGeneration != readGeneration ||
		snapshot.Flow.Lifecycle.State != flow.Recovering {
		t.Fatalf("recovering snapshot = %#v", snapshot)
	}

	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: readGeneration, Data: []byte("held"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messageAttachments[protocol.Data](t, actions)) != 0 || countRelayActions(actions, ApplicationRelayActionRead) != 0 {
		t.Fatalf("actions while all attachments lost = %#v", actions)
	}
	if relay.Snapshot().Flow.TxReplayBytes != 4 {
		t.Fatal("in-flight application read was not retained in replay window")
	}

	actions = publishRelayAttachment(t, relay, machine, testRelayB)
	dataSends := messageAttachments[protocol.Data](t, actions)
	if len(dataSends) != 1 || dataSends[0] != testRelayB {
		t.Fatalf("recovery DATA attachments = %#v", dataSends)
	}
	if snapshot := relay.Snapshot(); snapshot.ApplicationReadPaused || snapshot.ApplicationReadGeneration == 0 ||
		snapshot.Flow.Lifecycle.State != flow.Relaying {
		t.Fatalf("recovered snapshot = %#v", snapshot)
	}
}

func TestApplicationRelayRemoteResetClosesApplicationWithoutReply(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Reset{FlowID: testRelayFlowID, Reason: protocol.ResetCancelled},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionClose) != 1 || len(messageAttachments[protocol.Reset](t, actions)) != 0 {
		t.Fatalf("remote RESET actions = %#v", actions)
	}
	if countRelayActions(actions, ApplicationRelayActionAttachmentWithdrawn) != 1 {
		t.Fatalf("remote RESET did not withdraw attachment: %#v", actions)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Reset || !snapshot.ApplicationCloseIssued {
		t.Fatalf("remote RESET snapshot = %#v", snapshot)
	}
}

func TestApplicationRelayLateReadAfterTerminalCloseIsIgnored(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	readGeneration := relay.Snapshot().ApplicationReadGeneration
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Reset{FlowID: testRelayFlowID, Reason: protocol.ResetCancelled},
	}); err != nil {
		t.Fatal(err)
	}
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: readGeneration, Data: []byte("late"),
	})
	if err != nil || len(actions) != 0 {
		t.Fatalf("late read actions=%#v err=%v", actions, err)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Reset || snapshot.Flow.TxAllocatedOffset != 0 {
		t.Fatalf("late read changed terminal state = %#v", snapshot)
	}
}

func TestApplicationRelayResetClearsProvisionalAndDeadlineConverges(t *testing.T) {
	relay, _ := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReserveAttachment, Attachment: testRelayA, Generation: 101,
	}); err != nil {
		t.Fatal(err)
	}
	if got := len(relay.Snapshot().Flow.Lifecycle.Provisional); got != 1 {
		t.Fatalf("provisional attachments = %d, want 1", got)
	}
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayResetRequested, ResetReason: protocol.ResetCancelled,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, ApplicationRelayActionClose) != 1 ||
		countRelayActions(actions, ApplicationRelayActionArmResetDeadline) != 1 {
		t.Fatalf("reset actions = %#v", actions)
	}
	snapshot := relay.Snapshot()
	if snapshot.Flow.Lifecycle.State != flow.Resetting || len(snapshot.Flow.Lifecycle.Provisional) != 0 {
		t.Fatalf("resetting snapshot = %#v", snapshot)
	}
	late, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayPublishAttachment, Attachment: testRelayA, Generation: 101,
	})
	if err != nil || len(late) != 0 {
		t.Fatalf("late provisional publication actions=%#v err=%v", late, err)
	}
	resetGeneration := relay.Snapshot().Flow.Lifecycle.ResetDeadlineGeneration
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayResetDeadline, Generation: resetGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	if state := relay.Snapshot().Flow.Lifecycle.State; state != flow.Reset {
		t.Fatalf("flow state after reset deadline = %v, want Reset", state)
	}
}

func TestApplicationRelaySendLedgerBackpressureDoesNotResetFlow(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishActions := publishRelayAttachment(t, relay, machine, testRelayA)
	completeRelaySends(t, relay, publishActions)

	message := protocol.Data{FlowID: testRelayFlowID, Offset: 1, Bytes: []byte("x")}
	var firstGeneration uint64
	for index := 0; index < MaxApplicationRelayPendingSends; index++ {
		var ignored []ApplicationRelayAction
		err := relay.emitSend(message, transport.FrameData, testRelayA, applicationRelayPendingSend{
			kind: applicationRelayPendingAttempt, attachment: testRelayA, itemID: uint64(index + 1), attemptGeneration: 1,
		}, &ignored)
		if err != nil || len(ignored) != 1 {
			t.Fatalf("enqueue %d: %v", index, err)
		}
		if index == 0 {
			firstGeneration = ignored[0].Generation
		}
	}
	if got := relay.Snapshot().PendingSends; got != MaxApplicationRelayPendingSends {
		t.Fatalf("pending sends = %d, want %d", got, MaxApplicationRelayPendingSends)
	}
	actions, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA, Message: message})
	if err != nil {
		t.Fatalf("backpressure error = %v", err)
	}
	if state := relay.Snapshot().Flow.Lifecycle.State; state != flow.Relaying {
		t.Fatalf("backpressure flow state = %v, want Relaying", state)
	}
	if countRelayActions(actions, ApplicationRelayActionSendMessage) != 0 || len(relay.deferredControls) != 1 {
		t.Fatalf("backpressure actions=%#v deferred=%d", actions, len(relay.deferredControls))
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: firstGeneration, AttemptOutcome: flow.AttemptSucceeded,
	})
	if err != nil || len(messageAttachments[protocol.ACK](t, actions)) != 1 {
		t.Fatalf("deferred ACK actions=%#v err=%v", actions, err)
	}
}

func TestApplicationRelayRedundantSlowCopiesBackpressureApplicationReadBeforeAttemptHistoryLimit(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	attachments := make([]flow.AttachmentKey, 6)
	for index := range attachments {
		attachments[index] = flow.AttachmentKey{
			SessionGeneration:    uint64(index + 1),
			AttachmentGeneration: uint64(index + 1),
		}
		publishRelayAttachment(t, relay, machine, attachments[index])
	}

	const chunkBytes = 16 << 10
	data := make([]byte, chunkBytes)
	held := make([]uint64, 0, MaxApplicationRelayPendingSends)
	var acknowledged uint64
	paused := false
	for iteration := 0; iteration <= MaxApplicationRelayAttemptItems; iteration++ {
		readGeneration := relay.Snapshot().ApplicationReadGeneration
		if readGeneration == 0 {
			paused = true
			break
		}
		actions, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelayReadResult, Generation: readGeneration, Data: data,
		})
		if err != nil {
			t.Fatalf("application read %d reset under redundant backpressure: %v", iteration, err)
		}
		dataActions := dataMessageActions(t, actions)
		if len(dataActions) != len(attachments) {
			t.Fatalf("application read %d produced %d DATA copies, want %d", iteration, len(dataActions), len(attachments))
		}
		fastGeneration := dataActions[0].Generation
		for _, action := range dataActions[1:] {
			held = append(held, action.Generation)
		}
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendResult, Generation: fastGeneration, AttemptOutcome: flow.AttemptSucceeded,
		}); err != nil {
			t.Fatalf("fast send %d: %v", iteration, err)
		}
		acknowledged += chunkBytes
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelayRemoteMessage, Attachment: attachments[0],
			Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: acknowledged},
		}); err != nil {
			t.Fatalf("acknowledgement %d: %v", iteration, err)
		}
	}
	if !paused {
		t.Fatal("application reads did not pause before redundant send bookkeeping exhausted")
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Relaying || snapshot.PendingSends == 0 {
		t.Fatalf("backpressured redundant application relay = %#v", snapshot)
	}

	resumed := false
	for _, generation := range held {
		actions, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendResult, Generation: generation, AttemptOutcome: flow.AttemptSucceeded,
		})
		if err != nil {
			t.Fatalf("slow send completion: %v", err)
		}
		resumed = resumed || countRelayActions(actions, ApplicationRelayActionRead) != 0
	}
	if snapshot := relay.Snapshot(); !resumed || snapshot.ApplicationReadGeneration == 0 ||
		snapshot.Flow.Lifecycle.State != flow.Relaying {
		t.Fatalf("redundant application relay did not resume after send capacity returned: %#v", snapshot)
	}
}

func TestApplicationRelayCoalescesPendingAcknowledgementsToLatestSnapshot(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Bytes: []byte("abcd")},
	})
	if err != nil {
		t.Fatal(err)
	}
	firstACK := requireMessageAction[protocol.ACK](t, actions)
	write := requireRelayAction(t, actions, ApplicationRelayActionWrite)
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayWriteResult, Generation: write.Generation, N: write.DataLen(),
	})
	if err != nil || len(messageAttachments[protocol.ACK](t, actions)) != 0 {
		t.Fatalf("coalesced ACK actions=%#v err=%v", actions, err)
	}
	if relay.Snapshot().PendingSends != 1 || len(relay.deferredControls) != 1 {
		t.Fatalf("coalesced state pending=%d deferred=%d", relay.Snapshot().PendingSends, len(relay.deferredControls))
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayObserveProbeQuality, Attachment: testRelayA, RTT: time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if relay.Snapshot().PendingSends != 1 || len(relay.deferredControls) != 1 {
		t.Fatalf("blocked replacement state pending=%d deferred=%d", relay.Snapshot().PendingSends, len(relay.deferredControls))
	}
	actions, err = relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: firstACK.Generation, AttemptOutcome: flow.AttemptSucceeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertACKOffset(t, actions, 4)
	if relay.Snapshot().PendingSends != 1 || len(relay.deferredControls) != 0 {
		t.Fatalf("replacement state pending=%d deferred=%d", relay.Snapshot().PendingSends, len(relay.deferredControls))
	}
}

func TestApplicationRelayRejectsOversizedFlowActionBatchWithoutMutation(t *testing.T) {
	relay, _ := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	actions := make([]flow.FlowAction, MaxApplicationRelayFlowActions+1)
	before := relay.Snapshot()
	produced, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayApplyFlowActions, FlowActions: actions,
	})
	if !errors.Is(err, ErrApplicationRelayActionLimit) || len(produced) != 0 {
		t.Fatalf("oversized batch actions=%#v err=%v", produced, err)
	}
	after := relay.Snapshot()
	if after.Flow.Lifecycle.State != before.Flow.Lifecycle.State || after.PendingSends != before.PendingSends ||
		after.ApplicationReadGeneration != before.ApplicationReadGeneration {
		t.Fatalf("oversized batch mutated relay: before=%#v after=%#v", before, after)
	}
}

func TestApplicationRelayInvalidSendOutcomeDoesNotConsumeCurrentGeneration(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration, Data: []byte("pending"),
	})
	if err != nil {
		t.Fatal(err)
	}
	send := requireMessageAction[protocol.Data](t, actions)
	pendingBefore := relay.Snapshot().PendingSends
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: send.Generation,
	}); !errors.Is(err, ErrInvalidApplicationRelayEvent) {
		t.Fatalf("invalid send outcome error = %v", err)
	}
	if got := relay.Snapshot().PendingSends; got != pendingBefore {
		t.Fatalf("pending sends after invalid outcome = %d, want %d", got, pendingBefore)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelaySendResult, Generation: send.Generation, AttemptOutcome: flow.AttemptSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if got := relay.Snapshot().PendingSends; got != pendingBefore-1 {
		t.Fatalf("pending sends after valid outcome = %d, want %d", got, pendingBefore-1)
	}
}

func TestApplicationRelayRejectsZeroByteNilErrorApplicationRead(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind: ApplicationRelayReadResult, Generation: relay.Snapshot().ApplicationReadGeneration,
	})
	if !errors.Is(err, ErrInvalidApplicationRead) {
		t.Fatalf("read error = %v", err)
	}
	if countRelayActions(actions, ApplicationRelayActionClose) != 1 {
		t.Fatalf("invalid read actions = %#v", actions)
	}
}

func TestApplicationRelayGenerationExhaustionIsBounded(t *testing.T) {
	relay, _ := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	relay.nextGeneration = math.MaxUint64
	if generation, err := relay.allocateGeneration(); generation != 0 || !errors.Is(err, ErrApplicationRelayGenerationExhaust) {
		t.Fatalf("generation=%d err=%v", generation, err)
	}
}

func newRelayFixture(t *testing.T, config policy.Config) (*ApplicationRelay, *flow.Flow) {
	return newRelayFixtureWithClock(t, config, time.Now)
}

func newRelayFixtureWithClock(t *testing.T, config policy.Config, now func() time.Time) (*ApplicationRelay, *flow.Flow) {
	t.Helper()
	machine := flow.NewFlow()
	relay, err := NewApplicationRelayWithClock(testRelayFlowID, config, machine, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayStart}); err != nil {
		t.Fatal(err)
	}
	return relay, machine
}

func publishRelayAttachment(t *testing.T, relay *ApplicationRelay, machine *flow.Flow, attachment flow.AttachmentKey) []ApplicationRelayAction {
	t.Helper()
	actions := publishRelayAttachmentWithoutEnable(t, relay, machine, attachment)
	if !relay.Snapshot().ApplicationEnabled {
		enabled, err := relay.Handle(ApplicationRelayEvent{Kind: ApplicationRelayEnableApplication})
		if err != nil {
			t.Fatal(err)
		}
		actions = append(actions, enabled...)
	}
	completeControlSends(t, relay, actions)
	return actions
}

func publishRelayAttachmentWithoutEnable(t *testing.T, relay *ApplicationRelay, _ *flow.Flow, attachment flow.AttachmentKey) []ApplicationRelayAction {
	t.Helper()
	requestGeneration := attachment.AttachmentGeneration
	reserved, err := relay.Handle(ApplicationRelayEvent{
		Kind:       ApplicationRelayReserveAttachment,
		Generation: requestGeneration,
		Attachment: attachment,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reserved) != 0 {
		t.Fatalf("attachment reservation actions = %#v", reserved)
	}
	actions, err := relay.Handle(ApplicationRelayEvent{
		Kind:       ApplicationRelayPublishAttachment,
		Generation: requestGeneration,
		Attachment: attachment,
	})
	if err != nil {
		t.Fatal(err)
	}
	return actions
}

func requireRelayAction(t *testing.T, actions []ApplicationRelayAction, kind ApplicationRelayActionKind) ApplicationRelayAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("missing relay action %v in %#v", kind, actions)
	return ApplicationRelayAction{}
}

func requireMessageAction[T protocol.Message](t *testing.T, actions []ApplicationRelayAction) ApplicationRelayAction {
	t.Helper()
	for _, action := range actions {
		if _, ok := applicationRelayActionMessage(t, action).(T); ok {
			return action
		}
	}
	t.Fatalf("missing message %T in %#v", *new(T), actions)
	return ApplicationRelayAction{}
}

func messageAttachments[T protocol.Message](t *testing.T, actions []ApplicationRelayAction) []flow.AttachmentKey {
	t.Helper()
	var attachments []flow.AttachmentKey
	for _, action := range actions {
		if _, ok := applicationRelayActionMessage(t, action).(T); ok {
			attachments = append(attachments, action.Attachment)
		}
	}
	return attachments
}

func dataMessageActions(t *testing.T, actions []ApplicationRelayAction) []ApplicationRelayAction {
	t.Helper()
	result := make([]ApplicationRelayAction, 0, flow.MaxAttachments)
	for _, action := range actions {
		if _, ok := applicationRelayActionMessage(t, action).(protocol.Data); ok {
			result = append(result, action)
		}
	}
	return result
}

func applicationRelayActionMessage(t *testing.T, action ApplicationRelayAction) protocol.Message {
	t.Helper()
	if action.Message != nil || len(action.Encoded) == 0 {
		return action.Message
	}
	_, message, err := protocol.DecodeEncodedFrame(action.Encoded)
	if err != nil {
		t.Fatalf("encoded application relay action: %v", err)
	}
	return message
}

func assertAttachmentSet(t *testing.T, got []flow.AttachmentKey, want ...flow.AttachmentKey) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("attachments = %#v, want %#v", got, want)
	}
	remaining := append([]flow.AttachmentKey(nil), want...)
	for _, attachment := range got {
		found := false
		for index, candidate := range remaining {
			if attachment == candidate {
				remaining = append(remaining[:index], remaining[index+1:]...)
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unexpected attachment %#v in %#v", attachment, got)
		}
	}
}

func assertACKOffset(t *testing.T, actions []ApplicationRelayAction, want uint64) {
	t.Helper()
	action := requireMessageAction[protocol.ACK](t, actions)
	if got := action.Message.(protocol.ACK).NextOffset; got != want {
		t.Fatalf("ACK NextOffset = %d, want %d", got, want)
	}
}

func countRelayActions(actions []ApplicationRelayAction, kind ApplicationRelayActionKind) int {
	count := 0
	for _, action := range actions {
		if action.Kind == kind {
			count++
		}
	}
	return count
}

func completeRelaySends(t *testing.T, relay *ApplicationRelay, actions []ApplicationRelayAction) {
	t.Helper()
	for _, action := range actions {
		if action.Kind != ApplicationRelayActionSendMessage {
			continue
		}
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendResult, Generation: action.Generation, AttemptOutcome: flow.AttemptSucceeded,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func completeControlSends(t *testing.T, relay *ApplicationRelay, actions []ApplicationRelayAction) {
	t.Helper()
	for _, action := range actions {
		switch action.Message.(type) {
		case protocol.ACK, protocol.FINACK:
		default:
			continue
		}
		if _, err := relay.Handle(ApplicationRelayEvent{
			Kind: ApplicationRelaySendResult, Generation: action.Generation, AttemptOutcome: flow.AttemptSucceeded,
		}); err != nil {
			t.Fatal(err)
		}
	}
}
