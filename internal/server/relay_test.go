package server

import (
	"errors"
	"io"
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

func TestRelayRedundantTargetReadMapsDataAndEOFToBothAttachments(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)

	readGeneration := relay.Snapshot().TargetReadGeneration
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: readGeneration, Data: []byte("reply"), Err: io.EOF,
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
		if message, ok := relayActionMessage(t, action).(protocol.Data); ok {
			if string(message.Bytes) != "reply" {
				t.Fatalf("DATA bytes = %q", message.Bytes)
			}
			dataFrames = append(dataFrames, action.Encoded)
		}
		if action.Kind == RelayActionReadTarget {
			t.Fatal("read scheduled after local EOF")
		}
		if action.Kind == RelayActionSendMessage && action.Generation == 0 {
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

func TestRelayDataSendOwnsEncodedFrameAfterReadBufferChanges(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	input := []byte("owned-frame")
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: input,
	})
	if err != nil {
		t.Fatal(err)
	}
	action := requireMessageAction[protocol.Data](t, actions)
	if action.Message != nil || action.Class != transport.FrameData || action.SendDataBytes != len(input) || len(action.Encoded) == 0 {
		t.Fatalf("DATA action ownership = %#v", action)
	}
	input[0] = 'X'
	message := relayActionMessage(t, action).(protocol.Data)
	if string(message.Bytes) != "owned-frame" {
		t.Fatalf("encoded DATA changed with read buffer = %q", message.Bytes)
	}
}

func TestRelayAdaptiveFastestPlacesNewDataOnceAcrossTwoAttachments(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)

	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("one-copy"),
	})
	if err != nil {
		t.Fatal(err)
	}
	attachments := messageAttachments[protocol.Data](t, actions)
	if len(attachments) != 1 || attachments[0] != testRelayA && attachments[0] != testRelayB {
		t.Fatalf("fastest DATA attachments = %#v", attachments)
	}
	if countRelayActions(actions, RelayActionReadTarget) != 1 {
		t.Fatalf("next target reads = %d, want 1", countRelayActions(actions, RelayActionReadTarget))
	}
}

func TestRelayAdaptiveFastestReturnsInitialDataOnIngressAttachment(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	if _, err := relay.Handle(RelayEvent{Kind: RelaySetSessionQualities, SessionQualities: map[flow.AttachmentKey]policy.QualitySnapshot{
		testRelayA: {SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20, QueuedBytes: 2 << 20},
		testRelayB: {SRTT: 100 * time.Millisecond, CapacityBytesSec: 1 << 20},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Offset: 0, Bytes: []byte("request")},
	}); err != nil {
		t.Fatal(err)
	}
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("response"),
	})
	if err != nil {
		t.Fatal(err)
	}
	data := requireMessageAction[protocol.Data](t, actions)
	if data.Attachment != testRelayA {
		t.Fatalf("initial response attachment = %#v, want ingress %#v", data.Attachment, testRelayA)
	}
}

func TestRelayAdaptiveFirstRetryFansOutThenEscalates(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	third := flow.AttachmentKey{SessionGeneration: 3, AttachmentGeneration: 33}
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	publishRelayAttachment(t, relay, machine, third)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("gap"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := messageAttachments[protocol.Data](t, actions)
	if len(first) != 1 {
		t.Fatalf("initial placements = %#v", first)
	}

	retryGeneration := relay.Snapshot().Flow.RetryGeneration
	actions, err = relay.Handle(RelayEvent{Kind: RelayRetryDeadline, Generation: retryGeneration})
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
		t.Fatalf("policy state after targeted retry = %v", state)
	}

	retryGeneration = relay.Snapshot().Flow.RetryGeneration
	actions, err = relay.Handle(RelayEvent{Kind: RelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	assertAttachmentSet(t, messageAttachments[protocol.Data](t, actions), testRelayA, testRelayB, third)
	if state := relay.Snapshot().Policy.State; state != policy.AdaptiveFull {
		t.Fatalf("policy state after full retry = %v", state)
	}
}

func TestRelayQualityEventsDriveFastestPlacementAndRetryEstimate(t *testing.T) {
	now := time.Unix(300, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	for _, event := range []RelayEvent{
		{Kind: RelayObserveDataQuality, Attachment: testRelayA, RTT: 20 * time.Millisecond, Bytes: 1 << 20, Interval: time.Second},
		{Kind: RelayObserveDataQuality, Attachment: testRelayB, RTT: 80 * time.Millisecond, Bytes: 1 << 20, Interval: time.Second},
		{Kind: RelaySetAttachmentLoad, Attachment: testRelayA, QueuedBytes: 256 << 10},
		{Kind: RelaySetAttachmentLoad, Attachment: testRelayB, QueuedBytes: 256 << 10},
	} {
		if _, err := relay.Handle(event); err != nil {
			t.Fatal(err)
		}
	}
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("quality"),
	})
	if err != nil {
		t.Fatal(err)
	}
	attachments := messageAttachments[protocol.Data](t, actions)
	if len(attachments) != 1 || attachments[0] != testRelayA {
		t.Fatalf("quality placements = %#v", attachments)
	}
	retry := requireRelayAction(t, actions, RelayActionArmRetryDeadline)
	wantRetry := 20*time.Millisecond + time.Duration((256<<10)+len("quality"))*time.Second/time.Duration(1<<20)
	if retry.After != wantRetry {
		t.Fatalf("retry estimate = %s, want delivery-aware estimate %s", retry.After, wantRetry)
	}

	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySetAttachmentLoad, Attachment: testRelayA, QueuedBytes: 2 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	actions, err = relay.Handle(RelayEvent{
		Kind: RelaySetStallPenalty, Attachment: testRelayA, StallPenalty: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	attachments = messageAttachments[protocol.Data](t, actions)
	if len(attachments) != 1 || attachments[0] != testRelayB {
		t.Fatalf("stall did not immediately recover outstanding DATA = %#v", attachments)
	}
}

func TestRelayLateTelemetryForWithdrawnAttachmentIsHarmless(t *testing.T) {
	tests := []struct {
		name  string
		event RelayEvent
	}{
		{name: "data quality", event: RelayEvent{Kind: RelayObserveDataQuality, Attachment: testRelayA, RTT: 20 * time.Millisecond, Bytes: 1024, Interval: time.Second}},
		{name: "probe quality", event: RelayEvent{Kind: RelayObserveProbeQuality, Attachment: testRelayA, RTT: 20 * time.Millisecond}},
		{name: "attachment load", event: RelayEvent{Kind: RelaySetAttachmentLoad, Attachment: testRelayA, QueuedBytes: 1024, InFlightBytes: 512}},
		{name: "stall penalty", event: RelayEvent{Kind: RelaySetStallPenalty, Attachment: testRelayA, StallPenalty: time.Second}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			relay, machine := newRelayFixture(t, policy.Config{
				Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
			})
			publishRelayAttachment(t, relay, machine, testRelayA)
			publishRelayAttachment(t, relay, machine, testRelayB)
			flowActions, err := machine.Handle(flow.FlowEvent{
				Kind: flow.FlowLifecycle,
				Lifecycle: flow.LifecycleEvent{
					Kind: flow.LifecycleAttachmentLost, Attachment: testRelayA,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := relay.Handle(RelayEvent{Kind: RelayApplyFlowActions, FlowActions: flowActions}); err != nil {
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

func TestRelayWithdrawnAttachmentStillRejectsNonTelemetryAndPreservesOtherErrors(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	flowActions, err := machine.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind: flow.LifecycleAttachmentLost, Attachment: testRelayA,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{Kind: RelayApplyFlowActions, FlowActions: flowActions}); err != nil {
		t.Fatal(err)
	}
	before := relay.Snapshot()
	if _, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.ACK{FlowID: testRelayFlowID},
	}); !errors.Is(err, ErrInvalidRelayEvent) {
		t.Fatalf("withdrawn non-telemetry error = %v", err)
	}
	if after := relay.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("withdrawn non-telemetry mutated relay: before=%#v after=%#v", before, after)
	}
	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySetStallPenalty, Attachment: testRelayB, StallPenalty: -1,
	}); !errors.Is(err, policy.ErrInvalidSample) {
		t.Fatalf("known attachment telemetry error = %v", err)
	}
}

func TestRelayRemoteDataSerializesTargetShortWritesAndACKsActualProgress(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	incoming := []byte("abcd")
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Offset: 0, Bytes: incoming},
	})
	if err != nil {
		t.Fatal(err)
	}
	write := requireRelayAction(t, actions, RelayActionWriteTarget)
	completeControlSends(t, relay, actions)
	if write.Offset != 0 || string(write.CopyData()) != "abcd" {
		t.Fatalf("initial write = offset %d data %q", write.Offset, write.CopyData())
	}
	incoming[0] = 'X'
	if string(write.CopyData()) != "abcd" {
		t.Fatalf("target write aliases inbound DATA: %q", write.CopyData())
	}

	staleActions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetWriteResult, Generation: write.Generation + 1, N: 4,
	})
	if err != nil || len(staleActions) != 0 || relay.Snapshot().TargetWriteGeneration != write.Generation {
		t.Fatalf("stale write result actions=%#v err=%v snapshot=%#v", staleActions, err, relay.Snapshot())
	}

	actions, err = relay.Handle(RelayEvent{
		Kind: RelayTargetWriteResult, Generation: write.Generation, N: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertACKOffset(t, actions, 2)
	write = requireRelayAction(t, actions, RelayActionWriteTarget)
	completeControlSends(t, relay, actions)
	if write.Offset != 2 || string(write.CopyData()) != "cd" {
		t.Fatalf("short-write continuation = offset %d data %q", write.Offset, write.CopyData())
	}
	if countRelayActions(actions, RelayActionWriteTarget) != 1 {
		t.Fatalf("concurrent target writes = %d", countRelayActions(actions, RelayActionWriteTarget))
	}

	actions, err = relay.Handle(RelayEvent{
		Kind: RelayTargetWriteResult, Generation: write.Generation, N: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertACKOffset(t, actions, 4)
	if snapshot := relay.Snapshot(); snapshot.TargetWriteGeneration != 0 || snapshot.Flow.Rx.WrittenOffset != 4 {
		t.Fatalf("completed target write snapshot = %#v", snapshot)
	}
}

func TestRelayAdaptiveFastestSendsAcknowledgementOnce(t *testing.T) {
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

	actions, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: attachments[0],
		Message: protocol.Data{FlowID: testRelayFlowID, Bytes: []byte("data")},
	})
	if err != nil {
		t.Fatal(err)
	}
	acknowledgements := messageAttachments[protocol.ACK](t, actions)
	if len(acknowledgements) != 1 {
		t.Fatalf("adaptive fastest ACK copies = %d, want 1: %#v", len(acknowledgements), actions)
	}
	if countRelayActions(actions, RelayActionWriteTarget) != 1 {
		t.Fatalf("adaptive fastest target write actions = %#v", actions)
	}
}

func TestRelayTargetWriteProgressWithErrorACKsPrefixThenResets(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Bytes: []byte("abcd")},
	})
	if err != nil {
		t.Fatal(err)
	}
	write := requireRelayAction(t, actions, RelayActionWriteTarget)
	writeFailure := errors.New("target write failed")
	actions, err = relay.Handle(RelayEvent{
		Kind: RelayTargetWriteResult, Generation: write.Generation, N: 2, Err: writeFailure,
	})
	if !errors.Is(err, writeFailure) {
		t.Fatalf("write error = %v, want %v", err, writeFailure)
	}
	assertACKOffset(t, actions, 2)
	if countRelayActions(actions, RelayActionCloseTarget) != 1 {
		t.Fatalf("close target actions = %d", countRelayActions(actions, RelayActionCloseTarget))
	}
	resetSend := requireMessageAction[protocol.Reset](t, actions)
	if resetSend.Message.(protocol.Reset).Reason != protocol.ResetLocalIOFailure {
		t.Fatalf("RESET = %#v", resetSend.Message)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Resetting || !snapshot.TargetCloseIssued {
		t.Fatalf("resetting snapshot = %#v", snapshot)
	}

	terminalActions, err := relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: resetSend.Generation, AttemptOutcome: flow.AttemptSucceeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(terminalActions, RelayActionCloseTarget) != 0 {
		t.Fatal("target closed twice")
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Reset || snapshot.PendingSends != 0 {
		t.Fatalf("terminal snapshot = %#v", snapshot)
	}
}

func TestRelayFINCloseWriteFINACKAndClosing(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)

	actions, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.FIN{FlowID: testRelayFlowID, FinalOffset: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	closeWrite := requireRelayAction(t, actions, RelayActionCloseWriteTarget)
	completeControlSends(t, relay, actions)
	if closeWrite.FinalOffset != 0 {
		t.Fatalf("close-write final offset = %d", closeWrite.FinalOffset)
	}
	stale, err := relay.Handle(RelayEvent{
		Kind: RelayTargetCloseWriteResult, Generation: closeWrite.Generation + 1,
	})
	if err != nil || len(stale) != 0 {
		t.Fatalf("stale close-write actions=%#v err=%v", stale, err)
	}
	actions, err = relay.Handle(RelayEvent{
		Kind: RelayTargetCloseWriteResult, Generation: closeWrite.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireMessageAction[protocol.FINACK](t, actions)

	readGeneration := relay.Snapshot().TargetReadGeneration
	actions, err = relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: readGeneration, Err: io.EOF,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireMessageAction[protocol.FIN](t, actions)
	actions, err = relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.FINACK{FlowID: testRelayFlowID, FinalOffset: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, RelayActionCloseTarget) != 1 || countRelayActions(actions, RelayActionArmClosingDeadline) != 1 {
		t.Fatalf("closing actions = %#v", actions)
	}
	if state := relay.Snapshot().Flow.Lifecycle.State; state != flow.Closing {
		t.Fatalf("flow state = %v, want Closing", state)
	}
}

func TestRelayACKReleasesReverseReplayAndLateSendResultIsHarmless(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("response"),
	})
	if err != nil {
		t.Fatal(err)
	}
	dataSend := requireMessageAction[protocol.Data](t, actions)
	actions, err = relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.TxReplayBytes != 0 || snapshot.Flow.TxAcknowledgedOffset != 8 {
		t.Fatalf("ACK snapshot = %#v", snapshot)
	}
	if countRelayActions(actions, RelayActionCancelRetryDeadline) != 1 ||
		countRelayActions(actions, RelayActionCancelNoProgressDeadline) != 1 {
		t.Fatalf("ACK deadline actions = %#v", actions)
	}
	actions, err = relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: dataSend.Generation, AttemptOutcome: flow.AttemptSucceeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	if relay.Snapshot().Flow.TxReplayBytes != 0 {
		t.Fatal("late local send completion recreated replay")
	}
	credit := requireRelayAction(t, actions, RelayActionDataCredit)
	if credit.Attachment != dataSend.Attachment || credit.DataCreditBytes != 8 ||
		credit.WriteCompletedAt.IsZero() || credit.AcknowledgedAt.Before(credit.WriteCompletedAt) {
		t.Fatalf("late send completion DATA credit = %#v", credit)
	}
}

func TestRelayCreditsUniqueAttemptToSendingAttachment(t *testing.T) {
	now := time.Unix(900, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("credited"),
	})
	if err != nil {
		t.Fatal(err)
	}
	send := requireMessageAction[protocol.Data](t, actions)
	writeCompletedAt := now.Add(10 * time.Millisecond)
	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: send.Generation,
		AttemptOutcome: flow.AttemptSucceeded, WriteCompletedAt: writeCompletedAt,
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(20 * time.Millisecond)
	ackAttachment := testRelayA
	if send.Attachment == ackAttachment {
		ackAttachment = testRelayB
	}
	actions, err = relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: ackAttachment,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	credit := requireRelayAction(t, actions, RelayActionDataCredit)
	if credit.Attachment != send.Attachment || credit.DataCreditBytes != 8 ||
		credit.WriteCompletedAt != writeCompletedAt || credit.AcknowledgedAt != now {
		t.Fatalf("DATA credit = %#v, send attachment = %#v", credit, send.Attachment)
	}
}

func TestRelayResolvesDistributedAssignmentExactlyOnceAtAdmission(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathDistributed,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	firstActions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("first"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireMessageAction[protocol.Data](t, firstActions)
	if assigned := relayAssignedBytes(relay.Snapshot()); assigned != 5 {
		t.Fatalf("assigned bytes before admission = %d, want 5", assigned)
	}
	admitted := RelayEvent{
		Kind: RelaySendAdmitted, Generation: first.Generation,
		Quality: policy.QualitySnapshot{SRTT: time.Millisecond, CapacityBytesSec: 1 << 20, QueuedBytes: 5},
	}
	if _, err := relay.Handle(admitted); err != nil {
		t.Fatal(err)
	}
	if assigned := relayAssignedBytes(relay.Snapshot()); assigned != 0 {
		t.Fatalf("assigned bytes after admission = %d, want 0", assigned)
	}
	secondActions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("next"),
	})
	if err != nil {
		t.Fatal(err)
	}
	second := requireMessageAction[protocol.Data](t, secondActions)
	beforeDuplicate := relayAssignedBytes(relay.Snapshot())
	if beforeDuplicate != 4 {
		t.Fatalf("second assigned bytes = %d, want 4", beforeDuplicate)
	}
	if _, err := relay.Handle(admitted); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: first.Generation, AttemptOutcome: flow.AttemptSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	if assigned := relayAssignedBytes(relay.Snapshot()); assigned != beforeDuplicate {
		t.Fatalf("duplicate admission or completion changed assigned bytes: got %d, want %d", assigned, beforeDuplicate)
	}
	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: second.Generation, AttemptOutcome: flow.AttemptFailed,
	}); err != nil {
		t.Fatal(err)
	}
	if assigned := relayAssignedBytes(relay.Snapshot()); assigned != 0 {
		t.Fatalf("assigned bytes after admission failure = %d, want 0", assigned)
	}
}

func TestRelayRejectsDataCreditAfterOverlappingRetry(t *testing.T) {
	now := time.Unix(950, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("retry"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireMessageAction[protocol.Data](t, actions)
	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: first.Generation,
		AttemptOutcome: flow.AttemptSucceeded, WriteCompletedAt: now.Add(time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	retryGeneration := relay.Snapshot().Flow.RetryGeneration
	actions, err = relay.Handle(RelayEvent{Kind: RelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	if retried := messageAttachments[protocol.Data](t, actions); len(retried) != 0 {
		t.Fatalf("retry without alternate attachment sent DATA = %#v", actions)
	}
	now = now.Add(20 * time.Millisecond)
	actions, err = relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: first.Attachment,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, RelayActionDataCredit) != 0 {
		t.Fatalf("overlapping retry produced DATA credit = %#v", actions)
	}
}

func TestRelayCreditsSuccessfulRecoveryAttempt(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	firstActions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("retry"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireMessageAction[protocol.Data](t, firstActions)
	retryGeneration := relay.Snapshot().Flow.RetryGeneration
	retryActions, err := relay.Handle(RelayEvent{Kind: RelayRetryDeadline, Generation: retryGeneration})
	if err != nil {
		t.Fatal(err)
	}
	retried := requireMessageAction[protocol.Data](t, retryActions)
	if retried.Attachment == first.Attachment || retried.AttemptGeneration != first.AttemptGeneration+1 {
		t.Fatalf("recovery attempt = %#v, first = %#v", retried, first)
	}
	writeCompletedAt := time.Unix(500, 10*int64(time.Millisecond))
	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: retried.Generation,
		AttemptOutcome: flow.AttemptSucceeded, WriteCompletedAt: writeCompletedAt,
	}); err != nil {
		t.Fatal(err)
	}
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: retried.Attachment,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	credit := requireRelayAction(t, actions, RelayActionDataCredit)
	if credit.Attachment != retried.Attachment || credit.DataCreditBytes != 5 || credit.WriteCompletedAt != writeCompletedAt {
		t.Fatalf("recovery DATA credit = %#v, retry = %#v", credit, retried)
	}
}

func relayAssignedBytes(snapshot RelaySnapshot) uint64 {
	var assigned uint64
	for _, attachment := range snapshot.Policy.Attachments {
		assigned += attachment.Assigned
	}
	return assigned
}

func TestRelayACKRearmsRetryWithLearnedPathEstimate(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	if _, err := relay.Handle(RelayEvent{
		Kind: RelayObserveDataQuality, Attachment: testRelayA,
		RTT: 20 * time.Millisecond, Bytes: 1 << 20, Interval: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{
		Kind: RelaySetAttachmentLoad, Attachment: testRelayA, QueuedBytes: 256 << 10,
	}); err != nil {
		t.Fatal(err)
	}
	wantRetry := 20*time.Millisecond + time.Duration((256<<10)+len("abcdef"))*time.Second/time.Duration(1<<20)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	initial := requireRelayAction(t, actions, RelayActionArmRetryDeadline)
	if initial.After != wantRetry {
		t.Fatalf("initial learned retry = %v, want %v", initial.After, wantRetry)
	}

	actions, err = relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	rearmed := requireRelayAction(t, actions, RelayActionArmRetryDeadline)
	if rearmed.After != wantRetry {
		t.Fatalf("rearmed learned retry = %v, want %v", rearmed.After, wantRetry)
	}
}

func TestRelayRetryDeadlineFollowsEarliestUnacknowledgedItem(t *testing.T) {
	now := time.Unix(350, 0)
	relay, machine := newRelayFixtureWithClock(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	}, func() time.Time { return now })
	publishRelayAttachment(t, relay, machine, testRelayA)
	publishRelayAttachment(t, relay, machine, testRelayB)
	if _, err := relay.Handle(RelayEvent{Kind: RelaySetSessionQualities, SessionQualities: map[flow.AttachmentKey]policy.QualitySnapshot{
		testRelayA: {SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20, QueuedBytes: 2 << 20, RetryEstimate: policy.MaximumRetryEstimate},
		testRelayB: {SRTT: 3 * time.Second, CapacityBytesSec: 1 << 20, RetryEstimate: policy.MaximumRetryEstimate},
	}}); err != nil {
		t.Fatal(err)
	}
	firstActions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("first"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := requireMessageAction[protocol.Data](t, firstActions)
	if first.Attachment != testRelayA {
		t.Fatalf("first DATA attachment = %#v, want %#v", first.Attachment, testRelayA)
	}
	firstRetry := requireRelayAction(t, firstActions, RelayActionArmRetryDeadline).After
	if firstRetry <= policy.MaximumRetryEstimate {
		t.Fatalf("first retry estimate = %s, want queued delivery estimate above the RTT bound", firstRetry)
	}

	if _, err := relay.Handle(RelayEvent{Kind: RelaySetSessionQuality, Attachment: testRelayA, Quality: policy.QualitySnapshot{
		SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20, StallPenalty: 5 * time.Second, RetryEstimate: policy.MaximumRetryEstimate,
	}}); err != nil {
		t.Fatal(err)
	}
	bRetryRTT := firstRetry - time.Duration(len("first"))*time.Second/time.Duration(1<<20)
	if _, err := relay.Handle(RelayEvent{Kind: RelaySetSessionQuality, Attachment: testRelayB, Quality: policy.QualitySnapshot{
		SRTT: bRetryRTT, CapacityBytesSec: 1 << 20, RetryEstimate: policy.MaximumRetryEstimate,
	}}); err != nil {
		t.Fatal(err)
	}
	flowActions, err := machine.Handle(flow.FlowEvent{
		Kind:      flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleAttachmentLost, Attachment: testRelayA},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{Kind: RelayApplyFlowActions, FlowActions: flowActions}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{Kind: RelaySetSessionQuality, Attachment: testRelayB, Quality: policy.QualitySnapshot{
		SRTT: 20 * time.Millisecond, CapacityBytesSec: 1 << 20, RetryEstimate: policy.MinimumRetryEstimate,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("second"),
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(300 * time.Millisecond)
	thirdActions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration, Data: []byte("third"),
	})
	if err != nil {
		t.Fatal(err)
	}
	third := requireMessageAction[protocol.Data](t, thirdActions)
	if third.Attachment != testRelayB {
		t.Fatalf("third DATA attachment = %#v, want %#v", third.Attachment, testRelayB)
	}

	ackActions, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayB,
		Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: 0, Ranges: []protocol.ACKRange{{Start: 11, End: 16}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rearmed := requireRelayAction(t, ackActions, RelayActionArmRetryDeadline)
	if rearmed.After != firstRetry {
		t.Fatalf("retry after selective ACK = %s, want earliest gap estimate %s", rearmed.After, firstRetry)
	}
}

func TestRelayAttachmentLossPausesReadsAndRecoveryReplaysPendingData(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	readGeneration := relay.Snapshot().TargetReadGeneration
	flowActions, err := machine.Handle(flow.FlowEvent{
		Kind:      flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleAttachmentLost, Attachment: testRelayA},
	})
	if err != nil {
		t.Fatal(err)
	}
	actions, err := relay.Handle(RelayEvent{Kind: RelayApplyFlowActions, FlowActions: flowActions})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, RelayActionAttachmentWithdrawn) != 1 {
		t.Fatalf("withdraw actions = %#v", actions)
	}
	if snapshot := relay.Snapshot(); !snapshot.TargetReadPaused || snapshot.TargetReadGeneration != readGeneration ||
		snapshot.Flow.Lifecycle.State != flow.Recovering {
		t.Fatalf("recovering snapshot = %#v", snapshot)
	}

	actions, err = relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: readGeneration, Data: []byte("held"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messageAttachments[protocol.Data](t, actions)) != 0 || countRelayActions(actions, RelayActionReadTarget) != 0 {
		t.Fatalf("actions while all attachments lost = %#v", actions)
	}
	if relay.Snapshot().Flow.TxReplayBytes != 4 {
		t.Fatal("in-flight target read was not retained in replay window")
	}

	actions = publishRelayAttachment(t, relay, machine, testRelayB)
	dataSends := messageAttachments[protocol.Data](t, actions)
	if len(dataSends) != 1 || dataSends[0] != testRelayB {
		t.Fatalf("recovery DATA attachments = %#v", dataSends)
	}
	if snapshot := relay.Snapshot(); snapshot.TargetReadPaused || snapshot.TargetReadGeneration == 0 ||
		snapshot.Flow.Lifecycle.State != flow.Relaying {
		t.Fatalf("recovered snapshot = %#v", snapshot)
	}
}

func TestRelayRemoteResetClosesTargetWithoutReply(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Reset{FlowID: testRelayFlowID, Reason: protocol.ResetCancelled},
	})
	if err != nil {
		t.Fatal(err)
	}
	if countRelayActions(actions, RelayActionCloseTarget) != 1 || len(messageAttachments[protocol.Reset](t, actions)) != 0 {
		t.Fatalf("remote RESET actions = %#v", actions)
	}
	if countRelayActions(actions, RelayActionAttachmentWithdrawn) != 1 {
		t.Fatalf("remote RESET did not withdraw attachment: %#v", actions)
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Reset || !snapshot.TargetCloseIssued {
		t.Fatalf("remote RESET snapshot = %#v", snapshot)
	}
}

func TestRelaySendLedgerBackpressureDoesNotResetFlow(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishActions := publishRelayAttachment(t, relay, machine, testRelayA)
	completeRelaySends(t, relay, publishActions)

	message := protocol.Data{FlowID: testRelayFlowID, Offset: 1, Bytes: []byte("x")}
	var firstGeneration uint64
	for index := 0; index < MaxRelayPendingSends; index++ {
		var ignored []RelayAction
		err := relay.emitSend(message, transport.FrameData, testRelayA, relayPendingSend{
			kind: relayPendingAttempt, attachment: testRelayA, itemID: uint64(index + 1), attemptGeneration: 1,
		}, &ignored)
		if err != nil || len(ignored) != 1 {
			t.Fatalf("enqueue %d: %v", index, err)
		}
		if index == 0 {
			firstGeneration = ignored[0].Generation
		}
	}
	if got := relay.Snapshot().PendingSends; got != MaxRelayPendingSends {
		t.Fatalf("pending sends = %d, want %d", got, MaxRelayPendingSends)
	}
	actions, err := relay.Handle(RelayEvent{Kind: RelayRemoteMessage, Attachment: testRelayA, Message: message})
	if err != nil {
		t.Fatalf("backpressure error = %v", err)
	}
	if state := relay.Snapshot().Flow.Lifecycle.State; state != flow.Relaying {
		t.Fatalf("backpressure flow state = %v, want Relaying", state)
	}
	if countRelayActions(actions, RelayActionSendMessage) != 0 || len(relay.deferredControls) != 1 {
		t.Fatalf("backpressure actions=%#v deferred=%d", actions, len(relay.deferredControls))
	}
	actions, err = relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: firstGeneration, AttemptOutcome: flow.AttemptSucceeded,
	})
	if err != nil || len(messageAttachments[protocol.ACK](t, actions)) != 1 {
		t.Fatalf("deferred ACK actions=%#v err=%v", actions, err)
	}
}

func TestRelayRedundantSlowCopiesBackpressureTargetReadBeforeAttemptHistoryLimit(t *testing.T) {
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
	held := make([]uint64, 0, MaxRelayPendingSends)
	var acknowledged uint64
	paused := false
	for iteration := 0; iteration <= MaxRelayAttemptItems; iteration++ {
		readGeneration := relay.Snapshot().TargetReadGeneration
		if readGeneration == 0 {
			paused = true
			break
		}
		actions, err := relay.Handle(RelayEvent{
			Kind: RelayTargetReadResult, Generation: readGeneration, Data: data,
		})
		if err != nil {
			t.Fatalf("target read %d reset under redundant backpressure: %v", iteration, err)
		}
		var dataActions []RelayAction
		for _, action := range actions {
			if action.Kind != RelayActionSendMessage {
				continue
			}
			if _, ok := relayActionMessage(t, action).(protocol.Data); !ok {
				continue
			}
			dataActions = append(dataActions, action)
		}
		if len(dataActions) != len(attachments) {
			t.Fatalf("target read %d produced %d DATA copies, want %d", iteration, len(dataActions), len(attachments))
		}
		fastGeneration := dataActions[0].Generation
		for _, action := range dataActions[1:] {
			held = append(held, action.Generation)
		}
		if _, err := relay.Handle(RelayEvent{
			Kind: RelaySendResult, Generation: fastGeneration, AttemptOutcome: flow.AttemptSucceeded,
		}); err != nil {
			t.Fatalf("fast send %d: %v", iteration, err)
		}
		acknowledged += chunkBytes
		if _, err := relay.Handle(RelayEvent{
			Kind: RelayRemoteMessage, Attachment: attachments[0],
			Message: protocol.ACK{FlowID: testRelayFlowID, NextOffset: acknowledged},
		}); err != nil {
			t.Fatalf("acknowledgement %d: %v", iteration, err)
		}
	}
	if !paused {
		t.Fatal("target reads did not pause before redundant send bookkeeping exhausted")
	}
	if snapshot := relay.Snapshot(); snapshot.Flow.Lifecycle.State != flow.Relaying || snapshot.PendingSends == 0 {
		t.Fatalf("backpressured redundant relay = %#v", snapshot)
	}

	resumed := false
	for _, generation := range held {
		actions, err := relay.Handle(RelayEvent{
			Kind: RelaySendResult, Generation: generation, AttemptOutcome: flow.AttemptSucceeded,
		})
		if err != nil {
			t.Fatalf("slow send completion: %v", err)
		}
		resumed = resumed || countRelayActions(actions, RelayActionReadTarget) != 0
	}
	if snapshot := relay.Snapshot(); !resumed || snapshot.TargetReadGeneration == 0 || snapshot.Flow.Lifecycle.State != flow.Relaying {
		t.Fatalf("redundant relay did not resume after send capacity returned: %#v", snapshot)
	}
}

func TestRelayCoalescesPendingAcknowledgementsToLatestSnapshot(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{Mode: protocol.DeliveryAdaptive, Selection: protocol.PathFastest})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayRemoteMessage, Attachment: testRelayA,
		Message: protocol.Data{FlowID: testRelayFlowID, Bytes: []byte("abcd")},
	})
	if err != nil {
		t.Fatal(err)
	}
	firstACK := requireMessageAction[protocol.ACK](t, actions)
	write := requireRelayAction(t, actions, RelayActionWriteTarget)
	actions, err = relay.Handle(RelayEvent{
		Kind: RelayTargetWriteResult, Generation: write.Generation, N: write.DataLen(),
	})
	if err != nil || len(messageAttachments[protocol.ACK](t, actions)) != 0 {
		t.Fatalf("coalesced ACK actions=%#v err=%v", actions, err)
	}
	if relay.Snapshot().PendingSends != 1 || len(relay.deferredControls) != 1 {
		t.Fatalf("coalesced state pending=%d deferred=%d", relay.Snapshot().PendingSends, len(relay.deferredControls))
	}
	actions, err = relay.Handle(RelayEvent{
		Kind: RelaySendResult, Generation: firstACK.Generation, AttemptOutcome: flow.AttemptSucceeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertACKOffset(t, actions, 4)
	if relay.Snapshot().PendingSends != 1 || len(relay.deferredControls) != 0 {
		t.Fatalf("replacement state pending=%d deferred=%d", relay.Snapshot().PendingSends, len(relay.deferredControls))
	}
}

func TestRelayRejectsZeroByteNilErrorTargetRead(t *testing.T) {
	relay, machine := newRelayFixture(t, policy.Config{
		Mode: protocol.DeliveryRedundant, Selection: protocol.PathNone,
	})
	publishRelayAttachment(t, relay, machine, testRelayA)
	actions, err := relay.Handle(RelayEvent{
		Kind: RelayTargetReadResult, Generation: relay.Snapshot().TargetReadGeneration,
	})
	if !errors.Is(err, ErrInvalidTargetRead) {
		t.Fatalf("read error = %v", err)
	}
	if countRelayActions(actions, RelayActionCloseTarget) != 1 {
		t.Fatalf("invalid read actions = %#v", actions)
	}
}

func newRelayFixture(t *testing.T, config policy.Config) (*Relay, *flow.Flow) {
	return newRelayFixtureWithClock(t, config, time.Now)
}

func newRelayFixtureWithClock(t *testing.T, config policy.Config, now func() time.Time) (*Relay, *flow.Flow) {
	t.Helper()
	machine := flow.NewFlow()
	relay, err := NewRelayWithClock(testRelayFlowID, config, machine, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.Handle(RelayEvent{Kind: RelayStart}); err != nil {
		t.Fatal(err)
	}
	return relay, machine
}

func publishRelayAttachment(t *testing.T, relay *Relay, machine *flow.Flow, attachment flow.AttachmentKey) []RelayAction {
	t.Helper()
	requested, err := machine.Handle(flow.FlowEvent{
		Kind:      flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{Kind: flow.LifecycleJoinRequested, Attachment: attachment},
	})
	if err != nil {
		t.Fatal(err)
	}
	var generation uint64
	for _, action := range requested {
		if action.Kind == flow.FlowActionLifecycle && action.Lifecycle.Kind == flow.LifecycleActionSendJoinSuccess {
			generation = action.Lifecycle.Generation
		}
	}
	if generation == 0 {
		t.Fatalf("JOIN request actions = %#v", requested)
	}
	completed, err := machine.Handle(flow.FlowEvent{
		Kind: flow.FlowLifecycle,
		Lifecycle: flow.LifecycleEvent{
			Kind: flow.LifecycleJoinResultSendCompleted, Attachment: attachment, Generation: generation,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	actions, err := relay.Handle(RelayEvent{Kind: RelayApplyFlowActions, FlowActions: completed})
	if err != nil {
		t.Fatal(err)
	}
	completeControlSends(t, relay, actions)
	return actions
}

func requireRelayAction(t *testing.T, actions []RelayAction, kind RelayActionKind) RelayAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("missing relay action %v in %#v", kind, actions)
	return RelayAction{}
}

func requireMessageAction[T protocol.Message](t *testing.T, actions []RelayAction) RelayAction {
	t.Helper()
	for _, action := range actions {
		if _, ok := relayActionMessage(t, action).(T); ok {
			return action
		}
	}
	t.Fatalf("missing message %T in %#v", *new(T), actions)
	return RelayAction{}
}

func messageAttachments[T protocol.Message](t *testing.T, actions []RelayAction) []flow.AttachmentKey {
	t.Helper()
	var attachments []flow.AttachmentKey
	for _, action := range actions {
		if _, ok := relayActionMessage(t, action).(T); ok {
			attachments = append(attachments, action.Attachment)
		}
	}
	return attachments
}

func relayActionMessage(t *testing.T, action RelayAction) protocol.Message {
	t.Helper()
	if action.Message != nil || len(action.Encoded) == 0 {
		return action.Message
	}
	_, message, err := protocol.DecodeEncodedFrame(action.Encoded)
	if err != nil {
		t.Fatalf("encoded relay action: %v", err)
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

func assertACKOffset(t *testing.T, actions []RelayAction, want uint64) {
	t.Helper()
	action := requireMessageAction[protocol.ACK](t, actions)
	if got := action.Message.(protocol.ACK).NextOffset; got != want {
		t.Fatalf("ACK NextOffset = %d, want %d", got, want)
	}
}

func countRelayActions(actions []RelayAction, kind RelayActionKind) int {
	count := 0
	for _, action := range actions {
		if action.Kind == kind {
			count++
		}
	}
	return count
}

func completeRelaySends(t *testing.T, relay *Relay, actions []RelayAction) {
	t.Helper()
	for _, action := range actions {
		if action.Kind != RelayActionSendMessage {
			continue
		}
		if _, err := relay.Handle(RelayEvent{
			Kind: RelaySendResult, Generation: action.Generation, AttemptOutcome: flow.AttemptSucceeded,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func completeControlSends(t *testing.T, relay *Relay, actions []RelayAction) {
	t.Helper()
	for _, action := range actions {
		switch action.Message.(type) {
		case protocol.ACK, protocol.FINACK:
		default:
			continue
		}
		if _, err := relay.Handle(RelayEvent{
			Kind: RelaySendResult, Generation: action.Generation, AttemptOutcome: flow.AttemptSucceeded,
		}); err != nil {
			t.Fatal(err)
		}
	}
}
