package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
)

func TestWirePathGroupPublishesOnlyFirstReadyAndLastLost(t *testing.T) {
	pathGroupID := protocol.PathGroupID{1}
	group, err := newWirePathGroup(101, "client-01", pathGroupID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := newWireSession(context.Background(), 11, &clientTestTransportConnection{})
	if err != nil {
		t.Fatal(err)
	}
	first.principal = "client-01"
	first.pathGroupID = pathGroupID
	second, err := newWireSession(context.Background(), 12, &clientTestTransportConnection{})
	if err != nil {
		t.Fatal(err)
	}
	second.principal = "client-01"
	second.pathGroupID = pathGroupID

	if ready, err := group.add(first); err != nil || !ready {
		t.Fatalf("first add = %t, %v", ready, err)
	}
	if ready, err := group.add(second); err != nil || ready {
		t.Fatalf("second add = %t, %v", ready, err)
	}
	flowID := protocol.FlowID{2}
	attachment := flow.AttachmentKey{SessionGeneration: group.generation, AttachmentGeneration: 3}
	if err := first.reserve(flowID, attachment); err != nil {
		t.Fatal(err)
	}
	if err := first.publish(flowID, attachment); err != nil {
		t.Fatal(err)
	}
	if got, ok := second.attachment(flowID); !ok || got != attachment {
		t.Fatalf("shared attachment = %#v, %t", got, ok)
	}
	if lost := group.remove(first.generation); lost {
		t.Fatal("first lane removal published group lost")
	}
	if lost := group.remove(second.generation); !lost {
		t.Fatal("last lane removal did not publish group lost")
	}
	recovered, err := newWireSession(context.Background(), 13, &clientTestTransportConnection{})
	if err != nil {
		t.Fatal(err)
	}
	recovered.principal = "client-01"
	recovered.pathGroupID = pathGroupID
	if ready, err := group.add(recovered); err != nil || !ready || group.generation != 101 {
		t.Fatalf("recovered group = %t, generation %d, %v", ready, group.generation, err)
	}
}

func TestWirePathGroupReleaseClearsEveryLaneFlowLoad(t *testing.T) {
	pathGroupID := protocol.PathGroupID{1}
	group, err := newWirePathGroup(101, "client-01", pathGroupID)
	if err != nil {
		t.Fatal(err)
	}
	flowID := protocol.FlowID{2}
	attachment := flow.AttachmentKey{SessionGeneration: group.generation, AttachmentGeneration: 3}
	for generation := uint64(11); generation <= 12; generation++ {
		session, sessionErr := newWireSession(context.Background(), generation, &clientTestTransportConnection{})
		if sessionErr != nil {
			t.Fatal(sessionErr)
		}
		defer session.close()
		session.principal = "client-01"
		session.pathGroupID = pathGroupID
		if _, addErr := group.add(session); addErr != nil {
			t.Fatal(addErr)
		}
		session.runtime.mu.Lock()
		session.runtime.inFlightData = 64
		session.runtime.unacknowledgedData[flowID] = 64
		session.runtime.updateQualityLoadLocked()
		session.runtime.mu.Unlock()
	}
	if err := group.session().reserve(flowID, attachment); err != nil {
		t.Fatal(err)
	}
	if err := group.session().publish(flowID, attachment); err != nil {
		t.Fatal(err)
	}

	group.release(flowID, attachment)
	for generation, session := range group.lanes {
		if snapshot := session.runtime.snapshot(); snapshot.InFlightData != 0 || snapshot.Quality.InFlightBytes != 0 {
			t.Fatalf("lane %d retained released Flow load: %#v", generation, snapshot)
		}
	}
}

func TestSelectPathGroupLaneUsesDeliveryEstimateAndStableTie(t *testing.T) {
	lanes := []pathGroupLaneSnapshot{
		{generation: 12, quality: policy.QualitySnapshot{SRTT: 50 * time.Millisecond, CapacityBytesSec: 1 << 20}},
		{generation: 11, quality: policy.QualitySnapshot{SRTT: 10 * time.Millisecond, CapacityBytesSec: 1 << 20}},
	}
	if got := selectPathGroupLane(lanes, 64<<10); got != 11 {
		t.Fatalf("fast lane = %d, want 11", got)
	}
	lanes[1].quality.QueuedBytes = 1 << 20
	if got := selectPathGroupLane(lanes, 64<<10); got != 12 {
		t.Fatalf("unloaded lane = %d, want 12", got)
	}
	lanes[0].quality.StallPenalty = 2 * time.Second
	lanes[1].quality.QueuedBytes = 0
	if got := selectPathGroupLane(lanes, 64<<10); got != 11 {
		t.Fatalf("unstalled lane = %d, want 11", got)
	}
	lanes[0].quality = lanes[1].quality
	if got := selectPathGroupLane(lanes, 64<<10); got != 11 {
		t.Fatalf("stable tie lane = %d, want 11", got)
	}
}

func TestAggregatePathGroupQualitySumsCapacityAndLoad(t *testing.T) {
	quality := aggregatePathGroupQuality([]pathGroupLaneSnapshot{
		{generation: 11, quality: policy.QualitySnapshot{
			DataSamples: 2, ProbeSamples: 3, SRTT: 40 * time.Millisecond, CapacityBytesSec: 10,
			QueuedBytes: 100, InFlightBytes: 200, StallPenalty: time.Second, RetryEstimate: 300 * time.Millisecond,
			DataSampleFresh: true, DataSampleAge: 2 * time.Second, LastDataCapacity: 8,
		}},
		{generation: 12, quality: policy.QualitySnapshot{
			DataSamples: 5, ProbeSamples: 7, SRTT: 20 * time.Millisecond, CapacityBytesSec: 30,
			QueuedBytes: 400, InFlightBytes: 500, RetryEstimate: 200 * time.Millisecond,
			DataSampleFresh: true, DataSampleAge: time.Second, LastDataCapacity: 25,
		}},
	})
	if quality.DataSamples != 7 || quality.ProbeSamples != 10 || quality.SRTT != 20*time.Millisecond ||
		quality.CapacityBytesSec != 40 || quality.QueuedBytes != 500 || quality.InFlightBytes != 700 ||
		quality.StallPenalty != 0 || quality.RetryEstimate != 200*time.Millisecond ||
		!quality.DataSampleFresh || quality.DataSampleAge != time.Second || quality.LastDataCapacity != 33 {
		t.Fatalf("aggregate quality = %#v", quality)
	}
}

func TestAggregatePathGroupQualityUsesEachLaneCapacityEstimate(t *testing.T) {
	quality := aggregatePathGroupQuality([]pathGroupLaneSnapshot{
		{generation: 11, quality: policy.QualitySnapshot{
			DataSamples: 1, DataSampleFresh: true, CapacityBytesSec: 4 << 20, LastDataCapacity: 4 << 20,
		}},
		{generation: 12, quality: policy.QualitySnapshot{
			DataSamples: 1, CapacityBytesSec: 1 << 20, LastDataCapacity: 8 << 20, DataSampleAge: time.Hour,
		}},
	})
	if quality.CapacityBytesSec != 4<<20+64<<10 || !quality.DataSampleFresh {
		t.Fatalf("mixed-freshness aggregate quality = %#v", quality)
	}
}

func TestAggregatePathGroupQualityKeepsAllStaleCapacityEstimate(t *testing.T) {
	quality := aggregatePathGroupQuality([]pathGroupLaneSnapshot{
		{generation: 11, quality: policy.QualitySnapshot{
			DataSamples: 1, CapacityBytesSec: 1 << 20, LastDataCapacity: 4 << 20, DataSampleAge: time.Hour,
		}},
		{generation: 12, quality: policy.QualitySnapshot{
			DataSamples: 1, CapacityBytesSec: 1 << 20, LastDataCapacity: 8 << 20, DataSampleAge: time.Hour,
		}},
	})
	if quality.DataSampleFresh || quality.LastDataCapacity != 0 || quality.CapacityEstimate() != 128<<10 {
		t.Fatalf("all-stale aggregate quality = %#v, estimate %v", quality, quality.CapacityEstimate())
	}
}

func TestAggregatePathGroupQualityUsesOneHealthyLatencyLane(t *testing.T) {
	quality := aggregatePathGroupQuality([]pathGroupLaneSnapshot{
		{generation: 11, quality: policy.QualitySnapshot{
			SRTT: 10 * time.Millisecond, RTTVariation: time.Millisecond,
			StallPenalty: time.Second, RetryEstimate: 200 * time.Millisecond,
		}},
		{generation: 12, quality: policy.QualitySnapshot{
			SRTT: 30 * time.Millisecond, RTTVariation: 4 * time.Millisecond,
			RetryEstimate: 400 * time.Millisecond,
		}},
	})
	if quality.SRTT != 30*time.Millisecond || quality.RTTVariation != 4*time.Millisecond ||
		quality.StallPenalty != 0 || quality.RetryEstimate != 400*time.Millisecond {
		t.Fatalf("latency representative = %#v", quality)
	}
}
