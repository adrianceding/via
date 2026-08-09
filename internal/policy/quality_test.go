package policy

import (
	"errors"
	"testing"
	"time"
)

func TestQualityInitialAndRetryBounds(t *testing.T) {
	quality := NewQuality()
	initial := quality.Snapshot()
	if initial.RetryEstimate != InitialRetryEstimate || initial.CapacityBytesSec != defaultCapacity {
		t.Fatalf("initial quality = %#v", initial)
	}
	if err := quality.ObserveProbe(10 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := quality.Snapshot().RetryEstimate; got != MinimumRetryEstimate {
		t.Fatalf("minimum retry estimate = %v", got)
	}
	if err := quality.ObserveProbe(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := quality.Snapshot().RetryEstimate; got > MaximumRetryEstimate || got < MinimumRetryEstimate {
		t.Fatalf("bounded retry estimate = %v", got)
	}
}

func TestQualityDataSamplesTakePriorityOverProbe(t *testing.T) {
	quality := NewQuality()
	if err := quality.ObserveProbe(900 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := quality.ObserveData(100*time.Millisecond, 100_000, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	before := quality.Snapshot()
	if before.SRTT != 100*time.Millisecond || before.CapacityBytesSec != 1_000_000 {
		t.Fatalf("data quality = %#v", before)
	}
	if err := quality.ObserveProbe(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	after := quality.Snapshot()
	if after.SRTT != before.SRTT || after.RTTVariation != before.RTTVariation || after.ProbeSamples != before.ProbeSamples+1 {
		t.Fatalf("probe displaced data sample: before=%#v after=%#v", before, after)
	}
}

func TestQualityCapacityOnlyDataLearnsWithoutFabricatingRTT(t *testing.T) {
	now := time.Unix(50, 0)
	quality := NewQualityWithClock(func() time.Time { return now })
	if err := quality.ObserveData(0, 64<<10, time.Second); err != nil {
		t.Fatal(err)
	}
	snapshot := quality.Snapshot()
	if snapshot.DataSamples != 1 || !snapshot.DataSampleFresh || snapshot.CapacityBytesSec != 64<<10 ||
		snapshot.SRTT != 0 || snapshot.RTTVariation != 0 || snapshot.RetryEstimate != InitialRetryEstimate {
		t.Fatalf("capacity-only DATA quality = %#v", snapshot)
	}
	if err := quality.ObserveProbe(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	snapshot = quality.Snapshot()
	if snapshot.SRTT != 20*time.Millisecond || snapshot.ProbeSamples != 1 || snapshot.DataSamples != 1 {
		t.Fatalf("probe did not remain RTT source = %#v", snapshot)
	}
}

func TestQualityDeliveryEstimateIncludesLoadCapacityAndStall(t *testing.T) {
	quality := NewQuality()
	if err := quality.ObserveData(100*time.Millisecond, 1000, time.Second); err != nil {
		t.Fatal(err)
	}
	quality.SetLoad(500, 500)
	if err := quality.SetStallPenalty(200 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got, want := quality.DeliveryEstimate(1000), 2300*time.Millisecond; got != want {
		t.Fatalf("delivery estimate = %v, want %v", got, want)
	}
}

func TestQualityRejectsInvalidSamples(t *testing.T) {
	quality := NewQuality()
	for _, err := range []error{
		quality.ObserveProbe(0),
		quality.ObserveData(-time.Nanosecond, 1, time.Second),
		quality.ObserveData(time.Second, 0, time.Second),
		quality.ObserveData(time.Second, 1, 0),
		quality.ObserveData(maximumSamplePeriod+1, 1, time.Second),
		quality.ObserveProbe(maximumSamplePeriod + 1),
		quality.SetStallPenalty(-1),
	} {
		if !errors.Is(err, ErrInvalidSample) {
			t.Fatalf("invalid sample error = %v", err)
		}
	}
}

func TestQualityDataFreshnessUsesProbeRTTAndFakeClock(t *testing.T) {
	now := time.Unix(100, 0)
	quality := NewQualityWithClock(func() time.Time { return now })
	if err := quality.ObserveProbe(500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := quality.ObserveData(100*time.Millisecond, 64<<10, time.Second); err != nil {
		t.Fatal(err)
	}
	if snapshot := quality.Snapshot(); !snapshot.DataSampleFresh || snapshot.CapacityBytesSec != 64<<10 || snapshot.DataSampleAge != 0 {
		t.Fatalf("fresh quality snapshot = %#v", snapshot)
	}
	now = now.Add(2*time.Second - time.Nanosecond)
	if snapshot := quality.Snapshot(); !snapshot.DataSampleFresh || snapshot.CapacityBytesSec != 64<<10 {
		t.Fatalf("boundary quality snapshot = %#v", snapshot)
	}
	now = now.Add(2 * time.Second)
	if snapshot := quality.Snapshot(); snapshot.DataSampleFresh || snapshot.CapacityBytesSec != defaultCapacity || snapshot.LastDataCapacity != 64<<10 {
		t.Fatalf("stale quality snapshot = %#v", snapshot)
	}
}

func TestQualityFreshnessFallsBackToThreeSecondsWithoutProbe(t *testing.T) {
	now := time.Unix(100, 0)
	quality := NewQualityWithClock(func() time.Time { return now })
	if err := quality.ObserveData(100*time.Millisecond, 64<<10, time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(3*time.Second - time.Nanosecond)
	if !quality.Snapshot().DataSampleFresh {
		t.Fatal("sample became stale before the three-second boundary")
	}
	now = now.Add(2 * time.Nanosecond)
	if quality.Snapshot().DataSampleFresh {
		t.Fatal("sample remained fresh after the three-second boundary")
	}
}

func TestQualityStaleDataRTTFallsBackToProbe(t *testing.T) {
	now := time.Unix(400, 0)
	quality := NewQualityWithClock(func() time.Time { return now })
	if err := quality.ObserveProbe(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := quality.ObserveData(100*time.Millisecond, 64<<10, time.Second); err != nil {
		t.Fatal(err)
	}
	if snapshot := quality.Snapshot(); snapshot.SRTT != 100*time.Millisecond {
		t.Fatalf("fresh DATA RTT = %#v", snapshot)
	}
	now = now.Add(3*time.Second + time.Nanosecond)
	snapshot := quality.Snapshot()
	if snapshot.DataSampleFresh || snapshot.SRTT != 20*time.Millisecond || snapshot.CapacityBytesSec != defaultCapacity {
		t.Fatalf("stale DATA quality = %#v", snapshot)
	}
}

func TestQualityStaleCapacityDecaysConservatively(t *testing.T) {
	now := time.Unix(500, 0)
	quality := NewQualityWithClock(func() time.Time { return now })
	if err := quality.ObserveData(0, 32<<10, time.Second); err != nil {
		t.Fatal(err)
	}

	now = now.Add(4 * time.Second)
	snapshot := quality.Snapshot()
	if snapshot.DataSampleFresh || snapshot.CapacityBytesSec != defaultCapacity || snapshot.LastDataCapacity != 32<<10 {
		t.Fatalf("stale capacity snapshot = %#v", snapshot)
	}
	capacity := effectiveCapacity(snapshot)
	if capacity != snapshot.LastDataCapacity {
		t.Fatalf("known slow capacity was inflated while stale: got %v, snapshot=%#v", capacity, snapshot)
	}

	now = now.Add(staleCapacityHorizon)
	if got := effectiveCapacity(quality.Snapshot()); got != snapshot.LastDataCapacity {
		t.Fatalf("expired stale capacity = %v, want %v", got, snapshot.LastDataCapacity)
	}
}

func TestUnmeasuredCapacityIsConservative(t *testing.T) {
	quality := NewQuality()
	if err := quality.ObserveProbe(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	snapshot := quality.Snapshot()
	if snapshot.DataSamples != 0 || snapshot.LastDataCapacity != 0 {
		t.Fatalf("new quality snapshot = %#v", snapshot)
	}
	if got := effectiveCapacity(snapshot); got != unmeasuredCapacity {
		t.Fatalf("unmeasured effective capacity = %v, want %v", got, unmeasuredCapacity)
	}
}
