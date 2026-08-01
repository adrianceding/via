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
