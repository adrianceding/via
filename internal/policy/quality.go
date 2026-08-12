// Package policy places immutable flow descriptors on eligible attachments.
package policy

import (
	"errors"
	"math"
	"time"
)

const (
	InitialRetryEstimate = time.Second
	MinimumRetryEstimate = 200 * time.Millisecond
	MaximumRetryEstimate = 2 * time.Second
	minimumRTTVariation  = 50 * time.Millisecond
	defaultCapacity      = 1 << 20
	unmeasuredCapacity   = 64 << 10
	staleCapacityHorizon = 60 * time.Second
	maximumSamplePeriod  = 30 * time.Second
	minimumDataFreshness = 3 * time.Second
	maximumDataFreshness = 15 * time.Second
)

var ErrInvalidSample = errors.New("policy: invalid quality sample")

type QualitySnapshot struct {
	DataSamples      uint64
	ProbeSamples     uint64
	SRTT             time.Duration
	RTTVariation     time.Duration
	CapacityBytesSec float64
	QueuedBytes      uint64
	InFlightBytes    uint64
	StallPenalty     time.Duration
	RetryEstimate    time.Duration
	DataSampleFresh  bool
	DataSampleAge    time.Duration
	LastDataCapacity float64
}

type Quality struct {
	dataSamples    uint64
	dataRTTSamples uint64
	probeSamples   uint64
	dataSRTT       time.Duration
	dataRTTVar     time.Duration
	probeSRTT      time.Duration
	probeRTTVar    time.Duration
	capacity       float64
	lastCapacity   float64
	lastDataAt     time.Time
	queued         uint64
	inFlight       uint64
	stallPenalty   time.Duration
	now            func() time.Time
}

func NewQuality() *Quality { return NewQualityWithClock(time.Now) }

func NewQualityWithClock(now func() time.Time) *Quality {
	if now == nil {
		now = time.Now
	}
	return &Quality{capacity: defaultCapacity, now: now}
}

func (quality *Quality) ObserveData(rtt time.Duration, acknowledgedBytes uint64, interval time.Duration) error {
	if quality == nil || rtt < 0 || rtt > maximumSamplePeriod || acknowledgedBytes == 0 || interval <= 0 || interval > maximumSamplePeriod {
		return ErrInvalidSample
	}
	sampleCapacity := float64(acknowledgedBytes) / interval.Seconds()
	if !finitePositive(sampleCapacity) {
		return ErrInvalidSample
	}
	if rtt > 0 {
		quality.dataSRTT, quality.dataRTTVar = updateRTT(quality.dataSRTT, quality.dataRTTVar, quality.dataRTTSamples, rtt)
		quality.dataRTTSamples++
	}
	quality.dataSamples++
	if quality.dataSamples == 1 {
		quality.capacity = sampleCapacity
	} else {
		quality.capacity = (7*quality.capacity + sampleCapacity) / 8
	}
	quality.lastCapacity = quality.capacity
	quality.lastDataAt = quality.now()
	return nil
}

func (quality *Quality) ObserveProbe(rtt time.Duration) error {
	if quality == nil || rtt <= 0 || rtt > maximumSamplePeriod {
		return ErrInvalidSample
	}
	quality.probeSRTT, quality.probeRTTVar = updateRTT(quality.probeSRTT, quality.probeRTTVar, quality.probeSamples, rtt)
	quality.probeSamples++
	return nil
}

func (quality *Quality) SetLoad(queuedBytes, inFlightBytes uint64) {
	if quality == nil {
		return
	}
	quality.queued = queuedBytes
	quality.inFlight = inFlightBytes
}

func (quality *Quality) SetStallPenalty(penalty time.Duration) error {
	if quality == nil || penalty < 0 {
		return ErrInvalidSample
	}
	quality.stallPenalty = penalty
	return nil
}

func (quality *Quality) Snapshot() QualitySnapshot {
	return quality.SnapshotAt(quality.currentNow())
}

func (quality *Quality) SnapshotAt(now time.Time) QualitySnapshot {
	if quality == nil {
		return QualitySnapshot{CapacityBytesSec: defaultCapacity, RetryEstimate: InitialRetryEstimate}
	}
	srtt, variation := quality.preferredRTT(now)
	capacity := quality.capacity
	fresh := false
	var age time.Duration
	if !quality.lastDataAt.IsZero() && !now.Before(quality.lastDataAt) {
		age = now.Sub(quality.lastDataAt)
		freshness := dataFreshness(quality.probeSRTT)
		fresh = age <= freshness
	}
	if !fresh {
		capacity = defaultCapacity
	}
	return QualitySnapshot{
		DataSamples:      quality.dataSamples,
		ProbeSamples:     quality.probeSamples,
		SRTT:             srtt,
		RTTVariation:     variation,
		CapacityBytesSec: capacity,
		QueuedBytes:      quality.queued,
		InFlightBytes:    quality.inFlight,
		StallPenalty:     quality.stallPenalty,
		RetryEstimate:    retryEstimate(srtt, variation, quality.dataRTTSamples+quality.probeSamples),
		DataSampleFresh:  fresh,
		DataSampleAge:    age,
		LastDataCapacity: quality.lastCapacity,
	}
}

func (quality *Quality) DeliveryEstimate(payloadBytes uint64) time.Duration {
	return deliveryEstimate(quality.Snapshot(), payloadBytes)
}

func (snapshot QualitySnapshot) DeliveryEstimate(payloadBytes uint64) time.Duration {
	return deliveryEstimate(snapshot, payloadBytes)
}

func (snapshot QualitySnapshot) CapacityEstimate() float64 {
	return effectiveCapacity(snapshot)
}

func deliveryEstimate(snapshot QualitySnapshot, payloadBytes uint64) time.Duration {
	base := snapshot.SRTT
	if base == 0 {
		base = InitialRetryEstimate
	}
	bytes := saturatingSum(snapshot.QueuedBytes, snapshot.InFlightBytes, payloadBytes)
	capacity := effectiveCapacity(snapshot)
	seconds := float64(bytes) / capacity
	if !finitePositive(capacity) || seconds > float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	serialization := time.Duration(seconds * float64(time.Second))
	return saturatingDurationSum(base, serialization, snapshot.StallPenalty)
}

// effectiveCapacity decays stale measurements toward the conservative prior.
// In particular, staleness must never make a known slow path look faster.
func effectiveCapacity(snapshot QualitySnapshot) float64 {
	if snapshot.DataSampleFresh {
		if finitePositive(snapshot.CapacityBytesSec) {
			return snapshot.CapacityBytesSec
		}
		return defaultCapacity
	}
	if !finitePositive(snapshot.LastDataCapacity) {
		if (snapshot.DataSamples != 0 || snapshot.ProbeSamples == 0) && finitePositive(snapshot.CapacityBytesSec) {
			return snapshot.CapacityBytesSec
		}
		return unmeasuredCapacity
	}
	if snapshot.DataSampleAge <= 0 {
		return snapshot.LastDataCapacity
	}
	staleBaseline := math.Min(snapshot.LastDataCapacity, unmeasuredCapacity)
	if snapshot.DataSampleAge >= staleCapacityHorizon {
		return staleBaseline
	}
	weight := float64(snapshot.DataSampleAge) / float64(staleCapacityHorizon)
	return snapshot.LastDataCapacity + (staleBaseline-snapshot.LastDataCapacity)*weight
}

func (quality *Quality) preferredRTT(now time.Time) (time.Duration, time.Duration) {
	if quality.dataRTTSamples != 0 && !quality.lastDataAt.IsZero() && !now.Before(quality.lastDataAt) && now.Sub(quality.lastDataAt) <= dataFreshness(quality.probeSRTT) {
		return quality.dataSRTT, quality.dataRTTVar
	}
	if quality.probeSamples != 0 {
		return quality.probeSRTT, quality.probeRTTVar
	}
	return 0, 0
}

func updateRTT(srtt, variation time.Duration, samples uint64, sample time.Duration) (time.Duration, time.Duration) {
	if samples == 0 {
		return sample, sample / 2
	}
	difference := srtt - sample
	if difference < 0 {
		difference = -difference
	}
	return (7*srtt + sample) / 8, (3*variation + difference) / 4
}

func retryEstimate(srtt, variation time.Duration, samples uint64) time.Duration {
	if samples == 0 {
		return InitialRetryEstimate
	}
	margin := 4 * variation
	if margin < minimumRTTVariation {
		margin = minimumRTTVariation
	}
	estimate := saturatingDurationSum(srtt, margin)
	if estimate < MinimumRetryEstimate {
		return MinimumRetryEstimate
	}
	if estimate > MaximumRetryEstimate {
		return MaximumRetryEstimate
	}
	return estimate
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}

func (quality *Quality) currentNow() time.Time {
	if quality == nil || quality.now == nil {
		return time.Now()
	}
	return quality.now()
}

func dataFreshness(probeRTT time.Duration) time.Duration {
	freshness := minimumDataFreshness
	if probeRTT > 0 && 4*probeRTT > freshness {
		freshness = 4 * probeRTT
	}
	if freshness > maximumDataFreshness {
		freshness = maximumDataFreshness
	}
	return freshness
}

func saturatingSum(values ...uint64) uint64 {
	var result uint64
	for _, value := range values {
		if math.MaxUint64-result < value {
			return math.MaxUint64
		}
		result += value
	}
	return result
}

func saturatingDurationSum(values ...time.Duration) time.Duration {
	var result time.Duration
	for _, value := range values {
		if value > 0 && result > time.Duration(math.MaxInt64)-value {
			return time.Duration(math.MaxInt64)
		}
		result += value
	}
	return result
}
