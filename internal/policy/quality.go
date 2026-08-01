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
	maximumSamplePeriod  = 30 * time.Second
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
}

type Quality struct {
	dataSamples  uint64
	probeSamples uint64
	dataSRTT     time.Duration
	dataRTTVar   time.Duration
	probeSRTT    time.Duration
	probeRTTVar  time.Duration
	capacity     float64
	queued       uint64
	inFlight     uint64
	stallPenalty time.Duration
}

func NewQuality() *Quality { return &Quality{capacity: defaultCapacity} }

func (quality *Quality) ObserveData(rtt time.Duration, acknowledgedBytes uint64, interval time.Duration) error {
	if quality == nil || rtt <= 0 || rtt > maximumSamplePeriod || acknowledgedBytes == 0 || interval <= 0 || interval > maximumSamplePeriod {
		return ErrInvalidSample
	}
	quality.dataSRTT, quality.dataRTTVar = updateRTT(quality.dataSRTT, quality.dataRTTVar, quality.dataSamples, rtt)
	quality.dataSamples++
	sampleCapacity := float64(acknowledgedBytes) / interval.Seconds()
	if !finitePositive(sampleCapacity) {
		return ErrInvalidSample
	}
	if quality.dataSamples == 1 {
		quality.capacity = sampleCapacity
	} else {
		quality.capacity = (7*quality.capacity + sampleCapacity) / 8
	}
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
	if quality == nil {
		return QualitySnapshot{CapacityBytesSec: defaultCapacity, RetryEstimate: InitialRetryEstimate}
	}
	srtt, variation := quality.preferredRTT()
	return QualitySnapshot{
		DataSamples:      quality.dataSamples,
		ProbeSamples:     quality.probeSamples,
		SRTT:             srtt,
		RTTVariation:     variation,
		CapacityBytesSec: quality.capacity,
		QueuedBytes:      quality.queued,
		InFlightBytes:    quality.inFlight,
		StallPenalty:     quality.stallPenalty,
		RetryEstimate:    retryEstimate(srtt, variation, quality.dataSamples+quality.probeSamples),
	}
}

func (quality *Quality) DeliveryEstimate(payloadBytes uint64) time.Duration {
	snapshot := quality.Snapshot()
	base := snapshot.SRTT
	if base == 0 {
		base = InitialRetryEstimate
	}
	bytes := saturatingSum(snapshot.QueuedBytes, snapshot.InFlightBytes, payloadBytes)
	seconds := float64(bytes) / snapshot.CapacityBytesSec
	if !finitePositive(snapshot.CapacityBytesSec) || seconds > float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	serialization := time.Duration(seconds * float64(time.Second))
	return saturatingDurationSum(base, serialization, snapshot.StallPenalty)
}

func (quality *Quality) preferredRTT() (time.Duration, time.Duration) {
	if quality.dataSamples != 0 {
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
