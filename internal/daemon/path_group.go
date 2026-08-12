package daemon

import (
	"math"
	"sort"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
)

const maxWirePathGroupLanes = 64

type wirePathGroup struct {
	generation uint64
	principal  string
	id         protocol.PathGroupID
	registry   *wireAttachmentRegistry
	lanes      map[uint64]*wireSession
}

type pathGroupLaneSnapshot struct {
	generation uint64
	quality    policy.QualitySnapshot
}

func newWirePathGroup(generation uint64, principal string, id protocol.PathGroupID) (*wirePathGroup, error) {
	if generation == 0 || principal == "" || !protocol.ValidPathGroupID(id) {
		return nil, ErrWireProtocol
	}
	registry, err := newWireAttachmentRegistry(generation)
	if err != nil {
		return nil, err
	}
	return &wirePathGroup{
		generation: generation, principal: principal, id: id, registry: registry,
		lanes: make(map[uint64]*wireSession, maxWirePathGroupLanes),
	}, nil
}

func (group *wirePathGroup) add(session *wireSession) (bool, error) {
	if group == nil || session == nil || session.generation == 0 || session.principal != group.principal ||
		session.pathGroupID != group.id || len(group.lanes) >= maxWirePathGroupLanes || group.lanes[session.generation] != nil {
		return false, ErrWireProtocol
	}
	if err := session.bindAttachmentRegistry(group.registry); err != nil {
		return false, err
	}
	ready := len(group.lanes) == 0
	group.lanes[session.generation] = session
	return ready, nil
}

func (group *wirePathGroup) remove(generation uint64) bool {
	if group == nil || generation == 0 || group.lanes[generation] == nil {
		return false
	}
	delete(group.lanes, generation)
	return len(group.lanes) == 0
}

func (group *wirePathGroup) release(flowID protocol.FlowID, attachment flow.AttachmentKey) {
	if group == nil {
		return
	}
	if session := group.session(); session != nil {
		session.release(flowID, attachment)
	}
	for _, session := range group.lanes {
		session.runtime.releaseFlow(flowID)
	}
}

func (group *wirePathGroup) session() *wireSession {
	if group == nil || len(group.lanes) == 0 {
		return nil
	}
	generations := make([]uint64, 0, len(group.lanes))
	for generation := range group.lanes {
		generations = append(generations, generation)
	}
	sort.Slice(generations, func(left, right int) bool { return generations[left] < generations[right] })
	return group.lanes[generations[0]]
}

func (group *wirePathGroup) selectSession(payloadBytes uint64) *wireSession {
	if group == nil || len(group.lanes) == 0 {
		return nil
	}
	snapshots := make([]pathGroupLaneSnapshot, 0, len(group.lanes))
	for generation, session := range group.lanes {
		snapshots = append(snapshots, pathGroupLaneSnapshot{generation: generation, quality: session.qualitySnapshot()})
	}
	return group.lanes[selectPathGroupLane(snapshots, payloadBytes)]
}

func selectPathGroupLane(lanes []pathGroupLaneSnapshot, payloadBytes uint64) uint64 {
	var selected uint64
	var selectedEstimate time.Duration
	for _, lane := range lanes {
		if lane.generation == 0 {
			continue
		}
		estimate := lane.quality.DeliveryEstimate(payloadBytes)
		if selected == 0 || estimate < selectedEstimate || estimate == selectedEstimate && lane.generation < selected {
			selected = lane.generation
			selectedEstimate = estimate
		}
	}
	return selected
}

func (group *wirePathGroup) qualitySnapshot() policy.QualitySnapshot {
	if group == nil {
		return policy.QualitySnapshot{}
	}
	lanes := make([]pathGroupLaneSnapshot, 0, len(group.lanes))
	for generation, session := range group.lanes {
		lanes = append(lanes, pathGroupLaneSnapshot{generation: generation, quality: session.qualitySnapshot()})
	}
	return aggregatePathGroupQuality(lanes)
}

func aggregatePathGroupQuality(lanes []pathGroupLaneSnapshot) policy.QualitySnapshot {
	var result policy.QualitySnapshot
	var representative pathGroupLaneSnapshot
	dataAgeSet := false
	for _, lane := range lanes {
		if lane.generation == 0 {
			continue
		}
		quality := lane.quality
		result.DataSamples = saturatingUint64(result.DataSamples, quality.DataSamples)
		result.ProbeSamples = saturatingUint64(result.ProbeSamples, quality.ProbeSamples)
		result.QueuedBytes = saturatingUint64(result.QueuedBytes, quality.QueuedBytes)
		result.InFlightBytes = saturatingUint64(result.InFlightBytes, quality.InFlightBytes)
		result.CapacityBytesSec = saturatingRate(result.CapacityBytesSec, quality.CapacityEstimate())
		result.LastDataCapacity = saturatingRate(result.LastDataCapacity, quality.LastDataCapacity)
		if representative.generation == 0 || pathGroupLatencyLaneLess(lane, representative) {
			representative = lane
		}
		if quality.DataSampleFresh {
			result.DataSampleFresh = true
		}
		if quality.DataSamples != 0 && (!dataAgeSet || quality.DataSampleAge < result.DataSampleAge) {
			result.DataSampleAge = quality.DataSampleAge
			dataAgeSet = true
		}
	}
	if representative.generation != 0 {
		result.SRTT = representative.quality.SRTT
		result.RTTVariation = representative.quality.RTTVariation
		result.StallPenalty = representative.quality.StallPenalty
		result.RetryEstimate = representative.quality.RetryEstimate
	}
	if !result.DataSampleFresh {
		result.LastDataCapacity = 0
	}
	return result
}

func pathGroupLatencyLaneLess(candidate, current pathGroupLaneSnapshot) bool {
	if candidate.quality.StallPenalty != current.quality.StallPenalty {
		return candidate.quality.StallPenalty < current.quality.StallPenalty
	}
	candidateRTT := candidate.quality.SRTT
	currentRTT := current.quality.SRTT
	if candidateRTT == 0 {
		candidateRTT = time.Duration(math.MaxInt64)
	}
	if currentRTT == 0 {
		currentRTT = time.Duration(math.MaxInt64)
	}
	return candidateRTT < currentRTT || candidateRTT == currentRTT && candidate.generation < current.generation
}

func saturatingRate(current, delta float64) float64 {
	if delta <= 0 || math.IsNaN(delta) {
		return current
	}
	if math.IsInf(delta, 1) || current > math.MaxFloat64-delta {
		return math.MaxFloat64
	}
	return current + delta
}
