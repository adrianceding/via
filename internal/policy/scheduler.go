package policy

import (
	"errors"

	"github.com/adrianceding/via/internal/protocol"
)

const (
	MaxDescriptorsPerFlow = 64
	MaxSchedulerFlows     = 8192
)

var (
	ErrInvalidDescriptor  = errors.New("policy: invalid descriptor")
	ErrDescriptorLimit    = errors.New("policy: descriptor limit exceeded")
	ErrDescriptorConflict = errors.New("policy: descriptor conflict")
)

type DescriptorKind uint8

const (
	DescriptorData DescriptorKind = iota + 1
	DescriptorACK
	DescriptorFIN
	DescriptorFINACK
	DescriptorReset
)

type Descriptor struct {
	Kind       DescriptorKind
	FlowID     protocol.FlowID
	ItemID     uint64
	Generation uint64
	Bytes      uint64
}

type schedulerQueue struct {
	data   []Descriptor
	ack    *Descriptor
	fin    *Descriptor
	finACK *Descriptor
	reset  *Descriptor
}

func (queue *schedulerQueue) empty() bool {
	return len(queue.data) == 0 && queue.ack == nil && queue.fin == nil && queue.finACK == nil && queue.reset == nil
}

func (queue *schedulerQueue) count() int {
	count := len(queue.data)
	for _, descriptor := range []*Descriptor{queue.ack, queue.fin, queue.finACK, queue.reset} {
		if descriptor != nil {
			count++
		}
	}
	return count
}

type SchedulerSnapshot struct {
	Flows       int
	Descriptors int
	Order       []protocol.FlowID
}

type Scheduler struct {
	maxFlows    int
	queues      map[protocol.FlowID]*schedulerQueue
	order       []protocol.FlowID
	descriptors int
}

func NewScheduler(maxFlows int) (*Scheduler, error) {
	if maxFlows < 1 || maxFlows > MaxSchedulerFlows {
		return nil, ErrDescriptorLimit
	}
	return &Scheduler{maxFlows: maxFlows, queues: make(map[protocol.FlowID]*schedulerQueue, maxFlows)}, nil
}

func (scheduler *Scheduler) Enqueue(descriptor Descriptor) error {
	if err := validateDescriptor(descriptor); err != nil {
		return err
	}
	queue, exists := scheduler.queues[descriptor.FlowID]
	if !exists {
		if len(scheduler.queues) >= scheduler.maxFlows {
			return ErrDescriptorLimit
		}
		queue = &schedulerQueue{}
		scheduler.queues[descriptor.FlowID] = queue
		scheduler.order = append(scheduler.order, descriptor.FlowID)
	}

	var err error
	switch descriptor.Kind {
	case DescriptorData:
		var added bool
		added, err = enqueueData(queue, descriptor)
		if added {
			scheduler.descriptors++
		}
	case DescriptorACK:
		copy := descriptor
		if queue.ack == nil {
			scheduler.descriptors++
		}
		queue.ack = &copy
	case DescriptorFIN:
		var added bool
		added, err = setSingleDescriptor(&queue.fin, descriptor)
		if added {
			scheduler.descriptors++
		}
	case DescriptorFINACK:
		var added bool
		added, err = setSingleDescriptor(&queue.finACK, descriptor)
		if added {
			scheduler.descriptors++
		}
	case DescriptorReset:
		copy := descriptor
		before := queue.count()
		queue.data = nil
		queue.ack = nil
		queue.fin = nil
		queue.finACK = nil
		queue.reset = &copy
		scheduler.descriptors += 1 - before
	}
	if err != nil && queue.empty() {
		scheduler.removeFlow(descriptor.FlowID)
	}
	return err
}

func (scheduler *Scheduler) Next() (Descriptor, bool) {
	if len(scheduler.order) == 0 {
		return Descriptor{}, false
	}
	flowID := scheduler.order[0]
	queue := scheduler.queues[flowID]
	descriptor := popDescriptor(queue)
	scheduler.descriptors--
	copy(scheduler.order, scheduler.order[1:])
	scheduler.order = scheduler.order[:len(scheduler.order)-1]
	if queue.empty() {
		delete(scheduler.queues, flowID)
	} else {
		scheduler.order = append(scheduler.order, flowID)
	}
	return descriptor, true
}

func (scheduler *Scheduler) DropFlow(flowID protocol.FlowID) {
	if _, exists := scheduler.queues[flowID]; exists {
		scheduler.removeFlow(flowID)
	}
}

func (scheduler *Scheduler) Cancel(flowID protocol.FlowID, itemID, generation uint64) bool {
	queue, exists := scheduler.queues[flowID]
	if !exists || itemID == 0 || generation == 0 {
		return false
	}
	removed := false
	for index := 0; index < len(queue.data); index++ {
		descriptor := queue.data[index]
		if descriptor.ItemID != itemID || descriptor.Generation != generation {
			continue
		}
		copy(queue.data[index:], queue.data[index+1:])
		queue.data = queue.data[:len(queue.data)-1]
		scheduler.descriptors--
		removed = true
		break
	}
	for _, slot := range []**Descriptor{&queue.ack, &queue.fin, &queue.finACK, &queue.reset} {
		if *slot != nil && (*slot).ItemID == itemID && (*slot).Generation == generation {
			*slot = nil
			scheduler.descriptors--
			removed = true
		}
	}
	if removed && queue.empty() {
		scheduler.removeFlow(flowID)
	}
	return removed
}

func (scheduler *Scheduler) Snapshot() SchedulerSnapshot {
	if scheduler == nil {
		return SchedulerSnapshot{}
	}
	return SchedulerSnapshot{
		Flows:       len(scheduler.queues),
		Descriptors: scheduler.descriptors,
		Order:       append([]protocol.FlowID(nil), scheduler.order...),
	}
}

func validateDescriptor(descriptor Descriptor) error {
	if descriptor.FlowID == (protocol.FlowID{}) || descriptor.ItemID == 0 || descriptor.Generation == 0 ||
		descriptor.Kind < DescriptorData || descriptor.Kind > DescriptorReset {
		return ErrInvalidDescriptor
	}
	if descriptor.Kind == DescriptorData && descriptor.Bytes == 0 {
		return ErrInvalidDescriptor
	}
	if descriptor.Kind != DescriptorData && descriptor.Bytes != 0 {
		return ErrInvalidDescriptor
	}
	return nil
}

func enqueueData(queue *schedulerQueue, descriptor Descriptor) (bool, error) {
	for index, current := range queue.data {
		if current.ItemID != descriptor.ItemID {
			continue
		}
		if descriptor.Generation < current.Generation {
			return false, nil
		}
		queue.data[index] = descriptor
		return false, nil
	}
	if len(queue.data) >= MaxDescriptorsPerFlow {
		return false, ErrDescriptorLimit
	}
	queue.data = append(queue.data, descriptor)
	return true, nil
}

func setSingleDescriptor(slot **Descriptor, descriptor Descriptor) (bool, error) {
	if *slot != nil {
		if (*slot).ItemID != descriptor.ItemID {
			return false, ErrDescriptorConflict
		}
		if descriptor.Generation < (*slot).Generation {
			return false, nil
		}
		copy := descriptor
		*slot = &copy
		return false, nil
	}
	copy := descriptor
	*slot = &copy
	return true, nil
}

func popDescriptor(queue *schedulerQueue) Descriptor {
	if queue.reset != nil {
		descriptor := *queue.reset
		queue.reset = nil
		return descriptor
	}
	if queue.finACK != nil {
		descriptor := *queue.finACK
		queue.finACK = nil
		return descriptor
	}
	if queue.ack != nil {
		descriptor := *queue.ack
		queue.ack = nil
		return descriptor
	}
	if len(queue.data) != 0 {
		descriptor := queue.data[0]
		copy(queue.data, queue.data[1:])
		queue.data = queue.data[:len(queue.data)-1]
		return descriptor
	}
	descriptor := *queue.fin
	queue.fin = nil
	return descriptor
}

func (scheduler *Scheduler) removeFlow(flowID protocol.FlowID) {
	queue := scheduler.queues[flowID]
	if queue != nil {
		scheduler.descriptors -= queue.count()
	}
	delete(scheduler.queues, flowID)
	for index, current := range scheduler.order {
		if current == flowID {
			copy(scheduler.order[index:], scheduler.order[index+1:])
			scheduler.order = scheduler.order[:len(scheduler.order)-1]
			return
		}
	}
}
