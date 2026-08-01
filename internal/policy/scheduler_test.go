package policy

import (
	"errors"
	"reflect"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

func TestSchedulerRoundRobinFairness(t *testing.T) {
	scheduler, err := NewScheduler(2)
	if err != nil {
		t.Fatal(err)
	}
	flowA := protocol.FlowID{1}
	flowB := protocol.FlowID{2}
	for _, descriptor := range []Descriptor{
		{Kind: DescriptorData, FlowID: flowA, ItemID: 1, Generation: 1, Bytes: 10},
		{Kind: DescriptorData, FlowID: flowA, ItemID: 2, Generation: 1, Bytes: 10},
		{Kind: DescriptorData, FlowID: flowB, ItemID: 3, Generation: 1, Bytes: 10},
	} {
		if err := scheduler.Enqueue(descriptor); err != nil {
			t.Fatal(err)
		}
	}
	var got []uint64
	for {
		descriptor, ok := scheduler.Next()
		if !ok {
			break
		}
		got = append(got, descriptor.ItemID)
	}
	if want := []uint64{1, 3, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("schedule order = %v, want %v", got, want)
	}
}

func TestSchedulerControlSlotsAndResetReplacement(t *testing.T) {
	scheduler, _ := NewScheduler(1)
	flowID := protocol.FlowID{1}
	for _, descriptor := range []Descriptor{
		{Kind: DescriptorData, FlowID: flowID, ItemID: 1, Generation: 1, Bytes: 10},
		{Kind: DescriptorACK, FlowID: flowID, ItemID: 2, Generation: 1},
		{Kind: DescriptorACK, FlowID: flowID, ItemID: 2, Generation: 2},
		{Kind: DescriptorFIN, FlowID: flowID, ItemID: 3, Generation: 1},
		{Kind: DescriptorFINACK, FlowID: flowID, ItemID: 4, Generation: 1},
	} {
		if err := scheduler.Enqueue(descriptor); err != nil {
			t.Fatal(err)
		}
	}
	if snapshot := scheduler.Snapshot(); snapshot.Descriptors != 4 {
		t.Fatalf("coalesced descriptor count = %d", snapshot.Descriptors)
	}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorReset, FlowID: flowID, ItemID: 5, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if snapshot := scheduler.Snapshot(); snapshot.Descriptors != 1 {
		t.Fatalf("reset descriptor count = %d", snapshot.Descriptors)
	}
	descriptor, ok := scheduler.Next()
	if !ok || descriptor.Kind != DescriptorReset || descriptor.ItemID != 5 {
		t.Fatalf("reset descriptor = %#v, %t", descriptor, ok)
	}
}

func TestSchedulerReplacesRetryGenerationInPlace(t *testing.T) {
	scheduler, _ := NewScheduler(1)
	flowID := protocol.FlowID{1}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorData, FlowID: flowID, ItemID: 1, Generation: 2, Bytes: 10}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorData, FlowID: flowID, ItemID: 1, Generation: 1, Bytes: 10}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorData, FlowID: flowID, ItemID: 1, Generation: 3, Bytes: 5}); err != nil {
		t.Fatal(err)
	}
	descriptor, _ := scheduler.Next()
	if descriptor.Generation != 3 || descriptor.Bytes != 5 {
		t.Fatalf("retry descriptor = %#v", descriptor)
	}
}

func TestSchedulerCancelsExactGeneration(t *testing.T) {
	scheduler, _ := NewScheduler(1)
	flowID := protocol.FlowID{1}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorData, FlowID: flowID, ItemID: 1, Generation: 2, Bytes: 10}); err != nil {
		t.Fatal(err)
	}
	if scheduler.Cancel(flowID, 1, 1) {
		t.Fatal("stale generation canceled current descriptor")
	}
	if !scheduler.Cancel(flowID, 1, 2) {
		t.Fatal("current generation was not canceled")
	}
	if _, ok := scheduler.Next(); ok {
		t.Fatal("canceled descriptor remained scheduled")
	}
}

func TestSchedulerHardLimitsAndInvalidDescriptors(t *testing.T) {
	if _, err := NewScheduler(0); !errors.Is(err, ErrDescriptorLimit) {
		t.Fatalf("zero scheduler limit error = %v", err)
	}
	scheduler, _ := NewScheduler(1)
	flowID := protocol.FlowID{1}
	for index := 0; index < MaxDescriptorsPerFlow; index++ {
		if err := scheduler.Enqueue(Descriptor{
			Kind: DescriptorData, FlowID: flowID, ItemID: uint64(index + 1), Generation: 1, Bytes: 1,
		}); err != nil {
			t.Fatalf("enqueue %d: %v", index, err)
		}
	}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorData, FlowID: flowID, ItemID: 1000, Generation: 1, Bytes: 1}); !errors.Is(err, ErrDescriptorLimit) {
		t.Fatalf("per-flow descriptor limit error = %v", err)
	}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorData, FlowID: protocol.FlowID{2}, ItemID: 1, Generation: 1, Bytes: 1}); !errors.Is(err, ErrDescriptorLimit) {
		t.Fatalf("flow limit error = %v", err)
	}
	if err := scheduler.Enqueue(Descriptor{}); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatalf("invalid descriptor error = %v", err)
	}
	scheduler.DropFlow(flowID)
	if snapshot := scheduler.Snapshot(); snapshot.Flows != 0 || snapshot.Descriptors != 0 {
		t.Fatalf("dropped scheduler snapshot = %#v", snapshot)
	}
}

func TestSchedulerRejectsConflictingFINSlot(t *testing.T) {
	scheduler, _ := NewScheduler(1)
	flowID := protocol.FlowID{1}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorFIN, FlowID: flowID, ItemID: 1, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Enqueue(Descriptor{Kind: DescriptorFIN, FlowID: flowID, ItemID: 2, Generation: 1}); !errors.Is(err, ErrDescriptorConflict) {
		t.Fatalf("conflicting FIN error = %v", err)
	}
}
