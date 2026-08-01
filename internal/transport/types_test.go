package transport

import (
	"errors"
	"testing"
)

func TestCapabilitiesAreImmutableValue(t *testing.T) {
	spec := CapabilitySpec{
		MaxEncodedFrame: 65_544,
		Reliable:        true,
		Ordered:         true,
		Encrypted:       false,
		Multiplexed:     true,
		HalfClose:       true,
	}
	capabilities, err := NewCapabilities(spec)
	if err != nil {
		t.Fatalf("NewCapabilities() error = %v", err)
	}
	spec.MaxEncodedFrame = 1
	spec.Reliable = false

	if capabilities.MaxEncodedFrame() != 65_544 || !capabilities.Reliable() || !capabilities.Ordered() ||
		capabilities.Encrypted() || !capabilities.Multiplexed() || !capabilities.HalfClose() {
		t.Fatalf("capabilities changed after input mutation: %#v", capabilities)
	}
	copyOfCapabilities := capabilities
	if copyOfCapabilities != capabilities {
		t.Fatal("capabilities value copy differs")
	}
}

func TestCapabilitiesRejectZeroMaximumFrame(t *testing.T) {
	_, err := NewCapabilities(CapabilitySpec{})
	if !errors.Is(err, ErrInvalidCapabilities) {
		t.Fatalf("NewCapabilities() error = %v, want ErrInvalidCapabilities", err)
	}
}

func TestV1QueueLimitsFixedVector(t *testing.T) {
	limits := V1QueueLimits()
	if limits.MaxFrames != 256 || limits.MaxBytes != 1<<20 ||
		limits.ReservedControlFrames != 16 || limits.ReservedControlBytes != 16<<10 {
		t.Fatalf("V1QueueLimits() = %#v", limits)
	}
	capabilities := mustCapabilities(t, uint32(limits.MaxBytes-limits.ReservedControlBytes))
	if err := limits.Validate(capabilities); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestQueueLimitsRejectInvalidBoundaries(t *testing.T) {
	capabilities := mustCapabilities(t, 100)
	tests := []struct {
		name   string
		limits QueueLimits
	}{
		{name: "zero frames", limits: QueueLimits{MaxBytes: 200}},
		{name: "zero bytes", limits: QueueLimits{MaxFrames: 2}},
		{name: "all frames reserved", limits: QueueLimits{MaxFrames: 2, MaxBytes: 200, ReservedControlFrames: 2}},
		{name: "frame reserve exceeds total", limits: QueueLimits{MaxFrames: 2, MaxBytes: 200, ReservedControlFrames: 3}},
		{name: "all bytes reserved", limits: QueueLimits{MaxFrames: 2, MaxBytes: 200, ReservedControlBytes: 200}},
		{name: "byte reserve exceeds total", limits: QueueLimits{MaxFrames: 2, MaxBytes: 200, ReservedControlBytes: 201}},
		{name: "maximum frame does not fit data", limits: QueueLimits{MaxFrames: 2, MaxBytes: 200, ReservedControlBytes: 101}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.limits.Validate(capabilities); !errors.Is(err, ErrInvalidQueueLimits) {
				t.Fatalf("Validate() error = %v, want ErrInvalidQueueLimits", err)
			}
		})
	}
	if err := (QueueLimits{MaxFrames: 2, MaxBytes: 200}).Validate(Capabilities{}); !errors.Is(err, ErrInvalidQueueLimits) {
		t.Fatalf("Validate(zero capabilities) error = %v, want ErrInvalidQueueLimits", err)
	}
}

func TestWriteRequestBoundaries(t *testing.T) {
	capabilities := mustCapabilities(t, 4)
	valid := []WriteRequest{
		{Class: FrameData, Encoded: []byte{1}},
		{Class: FrameControl, Encoded: []byte{1, 2, 3, 4}},
	}
	for _, request := range valid {
		if err := request.Validate(capabilities); err != nil {
			t.Fatalf("Validate(%#v) error = %v", request, err)
		}
	}

	tests := []struct {
		name    string
		request WriteRequest
		want    error
	}{
		{name: "unknown class", request: WriteRequest{Encoded: []byte{1}}, want: ErrInvalidFrame},
		{name: "empty", request: WriteRequest{Class: FrameData}, want: ErrInvalidFrame},
		{name: "too large", request: WriteRequest{Class: FrameData, Encoded: make([]byte, 5)}, want: ErrFrameTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.request.Validate(capabilities); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
	if err := valid[0].Validate(Capabilities{}); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("Validate(zero capabilities) error = %v, want ErrInvalidFrame", err)
	}
}

func mustCapabilities(t *testing.T, maxFrame uint32) Capabilities {
	t.Helper()
	capabilities, err := NewCapabilities(CapabilitySpec{MaxEncodedFrame: maxFrame})
	if err != nil {
		t.Fatalf("NewCapabilities() error = %v", err)
	}
	return capabilities
}
