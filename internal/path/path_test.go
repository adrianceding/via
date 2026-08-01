package path

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"testing"
	"time"
)

func TestFilterUsesFullNameGlobAndDenyWins(t *testing.T) {
	filter, err := NewFilter([]string{"eth*", "wan?", "en[0-9]"}, []string{"*debug*", "eth9"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		allow  bool
		reason DecisionReason
	}{
		{name: "eth0", allow: true, reason: ReasonEligible},
		{name: "eth9", allow: false, reason: ReasonExcluded},
		{name: "eth0debug", allow: false, reason: ReasonExcluded},
		{name: "wan1", allow: true, reason: ReasonEligible},
		{name: "wan10", allow: false, reason: ReasonNotIncluded},
		{name: "en7", allow: true, reason: ReasonEligible},
		{name: "xeth0", allow: false, reason: ReasonNotIncluded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allow, reason := filter.Match(test.name)
			if allow != test.allow || reason != test.reason {
				t.Fatalf("Match(%q) = %t, %v", test.name, allow, reason)
			}
		})
	}
}

func TestFilterRejectsInvalidAndExcessivePatterns(t *testing.T) {
	for _, patterns := range [][]string{{""}, {"["}, {string(make([]byte, MaxPatternBytes+1))}} {
		if _, err := NewFilter(patterns, nil); !errors.Is(err, ErrInvalidPattern) {
			t.Fatalf("patterns %q error = %v", patterns, err)
		}
	}
	tooMany := make([]string, MaxIncludePatterns+1)
	for index := range tooMany {
		tooMany[index] = "*"
	}
	if _, err := NewFilter(tooMany, nil); !errors.Is(err, ErrInvalidPattern) {
		t.Fatalf("pattern count error = %v", err)
	}
}

func TestManagerFiltersAddressesAndReportsReasons(t *testing.T) {
	filter, err := NewFilter([]string{"eth*", "lo"}, []string{"eth9"})
	if err != nil {
		t.Fatal(err)
	}
	enumerator := &fakeEnumerator{interfaces: []Interface{
		{Index: 1, Name: "lo", Up: true, Loopback: true, Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}},
		{Index: 2, Name: "eth0", Up: true, Addresses: []netip.Addr{
			netip.MustParseAddr("fe80::1"),
			netip.MustParseAddr("2001:db8::2"),
			netip.MustParseAddr("192.0.2.2"),
			netip.MustParseAddr("192.0.2.2"),
		}},
		{Index: 3, Name: "eth1", Addresses: []netip.Addr{netip.MustParseAddr("192.0.2.3")}},
		{Index: 4, Name: "eth9", Up: true, Addresses: []netip.Addr{netip.MustParseAddr("192.0.2.9")}},
		{Index: 5, Name: "docker0", Up: true, Addresses: []netip.Addr{netip.MustParseAddr("172.17.0.1")}},
	}}
	manager, err := NewManager(enumerator, filter, netip.MustParseAddr("198.51.100.1"))
	if err != nil {
		t.Fatal(err)
	}
	events, snapshot, err := manager.Refresh()
	if err != nil {
		t.Fatal(err)
	}
	wantCandidate := Candidate{InterfaceIndex: 2, InterfaceName: "eth0", LocalAddress: netip.MustParseAddr("192.0.2.2")}
	if !reflect.DeepEqual(snapshot.Candidates, []Candidate{wantCandidate}) ||
		!reflect.DeepEqual(events, []Event{{Kind: CandidateAdded, Candidate: wantCandidate}}) {
		t.Fatalf("snapshot/events = %#v / %#v", snapshot, events)
	}
	reasons := make(map[string]DecisionReason)
	for _, decision := range snapshot.Decisions {
		reasons[decision.InterfaceName] = decision.Reason
	}
	if reasons["lo"] != ReasonLoopbackMismatch || reasons["eth1"] != ReasonInterfaceDown ||
		reasons["eth9"] != ReasonExcluded || reasons["docker0"] != ReasonNotIncluded {
		t.Fatalf("decision reasons = %#v", reasons)
	}
}

func TestManagerEmitsDeterministicHotplugRenameAndAddressEvents(t *testing.T) {
	filter, err := NewFilter(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	enumerator := &fakeEnumerator{interfaces: []Interface{{
		Index: 2, Name: "eth0", Up: true, Addresses: []netip.Addr{netip.MustParseAddr("192.0.2.2")},
	}}}
	manager, err := NewManager(enumerator, filter, netip.MustParseAddr("198.51.100.1"))
	if err != nil {
		t.Fatal(err)
	}
	initial, _, err := manager.Refresh()
	if err != nil || len(initial) != 1 || initial[0].Kind != CandidateAdded {
		t.Fatalf("initial refresh = %#v, %v", initial, err)
	}

	enumerator.interfaces = []Interface{
		{Index: 2, Name: "wan0", Up: true, Addresses: []netip.Addr{netip.MustParseAddr("192.0.2.2")}},
		{Index: 3, Name: "eth1", Up: true, Addresses: []netip.Addr{netip.MustParseAddr("192.0.2.3")}},
	}
	events, _, err := manager.Refresh()
	if err != nil {
		t.Fatal(err)
	}
	want := []Event{
		{Kind: CandidateRemoved, Candidate: Candidate{InterfaceIndex: 2, InterfaceName: "eth0", LocalAddress: netip.MustParseAddr("192.0.2.2")}},
		{Kind: CandidateAdded, Candidate: Candidate{InterfaceIndex: 3, InterfaceName: "eth1", LocalAddress: netip.MustParseAddr("192.0.2.3")}},
		{Kind: CandidateAdded, Candidate: Candidate{InterfaceIndex: 2, InterfaceName: "wan0", LocalAddress: netip.MustParseAddr("192.0.2.2")}},
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("hotplug events = %#v, want %#v", events, want)
	}

	enumerator.interfaces[0].Addresses = nil
	events, _, err = manager.Refresh()
	if err != nil || len(events) != 1 || events[0].Kind != CandidateRemoved || events[0].Candidate.InterfaceName != "wan0" {
		t.Fatalf("address loss events = %#v, %v", events, err)
	}
	enumerator.interfaces[0].Addresses = []netip.Addr{netip.MustParseAddr("192.0.2.4")}
	events, _, err = manager.Refresh()
	if err != nil || len(events) != 1 || events[0].Kind != CandidateAdded || events[0].Candidate.LocalAddress.String() != "192.0.2.4" {
		t.Fatalf("address recovery events = %#v, %v", events, err)
	}
}

func TestManagerKeepsPriorSnapshotOnEnumerationOrLimitFailure(t *testing.T) {
	filter, _ := NewFilter(nil, nil)
	enumerator := &fakeEnumerator{interfaces: []Interface{{
		Index: 1, Name: "eth0", Up: true, Addresses: []netip.Addr{netip.MustParseAddr("192.0.2.1")},
	}}}
	manager, err := NewManager(enumerator, filter, netip.MustParseAddr("198.51.100.1"))
	if err != nil {
		t.Fatal(err)
	}
	_, before, err := manager.Refresh()
	if err != nil {
		t.Fatal(err)
	}
	enumerator.err = ErrEnumeration
	if _, after, err := manager.Refresh(); !errors.Is(err, ErrEnumeration) || !reflect.DeepEqual(after, before) {
		t.Fatalf("enumeration failure snapshot = %#v, %v", after, err)
	}
	enumerator.err = nil
	enumerator.interfaces = make([]Interface, MaxInterfaces+1)
	if _, after, err := manager.Refresh(); !errors.Is(err, ErrSnapshotLimit) || !reflect.DeepEqual(after, before) {
		t.Fatalf("limit failure snapshot = %#v, %v", after, err)
	}
}

func TestLoopbackCandidateAllowedForLoopbackRelay(t *testing.T) {
	filter, _ := NewFilter(nil, nil)
	enumerator := &fakeEnumerator{interfaces: []Interface{{
		Index: 1, Name: "lo", Up: true, Loopback: true, Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")},
	}}}
	manager, err := NewManager(enumerator, filter, netip.MustParseAddr("127.0.0.1"))
	if err != nil {
		t.Fatal(err)
	}
	_, snapshot, err := manager.Refresh()
	if err != nil || len(snapshot.Candidates) != 1 || snapshot.Candidates[0].LocalAddress.String() != "127.0.0.1" {
		t.Fatalf("loopback snapshot = %#v, %v", snapshot, err)
	}
}

func TestWatchUsesInjectedTicksAndBoundedOutput(t *testing.T) {
	filter, _ := NewFilter(nil, nil)
	enumerator := &fakeEnumerator{interfaces: []Interface{{
		Index: 1, Name: "eth0", Up: true, Addresses: []netip.Addr{netip.MustParseAddr("192.0.2.1")},
	}}}
	manager, _ := NewManager(enumerator, filter, netip.MustParseAddr("198.51.100.1"))
	clock := newManualClock()
	updates := make(chan Update, 2)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- manager.Watch(ctx, clock, updates) }()
	initial := <-updates
	if initial.Err != nil || len(initial.Events) != 1 || initial.Events[0].Kind != CandidateAdded {
		t.Fatalf("initial update = %#v", initial)
	}
	enumerator.interfaces = nil
	clock.Tick()
	removed := <-updates
	if removed.Err != nil || len(removed.Events) != 1 || removed.Events[0].Kind != CandidateRemoved {
		t.Fatalf("tick update = %#v", removed)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("watch result = %v", err)
	}
	if !clock.ticker.stopped {
		t.Fatal("watch did not stop ticker")
	}
}

func TestWatchFailsInsteadOfBlockingOnFullOutput(t *testing.T) {
	filter, _ := NewFilter(nil, nil)
	manager, _ := NewManager(&fakeEnumerator{}, filter, netip.MustParseAddr("198.51.100.1"))
	clock := newManualClock()
	updates := make(chan Update, 1)
	updates <- Update{}
	if err := manager.Watch(context.Background(), clock, updates); !errors.Is(err, ErrUpdateQueueFull) {
		t.Fatalf("full output error = %v", err)
	}
}

type fakeEnumerator struct {
	interfaces []Interface
	err        error
}

type manualClock struct{ ticker *manualTicker }

func newManualClock() *manualClock {
	return &manualClock{ticker: &manualTicker{ticks: make(chan time.Time, 1)}}
}

func (clock *manualClock) NewTicker(interval time.Duration) Ticker {
	if interval != DiscoveryInterval {
		panic("unexpected discovery interval")
	}
	return clock.ticker
}

func (clock *manualClock) Tick() { clock.ticker.ticks <- time.Unix(1, 0) }

type manualTicker struct {
	ticks   chan time.Time
	stopped bool
}

func (ticker *manualTicker) C() <-chan time.Time { return ticker.ticks }
func (ticker *manualTicker) Stop()               { ticker.stopped = true }

func (enumerator *fakeEnumerator) Interfaces() ([]Interface, error) {
	if enumerator.err != nil {
		return nil, enumerator.err
	}
	result := make([]Interface, len(enumerator.interfaces))
	for index, networkInterface := range enumerator.interfaces {
		result[index] = networkInterface
		result[index].Addresses = append([]netip.Addr(nil), networkInterface.Addresses...)
	}
	return result, nil
}
