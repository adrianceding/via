package path

import (
	"errors"
	"net"
	"net/netip"
	"sort"
)

const (
	MaxInterfaces            = 64
	MaxAddressesPerInterface = 8
	MaxEventsPerRefresh      = 128
)

var (
	ErrEnumeration   = errors.New("path: interface enumeration failed")
	ErrSnapshotLimit = errors.New("path: interface snapshot limit exceeded")
)

type DecisionReason uint8

const (
	ReasonEligible DecisionReason = iota + 1
	ReasonNotIncluded
	ReasonExcluded
	ReasonInterfaceDown
	ReasonNoEligibleAddress
	ReasonLoopbackMismatch
)

type Interface struct {
	Index     int
	Name      string
	Up        bool
	Loopback  bool
	Addresses []netip.Addr
}

type Enumerator interface {
	Interfaces() ([]Interface, error)
}

type SystemEnumerator struct{}

func (SystemEnumerator) Interfaces() ([]Interface, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, errors.Join(ErrEnumeration, err)
	}
	result := make([]Interface, 0, len(interfaces))
	for _, networkInterface := range interfaces {
		addresses, err := networkInterface.Addrs()
		if err != nil {
			return nil, errors.Join(ErrEnumeration, err)
		}
		entry := Interface{
			Index:    networkInterface.Index,
			Name:     networkInterface.Name,
			Up:       networkInterface.Flags&net.FlagUp != 0,
			Loopback: networkInterface.Flags&net.FlagLoopback != 0,
		}
		for _, address := range addresses {
			prefix, err := netip.ParsePrefix(address.String())
			if err != nil {
				continue
			}
			entry.Addresses = append(entry.Addresses, prefix.Addr().Unmap())
		}
		result = append(result, entry)
	}
	return result, nil
}

type Candidate struct {
	InterfaceIndex int
	InterfaceName  string
	LocalAddress   netip.Addr
}

type Decision struct {
	InterfaceIndex int
	InterfaceName  string
	Reason         DecisionReason
	Candidates     []Candidate
}

type Snapshot struct {
	Decisions  []Decision
	Candidates []Candidate
}

type EventKind uint8

const (
	CandidateAdded EventKind = iota + 1
	CandidateRemoved
)

type Event struct {
	Kind      EventKind
	Candidate Candidate
}

type Manager struct {
	enumerator Enumerator
	filter     Filter
	remote     netip.Addr
	current    Snapshot
}

func NewManager(enumerator Enumerator, filter Filter, remote netip.Addr) (*Manager, error) {
	if enumerator == nil || !remote.IsValid() || remote.IsUnspecified() {
		return nil, ErrEnumeration
	}
	return &Manager{enumerator: enumerator, filter: filter, remote: remote.Unmap()}, nil
}

func (manager *Manager) Refresh() ([]Event, Snapshot, error) {
	interfaces, err := manager.enumerator.Interfaces()
	if err != nil {
		return nil, manager.Snapshot(), err
	}
	next, err := evaluate(manager.filter, manager.remote, interfaces)
	if err != nil {
		return nil, manager.Snapshot(), err
	}
	events := diffCandidates(manager.current.Candidates, next.Candidates)
	if len(events) > MaxEventsPerRefresh {
		return nil, manager.Snapshot(), ErrSnapshotLimit
	}
	manager.current = next
	return events, manager.Snapshot(), nil
}

func (manager *Manager) Snapshot() Snapshot {
	if manager == nil {
		return Snapshot{}
	}
	return cloneSnapshot(manager.current)
}

func evaluate(filter Filter, remote netip.Addr, interfaces []Interface) (Snapshot, error) {
	if len(interfaces) > MaxInterfaces {
		return Snapshot{}, ErrSnapshotLimit
	}
	interfaces = append([]Interface(nil), interfaces...)
	sort.Slice(interfaces, func(i, j int) bool {
		if interfaces[i].Name != interfaces[j].Name {
			return interfaces[i].Name < interfaces[j].Name
		}
		return interfaces[i].Index < interfaces[j].Index
	})
	snapshot := Snapshot{Decisions: make([]Decision, 0, len(interfaces))}
	for _, networkInterface := range interfaces {
		decision := Decision{InterfaceIndex: networkInterface.Index, InterfaceName: networkInterface.Name}
		matched, reason := filter.Match(networkInterface.Name)
		switch {
		case !matched:
			decision.Reason = reason
		case !networkInterface.Up:
			decision.Reason = ReasonInterfaceDown
		case networkInterface.Loopback && !remote.IsLoopback():
			decision.Reason = ReasonLoopbackMismatch
		default:
			addresses := eligibleAddresses(networkInterface.Addresses, remote.BitLen())
			if len(addresses) > MaxAddressesPerInterface {
				return Snapshot{}, ErrSnapshotLimit
			}
			if len(addresses) == 0 {
				decision.Reason = ReasonNoEligibleAddress
			} else {
				decision.Reason = ReasonEligible
				for _, address := range addresses {
					candidate := Candidate{
						InterfaceIndex: networkInterface.Index,
						InterfaceName:  networkInterface.Name,
						LocalAddress:   address,
					}
					decision.Candidates = append(decision.Candidates, candidate)
					snapshot.Candidates = append(snapshot.Candidates, candidate)
				}
			}
		}
		snapshot.Decisions = append(snapshot.Decisions, decision)
	}
	return snapshot, nil
}

func eligibleAddresses(addresses []netip.Addr, remoteBitLength int) []netip.Addr {
	seen := make(map[netip.Addr]struct{}, len(addresses))
	result := make([]netip.Addr, 0, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if !address.IsValid() || address.BitLen() != remoteBitLength || address.IsUnspecified() || address.IsMulticast() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() {
			continue
		}
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		result = append(result, address)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Compare(result[j]) < 0 })
	return result
}

func diffCandidates(previous, next []Candidate) []Event {
	previousSet := make(map[Candidate]struct{}, len(previous))
	nextSet := make(map[Candidate]struct{}, len(next))
	for _, candidate := range previous {
		previousSet[candidate] = struct{}{}
	}
	for _, candidate := range next {
		nextSet[candidate] = struct{}{}
	}
	var events []Event
	for _, candidate := range previous {
		if _, exists := nextSet[candidate]; !exists {
			events = append(events, Event{Kind: CandidateRemoved, Candidate: candidate})
		}
	}
	for _, candidate := range next {
		if _, exists := previousSet[candidate]; !exists {
			events = append(events, Event{Kind: CandidateAdded, Candidate: candidate})
		}
	}
	return events
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	clone := Snapshot{Candidates: append([]Candidate(nil), snapshot.Candidates...)}
	clone.Decisions = make([]Decision, len(snapshot.Decisions))
	for index, decision := range snapshot.Decisions {
		clone.Decisions[index] = decision
		clone.Decisions[index].Candidates = append([]Candidate(nil), decision.Candidates...)
	}
	return clone
}
