package status

import (
	"encoding/json"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
)

func TestQualityDataSampleAgePreservesNullAndZero(t *testing.T) {
	snapshot := Snapshot{Sessions: []Session{{Quality: Quality{}}}}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"data_sample_age_ms":null`) {
		t.Fatalf("missing null DATA sample age: %s", encoded)
	}

	age := uint64(0)
	snapshot.Sessions[0].Quality.DataSampleAgeMillis = &age
	encoded, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"data_sample_age_ms":0`) {
		t.Fatalf("missing zero DATA sample age: %s", encoded)
	}
}

func TestCountersExposeDistinctDataPayloadJSONFields(t *testing.T) {
	encoded, err := json.Marshal(Counters{
		BytesSent: 11, BytesReceived: 13,
		DataPayloadBytesSent: 17, DataPayloadBytesReceived: 19,
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, field := range []string{`"bytes_sent":11`, `"bytes_received":13`, `"data_payload_bytes_sent":17`, `"data_payload_bytes_received":19`} {
		if !strings.Contains(text, field) {
			t.Fatalf("missing counter field %q in %s", field, text)
		}
	}
}

func TestHasherIsDeterministicDomainSeparatedAndOpaque(t *testing.T) {
	key := [32]byte{1, 2, 3}
	hasher, err := NewHasher(key)
	if err != nil {
		t.Fatal(err)
	}
	flowID := protocol.FlowID{1, 2, 3}
	pathGroupID := protocol.PathGroupID{1, 2, 3}
	target := protocol.Target{DNSName: "secret.example", Port: 443}
	flowHash := hasher.FlowID(flowID)
	if flowHash == "" || flowHash != hasher.FlowID(flowID) || len(flowHash) != 2*HashBytes {
		t.Fatalf("flow hash = %q", flowHash)
	}
	pathGroupHash := hasher.PathGroupID(pathGroupID)
	if pathGroupHash == "" || pathGroupHash != hasher.PathGroupID(pathGroupID) || len(pathGroupHash) != 2*HashBytes {
		t.Fatalf("path group hash = %q", pathGroupHash)
	}
	if flowHash == hasher.SessionID(0x010203) || flowHash == pathGroupHash || flowHash == hasher.Target(target) {
		t.Fatal("hash domains collided")
	}
	if strings.Contains(hasher.Target(target), target.DNSName) {
		t.Fatal("target hash exposed target")
	}
	if _, err := NewHasher([32]byte{}); err == nil {
		t.Fatal("zero hash key accepted")
	}
}

func TestSnapshotCloneDoesNotExposeMutableSlices(t *testing.T) {
	source := Snapshot{
		Interfaces: []Interface{{Index: 1, Name: "eth0", Addresses: []string{"192.0.2.1"}, Reason: InterfaceEligible}},
		Sessions:   []Session{{IDHash: testHash(1)}},
		Flows:      []Flow{{IDHash: testHash(2)}},
		Terminals:  []Terminal{{IDHash: testHash(3)}},
	}
	clone := cloneSnapshot(source)
	clone.Interfaces[0].Addresses[0] = "192.0.2.99"
	clone.Interfaces[0].Name = "changed"
	clone.Sessions[0].IDHash = testHash(9)
	clone.Flows[0].IDHash = testHash(9)
	clone.Terminals[0].IDHash = testHash(9)
	if source.Interfaces[0].Addresses[0] != "192.0.2.1" || source.Interfaces[0].Name != "eth0" ||
		source.Sessions[0].IDHash != testHash(1) || source.Flows[0].IDHash != testHash(2) || source.Terminals[0].IDHash != testHash(3) {
		t.Fatalf("source mutated = %#v", source)
	}
}

func TestModelValidationCanonicalizesAddressesAndRejectsUnsafeValues(t *testing.T) {
	value, err := normalizeInterface(Interface{
		Index: 1, Name: "eth0", Reason: InterfaceEligible,
		Addresses: []string{"2001:db8::2", "192.0.2.1", "192.0.2.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(value.Addresses) != 2 || value.Addresses[0] != "192.0.2.1" || value.Addresses[1] != "2001:db8::2" {
		t.Fatalf("addresses = %#v", value.Addresses)
	}
	if _, err := normalizeInterface(Interface{Index: 1, Name: "<script>", Reason: InterfaceEligible}); err == nil {
		t.Fatal("unsafe interface label accepted")
	}
	validSessionValue := Session{
		IDHash: testHash(1), Transport: "tcp", Interface: "eth0", LocalAddress: "192.0.2.1",
		PathGroupID: testHash(2), Lane: 1, State: SessionReady, Reason: ReasonStarted,
	}
	if !validSession(validSessionValue) {
		t.Fatal("valid session rejected")
	}
	validSessionValue.LocalAddress = netip.IPv6LinkLocalAllNodes().String()
	if !validSession(validSessionValue) {
		t.Fatal("status model should describe, not schedule, multicast addresses")
	}
	for _, name := range []string{"udp", "http2", "direct-tcp"} {
		validSessionValue.Transport = name
		if !validSession(validSessionValue) {
			t.Fatalf("future transport name %q rejected", name)
		}
	}
	for _, name := range []string{"", "UDP", "1tcp", "tcp_2", "tcp--direct"} {
		validSessionValue.Transport = name
		if validSession(validSessionValue) {
			t.Fatalf("invalid transport name %q accepted", name)
		}
	}
	validSessionValue.Transport = "tcp"
	validSessionValue.PathGroupID = ""
	if validSession(validSessionValue) {
		t.Fatal("lane without path group accepted")
	}
	validSessionValue.PathGroupID = testHash(2)
	validSessionValue.Lane = 65
	if validSession(validSessionValue) {
		t.Fatal("lane above 64 accepted")
	}
	if validTerminal(Terminal{IDHash: testHash(2), State: FlowRelaying, FinishedAt: time.Unix(1, 0)}) {
		t.Fatal("non-terminal flow accepted as terminal summary")
	}
}

func TestExtendedSessionFlowAndTerminalValidation(t *testing.T) {
	startedAt := time.Unix(100, 0).UTC()
	session := validTestSession(1)
	session.ConnectionID = testHash(11)
	session.PrincipalHash = testHash(12)
	session.LocalEndpoint = "192.0.2.1:41000"
	session.RemoteEndpoint = "198.51.100.2:9443"
	session.StateSince = startedAt
	session.LastProbeAt = startedAt.Add(time.Second)
	session.Reconnects = 3
	if !validSession(session) {
		t.Fatalf("extended session rejected = %#v", session)
	}

	flow := validTestFlow(2)
	flow.FlowID = testHash(21)
	flow.PublishedAttachments = 2
	flow.PolicyAttachments = 2
	flow.PreferredConnectionID = session.ConnectionID
	flow.TxAllocatedOffset = 100
	flow.TxAcknowledged = 50
	flow.UnacknowledgedBytes = 50
	flow.AdaptiveTransition = AdaptiveTransitionRetryEscalated
	flow.StartedAt = startedAt
	flow.StateSince = startedAt.Add(time.Second)
	if !validFlow(flow) {
		t.Fatalf("extended flow rejected = %#v", flow)
	}

	terminal := Terminal{
		IDHash: flow.IDHash, FlowID: flow.FlowID, State: FlowClosed, Reason: ReasonCompleted,
		DeliveryMode: flow.DeliveryMode, PathSelection: flow.PathSelection,
		StartedAt: startedAt, FinishedAt: startedAt.Add(2 * time.Second),
	}
	if !validTerminal(terminal) {
		t.Fatalf("extended terminal rejected = %#v", terminal)
	}

	session.ConnectionID = "bad"
	if validSession(session) {
		t.Fatal("invalid connection ID accepted")
	}
	session.ConnectionID = testHash(11)
	session.RemoteEndpoint = "missing-port"
	if validSession(session) {
		t.Fatal("invalid endpoint accepted")
	}
	flow.PolicyAttachments = flow.PublishedAttachments + 1
	if validFlow(flow) {
		t.Fatal("policy attachments above published attachments accepted")
	}
	terminal.FinishedAt = terminal.StartedAt.Add(-time.Second)
	if validTerminal(terminal) {
		t.Fatal("terminal finishing before start accepted")
	}
}

func testHash(value uint64) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, 2*HashBytes)
	for index := len(encoded) - 1; index >= 0; index-- {
		encoded[index] = digits[value&0xf]
		value >>= 4
	}
	return string(encoded)
}
