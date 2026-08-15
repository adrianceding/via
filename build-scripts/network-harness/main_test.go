package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSOCKSConnectSupportsOptionalAuthentication(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		username string
		password string
		method   byte
	}{
		{name: "no authentication", method: 0},
		{name: "username password", username: "user", password: "password", method: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			serverResult := make(chan error, 1)
			go func() {
				greeting := make([]byte, 3)
				if _, err := io.ReadFull(server, greeting); err != nil {
					serverResult <- err
					return
				}
				if !bytes.Equal(greeting, []byte{5, 1, testCase.method}) {
					serverResult <- io.ErrUnexpectedEOF
					return
				}
				if _, err := server.Write([]byte{5, testCase.method}); err != nil {
					serverResult <- err
					return
				}
				if testCase.method == 2 {
					authentication := make([]byte, 2+len(testCase.username)+1+len(testCase.password))
					if _, err := io.ReadFull(server, authentication); err != nil {
						serverResult <- err
						return
					}
					if _, err := server.Write([]byte{1, 0}); err != nil {
						serverResult <- err
						return
					}
				}
				request := make([]byte, 10)
				if _, err := io.ReadFull(server, request); err != nil {
					serverResult <- err
					return
				}
				_, err := server.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
				serverResult <- err
			}()
			if err := socksConnect(client, "192.0.2.1:443", testCase.username, testCase.password); err != nil {
				t.Fatal(err)
			}
			if err := <-serverResult; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransferHeaderRoundTrip(t *testing.T) {
	want := transferHeader{
		mode: transferBidirectional, flags: flagSynchronizeDownload,
		requestID: 41, seed: 97, uploadBytes: maximumTransfer, downloadBytes: 1,
	}
	var encoded bytes.Buffer
	if err := writeTransferHeader(&encoded, want); err != nil {
		t.Fatal(err)
	}
	got, err := readTransferHeader(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("header = %+v, want %+v", got, want)
	}
}

func TestTransferHeaderRejectsMalformedFields(t *testing.T) {
	valid := transferHeader{mode: transferUpload, requestID: 1, seed: 2, uploadBytes: 1}
	var encoded bytes.Buffer
	if err := writeTransferHeader(&encoded, valid); err != nil {
		t.Fatal(err)
	}
	original := encoded.Bytes()
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "magic", mutate: func(data []byte) { data[0] ^= 1 }},
		{name: "mode", mutate: func(data []byte) { data[4] = 0xff }},
		{name: "flags", mutate: func(data []byte) { data[5] = 0x80 }},
		{name: "reserved first", mutate: func(data []byte) { data[6] = 1 }},
		{name: "reserved second", mutate: func(data []byte) { data[7] = 1 }},
		{name: "zero request id", mutate: func(data []byte) { binary.BigEndian.PutUint64(data[8:16], 0) }},
		{name: "upload too large", mutate: func(data []byte) { binary.BigEndian.PutUint64(data[24:32], maximumTransfer+1) }},
		{name: "download too large", mutate: func(data []byte) { binary.BigEndian.PutUint64(data[32:40], maximumTransfer+1) }},
		{name: "upload with download", mutate: func(data []byte) { binary.BigEndian.PutUint64(data[32:40], 1) }},
		{name: "download without download bytes", mutate: func(data []byte) { data[4] = transferDownload }},
		{name: "bidirectional without download", mutate: func(data []byte) { data[4] = transferBidirectional }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := append([]byte(nil), original...)
			test.mutate(data)
			if _, err := readTransferHeader(bytes.NewReader(data)); err == nil {
				t.Fatal("malformed transfer header was accepted")
			}
		})
	}
	for length := 0; length < transferHeaderBytes; length++ {
		if _, err := readTransferHeader(bytes.NewReader(original[:length])); !errorsIsUnexpectedEOF(err) {
			t.Fatalf("length %d: error = %v, want truncated input", length, err)
		}
	}
}

func TestWriteResultFilePersistsPositiveDuration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "duration")
	if err := writeResultFile(path, 125*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "125000000\n" {
		t.Fatalf("duration file = %q", data)
	}
	if err := writeResultFile(path, 0); err == nil {
		t.Fatal("zero duration was accepted")
	}
}

func errorsIsUnexpectedEOF(err error) bool {
	return err == io.EOF || err == io.ErrUnexpectedEOF
}

func TestPatternDetectsExactCorruptionOffset(t *testing.T) {
	data := make([]byte, 4096)
	fillPattern(data, 71, 8192)
	if err := verifyPattern(data, 71, 8192); err != nil {
		t.Fatal(err)
	}
	data[123] ^= 1
	if err := verifyPattern(data, 71, 8192); err == nil {
		t.Fatal("corrupt payload was accepted")
	} else if !strings.Contains(err.Error(), "byte 8315") {
		t.Fatalf("corruption error = %q, want exact absolute offset", err)
	}
}

func TestSessionsReadyRequiresNamedReadyPathsAndMeasuredFastest(t *testing.T) {
	var sessions sessionList
	sessions.Items = make([]sessionStatus, 2)
	sessions.Items[0].Interface = "vianet-a"
	sessions.Items[0].State = 3
	sessions.Items[0].Quality.SmoothedRTTMicros = 100
	sessions.Items[1].Interface = "vianet-b"
	sessions.Items[1].State = 3
	sessions.Items[1].Quality.SmoothedRTTMicros = 200
	if !sessionsReady(sessions, 2, []string{"vianet-a", "vianet-b"}, "vianet-a") {
		t.Fatal("valid ready paths were rejected")
	}
	sessions.Items[1].State = 4
	if sessionsReady(sessions, 2, []string{"vianet-a", "vianet-b"}, "vianet-a") {
		t.Fatal("backoff path was counted as ready")
	}
}

func TestSelectSessionWrittenUsesInterfaceOrRemoteHost(t *testing.T) {
	sessions := sessionList{Items: make([]sessionStatus, 2)}
	sessions.Items[0].Interface = "vianet-a"
	sessions.Items[0].RemoteEndpoint = "10.201.0.2:40000"
	sessions.Items[0].Quality.WrittenDataPayloadBytes = 100
	sessions.Items[1].Interface = "vianet-b"
	sessions.Items[1].RemoteEndpoint = "10.202.0.2:40001"
	sessions.Items[1].Quality.WrittenDataPayloadBytes = 200

	if written, matches := selectSessionWritten(sessions, "vianet-b", ""); written != 200 || matches != 1 {
		t.Fatalf("interface selection = %d/%d", written, matches)
	}
	if written, matches := selectSessionWritten(sessions, "", "10.201.0.2"); written != 100 || matches != 1 {
		t.Fatalf("remote selection = %d/%d", written, matches)
	}
	if written, matches := selectSessionWritten(sessions, "missing", ""); written != 0 || matches != 0 {
		t.Fatalf("missing selection = %d/%d", written, matches)
	}
}

func TestObserveFlowWindowValidatesOffsetsAndCountsFullWindow(t *testing.T) {
	flows := flowList{Items: []flowStatus{
		{UnacknowledgedBytes: 320 << 10, TxAllocatedOffset: 640 << 10, TxAcknowledged: 320 << 10},
		{UnacknowledgedBytes: 64 << 10, TxAllocatedOffset: 96 << 10, TxAcknowledged: 32 << 10},
	}}
	observation, err := observeFlowWindow(flows, 320<<10)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ActiveFlows != 2 || observation.FullWindowFlows != 1 || observation.MaximumUnacknowledged != 320<<10 {
		t.Fatalf("observation = %+v", observation)
	}

	flows.Items[1].UnacknowledgedBytes++
	if _, err := observeFlowWindow(flows, 320<<10); err == nil {
		t.Fatal("inconsistent flow offsets were accepted")
	}
}

func TestFlowWindowSummaryRetainsBoundedAggregate(t *testing.T) {
	summary := flowWindowSummary{WindowBytes: 320 << 10}
	var streak uint64
	summary.add(flowWindowObservation{ActiveFlows: 1, FullWindowFlows: 1, MaximumUnacknowledged: 320 << 10}, false, 1, &streak)
	summary.add(flowWindowObservation{ActiveFlows: 1, FullWindowFlows: 1, MaximumUnacknowledged: 320 << 10}, false, 2, &streak)
	summary.add(flowWindowObservation{ActiveFlows: 1, MaximumUnacknowledged: 64 << 10}, true, 0, &streak)
	if summary.Samples != 3 || summary.ActiveSamples != 3 || summary.FullWindowSamples != 2 || summary.MaximumUnacknowledged != 320<<10 ||
		summary.ACKProgressSamples != 1 || summary.OneSessionProgress != 1 || summary.BothSessionsProgress != 1 ||
		summary.FullWindowNoProgress != 2 || summary.LongestFullWindowStall != 2 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestWaitTargetRequiresExactUniqueConnectionCount(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.json")
	writeState := func(state targetState) {
		encoded, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeState(targetState{Accepted: 2, Completed: 2})
	if err := waitTarget([]string{"--state-file", path, "--accepted", "2", "--completed", "2", "--timeout", "100ms"}); err != nil {
		t.Fatal(err)
	}
	writeState(targetState{Accepted: 3, Completed: 2})
	if err := waitTarget([]string{"--state-file", path, "--accepted", "2", "--completed", "2", "--timeout", "100ms"}); err == nil {
		t.Fatal("extra target connection was accepted")
	}
}

func TestWaitResourcesUsesExactBaseline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"resources":{"flows":0,"sessions":2,"socks_connections":0,"pending_target_dials":0}}`))
	}))
	defer server.Close()
	if err := waitResources([]string{
		"--url", server.URL, "--flows", "0", "--sessions", "2",
		"--socks-connections", "0", "--target-dials", "0", "--timeout", time.Second.String(),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestInspectStatusRequiresAllJSONRoutesAndRejectsSensitiveValues(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.URL.Path == "/api/v1/flows" {
			_, _ = writer.Write([]byte(`{"items":[],"marker":"redacted"}`))
			return
		}
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	var output bytes.Buffer
	if err := inspectStatus(server.URL, []string{"secret"}, &output); err != nil {
		t.Fatal(err)
	}
	if requests != 5 {
		t.Fatalf("requests = %d, want all 5 status routes", requests)
	}
	if lines := strings.Count(strings.TrimSpace(output.String()), "\n") + 1; lines != 5 {
		t.Fatalf("status lines = %d, want 5", lines)
	}
	var rejectedOutput bytes.Buffer
	if err := inspectStatus(server.URL, []string{"redacted"}, &rejectedOutput); err == nil {
		t.Fatal("sensitive status value was accepted")
	}
	if strings.Contains(rejectedOutput.String(), "redacted") {
		t.Fatal("sensitive status response was written to diagnostics")
	}
}

func TestInspectStatusRoutesCanLimitFailureSummary(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	var output bytes.Buffer
	if err := inspectStatusRoutes(server.URL, nil, &output, []string{"health", "summary"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(paths, []string{"/api/v1/health", "/api/v1/summary"}) {
		t.Fatalf("status paths = %v", paths)
	}
	if got := output.String(); strings.Contains(got, "interfaces:") || strings.Count(strings.TrimSpace(got), "\n")+1 != 2 {
		t.Fatalf("compact status output = %q", got)
	}
}
