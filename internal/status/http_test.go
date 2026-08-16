package status

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type staticProvider struct{ snapshot Snapshot }

func (provider staticProvider) Snapshot() Snapshot { return cloneSnapshot(provider.snapshot) }

type countingProvider struct{ calls int }

var viteAssetPattern = regexp.MustCompile(`(?:src|href)="(/assets/[^"]+)"`)

func (provider *countingProvider) Snapshot() Snapshot {
	provider.calls++
	return Snapshot{Healthy: true}
}

func TestHTTPHandlerServesViteAssets(t *testing.T) {
	provider := &countingProvider{}
	handler, err := NewHandler(provider)
	if err != nil {
		t.Fatal(err)
	}
	index := httptest.NewRecorder()
	handler.ServeHTTP(index, httptest.NewRequest(http.MethodGet, "/", nil))
	matches := viteAssetPattern.FindAllStringSubmatch(index.Body.String(), -1)
	if index.Code != http.StatusOK || len(matches) < 2 {
		t.Fatalf("Vite index = status %d assets %v", index.Code, matches)
	}
	for _, match := range matches {
		asset := httptest.NewRecorder()
		handler.ServeHTTP(asset, httptest.NewRequest(http.MethodGet, match[1], nil))
		if asset.Code != http.StatusOK || asset.Body.Len() == 0 {
			t.Fatalf("GET %s = status %d bytes %d", match[1], asset.Code, asset.Body.Len())
		}
		if asset.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" {
			t.Fatalf("GET %s cache = %q", match[1], asset.Header().Get("Cache-Control"))
		}
		if asset.Header().Get("Content-Type") == "" || asset.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("GET %s headers = %#v", match[1], asset.Header())
		}
		head := httptest.NewRecorder()
		handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, match[1], nil))
		if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") != asset.Header().Get("Content-Length") {
			t.Fatalf("HEAD %s = status %d body %d headers %#v", match[1], head.Code, head.Body.Len(), head.Header())
		}
	}
	traversal := httptest.NewRecorder()
	handler.ServeHTTP(traversal, httptest.NewRequest(http.MethodGet, "/assets/../index.html", nil))
	if traversal.Code != http.StatusNotFound || provider.calls != 0 {
		t.Fatalf("asset traversal = status %d snapshot calls %d", traversal.Code, provider.calls)
	}
}

func TestHTTPHandlerExposesOnlyFixedReadRoutes(t *testing.T) {
	now := time.Unix(4_000, 0).UTC()
	snapshot := Snapshot{
		GeneratedAt: now, Healthy: true, Role: RoleClient,
		Resources: Resources{Flows: 1, Sessions: 1},
		Rejected: Rejected{
			Flows: 3, FlowRateLimited: 1, FlowOpeningCapacity: 1, FlowTargetDialCapacity: 1,
		},
		Counters:   Counters{BytesSent: 10},
		Interfaces: []Interface{{Index: 1, Name: "eth0", Addresses: []string{"192.0.2.1"}, Reason: InterfaceEligible}},
		Sessions:   []Session{validTestSession(1)},
		Flows:      []Flow{validTestFlow(2)},
		Terminals:  []Terminal{{IDHash: testHash(3), State: FlowClosed, Reason: ReasonCompleted, FinishedAt: now}},
	}
	handler, err := NewHandler(staticProvider{snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"/api/v1/health", "/api/v1/summary", "/api/v1/interfaces", "/api/v1/sessions", "/api/v1/flows"}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			get := httptest.NewRecorder()
			handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, path, nil))
			if get.Code != http.StatusOK || get.Body.Len() == 0 || get.Body.Len() > MaxStatusResponse {
				t.Fatalf("GET %s = status %d bytes %d", path, get.Code, get.Body.Len())
			}
			if get.Header().Get("Content-Type") != "application/json" || get.Header().Get("Cache-Control") != "no-store" || get.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatalf("GET %s headers = %#v", path, get.Header())
			}
			if strings.Contains(get.Body.String(), "secret.example") || strings.Contains(get.Body.String(), "application-payload") {
				t.Fatalf("GET %s exposed secret: %s", path, get.Body.String())
			}

			head := httptest.NewRecorder()
			handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, path, nil))
			if head.Code != get.Code || head.Body.Len() != 0 || head.Header().Get("Content-Length") != get.Header().Get("Content-Length") {
				t.Fatalf("HEAD %s = status %d body %d headers %#v", path, head.Code, head.Body.Len(), head.Header())
			}
		})
	}
	var summary summaryResponse
	summaryRecorder := httptest.NewRecorder()
	handler.ServeHTTP(summaryRecorder, httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil))
	if err := json.Unmarshal(summaryRecorder.Body.Bytes(), &summary); err != nil || summary.Role != RoleClient ||
		summary.Rejected.Flows != 3 || summary.Rejected.FlowRateLimited != 1 ||
		summary.Rejected.FlowOpeningCapacity != 1 || summary.Rejected.FlowTargetDialCapacity != 1 ||
		summary.Sessions != 1 || summary.ReadySessions != 1 {
		t.Fatalf("summary = %#v, %v", summary, err)
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(method, "/api/v1/summary", nil))
		if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != "GET, HEAD" {
			t.Fatalf("%s = %d %#v", method, recorder.Code, recorder.Header())
		}
	}
	notFound := httptest.NewRecorder()
	handler.ServeHTTP(notFound, httptest.NewRequest(http.MethodGet, "/api/v1/control", nil))
	if notFound.Code != http.StatusNotFound {
		t.Fatalf("write route status = %d", notFound.Code)
	}
}

func TestHTTPSummaryReportsReadySessionsFromCompleteSnapshot(t *testing.T) {
	tests := []struct {
		name         string
		sessions     []Session
		wantSessions int
		wantReady    int
	}{
		{name: "empty", sessions: nil},
		{
			name: "mixed",
			sessions: []Session{
				{State: SessionReady},
				{State: SessionDialing},
				{State: SessionBackoff},
				{State: SessionReady},
			},
			wantSessions: 4,
			wantReady:    2,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			handler, err := NewHandler(staticProvider{snapshot: Snapshot{
				Healthy: true, Role: RoleClient, Sessions: testCase.sessions,
			}})
			if err != nil {
				t.Fatal(err)
			}
			get := httptest.NewRecorder()
			handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil))
			if get.Code != http.StatusOK {
				t.Fatalf("GET summary status = %d", get.Code)
			}
			var summary summaryResponse
			if err := json.Unmarshal(get.Body.Bytes(), &summary); err != nil {
				t.Fatal(err)
			}
			if summary.Sessions != testCase.wantSessions || summary.ReadySessions != testCase.wantReady {
				t.Fatalf("summary sessions = %d ready = %d, want %d/%d", summary.Sessions, summary.ReadySessions, testCase.wantSessions, testCase.wantReady)
			}

			head := httptest.NewRecorder()
			handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/api/v1/summary", nil))
			if head.Code != get.Code || head.Body.Len() != 0 || head.Header().Get("Content-Length") != get.Header().Get("Content-Length") {
				t.Fatalf("HEAD summary = status %d body %d headers %#v", head.Code, head.Body.Len(), head.Header())
			}
		})
	}
}

func TestHTTPHealthFailureAndListenValidation(t *testing.T) {
	handler, err := NewHandler(staticProvider{snapshot: Snapshot{Healthy: false}})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy status = %d", recorder.Code)
	}
	valid := []string{"127.0.0.1:8080", "[::1]:8080", "0.0.0.0:8080", "[::]:8080", "192.0.2.1:8080"}
	for _, address := range valid {
		if err := ValidateListenAddress(address); err != nil {
			t.Fatalf("valid listen %q: %v", address, err)
		}
	}
	if err := ValidateListenAddress("[::ffff:127.0.0.1]:8080"); err != nil {
		t.Fatalf("IPv4-mapped loopback: %v", err)
	}
	invalid := []string{"", "localhost:8080", "127.0.0.1:0", "127.0.0.1:http"}
	for _, address := range invalid {
		if err := ValidateListenAddress(address); err == nil {
			t.Fatalf("invalid listen %q accepted", address)
		}
	}
}

func TestHTTPUnknownRouteDoesNotMaterializeSnapshot(t *testing.T) {
	provider := &countingProvider{}
	handler, err := NewHandler(provider)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/unknown", nil))
	if recorder.Code != http.StatusNotFound || provider.calls != 0 {
		t.Fatalf("unknown route = status %d snapshot calls %d", recorder.Code, provider.calls)
	}
}

func TestHTTPBasicAuthProtectsEveryRouteBeforeRoutingAndSnapshots(t *testing.T) {
	provider := &countingProvider{}
	handler, err := NewHandler(provider, BasicAuth{Username: "manager", Password: "status-password"})
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"/", "/assets/app.js", "/api/v1/health", "/api/v1/summary", "/api/v1/unknown"}
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
		for _, path := range paths {
			request := httptest.NewRequest(method, path, nil)
			if path == "/api/v1/summary" && method == http.MethodGet {
				request.SetBasicAuth("manager", "wrong-password")
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnauthorized ||
				recorder.Header().Get("WWW-Authenticate") != `Basic realm="Via status", charset="UTF-8"` ||
				recorder.Header().Get("Cache-Control") != "no-store" || method == http.MethodHead && recorder.Body.Len() != 0 {
				t.Fatalf("%s %s = %d body %q headers %#v", method, path, recorder.Code, recorder.Body.String(), recorder.Header())
			}
		}
	}
	if provider.calls != 0 {
		t.Fatalf("unauthorized requests materialized %d snapshots", provider.calls)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil)
	request.SetBasicAuth("manager", "status-password")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || provider.calls != 1 {
		t.Fatalf("authorized response = %d snapshot calls %d", recorder.Code, provider.calls)
	}

	malformed := httptest.NewRecorder()
	malformedRequest := httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil)
	malformedRequest.Header.Set("Authorization", "Basic !!!")
	handler.ServeHTTP(malformed, malformedRequest)
	if malformed.Code != http.StatusUnauthorized || provider.calls != 1 {
		t.Fatalf("malformed authorization = %d snapshot calls %d", malformed.Code, provider.calls)
	}
}

func TestHTTPBasicAuthRunsBeforeConcurrencyLimit(t *testing.T) {
	provider := &blockingProvider{started: make(chan struct{}, MaxStatusConnections), release: make(chan struct{})}
	handler, err := NewHandler(provider, BasicAuth{Username: "manager", Password: "status-password"})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	wait.Add(MaxStatusConnections)
	for range MaxStatusConnections {
		go func() {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil)
			request.SetBasicAuth("manager", "status-password")
			handler.ServeHTTP(httptest.NewRecorder(), request)
		}()
	}
	for range MaxStatusConnections {
		select {
		case <-provider.started:
		case <-time.After(time.Second):
			t.Fatal("authorized request did not enter provider")
		}
	}
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized saturated response = %d %q", unauthorized.Code, unauthorized.Body.String())
	}
	close(provider.release)
	wait.Wait()
}

func TestHTTPHandlerServesEmbeddedWebManagerWithoutSnapshot(t *testing.T) {
	provider := &countingProvider{}
	handler, err := NewHandler(provider)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		path        string
		contentType string
		cache       string
	}{
		{path: "/", contentType: "text/html; charset=utf-8", cache: "no-store"},
		{path: "/index.html", contentType: "text/html; charset=utf-8", cache: "no-store"},
	}
	for _, testCase := range tests {
		t.Run(testCase.path, func(t *testing.T) {
			get := httptest.NewRecorder()
			handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, testCase.path, nil))
			if get.Code != http.StatusOK || get.Body.Len() == 0 {
				t.Fatalf("GET %s = status %d bytes %d", testCase.path, get.Code, get.Body.Len())
			}
			if testCase.path == "/" && !strings.Contains(get.Body.String(), `id="app"`) {
				t.Fatal("Web Manager is missing the Vue application mount point")
			}
			if get.Header().Get("Content-Type") != testCase.contentType || get.Header().Get("Cache-Control") != testCase.cache {
				t.Fatalf("GET %s headers = %#v", testCase.path, get.Header())
			}
			if get.Header().Get("Content-Security-Policy") == "" || get.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatalf("GET %s security headers = %#v", testCase.path, get.Header())
			}

			head := httptest.NewRecorder()
			handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, testCase.path, nil))
			if head.Code != get.Code || head.Body.Len() != 0 || head.Header().Get("Content-Length") != get.Header().Get("Content-Length") {
				t.Fatalf("HEAD %s = status %d body %d headers %#v", testCase.path, head.Code, head.Body.Len(), head.Header())
			}
		})
	}
	if provider.calls != 0 {
		t.Fatalf("static assets materialized %d snapshots", provider.calls)
	}

	notFound := httptest.NewRecorder()
	handler.ServeHTTP(notFound, httptest.NewRequest(http.MethodGet, "/assets/missing.js", nil))
	if notFound.Code != http.StatusNotFound || provider.calls != 0 {
		t.Fatalf("unknown asset = status %d snapshot calls %d", notFound.Code, provider.calls)
	}

	post := httptest.NewRecorder()
	handler.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/", nil))
	if post.Code != http.StatusMethodNotAllowed || post.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST / = status %d headers %#v", post.Code, post.Header())
	}
}

func TestHTTPFlowResponseIsTruncatedBelowHardByteLimit(t *testing.T) {
	flows := make([]Flow, MaxFlows)
	for index := range flows {
		flows[index] = validTestFlow(uint64(index + 1))
	}
	terminals := make([]Terminal, MaxTerminalSummaries)
	for index := range terminals {
		terminals[index] = Terminal{
			IDHash: testHash(uint64(index + 20_000)), State: FlowReset,
			Reason: ReasonDeadlineExceeded, FinishedAt: time.Unix(int64(index+1), 0).UTC(),
		}
	}
	handler, err := NewHandler(staticProvider{snapshot: Snapshot{
		Healthy: true, GeneratedAt: time.Unix(1, 0).UTC(), Flows: flows, Terminals: terminals,
	}})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/flows", nil))
	if recorder.Code != http.StatusOK || recorder.Body.Len() > MaxStatusResponse {
		t.Fatalf("flow response = status %d bytes %d", recorder.Code, recorder.Body.Len())
	}
	var response flowsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Truncated || response.Total != MaxFlows || response.TerminalTotal != MaxTerminalSummaries || len(response.Items) == 0 || len(response.Items) >= MaxFlows {
		t.Fatalf("flow response metadata = %#v", response)
	}
}

func TestHTTPFlowResponseExcludesClosingFlowsFromActiveItems(t *testing.T) {
	active := validTestFlow(1)
	closing := validTestFlow(2)
	closing.State = FlowClosing
	handler, err := NewHandler(staticProvider{snapshot: Snapshot{
		Healthy: true, GeneratedAt: time.Unix(1, 0).UTC(), Flows: []Flow{active, closing},
	}})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/flows", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("flow response status = %d", recorder.Code)
	}
	var response flowsResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Total != 1 || len(response.Items) != 1 || response.Items[0].IDHash != active.IDHash {
		t.Fatalf("active flow response = %#v", response)
	}
	var summary summaryResponse
	summaryRecorder := httptest.NewRecorder()
	handler.ServeHTTP(summaryRecorder, httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil))
	if err := json.Unmarshal(summaryRecorder.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Flows != 1 {
		t.Fatalf("summary active flow count = %d, want 1", summary.Flows)
	}
}

type blockingProvider struct {
	started chan struct{}
	release chan struct{}
}

func (provider *blockingProvider) Snapshot() Snapshot {
	provider.started <- struct{}{}
	<-provider.release
	return Snapshot{Healthy: true}
}

func TestHTTPHandlerRejectsNinthConcurrentRequest(t *testing.T) {
	provider := &blockingProvider{
		started: make(chan struct{}, MaxStatusConnections),
		release: make(chan struct{}),
	}
	handler, err := NewHandler(provider)
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	wait.Add(MaxStatusConnections)
	for index := 0; index < MaxStatusConnections; index++ {
		go func() {
			defer wait.Done()
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil))
		}()
	}
	for index := 0; index < MaxStatusConnections; index++ {
		select {
		case <-provider.started:
		case <-time.After(time.Second):
			t.Fatal("concurrent request did not enter provider")
		}
	}
	ninth := httptest.NewRecorder()
	handler.ServeHTTP(ninth, httptest.NewRequest(http.MethodGet, "/api/v1/summary", nil))
	if ninth.Code != http.StatusServiceUnavailable || !strings.Contains(ninth.Body.String(), "busy") {
		t.Fatalf("ninth response = %d %q", ninth.Code, ninth.Body.String())
	}
	close(provider.release)
	wait.Wait()
}

func TestHTTPConstructorRejectsNilProvider(t *testing.T) {
	if _, err := NewHandler(nil); err == nil {
		t.Fatal("nil provider accepted")
	}
}
