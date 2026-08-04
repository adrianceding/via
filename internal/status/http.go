package status

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"time"
)

var ErrInvalidHTTP = errors.New("status: invalid HTTP configuration")

type SnapshotProvider interface {
	Snapshot() Snapshot
}

type BasicAuth struct {
	Username string
	Password string
}

type Handler struct {
	provider       SnapshotProvider
	requests       chan struct{}
	basicAuth      bool
	usernameDigest [sha256.Size]byte
	passwordDigest [sha256.Size]byte
}

func NewHandler(provider SnapshotProvider, credentials ...BasicAuth) (*Handler, error) {
	if provider == nil || len(credentials) > 1 {
		return nil, ErrInvalidHTTP
	}
	handler := &Handler{provider: provider, requests: make(chan struct{}, MaxStatusConnections)}
	if len(credentials) == 1 {
		if credentials[0].Username == "" || credentials[0].Password == "" {
			return nil, ErrInvalidHTTP
		}
		handler.basicAuth = true
		handler.usernameDigest = sha256.Sum256([]byte(credentials[0].Username))
		handler.passwordDigest = sha256.Sum256([]byte(credentials[0].Password))
	}
	return handler, nil
}

func ValidateListenAddress(value string) error {
	host, port, err := net.SplitHostPort(value)
	if err != nil || port == "" {
		return ErrInvalidHTTP
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !address.IsValid() || address.Zone() != "" {
		return ErrInvalidHTTP
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		return ErrInvalidHTTP
	}
	return nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if handler == nil || handler.provider == nil || writer == nil || request == nil {
		return
	}
	if !handler.authorized(request) {
		writer.Header().Set("WWW-Authenticate", `Basic realm="Via status", charset="UTF-8"`)
		writeFixedError(writer, request.Method, http.StatusUnauthorized, "unauthorized")
		return
	}
	select {
	case handler.requests <- struct{}{}:
		defer func() { <-handler.requests }()
	default:
		writeFixedError(writer, request.Method, http.StatusServiceUnavailable, "busy")
		return
	}

	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writeFixedError(writer, request.Method, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}

	if serveWebManager(writer, request) {
		return
	}

	if !readRoute(request.URL.Path) {
		writeFixedError(writer, request.Method, http.StatusNotFound, "not_found")
		return
	}

	snapshot := handler.provider.Snapshot()
	var (
		encoded []byte
		status  = http.StatusOK
		err     error
	)
	switch request.URL.Path {
	case "/api/v1/health":
		encoded, err = json.Marshal(healthResponse{Healthy: snapshot.Healthy, GeneratedAt: snapshot.GeneratedAt})
		if !snapshot.Healthy {
			status = http.StatusServiceUnavailable
		}
	case "/api/v1/summary":
		encoded, err = json.Marshal(summaryResponse{
			GeneratedAt: snapshot.GeneratedAt, Healthy: snapshot.Healthy, Role: snapshot.Role,
			Resources: snapshot.Resources, Counters: snapshot.Counters,
			Interfaces: len(snapshot.Interfaces), Sessions: len(snapshot.Sessions),
			Flows: activeFlowCount(snapshot.Flows), Terminals: len(snapshot.Terminals),
		})
	case "/api/v1/interfaces":
		encoded, err = encodeBoundedList(snapshot.GeneratedAt, snapshot.Interfaces)
	case "/api/v1/sessions":
		encoded, err = encodeBoundedList(snapshot.GeneratedAt, snapshot.Sessions)
	case "/api/v1/flows":
		encoded, err = encodeBoundedFlows(snapshot)
	}
	if err != nil || len(encoded) > MaxStatusResponse {
		writeFixedError(writer, request.Method, http.StatusServiceUnavailable, "snapshot_unavailable")
		return
	}
	writeJSON(writer, request.Method, status, encoded)
}

func (handler *Handler) authorized(request *http.Request) bool {
	if !handler.basicAuth {
		return true
	}
	username, password, _ := request.BasicAuth()
	usernameDigest := sha256.Sum256([]byte(username))
	passwordDigest := sha256.Sum256([]byte(password))
	return subtle.ConstantTimeCompare(usernameDigest[:], handler.usernameDigest[:])&
		subtle.ConstantTimeCompare(passwordDigest[:], handler.passwordDigest[:]) == 1
}

func readRoute(path string) bool {
	switch path {
	case "/api/v1/health", "/api/v1/summary", "/api/v1/interfaces", "/api/v1/sessions", "/api/v1/flows":
		return true
	default:
		return false
	}
}

type healthResponse struct {
	Healthy     bool      `json:"healthy"`
	GeneratedAt time.Time `json:"generated_at"`
}

type summaryResponse struct {
	GeneratedAt time.Time `json:"generated_at"`
	Healthy     bool      `json:"healthy"`
	Role        Role      `json:"role,omitempty"`
	Resources   Resources `json:"resources"`
	Counters    Counters  `json:"counters"`
	Interfaces  int       `json:"interfaces"`
	Sessions    int       `json:"sessions"`
	Flows       int       `json:"flows"`
	Terminals   int       `json:"terminals"`
}

type listResponse[T any] struct {
	GeneratedAt time.Time `json:"generated_at"`
	Items       []T       `json:"items"`
	Total       int       `json:"total"`
	Truncated   bool      `json:"truncated"`
}

type flowsResponse struct {
	GeneratedAt   time.Time  `json:"generated_at"`
	Items         []Flow     `json:"items"`
	Terminals     []Terminal `json:"terminals"`
	Total         int        `json:"total"`
	TerminalTotal int        `json:"terminal_total"`
	Truncated     bool       `json:"truncated"`
}

func encodeBoundedList[T any](generatedAt time.Time, items []T) ([]byte, error) {
	total := len(items)
	encode := func(count int) ([]byte, error) {
		return json.Marshal(listResponse[T]{
			GeneratedAt: generatedAt, Items: items[:count], Total: total, Truncated: count != total,
		})
	}
	return largestBounded(total, encode)
}

func encodeBoundedFlows(snapshot Snapshot) ([]byte, error) {
	activeFlows := nonClosingFlows(snapshot.Flows)
	flowCount := len(activeFlows)
	terminalCount := len(snapshot.Terminals)
	encodeFlows := func(count int) ([]byte, error) {
		return json.Marshal(flowsResponse{
			GeneratedAt: snapshot.GeneratedAt, Items: activeFlows[:count],
			Total: flowCount, TerminalTotal: terminalCount, Truncated: count != flowCount || terminalCount != 0,
		})
	}
	encoded, err := largestBounded(flowCount, encodeFlows)
	if err != nil {
		return nil, err
	}
	var base flowsResponse
	if err := json.Unmarshal(encoded, &base); err != nil {
		return nil, err
	}
	encodeTerminals := func(count int) ([]byte, error) {
		return json.Marshal(flowsResponse{
			GeneratedAt: snapshot.GeneratedAt, Items: base.Items, Terminals: snapshot.Terminals[:count],
			Total: flowCount, TerminalTotal: terminalCount,
			Truncated: len(base.Items) != flowCount || count != terminalCount,
		})
	}
	return largestBounded(terminalCount, encodeTerminals)
}

func activeFlowCount(flows []Flow) int {
	count := 0
	for _, flow := range flows {
		if flow.State != FlowClosing {
			count++
		}
	}
	return count
}

func nonClosingFlows(flows []Flow) []Flow {
	active := make([]Flow, 0, activeFlowCount(flows))
	for _, flow := range flows {
		if flow.State != FlowClosing {
			active = append(active, flow)
		}
	}
	return active
}

func largestBounded(maximum int, encode func(int) ([]byte, error)) ([]byte, error) {
	low, high := 0, maximum
	var best []byte
	for low <= high {
		middle := low + (high-low)/2
		encoded, err := encode(middle)
		if err != nil {
			return nil, err
		}
		if len(encoded) <= MaxStatusResponse {
			best = encoded
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	if best == nil {
		return nil, ErrInvalidHTTP
	}
	return best, nil
}

func writeJSON(writer http.ResponseWriter, method string, status int, encoded []byte) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
	writer.WriteHeader(status)
	if method == http.MethodHead {
		return
	}
	_, _ = writer.Write(encoded)
}

func writeFixedError(writer http.ResponseWriter, method string, status int, code string) {
	encoded, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: code})
	writeJSON(writer, method, status, encoded)
}
