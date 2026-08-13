package server

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
)

func TestRateLimiterAuthBurstRefillAndIPv6Aggregation(t *testing.T) {
	limiter := mustRateLimiter(t, DefaultRateLimitKeys)
	now := time.Unix(100, 0)
	source := netip.MustParseAddr("192.0.2.10")
	for attempt := 0; attempt < authSourceSpec.burst; attempt++ {
		if !limiter.AllowAuth(now, source) {
			t.Fatalf("auth burst attempt %d rejected", attempt+1)
		}
	}
	if limiter.AllowAuth(now, source) {
		t.Fatal("auth source exceeded burst")
	}
	if !limiter.AllowAuth(now.Add(6*time.Second), source) {
		t.Fatal("auth source did not refill one token")
	}
	if limiter.AllowAuth(now.Add(6*time.Second), source) {
		t.Fatal("auth source refilled more than one token")
	}

	limiter = mustRateLimiter(t, DefaultRateLimitKeys)
	first := netip.MustParseAddr("2001:db8:1:2::1")
	samePrefix := netip.MustParseAddr("2001:db8:1:2::ffff")
	otherPrefix := netip.MustParseAddr("2001:db8:1:3::1")
	for attempt := 0; attempt < authSourceSpec.burst; attempt++ {
		if !limiter.AllowAuth(now, first) {
			t.Fatalf("IPv6 burst attempt %d rejected", attempt+1)
		}
	}
	if limiter.AllowAuth(now, samePrefix) {
		t.Fatal("addresses in one IPv6 /64 did not share a bucket")
	}
	if !limiter.AllowAuth(now, otherPrefix) {
		t.Fatal("different IPv6 /64 unexpectedly shared a bucket")
	}
}

func TestRateLimiterGlobalDenialDoesNotConsumeSource(t *testing.T) {
	limiter := mustRateLimiter(t, DefaultRateLimitKeys)
	now := time.Unix(200, 0)
	for index := 1; index <= 100; index++ {
		source := netip.AddrFrom4([4]byte{198, 51, 100, byte(index)})
		if !limiter.AllowAuth(now, source) {
			t.Fatalf("global burst rejected attempt %d", index)
		}
	}
	newSource := netip.MustParseAddr("203.0.113.1")
	if limiter.AllowAuth(now, newSource) {
		t.Fatal("auth global burst exceeded")
	}
	refilled := now.Add(60 * time.Millisecond)
	for attempt := 0; attempt < 5; attempt++ {
		want := attempt == 0
		if got := limiter.AllowAuth(refilled, newSource); got != want {
			t.Fatalf("post-global-denial attempt %d = %v, want %v", attempt+1, got, want)
		}
	}
	// Only the single global token is available. A fresh limiter proves the
	// denied attempt itself did not create a source key.
	if snapshot := limiter.Snapshot(); snapshot.AuthSourceKeys != 101 {
		t.Fatalf("auth source keys = %d, want 101", snapshot.AuthSourceKeys)
	}
}

func TestRateLimiterOpenAndJoinFixedLimits(t *testing.T) {
	limiter := mustRateLimiter(t, DefaultRateLimitKeys)
	now := time.Unix(300, 0)
	for attempt := 0; attempt < 128; attempt++ {
		if !limiter.AllowOpen(now, "client-01") {
			t.Fatalf("OPEN burst attempt %d rejected", attempt+1)
		}
	}
	if limiter.AllowOpen(now, "client-01") {
		t.Fatal("OPEN principal burst exceeded")
	}
	if !limiter.AllowOpen(now.Add(60*time.Millisecond), "client-01") {
		t.Fatal("OPEN principal did not refill")
	}

	flowID := testRateFlowID(1)
	for attempt := 0; attempt < flow.MaxAttachments; attempt++ {
		if !limiter.AllowJoin(now, "client-02", flowID, true) {
			t.Fatalf("JOIN flow burst attempt %d rejected", attempt+1)
		}
	}
	if limiter.AllowJoin(now, "client-02", flowID, true) {
		t.Fatal("JOIN flow burst exceeded")
	}
	if !limiter.AllowJoin(now.Add(6*time.Second), "client-02", flowID, true) {
		t.Fatal("JOIN flow did not refill")
	}
}

func TestRateLimiterUnknownJoinDoesNotCreateFlowKey(t *testing.T) {
	limiter := mustRateLimiter(t, DefaultRateLimitKeys)
	now := time.Unix(400, 0)
	for attempt := 0; attempt < 10; attempt++ {
		if !limiter.AllowJoin(now, "client-01", testRateFlowID(byte(attempt+1)), false) {
			t.Fatalf("unknown JOIN attempt %d rejected", attempt+1)
		}
	}
	snapshot := limiter.Snapshot()
	if snapshot.Keys != 1 || snapshot.JoinPrincipalKeys != 1 || snapshot.JoinFlowKeys != 0 {
		t.Fatalf("snapshot after unknown JOINs = %#v", snapshot)
	}
}

func TestRateLimiterKeyCapacityIsAtomicAndReclaimsFullEntries(t *testing.T) {
	now := time.Unix(500, 0)
	limiter := mustRateLimiter(t, 1)
	if limiter.AllowJoin(now, "client-01", testRateFlowID(1), true) {
		t.Fatal("two-key JOIN admitted with one-key capacity")
	}
	if snapshot := limiter.Snapshot(); snapshot.Keys != 0 {
		t.Fatalf("failed atomic JOIN left keys: %#v", snapshot)
	}

	limiter = mustRateLimiter(t, 2)
	if !limiter.AllowOpen(now, "client-01") || !limiter.AllowOpen(now, "client-02") {
		t.Fatal("initial keys were not admitted")
	}
	if limiter.AllowOpen(now, "client-03") {
		t.Fatal("active key capacity was exceeded")
	}
	if !limiter.AllowOpen(now.Add(60*time.Millisecond), "client-03") {
		t.Fatal("full idle entry was not reclaimed")
	}
	if snapshot := limiter.Snapshot(); snapshot.Keys != 2 || snapshot.OpenPrincipalKeys != 2 {
		t.Fatalf("snapshot after reclaim = %#v", snapshot)
	}
}

func TestRateLimiterRejectsInvalidInputsAndConfiguration(t *testing.T) {
	for _, maxKeys := range []int{-1, 0, MaxRateLimitKeys + 1} {
		if _, err := NewRateLimiter(maxKeys); err == nil {
			t.Fatalf("NewRateLimiter(%d) succeeded", maxKeys)
		}
	}
	limiter := mustRateLimiter(t, DefaultRateLimitKeys)
	now := time.Unix(600, 0)
	if limiter.AllowAuth(now, netip.Addr{}) || limiter.AllowAuth(now, netip.IPv4Unspecified()) {
		t.Fatal("invalid auth source accepted")
	}
	if limiter.AllowOpen(now, "bad principal!") {
		t.Fatal("invalid principal accepted for OPEN")
	}
	if limiter.AllowJoin(now, "client-01", protocol.FlowID{}, true) {
		t.Fatal("zero known FlowID accepted for JOIN")
	}
	var nilLimiter *RateLimiter
	if nilLimiter.AllowAuth(now, netip.MustParseAddr("192.0.2.1")) || nilLimiter.AllowOpen(now, "client-01") || nilLimiter.AllowJoin(now, "client-01", protocol.FlowID{}, false) {
		t.Fatal("nil limiter admitted an event")
	}
}

func TestRateLimiterConcurrentOpenBurst(t *testing.T) {
	limiter := mustRateLimiter(t, DefaultRateLimitKeys)
	now := time.Unix(700, 0)
	var admitted atomic.Int64
	var wait sync.WaitGroup
	for attempt := 0; attempt < 512; attempt++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if limiter.AllowOpen(now, "client-01") {
				admitted.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := admitted.Load(); got != 128 {
		t.Fatalf("concurrent OPEN admitted %d, want 128", got)
	}
}

func mustRateLimiter(t *testing.T, maxKeys int) *RateLimiter {
	t.Helper()
	limiter, err := NewRateLimiter(maxKeys)
	if err != nil {
		t.Fatalf("NewRateLimiter: %v", err)
	}
	return limiter
}

func testRateFlowID(value byte) protocol.FlowID {
	var flowID protocol.FlowID
	flowID[len(flowID)-1] = value
	return flowID
}
