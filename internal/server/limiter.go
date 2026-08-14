package server

import (
	"bytes"
	"errors"
	"net/netip"
	"sync"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
)

const (
	DefaultRateLimitKeys = 4096
	MaxRateLimitKeys     = 16384
)

var ErrRateLimitConfiguration = errors.New("server: invalid rate limit configuration")

type OpenRateLimits struct {
	PrincipalRatePerMinute int
	PrincipalBurst         int
	GlobalRatePerMinute    int
	GlobalBurst            int
}

type rateKeyKind uint8

const (
	rateKeyAuthSource rateKeyKind = iota + 1
	rateKeyOpenPrincipal
	rateKeyJoinFlow
	rateKeyJoinPrincipal
)

type bucketSpec struct {
	interval time.Duration
	burst    int
}

var (
	authSourceSpec    = bucketSpec{interval: 6 * time.Second, burst: 64}
	authGlobalSpec    = bucketSpec{interval: 60 * time.Millisecond, burst: 100}
	defaultOpenLimits = OpenRateLimits{
		PrincipalRatePerMinute: 1_000, PrincipalBurst: 128,
		GlobalRatePerMinute: 10_000, GlobalBurst: 512,
	}
	joinFlowSpec      = bucketSpec{interval: 6 * time.Second, burst: flow.MaxAttachments}
	joinPrincipalSpec = bucketSpec{interval: 30 * time.Millisecond, burst: 128}
)

type rateKey struct {
	kind    rateKeyKind
	subject string
	flowID  protocol.FlowID
}

type rateBucket struct {
	initialized bool
	theoretical time.Time
}

func (bucket rateBucket) preview(now time.Time, spec bucketSpec) (time.Time, bool) {
	if spec.interval <= 0 || spec.burst < 1 {
		return time.Time{}, false
	}
	if !bucket.initialized {
		return now.Add(spec.interval), true
	}
	tolerance := time.Duration(spec.burst-1) * spec.interval
	if now.Before(bucket.theoretical.Add(-tolerance)) {
		return time.Time{}, false
	}
	base := bucket.theoretical
	if now.After(base) {
		base = now
	}
	return base.Add(spec.interval), true
}

func (bucket rateBucket) full(now time.Time) bool {
	return !bucket.initialized || !bucket.theoretical.After(now)
}

type rateEntry struct {
	bucket   rateBucket
	lastSeen time.Time
}

type localRateRequest struct {
	key  rateKey
	spec bucketSpec
}

type globalRateRequest struct {
	bucket *rateBucket
	spec   bucketSpec
}

// RateLimitSnapshot exposes only low-cardinality capacity data, without source addresses or principal identifiers.
type RateLimitSnapshot struct {
	Keys              int
	AuthSourceKeys    int
	OpenPrincipalKeys int
	JoinFlowKeys      int
	JoinPrincipalKeys int
}

// RateLimiter is the fixed v1 authentication, OPEN, and JOIN limiter. Callers
// provide event time so transitions do not read the wall clock; the internal lock
// protects only short, bounded in-memory operations.
type RateLimiter struct {
	mu         sync.Mutex
	maxKeys    int
	keys       map[rateKey]*rateEntry
	authGlobal rateBucket
	openGlobal rateBucket
	openLocal  bucketSpec
	openAll    bucketSpec
}

func NewRateLimiter(maxKeys int, configured ...OpenRateLimits) (*RateLimiter, error) {
	if maxKeys < 1 || maxKeys > MaxRateLimitKeys || len(configured) > 1 {
		return nil, ErrRateLimitConfiguration
	}
	limits := defaultOpenLimits
	if len(configured) == 1 {
		limits = configured[0]
	}
	openLocal, ok := openBucketSpec(limits.PrincipalRatePerMinute, limits.PrincipalBurst)
	if !ok {
		return nil, ErrRateLimitConfiguration
	}
	openAll, ok := openBucketSpec(limits.GlobalRatePerMinute, limits.GlobalBurst)
	if !ok {
		return nil, ErrRateLimitConfiguration
	}
	return &RateLimiter{
		maxKeys: maxKeys, keys: make(map[rateKey]*rateEntry, maxKeys),
		openLocal: openLocal, openAll: openAll,
	}, nil
}

func openBucketSpec(ratePerMinute, burst int) (bucketSpec, bool) {
	if ratePerMinute < 1 || burst < 1 {
		return bucketSpec{}, false
	}
	interval := time.Minute / time.Duration(ratePerMinute)
	return bucketSpec{interval: interval, burst: burst}, interval > 0
}

// AllowAuth applies 10/minute with burst 64 per source and 1000/minute with burst 100 globally.
func (limiter *RateLimiter) AllowAuth(now time.Time, source netip.Addr) bool {
	if limiter == nil {
		return false
	}
	subject, ok := canonicalSource(source)
	if !ok {
		return false
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	return limiter.allow(now,
		[]localRateRequest{{key: rateKey{kind: rateKeyAuthSource, subject: subject}, spec: authSourceSpec}},
		[]globalRateRequest{{bucket: &limiter.authGlobal, spec: authGlobalSpec}},
	)
}

// AllowOpen applies the configured per-principal and global OPEN limits.
func (limiter *RateLimiter) AllowOpen(now time.Time, principalID string) bool {
	if limiter == nil || !protocol.ValidPrincipalID(principalID) {
		return false
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	return limiter.allow(now,
		[]localRateRequest{{key: rateKey{kind: rateKeyOpenPrincipal, subject: principalID}, spec: limiter.openLocal}},
		[]globalRateRequest{{bucket: &limiter.openGlobal, spec: limiter.openAll}},
	)
}

// AllowJoin applies 10/minute with burst 64 per known flow and 2000/minute with
// burst 128 per principal. Unknown flows remain principal-limited but do not
// create per-flow limiter keys.
func (limiter *RateLimiter) AllowJoin(now time.Time, principalID string, flowID protocol.FlowID, knownFlow bool) bool {
	if limiter == nil || !protocol.ValidPrincipalID(principalID) || knownFlow && flowID == (protocol.FlowID{}) {
		return false
	}
	requests := []localRateRequest{{
		key:  rateKey{kind: rateKeyJoinPrincipal, subject: principalID},
		spec: joinPrincipalSpec,
	}}
	if knownFlow {
		requests = append(requests, localRateRequest{
			key:  rateKey{kind: rateKeyJoinFlow, subject: principalID, flowID: flowID},
			spec: joinFlowSpec,
		})
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	return limiter.allow(now, requests, nil)
}

func (limiter *RateLimiter) Snapshot() RateLimitSnapshot {
	if limiter == nil {
		return RateLimitSnapshot{}
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	snapshot := RateLimitSnapshot{Keys: len(limiter.keys)}
	for key := range limiter.keys {
		switch key.kind {
		case rateKeyAuthSource:
			snapshot.AuthSourceKeys++
		case rateKeyOpenPrincipal:
			snapshot.OpenPrincipalKeys++
		case rateKeyJoinFlow:
			snapshot.JoinFlowKeys++
		case rateKeyJoinPrincipal:
			snapshot.JoinPrincipalKeys++
		}
	}
	return snapshot
}

func (limiter *RateLimiter) allow(now time.Time, locals []localRateRequest, globals []globalRateRequest) bool {
	protected := make(map[rateKey]struct{}, len(locals))
	missing := 0
	localNext := make(map[rateKey]time.Time, len(locals))
	for _, request := range locals {
		if request.spec.interval <= 0 || request.spec.burst < 1 {
			return false
		}
		if _, duplicate := protected[request.key]; duplicate {
			return false
		}
		protected[request.key] = struct{}{}
		entry, exists := limiter.keys[request.key]
		if !exists {
			missing++
			continue
		}
		next, ok := entry.bucket.preview(now, request.spec)
		if !ok {
			return false
		}
		localNext[request.key] = next
	}

	globalNext := make([]time.Time, len(globals))
	for index, request := range globals {
		if request.bucket == nil {
			return false
		}
		next, ok := request.bucket.preview(now, request.spec)
		if !ok {
			return false
		}
		globalNext[index] = next
	}

	if !limiter.makeRoom(now, missing, protected) {
		return false
	}
	for _, request := range locals {
		entry, exists := limiter.keys[request.key]
		if !exists {
			entry = &rateEntry{}
			limiter.keys[request.key] = entry
		}
		if _, exists := localNext[request.key]; !exists {
			next, ok := entry.bucket.preview(now, request.spec)
			if !ok {
				return false
			}
			localNext[request.key] = next
		}
	}

	for index, request := range globals {
		request.bucket.initialized = true
		request.bucket.theoretical = globalNext[index]
	}
	for _, request := range locals {
		entry := limiter.keys[request.key]
		entry.bucket.initialized = true
		entry.bucket.theoretical = localNext[request.key]
		entry.lastSeen = now
	}
	return true
}

func (limiter *RateLimiter) makeRoom(now time.Time, missing int, protected map[rateKey]struct{}) bool {
	needed := len(limiter.keys) + missing - limiter.maxKeys
	for needed > 0 {
		var selected rateKey
		var selectedEntry *rateEntry
		for key, entry := range limiter.keys {
			if _, keep := protected[key]; keep || !entry.bucket.full(now) {
				continue
			}
			if selectedEntry == nil || rateEntryLess(key, entry, selected, selectedEntry) {
				selected = key
				selectedEntry = entry
			}
		}
		if selectedEntry == nil {
			return false
		}
		delete(limiter.keys, selected)
		needed--
	}
	return true
}

func rateEntryLess(leftKey rateKey, left *rateEntry, rightKey rateKey, right *rateEntry) bool {
	if !left.lastSeen.Equal(right.lastSeen) {
		return left.lastSeen.Before(right.lastSeen)
	}
	if leftKey.kind != rightKey.kind {
		return leftKey.kind < rightKey.kind
	}
	if leftKey.subject != rightKey.subject {
		return leftKey.subject < rightKey.subject
	}
	return bytes.Compare(leftKey.flowID[:], rightKey.flowID[:]) < 0
}

func canonicalSource(source netip.Addr) (string, bool) {
	if !source.IsValid() || source.IsUnspecified() || source.IsMulticast() {
		return "", false
	}
	if source.Zone() != "" {
		source = source.WithZone("")
	}
	source = source.Unmap()
	bits := 64
	if source.Is4() {
		bits = 32
	}
	return netip.PrefixFrom(source, bits).Masked().String(), true
}
