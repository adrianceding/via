package auth

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

func TestChallengeGeneratorFixedVector(t *testing.T) {
	entropy := make([]byte, 48)
	for index := range entropy {
		entropy[index] = byte(index)
	}
	generator, err := NewChallengeGenerator(bytes.NewReader(entropy))
	if err != nil {
		t.Fatalf("NewChallengeGenerator() error = %v", err)
	}
	challenge, err := generator.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	want, _ := hex.DecodeString("7f965be1a813bba4ca91e4033f3e627beabecda8d19d21b30791e8e390a3d732")
	if !bytes.Equal(challenge[:], want) {
		t.Fatalf("challenge = %x, want %x", challenge, want)
	}
	second, err := generator.Next()
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	if challenge == second {
		t.Fatal("consecutive challenges are equal")
	}
}

func TestChallengeGeneratorConcurrentUniqueness(t *testing.T) {
	generator, err := NewChallengeGenerator(bytes.NewReader(make([]byte, 48)))
	if err != nil {
		t.Fatal(err)
	}
	const count = 1_000
	results := make(chan [ChallengeSize]byte, count)
	var group sync.WaitGroup
	for range count {
		group.Add(1)
		go func() {
			defer group.Done()
			challenge, nextErr := generator.Next()
			if nextErr != nil {
				t.Errorf("Next() error = %v", nextErr)
				return
			}
			results <- challenge
		}()
	}
	group.Wait()
	close(results)
	seen := make(map[[ChallengeSize]byte]struct{}, count)
	for challenge := range results {
		if _, exists := seen[challenge]; exists {
			t.Fatalf("duplicate challenge %x", challenge)
		}
		seen[challenge] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("challenge count = %d, want %d", len(seen), count)
	}
}

func TestChallengeGeneratorFailures(t *testing.T) {
	if _, err := NewChallengeGenerator(nil); !errors.Is(err, ErrEntropy) {
		t.Fatalf("nil entropy error = %v", err)
	}
	if _, err := NewChallengeGenerator(io.LimitReader(bytes.NewReader(make([]byte, 48)), 47)); !errors.Is(err, ErrEntropy) {
		t.Fatalf("short entropy error = %v", err)
	}
	generator, err := NewChallengeGenerator(bytes.NewReader(make([]byte, 48)))
	if err != nil {
		t.Fatal(err)
	}
	generator.counter.Store(math.MaxUint64)
	if _, err := generator.Next(); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("exhausted counter error = %v", err)
	}
	if _, err := (*ChallengeGenerator)(nil).Next(); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("nil generator error = %v", err)
	}
}

func TestComputeProofFixedVector(t *testing.T) {
	var key Key
	for index := range key {
		key[index] = byte(0xa0 + index)
	}
	var challenge [ChallengeSize]byte
	var nonce [NonceSize]byte
	for index := range challenge {
		challenge[index] = byte(index)
		nonce[index] = byte(0x20 + index)
	}
	pathGroupID := protocol.PathGroupID{1}
	proof, err := ComputeProof(key, "edge-1", pathGroupID, challenge, nonce)
	if err != nil {
		t.Fatalf("ComputeProof() error = %v", err)
	}
	want, _ := hex.DecodeString("f8819e1d97c5ac0e12d85ad1428d8842fbb6d4b5d70265bbd1a9d611de00faa0")
	if !bytes.Equal(proof[:], want) {
		t.Fatalf("proof = %x, want %x", proof, want)
	}
	if _, err := ComputeProof(key, "bad principal", pathGroupID, challenge, nonce); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("invalid principal error = %v", err)
	}
	if _, err := ComputeProof(key, "edge-1", protocol.PathGroupID{}, challenge, nonce); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("invalid path group error = %v", err)
	}
}

func TestCorrelationIDsAreDeterministicDomainSeparatedAndOpaque(t *testing.T) {
	key := Key{1, 2, 3}
	proof := [32]byte{4, 5, 6}
	flowID := protocol.FlowID{7, 8, 9}

	connectionID := DeriveConnectionID(proof)
	if connectionID == (CorrelationID{}) || connectionID != DeriveConnectionID(proof) ||
		len(connectionID.String()) != 2*CorrelationIDSize {
		t.Fatalf("connection correlation ID = %q", connectionID.String())
	}
	flowCorrelationID, err := DeriveFlowID(key, "edge-1", flowID)
	if err != nil {
		t.Fatalf("DeriveFlowID() error = %v", err)
	}
	if flowCorrelationID == (CorrelationID{}) || flowCorrelationID != mustFlowCorrelationID(t, key, "edge-1", flowID) ||
		len(flowCorrelationID.String()) != 2*CorrelationIDSize {
		t.Fatalf("flow correlation ID = %q", flowCorrelationID.String())
	}
	if connectionID == flowCorrelationID {
		t.Fatal("connection and flow correlation domains collided")
	}
	if strings.Contains(connectionID.String(), hex.EncodeToString(proof[:])) ||
		strings.Contains(flowCorrelationID.String(), hex.EncodeToString(flowID[:])) {
		t.Fatal("correlation ID exposed source material")
	}

	changedProof := proof
	changedProof[0] ^= 1
	if DeriveConnectionID(changedProof) == connectionID {
		t.Fatal("different authentication proofs produced the same connection ID")
	}
	changedFlow := flowID
	changedFlow[0] ^= 1
	if mustFlowCorrelationID(t, key, "edge-1", changedFlow) == flowCorrelationID ||
		mustFlowCorrelationID(t, key, "edge-2", flowID) == flowCorrelationID ||
		mustFlowCorrelationID(t, Key{9}, "edge-1", flowID) == flowCorrelationID {
		t.Fatal("different flow identity material produced the same flow ID")
	}
}

func TestDeriveFlowIDRejectsInvalidIdentityMaterial(t *testing.T) {
	validKey := Key{1}
	validFlowID := protocol.FlowID{1}
	tests := []struct {
		name      string
		key       Key
		principal string
		flowID    protocol.FlowID
	}{
		{name: "zero key", principal: "edge-1", flowID: validFlowID},
		{name: "invalid principal", key: validKey, principal: "bad principal", flowID: validFlowID},
		{name: "zero flow", key: validKey, principal: "edge-1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DeriveFlowID(test.key, test.principal, test.flowID); !errors.Is(err, ErrInvalidCorrelation) {
				t.Fatalf("DeriveFlowID() error = %v", err)
			}
		})
	}
}

func mustFlowCorrelationID(t *testing.T, key Key, principal string, flowID protocol.FlowID) CorrelationID {
	t.Helper()
	correlationID, err := DeriveFlowID(key, principal, flowID)
	if err != nil {
		t.Fatalf("DeriveFlowID() error = %v", err)
	}
	return correlationID
}

func TestVerifierKnownUnknownAndWrongProof(t *testing.T) {
	knownKey := Key{1}
	dummyKey := Key{2}
	keys := map[string]Key{"edge-1": knownKey}
	verifier := NewVerifier(keys, dummyKey)
	keys["edge-1"] = Key{9}

	challenge := [ChallengeSize]byte{3}
	nonce := [NonceSize]byte{4}
	pathGroupID := protocol.PathGroupID{5}
	validProof, err := ComputeProof(knownKey, "edge-1", pathGroupID, challenge, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if !verifier.Verify(protocol.AuthProof{PrincipalID: "edge-1", PathGroupID: pathGroupID, ClientNonce: nonce, Proof: validProof}, challenge) {
		t.Fatal("known valid proof rejected")
	}
	changedPathGroupID := pathGroupID
	changedPathGroupID[0]++
	if verifier.Verify(protocol.AuthProof{PrincipalID: "edge-1", PathGroupID: changedPathGroupID, ClientNonce: nonce, Proof: validProof}, challenge) {
		t.Fatal("proof accepted for a different path group")
	}
	validProof[0] ^= 1
	if verifier.Verify(protocol.AuthProof{PrincipalID: "edge-1", PathGroupID: pathGroupID, ClientNonce: nonce, Proof: validProof}, challenge) {
		t.Fatal("wrong proof accepted")
	}
	unknownProof, err := ComputeProof(dummyKey, "edge-2", pathGroupID, challenge, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if verifier.Verify(protocol.AuthProof{PrincipalID: "edge-2", PathGroupID: pathGroupID, ClientNonce: nonce, Proof: unknownProof}, challenge) {
		t.Fatal("unknown principal accepted with dummy proof")
	}
	if (*Verifier)(nil).Verify(protocol.AuthProof{PrincipalID: "edge-1"}, challenge) {
		t.Fatal("nil verifier accepted proof")
	}
}
