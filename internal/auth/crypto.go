package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"github.com/adrianceding/via/internal/protocol"
)

const (
	KeySize           = 32
	ChallengeSize     = 32
	NonceSize         = 32
	CorrelationIDSize = 12
)

var (
	ErrEntropy            = errors.New("auth: entropy source failed")
	ErrCounterExhausted   = errors.New("auth: challenge counter exhausted")
	ErrInvalidPrincipal   = errors.New("auth: invalid principal id")
	ErrInvalidCorrelation = errors.New("auth: invalid correlation identity")
)

type Key [KeySize]byte

type CorrelationID [CorrelationIDSize]byte

func (identifier CorrelationID) String() string {
	if identifier == (CorrelationID{}) {
		return ""
	}
	return hex.EncodeToString(identifier[:])
}

func DeriveConnectionID(proof [32]byte) CorrelationID {
	digest := sha256.New()
	_, _ = digest.Write([]byte("via status connection correlation v1\x00"))
	_, _ = digest.Write(proof[:])
	var identifier CorrelationID
	copy(identifier[:], digest.Sum(nil))
	return identifier
}

func DeriveFlowID(key Key, principalID string, flowID protocol.FlowID) (CorrelationID, error) {
	if !validKey(key) || !protocol.ValidPrincipalID(principalID) || flowID == (protocol.FlowID{}) {
		return CorrelationID{}, ErrInvalidCorrelation
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("via status flow correlation v1\x00"))
	_, _ = mac.Write([]byte{protocol.Version, byte(len(principalID))})
	_, _ = mac.Write([]byte(principalID))
	_, _ = mac.Write(flowID[:])
	var identifier CorrelationID
	copy(identifier[:], mac.Sum(nil))
	return identifier, nil
}

func validKey(key Key) bool {
	var aggregate byte
	for _, value := range key {
		aggregate |= value
	}
	return aggregate != 0
}

type ChallengeGenerator struct {
	key     Key
	epoch   [16]byte
	counter atomic.Uint64
}

func NewChallengeGenerator(random io.Reader) (*ChallengeGenerator, error) {
	if random == nil {
		return nil, fmt.Errorf("%w: nil reader", ErrEntropy)
	}
	generator := &ChallengeGenerator{}
	if _, err := io.ReadFull(random, generator.key[:]); err != nil {
		return nil, fmt.Errorf("%w: challenge key", ErrEntropy)
	}
	if _, err := io.ReadFull(random, generator.epoch[:]); err != nil {
		return nil, fmt.Errorf("%w: process epoch", ErrEntropy)
	}
	return generator, nil
}

func (generator *ChallengeGenerator) Next() ([ChallengeSize]byte, error) {
	if generator == nil {
		return [ChallengeSize]byte{}, ErrCounterExhausted
	}
	var counter uint64
	for {
		current := generator.counter.Load()
		if current == math.MaxUint64 {
			return [ChallengeSize]byte{}, ErrCounterExhausted
		}
		counter = current + 1
		if generator.counter.CompareAndSwap(current, counter) {
			break
		}
	}

	mac := hmac.New(sha256.New, generator.key[:])
	_, _ = mac.Write([]byte("via server challenge v1\x00"))
	_, _ = mac.Write(generator.epoch[:])
	var encodedCounter [8]byte
	binary.BigEndian.PutUint64(encodedCounter[:], counter)
	_, _ = mac.Write(encodedCounter[:])
	var challenge [ChallengeSize]byte
	copy(challenge[:], mac.Sum(nil))
	return challenge, nil
}

func ComputeProof(key Key, principalID string, challenge [ChallengeSize]byte, nonce [NonceSize]byte) ([32]byte, error) {
	if !protocol.ValidPrincipalID(principalID) {
		return [32]byte{}, ErrInvalidPrincipal
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte("via relay auth v1\x00"))
	_, _ = mac.Write([]byte{protocol.Version, byte(len(principalID))})
	_, _ = mac.Write([]byte(principalID))
	_, _ = mac.Write(challenge[:])
	_, _ = mac.Write(nonce[:])
	var proof [32]byte
	copy(proof[:], mac.Sum(nil))
	return proof, nil
}

type Verifier struct {
	keys  map[string]Key
	dummy Key
}

func NewVerifier(keys map[string]Key, dummy Key) *Verifier {
	copyOfKeys := make(map[string]Key, len(keys))
	for principalID, key := range keys {
		copyOfKeys[principalID] = key
	}
	return &Verifier{keys: copyOfKeys, dummy: dummy}
}

func (verifier *Verifier) Verify(message protocol.AuthProof, challenge [ChallengeSize]byte) bool {
	if verifier == nil || !protocol.ValidPrincipalID(message.PrincipalID) {
		return false
	}
	key, known := verifier.keys[message.PrincipalID]
	if !known {
		key = verifier.dummy
	}
	expected, err := ComputeProof(key, message.PrincipalID, challenge, message.ClientNonce)
	if err != nil {
		return false
	}
	matched := subtle.ConstantTimeCompare(expected[:], message.Proof[:])
	return known && matched == 1
}
