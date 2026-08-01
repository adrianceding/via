package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

const (
	authTimeout   = 5 * time.Second
	sendTimeout   = 10 * time.Second
	probeInterval = time.Second
	probeTimeout  = 3 * time.Second
)

var (
	ErrAuthentication = errors.New("daemon: authentication failed")
	ErrWireProtocol   = errors.New("daemon: wire protocol error")
	ErrWireCapacity   = errors.New("daemon: attachment capacity exceeded")
)

type wireSession struct {
	generation   uint64
	principal    string
	connectionID auth.CorrelationID
	connection   transport.Connection
	ctx          context.Context
	cancel       context.CancelFunc

	attachmentsMu sync.RWMutex
	attachments   map[protocol.FlowID]flow.AttachmentKey
	reservations  map[protocol.FlowID]flow.AttachmentKey
	closeOnce     sync.Once
	status        *runtimeStatus

	probeMu    sync.Mutex
	nextProbe  uint64
	probeToken uint64
	probeSent  time.Time
	lastProbe  time.Time
	probeSRTT  time.Duration
	probeStall bool
}

func newWireSession(ctx context.Context, generation uint64, connection transport.Connection) (*wireSession, error) {
	if ctx == nil || generation == 0 || connection == nil {
		return nil, ErrWireProtocol
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	return &wireSession{
		generation: generation, connection: connection, ctx: sessionCtx, cancel: cancel,
		attachments:  make(map[protocol.FlowID]flow.AttachmentKey, transport.MaxSessionAttachments),
		reservations: make(map[protocol.FlowID]flow.AttachmentKey, transport.MaxSessionAttachments),
	}, nil
}

func (session *wireSession) startProbe(now time.Time) (protocol.Probe, bool, bool) {
	if session == nil || now.IsZero() {
		return protocol.Probe{}, false, false
	}
	session.probeMu.Lock()
	defer session.probeMu.Unlock()
	expired := session.probeToken != 0 && now.Sub(session.probeSent) >= probeTimeout
	if expired {
		session.probeStall = true
	}
	if session.probeToken != 0 && !expired ||
		!session.lastProbe.IsZero() && now.Sub(session.lastProbe) < probeInterval ||
		session.nextProbe == math.MaxUint64 {
		return protocol.Probe{}, false, expired
	}
	session.nextProbe++
	session.probeToken = session.nextProbe
	session.probeSent = now
	session.lastProbe = now
	return protocol.Probe{Token: session.probeToken}, true, expired
}

func (session *wireSession) completeProbe(token uint64, now time.Time) (time.Duration, bool) {
	if session == nil || token == 0 || now.IsZero() {
		return 0, false
	}
	session.probeMu.Lock()
	defer session.probeMu.Unlock()
	if token != session.probeToken || session.probeSent.IsZero() || !now.After(session.probeSent) {
		return 0, false
	}
	rtt := now.Sub(session.probeSent)
	session.probeToken = 0
	session.probeSent = time.Time{}
	session.probeStall = false
	if session.probeSRTT == 0 {
		session.probeSRTT = rtt
	} else {
		session.probeSRTT = (7*session.probeSRTT + rtt) / 8
	}
	return rtt, true
}

func (session *wireSession) smoothedProbeRTT() time.Duration {
	rtt, _ := session.probeQuality()
	return rtt
}

func (session *wireSession) probeQuality() (time.Duration, time.Duration) {
	if session == nil {
		return 0, 0
	}
	session.probeMu.Lock()
	defer session.probeMu.Unlock()
	if session.probeStall {
		return session.probeSRTT, probeTimeout
	}
	return session.probeSRTT, 0
}

func (session *wireSession) send(message protocol.Message) error {
	return session.sendContext(session.ctx, message)
}

func (session *wireSession) sendContext(parent context.Context, message protocol.Message) error {
	if session == nil || message == nil {
		return ErrWireProtocol
	}
	if parent == nil {
		return ErrWireProtocol
	}
	encoded, err := protocol.EncodeMessage(message)
	if err != nil {
		return err
	}
	return session.sendEncodedContext(parent, messageClass(message), encoded)
}

func (session *wireSession) sendEncodedContext(parent context.Context, class transport.FrameClass, encoded []byte) error {
	if session == nil || parent == nil || len(encoded) == 0 {
		return ErrWireProtocol
	}
	ctx, cancel := context.WithTimeout(parent, sendTimeout)
	defer cancel()
	if err := session.connection.WriteFrame(ctx, transport.WriteRequest{Class: class, Encoded: encoded}); err != nil {
		return err
	}
	if session.status != nil {
		session.status.frameSent(uint64(len(encoded)))
	}
	return nil
}

func (session *wireSession) read(ctx context.Context) (protocol.Message, error) {
	if session == nil {
		return nil, ErrWireProtocol
	}
	encoded, err := session.connection.ReadFrame(ctx)
	if err != nil {
		return nil, err
	}
	if session.status != nil {
		session.status.frameReceived(uint64(len(encoded)))
	}
	_, message, err := protocol.DecodeEncodedFrame(encoded)
	return message, err
}

func (session *wireSession) reserve(flowID protocol.FlowID, attachment flow.AttachmentKey) error {
	if session == nil || flowID == (protocol.FlowID{}) || attachment.SessionGeneration != session.generation || attachment.AttachmentGeneration == 0 {
		return ErrWireProtocol
	}
	session.attachmentsMu.Lock()
	defer session.attachmentsMu.Unlock()
	if current, exists := session.attachments[flowID]; exists {
		if current == attachment {
			return nil
		}
		return ErrWireProtocol
	}
	if current, exists := session.reservations[flowID]; exists {
		if current == attachment {
			return nil
		}
		return ErrWireProtocol
	}
	if len(session.attachments)+len(session.reservations) >= transport.MaxSessionAttachments {
		return ErrWireCapacity
	}
	session.reservations[flowID] = attachment
	return nil
}

func (session *wireSession) publish(flowID protocol.FlowID, attachment flow.AttachmentKey) error {
	if session == nil {
		return ErrWireProtocol
	}
	session.attachmentsMu.Lock()
	defer session.attachmentsMu.Unlock()
	if current, exists := session.attachments[flowID]; exists {
		if current == attachment {
			return nil
		}
		return ErrWireProtocol
	}
	if session.reservations[flowID] != attachment {
		return ErrWireProtocol
	}
	delete(session.reservations, flowID)
	session.attachments[flowID] = attachment
	return nil
}

func (session *wireSession) release(flowID protocol.FlowID, attachment flow.AttachmentKey) {
	if session == nil {
		return
	}
	session.attachmentsMu.Lock()
	if session.reservations[flowID] == attachment {
		delete(session.reservations, flowID)
	}
	if session.attachments[flowID] == attachment {
		delete(session.attachments, flowID)
	}
	session.attachmentsMu.Unlock()
}

func (session *wireSession) attachment(flowID protocol.FlowID) (flow.AttachmentKey, bool) {
	if session == nil {
		return flow.AttachmentKey{}, false
	}
	session.attachmentsMu.RLock()
	attachment, ok := session.attachments[flowID]
	session.attachmentsMu.RUnlock()
	return attachment, ok
}

func (session *wireSession) allAttachments() map[protocol.FlowID]flow.AttachmentKey {
	if session == nil {
		return nil
	}
	session.attachmentsMu.RLock()
	result := make(map[protocol.FlowID]flow.AttachmentKey, len(session.attachments))
	for flowID, attachment := range session.attachments {
		result[flowID] = attachment
	}
	session.attachmentsMu.RUnlock()
	return result
}

func (session *wireSession) close() {
	if session == nil {
		return
	}
	session.closeOnce.Do(func() {
		session.cancel()
		_ = session.connection.Close()
	})
}

func authenticateClient(ctx context.Context, session *wireSession, principal string, key auth.Key, random io.Reader) error {
	if random == nil {
		random = rand.Reader
	}
	authCtx, cancel := context.WithTimeout(ctx, authTimeout)
	defer cancel()
	message, err := session.read(authCtx)
	challenge, ok := message.(protocol.AuthChallenge)
	if err != nil || !ok {
		return ErrAuthentication
	}
	var nonce [auth.NonceSize]byte
	if _, err := io.ReadFull(random, nonce[:]); err != nil {
		return ErrAuthentication
	}
	proof, err := auth.ComputeProof(key, principal, challenge.Challenge, nonce)
	if err != nil || session.sendContext(authCtx, protocol.AuthProof{PrincipalID: principal, ClientNonce: nonce, Proof: proof}) != nil {
		return ErrAuthentication
	}
	message, err = session.read(authCtx)
	result, ok := message.(protocol.AuthResult)
	if err != nil || !ok || result.Result != protocol.AuthSuccess {
		return ErrAuthentication
	}
	session.principal = principal
	session.connectionID = auth.DeriveConnectionID(proof)
	return nil
}

func authenticateServer(ctx context.Context, session *wireSession, challenges *auth.ChallengeGenerator, verifier *auth.Verifier) error {
	authCtx, cancel := context.WithTimeout(ctx, authTimeout)
	defer cancel()
	challenge, err := challenges.Next()
	if err != nil || session.sendContext(authCtx, protocol.AuthChallenge{Challenge: challenge}) != nil {
		return ErrAuthentication
	}
	message, err := session.read(authCtx)
	proof, ok := message.(protocol.AuthProof)
	if err != nil || !ok || !verifier.Verify(proof, challenge) {
		_ = session.sendContext(authCtx, protocol.AuthResult{Result: protocol.AuthFailure})
		return ErrAuthentication
	}
	if session.sendContext(authCtx, protocol.AuthResult{Result: protocol.AuthSuccess}) != nil {
		return ErrAuthentication
	}
	session.principal = proof.PrincipalID
	session.connectionID = auth.DeriveConnectionID(proof.Proof)
	return nil
}

func messageClass(message protocol.Message) transport.FrameClass {
	if _, ok := message.(protocol.Data); ok {
		return transport.FrameData
	}
	return transport.FrameControl
}
