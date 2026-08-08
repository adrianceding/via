package client

import (
	"errors"
	"sort"
	"time"

	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/protocol"
)

const (
	OpenDeadline      = 15 * time.Second
	JoinDeadline      = 5 * time.Second
	OpenRetryInterval = time.Second
	JoinRetryInterval = time.Second

	MaxOpenAttempts        = 15
	MaxJoinAttempts        = 5
	MaxOpenJoinSessions    = MaxSessions
	MaxOpenJoinAttachments = flow.MaxAttachments
	MaxOpenJoinActions     = 2*MaxOpenJoinSessions + 8
)

var (
	ErrInvalidOpenJoin  = errors.New("client: invalid OPEN/JOIN coordinator")
	ErrOpenJoinSessions = errors.New("client: OPEN/JOIN ready session limit exceeded")
)

type OpenJoinSpec struct {
	Target        protocol.Target
	DeliveryMode  protocol.DeliveryMode
	PathSelection protocol.PathSelection
	Constraints   protocol.DeliveryConstraints
}

type OpenJoinState uint8

const (
	OpenJoinIdle OpenJoinState = iota + 1
	OpenJoinGeneratingIdentity
	OpenJoinWaitingOpenSession
	OpenJoinOpening
	OpenJoinWaitingJoinSession
	OpenJoinJoining
	OpenJoinPublishingJoin
	OpenJoinActive
	OpenJoinFailed
)

type OpenJoinEventKind uint8

const (
	OpenJoinStart OpenJoinEventKind = iota + 1
	OpenJoinIdentityGenerated
	OpenJoinIdentityFailed
	OpenJoinSessionReady
	OpenJoinSessionQuality
	OpenJoinSessionLost
	OpenJoinOpenRetryDue
	OpenJoinOpenDeadlineReached
	OpenJoinOpenResultReceived
	OpenJoinJoinRetryDue
	OpenJoinJoinDeadlineReached
	OpenJoinJoinReservationFailed
	OpenJoinJoinResultReceived
	OpenJoinAttachmentPublished
	OpenJoinAttachmentPublishFailed
	OpenJoinCancelled
)

type OpenJoinEvent struct {
	Kind              OpenJoinEventKind
	Generation        uint64
	SessionGeneration uint64
	SessionRTT        time.Duration
	SessionStall      time.Duration
	FlowID            protocol.FlowID
	OpenToken         protocol.OpenToken
	OpenResult        protocol.OpenResult
	JoinResult        protocol.JoinResult
}

type OpenJoinActionKind uint8

const (
	OpenJoinActionGenerateIdentity OpenJoinActionKind = iota + 1
	OpenJoinActionArmOpenDeadline
	OpenJoinActionCancelOpenDeadline
	OpenJoinActionSendOpen
	OpenJoinActionArmOpenRetry
	OpenJoinActionCancelOpenRetry
	OpenJoinActionArmJoinDeadline
	OpenJoinActionCancelJoinDeadline
	OpenJoinActionReserveAttachment
	OpenJoinActionSendJoin
	OpenJoinActionArmJoinRetry
	OpenJoinActionCancelJoinRetry
	OpenJoinActionPublishAttachment
	OpenJoinActionReleaseAttachment
	OpenJoinActionReplyApplicationSuccess
	OpenJoinActionFailFlow
)

type OpenJoinFailure uint8

const (
	OpenJoinFailureEntropy OpenJoinFailure = iota + 1
	OpenJoinFailureOpenDeadline
	OpenJoinFailureOpenRejected
	OpenJoinFailureJoinDeadline
	OpenJoinFailureJoinRejected
	OpenJoinFailureProtocol
	OpenJoinFailureRetryLimit
	OpenJoinFailureCancelled
)

type OpenJoinAction struct {
	Kind              OpenJoinActionKind
	Generation        uint64
	SessionGeneration uint64
	Duration          time.Duration
	Attachment        flow.AttachmentKey
	Open              protocol.Open
	Join              protocol.Join
	Failure           OpenJoinFailure
	OpenResult        protocol.OpenResultCode
}

type OpenJoinAttemptKind uint8

const (
	OpenJoinAttemptNone OpenJoinAttemptKind = iota
	OpenJoinAttemptOpen
	OpenJoinAttemptJoin
	OpenJoinAttemptPublish
)

type OpenJoinAttempt struct {
	Kind              OpenJoinAttemptKind
	Generation        uint64
	SessionGeneration uint64
	Attachment        flow.AttachmentKey
}

type OpenJoinSnapshot struct {
	State                  OpenJoinState
	ReadySessions          []uint64
	Attachments            []flow.AttachmentKey
	Attempt                OpenJoinAttempt
	IdentityGeneration     uint64
	OpenDeadlineGeneration uint64
	OpenRetryGeneration    uint64
	JoinDeadlineGeneration uint64
	JoinRetryGeneration    uint64
	OpenAttempts           int
	JoinAttempts           int
	ApplicationAccepted    bool
	Failure                OpenJoinFailure
	OpenResult             protocol.OpenResultCode
}

type openJoinSession struct {
	generation    uint64
	rtt           time.Duration
	stall         time.Duration
	openAttempt   OpenJoinAttempt
	openFinished  bool
	openSucceeded bool
	joinAttempt   OpenJoinAttempt
	joinFinished  bool
}

type openJoinActionBatch struct {
	items [MaxOpenJoinActions]OpenJoinAction
	count int
}

func (batch *openJoinActionBatch) add(action OpenJoinAction) {
	if batch.count >= len(batch.items) {
		panic("client: OPEN/JOIN action limit exceeded")
	}
	batch.items[batch.count] = action
	batch.count++
}

func (batch *openJoinActionBatch) slice() []OpenJoinAction {
	if batch.count == 0 {
		return nil
	}
	actions := make([]OpenJoinAction, batch.count)
	copy(actions, batch.items[:batch.count])
	return actions
}

// OpenJoinCoordinator is the pure OPEN/JOIN state owner for one client flow.
//
// Primary state transitions:
//
//	Idle --Start--> GeneratingIdentity --IdentityGenerated--> WaitingOpenSession/Opening
//	Opening --first OPEN_RESULT success--> PublishingJoin/Joining
//	Opening/Active --other OPEN_RESULT success--> PublishingJoin
//	Joining --JOIN_RESULT success--> PublishingJoin --Published--> Active
//	Active --all attachments lost--> WaitingJoinSession/Joining
//	any non-terminal state --deadline, protocol error, or cancellation--> Failed
//
// It emits only descriptive actions for randomness, timers, transport sessions,
// and the application boundary; it performs no network I/O. The same client flow
// owner must call Handle serially.
type OpenJoinCoordinator struct {
	spec OpenJoinSpec

	state          OpenJoinState
	nextGeneration uint64

	flowID     protocol.FlowID
	openToken  protocol.OpenToken
	capability protocol.Capability

	sessions        [MaxOpenJoinSessions]openJoinSession
	sessionCount    int
	attachments     [MaxOpenJoinAttachments]flow.AttachmentKey
	attachmentCount int

	identityGeneration     uint64
	openDeadlineGeneration uint64
	openRetryGeneration    uint64
	joinDeadlineGeneration uint64
	joinRetryGeneration    uint64
	joinRetrySession       uint64
	openAttempts           int
	joinAttempts           int
	attempt                OpenJoinAttempt

	applicationAccepted  bool
	serialFallback       bool
	failure              OpenJoinFailure
	openResult           protocol.OpenResultCode
	redundantOpenFailure OpenJoinFailure
	redundantOpenResult  protocol.OpenResultCode
	redundantJoinFailure OpenJoinFailure
}

func NewOpenJoinCoordinator(spec OpenJoinSpec) (*OpenJoinCoordinator, error) {
	if err := protocol.ValidateTarget(spec.Target); err != nil ||
		!protocol.ValidDeliveryPolicy(spec.DeliveryMode, spec.PathSelection, spec.Constraints) {
		return nil, ErrInvalidOpenJoin
	}
	return &OpenJoinCoordinator{spec: spec, state: OpenJoinIdle}, nil
}

// Actions returned by Handle must execute in order. If
// OpenJoinActionReserveAttachment fails, the executor must not continue to
// OpenJoinActionSendJoin in the same batch and must instead feed back
// OpenJoinJoinReservationFailed. The randomness executor must fill FlowID and
// OpenToken independently from a cryptographic source; the state machine rejects
// an all-zero result for either value.
func (coordinator *OpenJoinCoordinator) Handle(event OpenJoinEvent) ([]OpenJoinAction, error) {
	if coordinator == nil {
		return nil, ErrInvalidOpenJoin
	}
	if coordinator.state == OpenJoinFailed {
		return nil, nil
	}
	var batch openJoinActionBatch
	var err error
	switch event.Kind {
	case OpenJoinStart:
		coordinator.start(&batch)
	case OpenJoinIdentityGenerated:
		coordinator.identityGenerated(event, &batch)
	case OpenJoinIdentityFailed:
		coordinator.identityFailed(event, &batch)
	case OpenJoinSessionReady:
		err = coordinator.sessionReady(event.SessionGeneration, event.SessionRTT, event.SessionStall, &batch)
	case OpenJoinSessionQuality:
		err = coordinator.sessionQuality(event.SessionGeneration, event.SessionRTT, event.SessionStall)
	case OpenJoinSessionLost:
		coordinator.sessionLost(event.SessionGeneration, &batch)
	case OpenJoinOpenRetryDue:
		coordinator.openRetryDue(event.Generation, &batch)
	case OpenJoinOpenDeadlineReached:
		coordinator.openDeadlineReached(event.Generation, &batch)
	case OpenJoinOpenResultReceived:
		coordinator.openResultReceived(event, &batch)
	case OpenJoinJoinRetryDue:
		coordinator.joinRetryDue(event.Generation, &batch)
	case OpenJoinJoinDeadlineReached:
		coordinator.joinDeadlineReached(event.Generation, &batch)
	case OpenJoinJoinReservationFailed:
		coordinator.joinReservationFailed(event, &batch)
	case OpenJoinJoinResultReceived:
		coordinator.joinResultReceived(event, &batch)
	case OpenJoinAttachmentPublished:
		coordinator.attachmentPublicationCompleted(event, true, &batch)
	case OpenJoinAttachmentPublishFailed:
		coordinator.attachmentPublicationCompleted(event, false, &batch)
	case OpenJoinCancelled:
		coordinator.fail(OpenJoinFailureCancelled, 0, &batch)
	default:
		err = ErrInvalidOpenJoin
	}
	return batch.slice(), err
}

func (coordinator *OpenJoinCoordinator) start(batch *openJoinActionBatch) {
	if coordinator.state != OpenJoinIdle {
		return
	}
	identityGeneration, ok := coordinator.allocateGeneration()
	if !ok {
		coordinator.fail(OpenJoinFailureEntropy, 0, batch)
		return
	}
	deadlineGeneration, ok := coordinator.allocateGeneration()
	if !ok {
		coordinator.fail(OpenJoinFailureEntropy, 0, batch)
		return
	}
	coordinator.state = OpenJoinGeneratingIdentity
	coordinator.identityGeneration = identityGeneration
	coordinator.openDeadlineGeneration = deadlineGeneration
	batch.add(OpenJoinAction{Kind: OpenJoinActionGenerateIdentity, Generation: identityGeneration})
	batch.add(OpenJoinAction{Kind: OpenJoinActionArmOpenDeadline, Generation: deadlineGeneration, Duration: OpenDeadline})
}

func (coordinator *OpenJoinCoordinator) identityGenerated(event OpenJoinEvent, batch *openJoinActionBatch) {
	if coordinator.state != OpenJoinGeneratingIdentity || event.Generation == 0 || event.Generation != coordinator.identityGeneration {
		return
	}
	coordinator.identityGeneration = 0
	if isZero(event.FlowID[:]) || isZero(event.OpenToken[:]) {
		coordinator.fail(OpenJoinFailureEntropy, 0, batch)
		return
	}
	coordinator.flowID = event.FlowID
	coordinator.openToken = event.OpenToken
	coordinator.state = OpenJoinWaitingOpenSession
	coordinator.beginOpenAttempt(0, batch)
}

func (coordinator *OpenJoinCoordinator) identityFailed(event OpenJoinEvent, batch *openJoinActionBatch) {
	if coordinator.state != OpenJoinGeneratingIdentity || event.Generation == 0 || event.Generation != coordinator.identityGeneration {
		return
	}
	coordinator.identityGeneration = 0
	coordinator.fail(OpenJoinFailureEntropy, 0, batch)
}

func (coordinator *OpenJoinCoordinator) sessionReady(generation uint64, rtt, stall time.Duration, batch *openJoinActionBatch) error {
	if generation == 0 || rtt < 0 || stall < 0 {
		return ErrInvalidOpenJoin
	}
	if session := coordinator.session(generation); session != nil {
		session.rtt = rtt
		session.stall = stall
		return nil
	}
	if coordinator.sessionCount >= len(coordinator.sessions) {
		return ErrOpenJoinSessions
	}
	coordinator.sessions[coordinator.sessionCount] = openJoinSession{generation: generation, rtt: rtt, stall: stall}
	coordinator.sessionCount++
	sort.Slice(coordinator.sessions[:coordinator.sessionCount], func(i, j int) bool {
		return coordinator.sessions[i].generation < coordinator.sessions[j].generation
	})
	if coordinator.parallelEstablishment() {
		switch {
		case coordinator.capability == (protocol.Capability{}) &&
			(coordinator.state == OpenJoinWaitingOpenSession || coordinator.state == OpenJoinOpening):
			coordinator.beginRedundantOpenAttempts(batch)
		case coordinator.capability != (protocol.Capability{}) &&
			(coordinator.state == OpenJoinWaitingJoinSession || coordinator.state == OpenJoinJoining ||
				coordinator.state == OpenJoinPublishingJoin || coordinator.state == OpenJoinActive):
			coordinator.startRedundantJoinCycle(generation, coordinator.attachmentCount == 0, batch)
		}
		return nil
	}

	switch coordinator.state {
	case OpenJoinWaitingOpenSession:
		coordinator.beginOpenAttempt(generation, batch)
	case OpenJoinWaitingJoinSession, OpenJoinActive:
		if coordinator.attempt.Kind == OpenJoinAttemptNone && coordinator.attachmentCount < MaxOpenJoinAttachments {
			coordinator.startJoinCycle(generation, coordinator.attachmentCount == 0, batch)
		}
	}
	return nil
}

func (coordinator *OpenJoinCoordinator) sessionQuality(generation uint64, rtt, stall time.Duration) error {
	if generation == 0 || rtt < 0 || stall < 0 || rtt == 0 && stall == 0 {
		return ErrInvalidOpenJoin
	}
	session := coordinator.session(generation)
	if session == nil {
		return nil
	}
	session.rtt = rtt
	session.stall = stall
	return nil
}

func (coordinator *OpenJoinCoordinator) sessionLost(generation uint64, batch *openJoinActionBatch) {
	if generation == 0 {
		return
	}
	session := coordinator.session(generation)
	if session == nil {
		return
	}
	removedSession := *session
	coordinator.removeReadySession(generation)
	if coordinator.parallelEstablishment() {
		coordinator.redundantSessionLost(removedSession, batch)
		return
	}

	attemptLost := coordinator.attempt.SessionGeneration == generation && coordinator.attempt.Kind != OpenJoinAttemptNone
	if attemptLost {
		switch coordinator.attempt.Kind {
		case OpenJoinAttemptOpen:
			coordinator.cancelOpenRetry(batch)
			coordinator.attempt = OpenJoinAttempt{}
			coordinator.state = OpenJoinWaitingOpenSession
			coordinator.beginOpenAttempt(0, batch)
		case OpenJoinAttemptJoin, OpenJoinAttemptPublish:
			coordinator.cancelJoinRetry(batch)
			coordinator.releaseAttemptAttachment(batch)
			coordinator.attempt = OpenJoinAttempt{}
			coordinator.state = coordinator.joinWaitingState()
		}
	}
	if coordinator.joinRetrySession == generation {
		coordinator.joinRetrySession = 0
	}

	removedAttachment := coordinator.removeAttachmentForSession(generation)
	if removedAttachment != (flow.AttachmentKey{}) {
		batch.add(OpenJoinAction{Kind: OpenJoinActionReleaseAttachment, Attachment: removedAttachment, SessionGeneration: generation})
	}

	if coordinator.capability == (protocol.Capability{}) || coordinator.state == OpenJoinOpening || coordinator.state == OpenJoinWaitingOpenSession {
		return
	}
	if coordinator.attempt.Kind != OpenJoinAttemptNone || coordinator.attachmentCount >= MaxOpenJoinAttachments {
		return
	}
	if coordinator.joinRetryGeneration != 0 {
		return
	}
	if coordinator.joinDeadlineGeneration != 0 {
		coordinator.beginJoinAttempt(0, batch)
		return
	}
	coordinator.startJoinCycle(0, coordinator.attachmentCount == 0, batch)
}

func (coordinator *OpenJoinCoordinator) openRetryDue(generation uint64, batch *openJoinActionBatch) {
	if coordinator.parallelEstablishment() {
		if generation == 0 || generation != coordinator.openRetryGeneration {
			return
		}
		coordinator.openRetryGeneration = 0
		if coordinator.openAttempts >= MaxOpenAttempts {
			for index := 0; index < coordinator.sessionCount; index++ {
				session := &coordinator.sessions[index]
				if session.openAttempt.Kind == OpenJoinAttemptOpen {
					session.openAttempt = OpenJoinAttempt{}
					session.openFinished = true
				}
			}
			if coordinator.capability != (protocol.Capability{}) && coordinator.attachmentCount != 0 {
				coordinator.cancelOpenDeadline(batch)
				coordinator.finishRedundantJoinRound(batch)
			}
			return
		}
		for index := 0; index < coordinator.sessionCount; index++ {
			session := &coordinator.sessions[index]
			if session.openAttempt.Kind == OpenJoinAttemptOpen {
				session.openAttempt = OpenJoinAttempt{}
				session.openFinished = false
			}
		}
		coordinator.beginRedundantOpenAttempts(batch)
		return
	}
	if coordinator.state != OpenJoinOpening || generation == 0 || generation != coordinator.openRetryGeneration {
		return
	}
	preferred := coordinator.retrySession(coordinator.attempt.SessionGeneration)
	coordinator.openRetryGeneration = 0
	coordinator.attempt = OpenJoinAttempt{}
	coordinator.beginOpenAttempt(preferred, batch)
}

func (coordinator *OpenJoinCoordinator) openDeadlineReached(generation uint64, batch *openJoinActionBatch) {
	if generation == 0 || generation != coordinator.openDeadlineGeneration {
		return
	}
	if coordinator.parallelEstablishment() {
		coordinator.redundantOpenDeadlineReached(batch)
		return
	}
	coordinator.openDeadlineGeneration = 0
	coordinator.fail(OpenJoinFailureOpenDeadline, 0, batch)
}

func (coordinator *OpenJoinCoordinator) openResultReceived(event OpenJoinEvent, batch *openJoinActionBatch) {
	if coordinator.parallelEstablishment() {
		if coordinator.spec.DeliveryMode == protocol.DeliveryAdaptive &&
			coordinator.validOpenResult(event.OpenResult) && event.OpenResult.Result == protocol.OpenSuccess &&
			!event.OpenResult.ImplicitAttachment {
			if coordinator.capability != (protocol.Capability{}) {
				if event.OpenResult.Capability != coordinator.capability {
					coordinator.fail(OpenJoinFailureProtocol, 0, batch)
					return
				}
				session := coordinator.session(event.SessionGeneration)
				if session == nil || session.openAttempt.Kind != OpenJoinAttemptOpen || event.Generation == 0 ||
					event.Generation != session.openAttempt.Generation {
					return
				}
				session.openAttempt = OpenJoinAttempt{}
				session.openFinished = true
				coordinator.startRedundantJoinCycle(session.generation, coordinator.attachmentCount == 0, batch)
				return
			}
			coordinator.adaptiveOpenFallback(event, batch)
			return
		}
		coordinator.redundantOpenResultReceived(event, batch)
		return
	}
	if coordinator.state != OpenJoinOpening || coordinator.attempt.Kind != OpenJoinAttemptOpen ||
		event.Generation == 0 || event.Generation != coordinator.attempt.Generation ||
		event.SessionGeneration != coordinator.attempt.SessionGeneration {
		return
	}
	if !coordinator.validOpenResult(event.OpenResult) {
		coordinator.fail(OpenJoinFailureProtocol, 0, batch)
		return
	}
	coordinator.cancelOpenRetry(batch)
	coordinator.attempt = OpenJoinAttempt{}
	if event.OpenResult.Result != protocol.OpenSuccess {
		coordinator.fail(OpenJoinFailureOpenRejected, event.OpenResult.Result, batch)
		return
	}
	coordinator.capability = event.OpenResult.Capability
	coordinator.cancelOpenDeadline(batch)
	if event.OpenResult.ImplicitAttachment {
		session := coordinator.session(event.SessionGeneration)
		coordinator.beginImplicitAttachment(session, batch)
		return
	}
	coordinator.startJoinCycle(event.SessionGeneration, true, batch)
}

func (coordinator *OpenJoinCoordinator) adaptiveOpenFallback(event OpenJoinEvent, batch *openJoinActionBatch) {
	session := coordinator.session(event.SessionGeneration)
	if session == nil || session.openAttempt.Kind != OpenJoinAttemptOpen || event.Generation == 0 ||
		event.Generation != session.openAttempt.Generation {
		return
	}
	for index := 0; index < coordinator.sessionCount; index++ {
		coordinator.sessions[index].openAttempt = OpenJoinAttempt{}
	}
	coordinator.cancelOpenRetry(batch)
	coordinator.cancelOpenDeadline(batch)
	coordinator.capability = event.OpenResult.Capability
	coordinator.attempt = OpenJoinAttempt{}
	coordinator.serialFallback = true
	coordinator.startJoinCycle(event.SessionGeneration, true, batch)
}

func (coordinator *OpenJoinCoordinator) joinRetryDue(generation uint64, batch *openJoinActionBatch) {
	if generation == 0 || generation != coordinator.joinRetryGeneration {
		return
	}
	preferred := coordinator.joinRetrySession
	coordinator.joinRetryGeneration = 0
	coordinator.joinRetrySession = 0
	if coordinator.attempt.Kind == OpenJoinAttemptJoin {
		preferred = coordinator.retrySession(coordinator.attempt.SessionGeneration)
		coordinator.releaseAttemptAttachment(batch)
		coordinator.attempt = OpenJoinAttempt{}
	}
	coordinator.beginJoinAttempt(preferred, batch)
}

func (coordinator *OpenJoinCoordinator) retrySession(previous uint64) uint64 {
	if alternative := coordinator.selectReadySession(0, previous); alternative != 0 {
		return alternative
	}
	return previous
}

func (coordinator *OpenJoinCoordinator) joinDeadlineReached(generation uint64, batch *openJoinActionBatch) {
	if generation == 0 || generation != coordinator.joinDeadlineGeneration {
		return
	}
	if coordinator.parallelEstablishment() {
		coordinator.redundantJoinDeadlineReached(batch)
		return
	}
	coordinator.joinDeadlineGeneration = 0
	coordinator.cancelJoinRetry(batch)
	coordinator.releaseAttemptAttachment(batch)
	coordinator.attempt = OpenJoinAttempt{}
	if coordinator.attachmentCount == 0 {
		if coordinator.applicationAccepted {
			coordinator.state = OpenJoinWaitingJoinSession
			coordinator.startJoinCycle(0, true, batch)
			return
		}
		coordinator.fail(OpenJoinFailureJoinDeadline, 0, batch)
		return
	}
	coordinator.state = OpenJoinActive
}

func (coordinator *OpenJoinCoordinator) joinReservationFailed(event OpenJoinEvent, batch *openJoinActionBatch) {
	if coordinator.parallelEstablishment() {
		coordinator.redundantJoinReservationFailed(event, batch)
		return
	}
	if coordinator.state != OpenJoinJoining || coordinator.attempt.Kind != OpenJoinAttemptJoin ||
		event.Generation == 0 || event.Generation != coordinator.attempt.Generation ||
		event.SessionGeneration != coordinator.attempt.SessionGeneration {
		return
	}
	failedSession := coordinator.attempt.SessionGeneration
	coordinator.cancelJoinRetry(batch)
	coordinator.releaseAttemptAttachment(batch)
	coordinator.attempt = OpenJoinAttempt{}
	coordinator.beginJoinAttemptExcluding(failedSession, batch)
}

func (coordinator *OpenJoinCoordinator) joinResultReceived(event OpenJoinEvent, batch *openJoinActionBatch) {
	if coordinator.parallelEstablishment() {
		coordinator.redundantJoinResultReceived(event, batch)
		return
	}
	if coordinator.state != OpenJoinJoining || coordinator.attempt.Kind != OpenJoinAttemptJoin ||
		event.Generation == 0 || event.Generation != coordinator.attempt.Generation ||
		event.SessionGeneration != coordinator.attempt.SessionGeneration {
		return
	}
	if !coordinator.validJoinResult(event.JoinResult) {
		coordinator.fail(OpenJoinFailureProtocol, 0, batch)
		return
	}
	if event.JoinResult.Result == protocol.JoinFailure {
		preferred := coordinator.attempt.SessionGeneration
		coordinator.releaseAttemptAttachment(batch)
		coordinator.attempt = OpenJoinAttempt{}
		coordinator.state = coordinator.joinWaitingState()
		if coordinator.joinRetryGeneration == 0 {
			if coordinator.attachmentCount != 0 {
				coordinator.cancelJoinDeadline(batch)
				coordinator.state = OpenJoinActive
			}
			return
		}
		coordinator.joinRetrySession = preferred
		if coordinator.attachmentCount != 0 {
			// An optional second attachment may retry without affecting the
			// already-relaying flow.
			coordinator.state = OpenJoinActive
		}
		if coordinator.joinAttempts >= MaxJoinAttempts && coordinator.attachmentCount != 0 {
			coordinator.cancelJoinDeadline(batch)
			coordinator.state = OpenJoinActive
		}
		return
	}
	coordinator.cancelJoinRetry(batch)
	coordinator.state = OpenJoinPublishingJoin
	coordinator.attempt.Kind = OpenJoinAttemptPublish
	batch.add(OpenJoinAction{
		Kind:              OpenJoinActionPublishAttachment,
		Generation:        coordinator.attempt.Generation,
		SessionGeneration: coordinator.attempt.SessionGeneration,
		Attachment:        coordinator.attempt.Attachment,
	})
}

func (coordinator *OpenJoinCoordinator) attachmentPublicationCompleted(event OpenJoinEvent, succeeded bool, batch *openJoinActionBatch) {
	if coordinator.parallelEstablishment() {
		coordinator.redundantAttachmentPublicationCompleted(event, succeeded, batch)
		return
	}
	if coordinator.state != OpenJoinPublishingJoin || coordinator.attempt.Kind != OpenJoinAttemptPublish ||
		event.Generation == 0 || event.Generation != coordinator.attempt.Generation ||
		event.SessionGeneration != coordinator.attempt.SessionGeneration {
		return
	}
	attachment := coordinator.attempt.Attachment
	failedSession := coordinator.attempt.SessionGeneration
	coordinator.attempt = OpenJoinAttempt{}
	if !succeeded {
		batch.add(OpenJoinAction{Kind: OpenJoinActionReleaseAttachment, Attachment: attachment, SessionGeneration: failedSession})
		coordinator.beginJoinAttemptExcluding(failedSession, batch)
		return
	}
	if !coordinator.addAttachment(attachment) {
		batch.add(OpenJoinAction{Kind: OpenJoinActionReleaseAttachment, Attachment: attachment, SessionGeneration: failedSession})
		coordinator.fail(OpenJoinFailureProtocol, 0, batch)
		return
	}
	coordinator.cancelJoinDeadline(batch)
	coordinator.state = OpenJoinActive
	if !coordinator.applicationAccepted {
		coordinator.applicationAccepted = true
		batch.add(OpenJoinAction{Kind: OpenJoinActionReplyApplicationSuccess})
	}
	if coordinator.attachmentCount < MaxOpenJoinAttachments {
		coordinator.startJoinCycle(0, false, batch)
	}
}

func (coordinator *OpenJoinCoordinator) beginRedundantOpenAttempts(batch *openJoinActionBatch) {
	count := 0
	indices := make([]int, 0, coordinator.sessionCount)
	for index := 0; index < coordinator.sessionCount; index++ {
		session := &coordinator.sessions[index]
		if session.openAttempt.Kind == OpenJoinAttemptNone && !session.openFinished {
			count++
			indices = append(indices, index)
		}
	}
	if count == 0 {
		if coordinator.attachmentCount != 0 {
			coordinator.state = OpenJoinActive
		} else if coordinator.hasPendingRedundantOpen() {
			coordinator.state = OpenJoinOpening
		} else {
			coordinator.state = OpenJoinWaitingOpenSession
		}
		return
	}
	if coordinator.openAttempts >= MaxOpenAttempts {
		if coordinator.capability != (protocol.Capability{}) && coordinator.attachmentCount != 0 {
			coordinator.state = OpenJoinActive
		}
		return
	}
	armRetry := coordinator.spec.DeliveryMode == protocol.DeliveryAdaptive && coordinator.openRetryGeneration == 0 &&
		coordinator.openAttempts < MaxOpenAttempts
	generationCount := uint64(count)
	if armRetry {
		generationCount++
	}
	if !coordinator.canAllocate(generationCount) {
		coordinator.fail(OpenJoinFailureRetryLimit, 0, batch)
		return
	}
	coordinator.openAttempts++
	if coordinator.attachmentCount != 0 {
		coordinator.state = OpenJoinActive
	} else {
		coordinator.state = OpenJoinOpening
	}
	// The server starts reading the target as soon as the first attachment is
	// published. Put the best currently-known session first so the initial
	// adaptive window does not get pinned to an arbitrary slow path while the
	// remaining sessions establish in parallel.
	sort.SliceStable(indices, func(left, right int) bool {
		return betterOpenJoinSession(coordinator.sessions[indices[left]], coordinator.sessions[indices[right]])
	})
	for _, index := range indices {
		session := &coordinator.sessions[index]
		if session.openAttempt.Kind != OpenJoinAttemptNone || session.openFinished {
			continue
		}
		generation, _ := coordinator.allocateGeneration()
		session.openAttempt = OpenJoinAttempt{
			Kind: OpenJoinAttemptOpen, Generation: generation, SessionGeneration: session.generation,
		}
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionSendOpen, Generation: generation, SessionGeneration: session.generation,
			Open: protocol.Open{
				FlowID: coordinator.flowID, OpenToken: coordinator.openToken,
				DeliveryMode: coordinator.spec.DeliveryMode, PathSelection: coordinator.spec.PathSelection,
				Constraints: coordinator.spec.Constraints, Target: coordinator.spec.Target,
			},
		})
	}
	if armRetry {
		generation, _ := coordinator.allocateGeneration()
		coordinator.openRetryGeneration = generation
		batch.add(OpenJoinAction{Kind: OpenJoinActionArmOpenRetry, Generation: generation, Duration: OpenRetryInterval})
	}
}

func (coordinator *OpenJoinCoordinator) redundantOpenResultReceived(event OpenJoinEvent, batch *openJoinActionBatch) {
	session := coordinator.session(event.SessionGeneration)
	if session == nil || session.openAttempt.Kind != OpenJoinAttemptOpen || event.Generation == 0 ||
		event.Generation != session.openAttempt.Generation {
		return
	}
	session.openAttempt = OpenJoinAttempt{}
	session.openFinished = true
	if !coordinator.hasPendingRedundantOpen() {
		coordinator.cancelOpenRetry(batch)
		coordinator.cancelOpenDeadline(batch)
	}
	if !coordinator.validOpenResult(event.OpenResult) {
		if coordinator.capability != (protocol.Capability{}) {
			session.joinFinished = true
			coordinator.redundantJoinFailure = OpenJoinFailureProtocol
			coordinator.finishRedundantJoinRound(batch)
			return
		}
		coordinator.redundantOpenFailure = OpenJoinFailureProtocol
		coordinator.finishRedundantOpenRound(batch)
		return
	}
	if event.OpenResult.Result != protocol.OpenSuccess {
		if coordinator.capability != (protocol.Capability{}) {
			coordinator.startRedundantJoinCycle(session.generation, coordinator.attachmentCount == 0, batch)
			return
		}
		coordinator.redundantOpenFailure = OpenJoinFailureOpenRejected
		coordinator.redundantOpenResult = event.OpenResult.Result
		coordinator.finishRedundantOpenRound(batch)
		return
	}
	if coordinator.capability != (protocol.Capability{}) && event.OpenResult.Capability != coordinator.capability {
		coordinator.fail(OpenJoinFailureProtocol, 0, batch)
		return
	}
	session.openSucceeded = true
	if coordinator.capability == (protocol.Capability{}) {
		coordinator.capability = event.OpenResult.Capability
		coordinator.redundantOpenFailure = 0
		coordinator.redundantOpenResult = 0
		coordinator.startRedundantOpenFallbackJoins(batch)
	}
	if event.OpenResult.ImplicitAttachment {
		coordinator.beginImplicitAttachment(session, batch)
		return
	}
	coordinator.startRedundantJoinCycle(session.generation, coordinator.attachmentCount == 0, batch)
}

func (coordinator *OpenJoinCoordinator) beginImplicitAttachment(session *openJoinSession, batch *openJoinActionBatch) {
	if session == nil || session.joinAttempt.Kind != OpenJoinAttemptNone ||
		coordinator.hasAttachmentForSession(session.generation) || !coordinator.canAllocate(2) {
		if session != nil && !coordinator.hasAttachmentForSession(session.generation) {
			coordinator.fail(OpenJoinFailureRetryLimit, 0, batch)
		}
		return
	}
	attemptGeneration, _ := coordinator.allocateGeneration()
	attachmentGeneration, _ := coordinator.allocateGeneration()
	attachment := flow.AttachmentKey{
		SessionGeneration: session.generation, AttachmentGeneration: attachmentGeneration,
	}
	session.joinAttempt = OpenJoinAttempt{
		Kind: OpenJoinAttemptPublish, Generation: attemptGeneration,
		SessionGeneration: session.generation, Attachment: attachment,
	}
	coordinator.attempt = session.joinAttempt
	coordinator.state = OpenJoinPublishingJoin
	batch.add(OpenJoinAction{
		Kind: OpenJoinActionReserveAttachment, Generation: attemptGeneration,
		SessionGeneration: session.generation, Attachment: attachment,
	})
	batch.add(OpenJoinAction{
		Kind: OpenJoinActionPublishAttachment, Generation: attemptGeneration,
		SessionGeneration: session.generation, Attachment: attachment,
	})
}

func (coordinator *OpenJoinCoordinator) startRedundantOpenFallbackJoins(batch *openJoinActionBatch) {
	for index := 0; index < coordinator.sessionCount; index++ {
		session := &coordinator.sessions[index]
		if !session.openFinished || session.openSucceeded ||
			session.joinAttempt.Kind != OpenJoinAttemptNone || coordinator.hasAttachmentForSession(session.generation) {
			continue
		}
		coordinator.startRedundantJoinCycle(session.generation, coordinator.attachmentCount == 0, batch)
	}
}

func (coordinator *OpenJoinCoordinator) finishRedundantOpenRound(batch *openJoinActionBatch) {
	if coordinator.hasPendingRedundantOpen() {
		return
	}
	coordinator.cancelOpenRetry(batch)
	if coordinator.capability != (protocol.Capability{}) {
		return
	}
	if coordinator.sessionCount == 0 {
		coordinator.state = OpenJoinWaitingOpenSession
		return
	}
	for index := 0; index < coordinator.sessionCount; index++ {
		if !coordinator.sessions[index].openFinished {
			coordinator.beginRedundantOpenAttempts(batch)
			return
		}
	}
	failure := coordinator.redundantOpenFailure
	if failure == 0 {
		failure = OpenJoinFailureOpenRejected
	}
	coordinator.fail(failure, coordinator.redundantOpenResult, batch)
}

func (coordinator *OpenJoinCoordinator) startRedundantJoinCycle(preferred uint64, required bool, batch *openJoinActionBatch) {
	if coordinator.capability == (protocol.Capability{}) || coordinator.attachmentCount >= MaxOpenJoinAttachments {
		return
	}
	if required && coordinator.attachmentCount == 0 {
		for index := 0; index < coordinator.sessionCount; index++ {
			if !coordinator.hasAttachmentForSession(coordinator.sessions[index].generation) &&
				coordinator.sessions[index].joinAttempt.Kind == OpenJoinAttemptNone {
				coordinator.sessions[index].joinFinished = false
			}
		}
	}
	coordinator.beginRedundantJoinAttempts(preferred, required, batch)
}

func (coordinator *OpenJoinCoordinator) beginRedundantJoinAttempts(preferred uint64, required bool, batch *openJoinActionBatch) {
	eligible := make([]int, 0, coordinator.sessionCount)
	for index := 0; index < coordinator.sessionCount; index++ {
		session := &coordinator.sessions[index]
		if preferred != 0 && session.generation != preferred {
			continue
		}
		if session.joinAttempt.Kind == OpenJoinAttemptNone && !session.joinFinished &&
			!coordinator.hasAttachmentForSession(session.generation) {
			eligible = append(eligible, index)
		}
	}
	needDeadline := coordinator.joinDeadlineGeneration == 0 &&
		(required || len(eligible) != 0 || coordinator.hasPendingRedundantJoin())
	generationCount := uint64(2 * len(eligible))
	if needDeadline {
		generationCount++
	}
	if generationCount != 0 && !coordinator.canAllocate(generationCount) {
		coordinator.fail(OpenJoinFailureRetryLimit, 0, batch)
		return
	}
	if needDeadline {
		generation, _ := coordinator.allocateGeneration()
		coordinator.joinDeadlineGeneration = generation
		batch.add(OpenJoinAction{Kind: OpenJoinActionArmJoinDeadline, Generation: generation, Duration: JoinDeadline})
	}
	for _, index := range eligible {
		session := &coordinator.sessions[index]
		attemptGeneration, _ := coordinator.allocateGeneration()
		attachmentGeneration, _ := coordinator.allocateGeneration()
		attachment := flow.AttachmentKey{
			SessionGeneration: session.generation, AttachmentGeneration: attachmentGeneration,
		}
		session.joinAttempt = OpenJoinAttempt{
			Kind: OpenJoinAttemptJoin, Generation: attemptGeneration,
			SessionGeneration: session.generation, Attachment: attachment,
		}
		coordinator.joinAttempts++
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionReserveAttachment, Generation: attemptGeneration,
			SessionGeneration: session.generation, Attachment: attachment,
		})
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionSendJoin, Generation: attemptGeneration,
			SessionGeneration: session.generation, Attachment: attachment,
			Join: protocol.Join{FlowID: coordinator.flowID, Capability: coordinator.capability},
		})
	}
	if coordinator.hasPendingRedundantJoin() {
		coordinator.state = OpenJoinJoining
	} else if coordinator.attachmentCount != 0 {
		coordinator.state = OpenJoinActive
	} else {
		coordinator.state = OpenJoinWaitingJoinSession
	}
}

func (coordinator *OpenJoinCoordinator) redundantJoinReservationFailed(event OpenJoinEvent, batch *openJoinActionBatch) {
	session := coordinator.redundantJoinSession(event, OpenJoinAttemptJoin)
	if session == nil {
		return
	}
	session.joinAttempt = OpenJoinAttempt{}
	session.joinFinished = true
	coordinator.redundantJoinFailure = OpenJoinFailureRetryLimit
	coordinator.finishRedundantJoinRound(batch)
}

func (coordinator *OpenJoinCoordinator) redundantJoinResultReceived(event OpenJoinEvent, batch *openJoinActionBatch) {
	session := coordinator.redundantJoinSession(event, OpenJoinAttemptJoin)
	if session == nil {
		return
	}
	if !coordinator.validJoinResult(event.JoinResult) {
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionReleaseAttachment, Generation: session.joinAttempt.Generation,
			SessionGeneration: session.generation, Attachment: session.joinAttempt.Attachment,
		})
		session.joinAttempt = OpenJoinAttempt{}
		session.joinFinished = true
		coordinator.redundantJoinFailure = OpenJoinFailureProtocol
		coordinator.finishRedundantJoinRound(batch)
		return
	}
	if event.JoinResult.Result == protocol.JoinFailure {
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionReleaseAttachment, Generation: session.joinAttempt.Generation,
			SessionGeneration: session.generation, Attachment: session.joinAttempt.Attachment,
		})
		session.joinAttempt = OpenJoinAttempt{}
		session.joinFinished = true
		coordinator.redundantJoinFailure = OpenJoinFailureJoinRejected
		coordinator.finishRedundantJoinRound(batch)
		return
	}
	session.joinAttempt.Kind = OpenJoinAttemptPublish
	coordinator.state = OpenJoinPublishingJoin
	batch.add(OpenJoinAction{
		Kind: OpenJoinActionPublishAttachment, Generation: session.joinAttempt.Generation,
		SessionGeneration: session.generation, Attachment: session.joinAttempt.Attachment,
	})
}

func (coordinator *OpenJoinCoordinator) redundantAttachmentPublicationCompleted(event OpenJoinEvent, succeeded bool, batch *openJoinActionBatch) {
	session := coordinator.redundantJoinSession(event, OpenJoinAttemptPublish)
	if session == nil {
		return
	}
	attachment := session.joinAttempt.Attachment
	session.joinAttempt = OpenJoinAttempt{}
	session.joinFinished = true
	if !succeeded {
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionReleaseAttachment, Generation: event.Generation,
			SessionGeneration: event.SessionGeneration, Attachment: attachment,
		})
		coordinator.redundantJoinFailure = OpenJoinFailureProtocol
		coordinator.finishRedundantJoinRound(batch)
		return
	}
	if !coordinator.addAttachment(attachment) {
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionReleaseAttachment, Generation: event.Generation,
			SessionGeneration: event.SessionGeneration, Attachment: attachment,
		})
		coordinator.fail(OpenJoinFailureProtocol, 0, batch)
		return
	}
	if !coordinator.applicationAccepted {
		coordinator.applicationAccepted = true
		batch.add(OpenJoinAction{Kind: OpenJoinActionReplyApplicationSuccess})
	}
	coordinator.finishRedundantJoinRound(batch)
}

func (coordinator *OpenJoinCoordinator) redundantJoinDeadlineReached(batch *openJoinActionBatch) {
	coordinator.joinDeadlineGeneration = 0
	for index := 0; index < coordinator.sessionCount; index++ {
		session := &coordinator.sessions[index]
		if session.joinAttempt.Attachment != (flow.AttachmentKey{}) {
			batch.add(OpenJoinAction{
				Kind: OpenJoinActionReleaseAttachment, Generation: session.joinAttempt.Generation,
				SessionGeneration: session.generation, Attachment: session.joinAttempt.Attachment,
			})
		}
		session.joinAttempt = OpenJoinAttempt{}
		session.joinFinished = false
	}
	if coordinator.attachmentCount != 0 {
		coordinator.finishRedundantJoinRound(batch)
		return
	}
	if !coordinator.applicationAccepted {
		coordinator.fail(OpenJoinFailureJoinDeadline, 0, batch)
		return
	}
	coordinator.state = OpenJoinWaitingJoinSession
	coordinator.startRedundantJoinCycle(0, true, batch)
}

func (coordinator *OpenJoinCoordinator) finishRedundantJoinRound(batch *openJoinActionBatch) {
	if coordinator.hasPendingRedundantEstablishment() {
		coordinator.state = OpenJoinJoining
		return
	}
	if coordinator.attachmentCount != 0 {
		coordinator.cancelJoinDeadline(batch)
		coordinator.state = OpenJoinActive
		if !coordinator.applicationAccepted {
			coordinator.applicationAccepted = true
			batch.add(OpenJoinAction{Kind: OpenJoinActionReplyApplicationSuccess})
		}
		return
	}
	if coordinator.sessionCount == 0 {
		coordinator.state = OpenJoinWaitingJoinSession
		return
	}
	for index := 0; index < coordinator.sessionCount; index++ {
		if !coordinator.sessions[index].joinFinished {
			coordinator.state = OpenJoinWaitingJoinSession
			return
		}
	}
	failure := coordinator.redundantJoinFailure
	if failure == 0 {
		failure = OpenJoinFailureJoinRejected
	}
	coordinator.fail(failure, 0, batch)
}

func (coordinator *OpenJoinCoordinator) redundantSessionLost(session openJoinSession, batch *openJoinActionBatch) {
	if session.joinAttempt.Attachment != (flow.AttachmentKey{}) {
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionReleaseAttachment, Generation: session.joinAttempt.Generation,
			SessionGeneration: session.generation, Attachment: session.joinAttempt.Attachment,
		})
	}
	if attachment := coordinator.removeAttachmentForSession(session.generation); attachment != (flow.AttachmentKey{}) {
		batch.add(OpenJoinAction{
			Kind: OpenJoinActionReleaseAttachment, SessionGeneration: session.generation, Attachment: attachment,
		})
	}
	if coordinator.capability == (protocol.Capability{}) {
		coordinator.finishRedundantOpenRound(batch)
		return
	}
	if coordinator.attachmentCount != 0 || coordinator.hasPendingRedundantJoin() {
		coordinator.finishRedundantJoinRound(batch)
		return
	}
	if coordinator.joinDeadlineGeneration == 0 {
		coordinator.startRedundantJoinCycle(0, true, batch)
		return
	}
	coordinator.state = OpenJoinWaitingJoinSession
}

func (coordinator *OpenJoinCoordinator) redundantJoinSession(event OpenJoinEvent, kind OpenJoinAttemptKind) *openJoinSession {
	if event.Generation == 0 || event.SessionGeneration == 0 {
		return nil
	}
	session := coordinator.session(event.SessionGeneration)
	if session == nil || session.joinAttempt.Kind != kind || session.joinAttempt.Generation != event.Generation {
		return nil
	}
	return session
}

func (coordinator *OpenJoinCoordinator) hasPendingRedundantOpen() bool {
	for index := 0; index < coordinator.sessionCount; index++ {
		if coordinator.sessions[index].openAttempt.Kind == OpenJoinAttemptOpen {
			return true
		}
	}
	return false
}

func (coordinator *OpenJoinCoordinator) hasPendingRedundantJoin() bool {
	for index := 0; index < coordinator.sessionCount; index++ {
		if coordinator.sessions[index].joinAttempt.Kind == OpenJoinAttemptJoin ||
			coordinator.sessions[index].joinAttempt.Kind == OpenJoinAttemptPublish {
			return true
		}
	}
	return false
}

func (coordinator *OpenJoinCoordinator) hasPendingRedundantEstablishment() bool {
	if coordinator.hasPendingRedundantOpen() {
		return true
	}
	for index := 0; index < coordinator.sessionCount; index++ {
		session := &coordinator.sessions[index]
		if !session.openSucceeded {
			continue
		}
		if session.joinAttempt.Kind == OpenJoinAttemptJoin || session.joinAttempt.Kind == OpenJoinAttemptPublish {
			return true
		}
	}
	return false
}

func (coordinator *OpenJoinCoordinator) redundantOpenDeadlineReached(batch *openJoinActionBatch) {
	coordinator.openDeadlineGeneration = 0
	coordinator.cancelOpenRetry(batch)
	for index := 0; index < coordinator.sessionCount; index++ {
		session := &coordinator.sessions[index]
		if session.openAttempt.Kind != OpenJoinAttemptOpen {
			continue
		}
		session.openAttempt = OpenJoinAttempt{}
		session.openFinished = true
	}
	if coordinator.capability != (protocol.Capability{}) && coordinator.attachmentCount != 0 {
		coordinator.finishRedundantJoinRound(batch)
		return
	}
	coordinator.fail(OpenJoinFailureOpenDeadline, 0, batch)
}

func (coordinator *OpenJoinCoordinator) beginOpenAttempt(preferred uint64, batch *openJoinActionBatch) {
	if coordinator.parallelEstablishment() {
		coordinator.beginRedundantOpenAttempts(batch)
		return
	}
	if coordinator.openAttempts >= MaxOpenAttempts {
		coordinator.fail(OpenJoinFailureRetryLimit, 0, batch)
		return
	}
	sessionGeneration := coordinator.selectReadySession(preferred, 0)
	if sessionGeneration == 0 {
		coordinator.state = OpenJoinWaitingOpenSession
		return
	}
	generationCount := uint64(1)
	armRetry := coordinator.openAttempts+1 < MaxOpenAttempts
	if armRetry {
		generationCount++
	}
	generations, ok := coordinator.allocateGenerationBatch(generationCount)
	if !ok {
		coordinator.fail(OpenJoinFailureRetryLimit, 0, batch)
		return
	}
	generation := generations[0]
	coordinator.openAttempts++
	coordinator.state = OpenJoinOpening
	coordinator.attempt = OpenJoinAttempt{
		Kind:              OpenJoinAttemptOpen,
		Generation:        generation,
		SessionGeneration: sessionGeneration,
	}
	batch.add(OpenJoinAction{
		Kind:              OpenJoinActionSendOpen,
		Generation:        generation,
		SessionGeneration: sessionGeneration,
		Open: protocol.Open{
			FlowID:        coordinator.flowID,
			OpenToken:     coordinator.openToken,
			DeliveryMode:  coordinator.spec.DeliveryMode,
			PathSelection: coordinator.spec.PathSelection,
			Constraints:   coordinator.spec.Constraints,
			Target:        coordinator.spec.Target,
		},
	})
	if armRetry {
		coordinator.openRetryGeneration = generations[1]
		batch.add(OpenJoinAction{Kind: OpenJoinActionArmOpenRetry, Generation: generations[1], Duration: OpenRetryInterval})
	}
}

func (coordinator *OpenJoinCoordinator) startJoinCycle(preferred uint64, required bool, batch *openJoinActionBatch) {
	if coordinator.parallelEstablishment() {
		coordinator.startRedundantJoinCycle(preferred, required, batch)
		return
	}
	if coordinator.attempt.Kind != OpenJoinAttemptNone || coordinator.attachmentCount >= MaxOpenJoinAttachments {
		return
	}
	if !required && coordinator.selectReadySession(preferred, 0) == 0 {
		coordinator.state = OpenJoinActive
		return
	}
	if coordinator.joinDeadlineGeneration == 0 {
		generation, ok := coordinator.allocateGeneration()
		if !ok {
			coordinator.fail(OpenJoinFailureRetryLimit, 0, batch)
			return
		}
		coordinator.joinDeadlineGeneration = generation
		coordinator.joinAttempts = 0
		batch.add(OpenJoinAction{Kind: OpenJoinActionArmJoinDeadline, Generation: generation, Duration: JoinDeadline})
	}
	coordinator.beginJoinAttempt(preferred, batch)
}

func (coordinator *OpenJoinCoordinator) beginJoinAttempt(preferred uint64, batch *openJoinActionBatch) {
	coordinator.beginJoinAttemptWithExclude(preferred, 0, batch)
}

func (coordinator *OpenJoinCoordinator) beginJoinAttemptExcluding(excluded uint64, batch *openJoinActionBatch) {
	coordinator.beginJoinAttemptWithExclude(0, excluded, batch)
}

func (coordinator *OpenJoinCoordinator) beginJoinAttemptWithExclude(preferred, excluded uint64, batch *openJoinActionBatch) {
	if coordinator.joinAttempts >= MaxJoinAttempts {
		if coordinator.attachmentCount == 0 {
			coordinator.fail(OpenJoinFailureRetryLimit, 0, batch)
		} else {
			coordinator.cancelJoinDeadline(batch)
			coordinator.state = OpenJoinActive
		}
		return
	}
	sessionGeneration := coordinator.selectReadySession(preferred, excluded)
	if sessionGeneration == 0 {
		coordinator.state = coordinator.joinWaitingState()
		return
	}
	generationCount := uint64(2)
	armRetry := coordinator.joinAttempts+1 < MaxJoinAttempts
	if armRetry {
		generationCount++
	}
	generations, ok := coordinator.allocateGenerationBatch(generationCount)
	if !ok {
		coordinator.fail(OpenJoinFailureRetryLimit, 0, batch)
		return
	}
	attemptGeneration := generations[0]
	attachmentGeneration := generations[1]
	attachment := flow.AttachmentKey{
		SessionGeneration:    sessionGeneration,
		AttachmentGeneration: attachmentGeneration,
	}
	coordinator.joinAttempts++
	coordinator.state = OpenJoinJoining
	coordinator.attempt = OpenJoinAttempt{
		Kind:              OpenJoinAttemptJoin,
		Generation:        attemptGeneration,
		SessionGeneration: sessionGeneration,
		Attachment:        attachment,
	}
	batch.add(OpenJoinAction{
		Kind:              OpenJoinActionReserveAttachment,
		Generation:        attemptGeneration,
		SessionGeneration: sessionGeneration,
		Attachment:        attachment,
	})
	batch.add(OpenJoinAction{
		Kind:              OpenJoinActionSendJoin,
		Generation:        attemptGeneration,
		SessionGeneration: sessionGeneration,
		Attachment:        attachment,
		Join: protocol.Join{
			FlowID:     coordinator.flowID,
			Capability: coordinator.capability,
		},
	})
	if armRetry {
		coordinator.joinRetryGeneration = generations[2]
		coordinator.joinRetrySession = sessionGeneration
		batch.add(OpenJoinAction{Kind: OpenJoinActionArmJoinRetry, Generation: generations[2], Duration: JoinRetryInterval})
	}
}

func (coordinator *OpenJoinCoordinator) cancelOpenDeadline(batch *openJoinActionBatch) {
	if coordinator.openDeadlineGeneration == 0 {
		return
	}
	batch.add(OpenJoinAction{Kind: OpenJoinActionCancelOpenDeadline, Generation: coordinator.openDeadlineGeneration})
	coordinator.openDeadlineGeneration = 0
}

func (coordinator *OpenJoinCoordinator) cancelOpenRetry(batch *openJoinActionBatch) {
	if coordinator.openRetryGeneration == 0 {
		return
	}
	batch.add(OpenJoinAction{Kind: OpenJoinActionCancelOpenRetry, Generation: coordinator.openRetryGeneration})
	coordinator.openRetryGeneration = 0
}

func (coordinator *OpenJoinCoordinator) cancelJoinDeadline(batch *openJoinActionBatch) {
	if coordinator.joinDeadlineGeneration == 0 {
		return
	}
	batch.add(OpenJoinAction{Kind: OpenJoinActionCancelJoinDeadline, Generation: coordinator.joinDeadlineGeneration})
	coordinator.joinDeadlineGeneration = 0
}

func (coordinator *OpenJoinCoordinator) cancelJoinRetry(batch *openJoinActionBatch) {
	if coordinator.joinRetryGeneration == 0 {
		return
	}
	batch.add(OpenJoinAction{Kind: OpenJoinActionCancelJoinRetry, Generation: coordinator.joinRetryGeneration})
	coordinator.joinRetryGeneration = 0
	coordinator.joinRetrySession = 0
}

func (coordinator *OpenJoinCoordinator) releaseAttemptAttachment(batch *openJoinActionBatch) {
	if coordinator.attempt.Attachment == (flow.AttachmentKey{}) {
		return
	}
	batch.add(OpenJoinAction{
		Kind:              OpenJoinActionReleaseAttachment,
		Generation:        coordinator.attempt.Generation,
		SessionGeneration: coordinator.attempt.SessionGeneration,
		Attachment:        coordinator.attempt.Attachment,
	})
}

func (coordinator *OpenJoinCoordinator) fail(reason OpenJoinFailure, result protocol.OpenResultCode, batch *openJoinActionBatch) {
	if coordinator.state == OpenJoinFailed {
		return
	}
	coordinator.cancelOpenDeadline(batch)
	coordinator.cancelOpenRetry(batch)
	coordinator.cancelJoinDeadline(batch)
	coordinator.cancelJoinRetry(batch)
	if coordinator.parallelEstablishment() {
		for index := 0; index < coordinator.sessionCount; index++ {
			session := &coordinator.sessions[index]
			if session.joinAttempt.Attachment != (flow.AttachmentKey{}) {
				batch.add(OpenJoinAction{
					Kind: OpenJoinActionReleaseAttachment, Generation: session.joinAttempt.Generation,
					SessionGeneration: session.generation, Attachment: session.joinAttempt.Attachment,
				})
			}
			session.openAttempt = OpenJoinAttempt{}
			session.joinAttempt = OpenJoinAttempt{}
		}
	} else {
		coordinator.releaseAttemptAttachment(batch)
	}
	coordinator.attempt = OpenJoinAttempt{}
	for coordinator.attachmentCount > 0 {
		coordinator.attachmentCount--
		attachment := coordinator.attachments[coordinator.attachmentCount]
		coordinator.attachments[coordinator.attachmentCount] = flow.AttachmentKey{}
		batch.add(OpenJoinAction{Kind: OpenJoinActionReleaseAttachment, Attachment: attachment, SessionGeneration: attachment.SessionGeneration})
	}
	if result == protocol.OpenSuccess {
		result = protocol.OpenInternalFailure
	}
	coordinator.state = OpenJoinFailed
	coordinator.failure = reason
	coordinator.openResult = result
	batch.add(OpenJoinAction{Kind: OpenJoinActionFailFlow, Failure: reason, OpenResult: result})
}

func (coordinator *OpenJoinCoordinator) validOpenResult(result protocol.OpenResult) bool {
	if result.FlowID != coordinator.flowID || result.Result > protocol.OpenUnsupportedPolicy {
		return false
	}
	if result.Result != protocol.OpenSuccess {
		return result.Capability == (protocol.Capability{}) && result.DeliveryMode == 0 &&
			result.PathSelection == 0 && result.Constraints == (protocol.DeliveryConstraints{}) && !result.ImplicitAttachment
	}
	return result.Capability != (protocol.Capability{}) &&
		result.DeliveryMode == coordinator.spec.DeliveryMode &&
		result.PathSelection == coordinator.spec.PathSelection &&
		result.Constraints == coordinator.spec.Constraints
}

func (coordinator *OpenJoinCoordinator) validJoinResult(result protocol.JoinResult) bool {
	return result.FlowID == coordinator.flowID && (result.Result == protocol.JoinSuccess || result.Result == protocol.JoinFailure)
}

func (coordinator *OpenJoinCoordinator) parallelEstablishment() bool {
	return !coordinator.serialFallback && (coordinator.spec.DeliveryMode == protocol.DeliveryRedundant ||
		coordinator.spec.DeliveryMode == protocol.DeliveryAdaptive)
}

func (coordinator *OpenJoinCoordinator) session(generation uint64) *openJoinSession {
	for index := 0; index < coordinator.sessionCount; index++ {
		if coordinator.sessions[index].generation == generation {
			return &coordinator.sessions[index]
		}
	}
	return nil
}

func (coordinator *OpenJoinCoordinator) hasReadySession(generation uint64) bool {
	for index := 0; index < coordinator.sessionCount; index++ {
		if coordinator.sessions[index].generation == generation {
			return true
		}
	}
	return false
}

func (coordinator *OpenJoinCoordinator) removeReadySession(generation uint64) bool {
	for index := 0; index < coordinator.sessionCount; index++ {
		if coordinator.sessions[index].generation != generation {
			continue
		}
		copy(coordinator.sessions[index:], coordinator.sessions[index+1:coordinator.sessionCount])
		coordinator.sessionCount--
		coordinator.sessions[coordinator.sessionCount] = openJoinSession{}
		return true
	}
	return false
}

func (coordinator *OpenJoinCoordinator) selectReadySession(preferred, excluded uint64) uint64 {
	if preferred != 0 && preferred != excluded && coordinator.hasReadySession(preferred) && !coordinator.hasAttachmentForSession(preferred) {
		return preferred
	}
	var selected *openJoinSession
	for index := 0; index < coordinator.sessionCount; index++ {
		session := &coordinator.sessions[index]
		if session.generation == excluded || coordinator.hasAttachmentForSession(session.generation) {
			continue
		}
		if selected == nil || betterOpenJoinSession(*session, *selected) {
			selected = session
		}
	}
	if selected == nil {
		return 0
	}
	return selected.generation
}

func betterOpenJoinSession(candidate, current openJoinSession) bool {
	if candidate.stall != current.stall {
		return candidate.stall < current.stall
	}
	if candidate.rtt == 0 || current.rtt == 0 {
		return candidate.rtt != 0 || current.rtt == 0 && candidate.generation < current.generation
	}
	return candidate.rtt < current.rtt ||
		candidate.rtt == current.rtt && candidate.generation < current.generation
}

func (coordinator *OpenJoinCoordinator) hasAttachmentForSession(generation uint64) bool {
	for index := 0; index < coordinator.attachmentCount; index++ {
		if coordinator.attachments[index].SessionGeneration == generation {
			return true
		}
	}
	return false
}

func (coordinator *OpenJoinCoordinator) addAttachment(attachment flow.AttachmentKey) bool {
	if attachment.SessionGeneration == 0 || attachment.AttachmentGeneration == 0 ||
		coordinator.attachmentCount >= len(coordinator.attachments) || coordinator.hasAttachmentForSession(attachment.SessionGeneration) {
		return false
	}
	coordinator.attachments[coordinator.attachmentCount] = attachment
	coordinator.attachmentCount++
	return true
}

func (coordinator *OpenJoinCoordinator) removeAttachmentForSession(generation uint64) flow.AttachmentKey {
	for index := 0; index < coordinator.attachmentCount; index++ {
		if coordinator.attachments[index].SessionGeneration != generation {
			continue
		}
		removed := coordinator.attachments[index]
		copy(coordinator.attachments[index:], coordinator.attachments[index+1:coordinator.attachmentCount])
		coordinator.attachmentCount--
		coordinator.attachments[coordinator.attachmentCount] = flow.AttachmentKey{}
		return removed
	}
	return flow.AttachmentKey{}
}

func (coordinator *OpenJoinCoordinator) joinWaitingState() OpenJoinState {
	if coordinator.attachmentCount > 0 {
		return OpenJoinActive
	}
	return OpenJoinWaitingJoinSession
}

func (coordinator *OpenJoinCoordinator) Snapshot() OpenJoinSnapshot {
	if coordinator == nil {
		return OpenJoinSnapshot{State: OpenJoinFailed}
	}
	snapshot := OpenJoinSnapshot{
		State:                  coordinator.state,
		Attempt:                coordinator.attempt,
		IdentityGeneration:     coordinator.identityGeneration,
		OpenDeadlineGeneration: coordinator.openDeadlineGeneration,
		OpenRetryGeneration:    coordinator.openRetryGeneration,
		JoinDeadlineGeneration: coordinator.joinDeadlineGeneration,
		JoinRetryGeneration:    coordinator.joinRetryGeneration,
		OpenAttempts:           coordinator.openAttempts,
		JoinAttempts:           coordinator.joinAttempts,
		ApplicationAccepted:    coordinator.applicationAccepted,
		Failure:                coordinator.failure,
		OpenResult:             coordinator.openResult,
	}
	if coordinator.parallelEstablishment() {
		for index := 0; index < coordinator.sessionCount; index++ {
			session := &coordinator.sessions[index]
			if session.joinAttempt.Kind != OpenJoinAttemptNone {
				snapshot.Attempt = session.joinAttempt
				break
			}
			if snapshot.Attempt.Kind == OpenJoinAttemptNone && session.openAttempt.Kind != OpenJoinAttemptNone {
				snapshot.Attempt = session.openAttempt
			}
		}
	}
	for index := 0; index < coordinator.sessionCount; index++ {
		snapshot.ReadySessions = append(snapshot.ReadySessions, coordinator.sessions[index].generation)
	}
	for index := 0; index < coordinator.attachmentCount; index++ {
		snapshot.Attachments = append(snapshot.Attachments, coordinator.attachments[index])
	}
	sort.Slice(snapshot.Attachments, func(i, j int) bool {
		if snapshot.Attachments[i].SessionGeneration != snapshot.Attachments[j].SessionGeneration {
			return snapshot.Attachments[i].SessionGeneration < snapshot.Attachments[j].SessionGeneration
		}
		return snapshot.Attachments[i].AttachmentGeneration < snapshot.Attachments[j].AttachmentGeneration
	})
	return snapshot
}

// PendingAttempt returns the control result currently pending for the specified transport session.
func (coordinator *OpenJoinCoordinator) PendingAttempt(sessionGeneration uint64, kind OpenJoinAttemptKind) (OpenJoinAttempt, bool) {
	if coordinator == nil || sessionGeneration == 0 || kind == OpenJoinAttemptNone {
		return OpenJoinAttempt{}, false
	}
	if !coordinator.parallelEstablishment() {
		if coordinator.attempt.Kind == kind && coordinator.attempt.SessionGeneration == sessionGeneration {
			return coordinator.attempt, true
		}
		return OpenJoinAttempt{}, false
	}
	session := coordinator.session(sessionGeneration)
	if session == nil {
		return OpenJoinAttempt{}, false
	}
	if kind == OpenJoinAttemptOpen && session.openAttempt.Kind == kind {
		return session.openAttempt, true
	}
	if kind == OpenJoinAttemptJoin && session.joinAttempt.Kind == kind {
		return session.joinAttempt, true
	}
	return OpenJoinAttempt{}, false
}

func (coordinator *OpenJoinCoordinator) allocateGeneration() (uint64, bool) {
	if coordinator.nextGeneration == ^uint64(0) {
		return 0, false
	}
	coordinator.nextGeneration++
	return coordinator.nextGeneration, true
}

func (coordinator *OpenJoinCoordinator) canAllocate(count uint64) bool {
	return count <= ^uint64(0)-coordinator.nextGeneration
}

func (coordinator *OpenJoinCoordinator) allocateGenerationBatch(count uint64) ([3]uint64, bool) {
	var generations [3]uint64
	if count == 0 || count > uint64(len(generations)) || count > ^uint64(0)-coordinator.nextGeneration {
		return generations, false
	}
	for index := uint64(0); index < count; index++ {
		coordinator.nextGeneration++
		generations[index] = coordinator.nextGeneration
	}
	return generations, true
}

func isZero(value []byte) bool {
	var aggregate byte
	for _, item := range value {
		aggregate |= item
	}
	return aggregate == 0
}
