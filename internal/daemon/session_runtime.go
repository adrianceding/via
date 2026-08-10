package daemon

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/transport"
)

var errSessionRuntimeClosed = errors.New("daemon: session runtime is closed")

type sessionRuntimeRequestState uint8

const (
	sessionRuntimeQueued sessionRuntimeRequestState = iota + 1
	sessionRuntimeSelected
	sessionRuntimeTerminal
	dataCapacityWindowBytes = 64 << 10
	dataCapacityWindowTime  = 250 * time.Millisecond
	// snapshotNotifyInterval bounds diagnostic observer notifications. Data-plane
	// snapshot reads are unaffected; only notifications to the status observer are
	// throttled, and terminal closure always notifies.
	snapshotNotifyInterval = 100 * time.Millisecond
)

type sessionRuntimeRequest struct {
	id                uint64
	ctx               context.Context
	class             transport.FrameClass
	flowID            protocol.FlowID
	itemID            uint64
	attemptGeneration uint64
	dataPayload       uint64
	encoded           []byte
	result            chan error
	completedAt       time.Time
	capacityEligible  bool
	state             sessionRuntimeRequestState
}

type sessionRuntimeSnapshot struct {
	QueuedFrames       uint32
	QueuedEncodedBytes uint64
	InFlightFrames     uint32
	InFlightEncoded    uint64
	QueuedDataPayload  uint64
	InFlightData       uint64
	ScheduledData      uint64
	WrittenData        uint64
	DataQueueFrames    uint32
	ActiveDataFlows    int
	Closed             bool
	Quality            policy.QualitySnapshot
	EligibleAckedData  uint64
}

type sessionRuntime struct {
	ctx         context.Context
	cancel      context.CancelFunc
	connection  transport.Connection
	queueLimits transport.QueueLimits
	onFailure   func(error)
	now         func() time.Time

	mu                sync.Mutex
	nextID            uint64
	closed            bool
	closeErr          error
	controlQueue      []*sessionRuntimeRequest
	dataScheduler     *policy.Scheduler
	requests          map[uint64]*sessionRuntimeRequest
	selected          *sessionRuntimeRequest
	queuedFrames      uint32
	queuedBytes       uint64
	inFlightFrames    uint32
	inFlightBytes     uint64
	queuedData        uint64
	inFlightData      uint64
	scheduledData     uint64
	writtenData       uint64
	quality           *policy.Quality
	dataWindowStart   time.Time
	dataWindowLastACK time.Time
	dataWindowBytes   uint64
	eligibleAckedData uint64
	lastNotify        time.Time
	wake              chan struct{}
	space             chan struct{}
	done              chan struct{}
	workerDone        chan struct{}
	onSnapshot        func(sessionRuntimeSnapshot)
	closeOnce         sync.Once
}

func newSessionRuntime(ctx context.Context, connection transport.Connection, onFailure func(error)) (*sessionRuntime, error) {
	return newSessionRuntimeWithClock(ctx, connection, onFailure, time.Now)
}

func sessionQueueLimits(connection transport.Connection) (transport.QueueLimits, error) {
	if connection == nil {
		return transport.QueueLimits{}, ErrWireProtocol
	}
	limits := connection.QueueLimits()
	if err := limits.Validate(connection.Capabilities()); err != nil {
		return transport.QueueLimits{}, ErrWireProtocol
	}
	return limits, nil
}

func newSessionRuntimeWithClock(ctx context.Context, connection transport.Connection, onFailure func(error), now func() time.Time) (*sessionRuntime, error) {
	if ctx == nil || connection == nil {
		return nil, ErrWireProtocol
	}
	if now == nil {
		return nil, ErrWireProtocol
	}
	limits, err := sessionQueueLimits(connection)
	if err != nil {
		return nil, err
	}
	runtimeCtx, cancel := context.WithCancel(ctx)
	scheduler, err := policy.NewScheduler(policy.MaxSchedulerFlows)
	if err != nil {
		cancel()
		return nil, err
	}
	runtime := &sessionRuntime{
		ctx: runtimeCtx, cancel: cancel, connection: connection, queueLimits: limits, onFailure: onFailure, now: now,
		quality:       policy.NewQualityWithClock(now),
		dataScheduler: scheduler, requests: make(map[uint64]*sessionRuntimeRequest, int(limits.MaxFrames)),
		wake: make(chan struct{}, 1), space: make(chan struct{}, 1), done: make(chan struct{}), workerDone: make(chan struct{}),
	}
	go runtime.run()
	return runtime, nil
}

func (runtime *sessionRuntime) submit(ctx context.Context, request transport.WriteRequest, flowID protocol.FlowID, itemID, attemptGeneration uint64, dataPayload uint64) error {
	_, err := runtime.submitWithCompletion(ctx, request, flowID, itemID, attemptGeneration, dataPayload)
	return err
}

func (runtime *sessionRuntime) submitWithCompletion(ctx context.Context, request transport.WriteRequest, flowID protocol.FlowID, itemID, attemptGeneration uint64, dataPayload uint64) (time.Time, error) {
	if ctx == nil {
		return time.Time{}, ErrWireProtocol
	}
	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	queued, err := runtime.admit(sendCtx, request, flowID, itemID, attemptGeneration, dataPayload)
	if err != nil {
		return time.Time{}, err
	}
	return runtime.waitCompletion(sendCtx, queued)
}

func (runtime *sessionRuntime) setSnapshotObserver(observer func(sessionRuntimeSnapshot)) {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	runtime.onSnapshot = observer
	snapshot := runtime.snapshotLocked()
	runtime.mu.Unlock()
	if observer != nil {
		observer(snapshot)
	}
}

func (runtime *sessionRuntime) notifySnapshot(snapshot sessionRuntimeSnapshot) {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	observer := runtime.onSnapshot
	runtime.mu.Unlock()
	if observer != nil {
		observer(snapshot)
	}
}

// snapshotIfNotifyDueLocked reports whether the diagnostic observer is due for a
// snapshot and, when due, returns a fresh snapshot. The caller must hold the
// lock. Terminal closure bypasses throttling so the final state is always
// observed.
func (runtime *sessionRuntime) snapshotIfNotifyDueLocked() (sessionRuntimeSnapshot, bool) {
	if runtime.closed {
		return runtime.snapshotLocked(), true
	}
	if runtime.onSnapshot == nil {
		return sessionRuntimeSnapshot{}, false
	}
	now := runtime.now()
	if !runtime.lastNotify.IsZero() && now.Sub(runtime.lastNotify) < snapshotNotifyInterval {
		return sessionRuntimeSnapshot{}, false
	}
	runtime.lastNotify = now
	return runtime.snapshotLocked(), true
}

func (runtime *sessionRuntime) admit(ctx context.Context, request transport.WriteRequest, flowID protocol.FlowID, itemID, attemptGeneration uint64, dataPayload uint64) (*sessionRuntimeRequest, error) {
	if runtime == nil || ctx == nil {
		return nil, ErrWireProtocol
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	connection := runtime.connection
	runtime.mu.Unlock()
	if connection == nil {
		return nil, ErrWireProtocol
	}
	if err := request.Validate(connection.Capabilities()); err != nil {
		return nil, err
	}
	if request.Class == transport.FrameData {
		if flowID == (protocol.FlowID{}) || itemID == 0 || attemptGeneration == 0 || dataPayload == 0 {
			return nil, ErrWireProtocol
		}
	} else if request.Class != transport.FrameControl || flowID != (protocol.FlowID{}) || dataPayload != 0 {
		return nil, ErrWireProtocol
	}
	runtime.mu.Lock()
	if runtime.closed {
		err := runtime.closeErrorLocked()
		runtime.mu.Unlock()
		return nil, err
	}
	if !runtime.fitsLocked(request.Class, uint64(len(request.Encoded))) {
		runtime.mu.Unlock()
		return nil, transport.ErrQueueFull
	}
	if runtime.nextID == math.MaxUint64 {
		runtime.mu.Unlock()
		return nil, ErrWireProtocol
	}
	runtime.nextID++
	// Encoded is borrowed as immutable data until completion. Complete DATA
	// frames may be shared by concurrent attachment attempts; the worker only
	// reads the bytes and never assumes unique ownership.
	queued := &sessionRuntimeRequest{
		id: runtime.nextID, ctx: ctx, class: request.Class, flowID: flowID, itemID: itemID,
		attemptGeneration: attemptGeneration, dataPayload: dataPayload,
		encoded: request.Encoded, result: make(chan error, 1),
		state: sessionRuntimeQueued,
	}
	if request.Class == transport.FrameData {
		err := runtime.dataScheduler.Enqueue(policy.Descriptor{
			Kind: policy.DescriptorData, FlowID: flowID, ItemID: queued.id,
			Generation: 1, Bytes: dataPayload,
		})
		if err != nil {
			runtime.mu.Unlock()
			return nil, err
		}
		runtime.queuedData += dataPayload
	} else {
		runtime.controlQueue = append(runtime.controlQueue, queued)
	}
	runtime.queuedFrames++
	runtime.queuedBytes += uint64(len(queued.encoded))
	runtime.updateQualityLoadLocked()
	runtime.requests[queued.id] = queued
	runtime.signalLocked()
	snapshot, notify := runtime.snapshotIfNotifyDueLocked()
	runtime.mu.Unlock()
	if notify {
		runtime.notifySnapshot(snapshot)
	}
	return queued, nil
}

func (runtime *sessionRuntime) admitControl(ctx context.Context, request transport.WriteRequest) (*sessionRuntimeRequest, error) {
	for {
		queued, err := runtime.admit(ctx, request, protocol.FlowID{}, 0, 0, 0)
		if !errors.Is(err, transport.ErrQueueFull) {
			return queued, err
		}
		select {
		case <-runtime.space:
		case <-runtime.done:
			runtime.mu.Lock()
			closedErr := runtime.closeErrorLocked()
			runtime.mu.Unlock()
			return nil, closedErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (runtime *sessionRuntime) wait(ctx context.Context, request *sessionRuntimeRequest) error {
	_, err := runtime.waitCompletion(ctx, request)
	return err
}

func (runtime *sessionRuntime) waitCompletion(ctx context.Context, request *sessionRuntimeRequest) (time.Time, error) {
	if request == nil {
		return time.Time{}, ErrWireProtocol
	}
	for {
		select {
		case err := <-request.result:
			return request.completedAt, err
		case <-ctx.Done():
			if runtime.cancelQueued(request, ctx.Err()) {
				return time.Time{}, ctx.Err()
			}
			err := <-request.result
			return request.completedAt, err
		}
	}
}

func (runtime *sessionRuntime) cancelQueued(request *sessionRuntimeRequest, err error) bool {
	runtime.mu.Lock()
	if request.state != sessionRuntimeQueued {
		runtime.mu.Unlock()
		return false
	}
	if request.class == transport.FrameControl {
		for index, current := range runtime.controlQueue {
			if current != request {
				continue
			}
			copy(runtime.controlQueue[index:], runtime.controlQueue[index+1:])
			runtime.controlQueue = runtime.controlQueue[:len(runtime.controlQueue)-1]
			break
		}
	} else {
		runtime.dataScheduler.Cancel(request.flowID, request.id, 1)
		runtime.queuedData -= minUint64(runtime.queuedData, request.dataPayload)
	}
	runtime.queuedFrames--
	runtime.queuedBytes -= minUint64(runtime.queuedBytes, uint64(len(request.encoded)))
	runtime.updateQualityLoadLocked()
	runtime.finishLocked(request, err)
	runtime.signalSpaceLocked()
	runtime.signalLocked()
	snapshot, notify := runtime.snapshotIfNotifyDueLocked()
	runtime.mu.Unlock()
	if notify {
		runtime.notifySnapshot(snapshot)
	}
	return true
}

func (runtime *sessionRuntime) run() {
	defer close(runtime.workerDone)
	for {
		request := runtime.selectNext()
		if request == nil {
			select {
			case <-runtime.wake:
			case <-runtime.done:
				return
			case <-runtime.ctx.Done():
				runtime.close(transport.ErrClosed)
				return
			}
			continue
		}
		writeCtx, cancel := context.WithTimeout(runtime.ctx, sendTimeout)
		connection := runtime.currentConnection()
		if connection == nil {
			err := ErrWireProtocol
			cancel()
			runtime.completeWrite(request, err)
			return
		}
		err := connection.WriteFrame(writeCtx, transport.WriteRequest{Class: request.class, Encoded: request.encoded})
		cancel()
		runtime.completeWrite(request, err)
		if err != nil {
			return
		}
	}
}

func (runtime *sessionRuntime) currentConnection() transport.Connection {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.connection
}

func (runtime *sessionRuntime) setConnection(connection transport.Connection) error {
	if runtime == nil || connection == nil {
		return ErrWireProtocol
	}
	limits, err := sessionQueueLimits(connection)
	if err != nil {
		return err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return runtime.closeErrorLocked()
	}
	runtime.connection = connection
	runtime.queueLimits = limits
	return nil
}

func (runtime *sessionRuntime) selectNext() *sessionRuntimeRequest {
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return nil
	}
	var request *sessionRuntimeRequest
	if len(runtime.controlQueue) != 0 {
		request = runtime.controlQueue[0]
		copy(runtime.controlQueue, runtime.controlQueue[1:])
		runtime.controlQueue = runtime.controlQueue[:len(runtime.controlQueue)-1]
	} else {
		descriptor, ok := runtime.dataScheduler.Next()
		if !ok {
			runtime.mu.Unlock()
			return nil
		}
		request = runtime.requests[descriptor.ItemID]
		if request == nil || request.state != sessionRuntimeQueued {
			runtime.mu.Unlock()
			return nil
		}
		runtime.queuedData -= minUint64(runtime.queuedData, request.dataPayload)
		runtime.inFlightData += request.dataPayload
		runtime.scheduledData = saturatingUint64(runtime.scheduledData, request.dataPayload)
	}
	runtime.queuedFrames--
	runtime.queuedBytes -= minUint64(runtime.queuedBytes, uint64(len(request.encoded)))
	runtime.inFlightFrames++
	runtime.inFlightBytes += uint64(len(request.encoded))
	runtime.updateQualityLoadLocked()
	request.state = sessionRuntimeSelected
	runtime.selected = request
	snapshot, notify := runtime.snapshotIfNotifyDueLocked()
	runtime.mu.Unlock()
	if notify {
		runtime.notifySnapshot(snapshot)
	}
	return request
}

func (runtime *sessionRuntime) completeWrite(request *sessionRuntimeRequest, err error) {
	runtime.mu.Lock()
	if request.state != sessionRuntimeSelected || runtime.selected != request {
		runtime.mu.Unlock()
		return
	}
	runtime.selected = nil
	runtime.inFlightFrames--
	runtime.inFlightBytes -= minUint64(runtime.inFlightBytes, uint64(len(request.encoded)))
	if request.class == transport.FrameData {
		request.capacityEligible = runtime.queuedData != 0
		runtime.inFlightData -= minUint64(runtime.inFlightData, request.dataPayload)
		if err == nil {
			runtime.writtenData = saturatingUint64(runtime.writtenData, request.dataPayload)
		}
	}
	if err == nil {
		request.completedAt = runtime.now()
	}
	runtime.updateQualityLoadLocked()
	runtime.finishLocked(request, err)
	runtime.signalSpaceLocked()
	snapshot, notify := runtime.snapshotIfNotifyDueLocked()
	runtime.mu.Unlock()
	if notify {
		runtime.notifySnapshot(snapshot)
	}
	if err != nil {
		runtime.close(err)
		if runtime.onFailure != nil {
			runtime.onFailure(err)
		}
	}
}

func (runtime *sessionRuntime) observeProbe(rtt time.Duration) {
	if runtime == nil || rtt <= 0 {
		return
	}
	runtime.mu.Lock()
	_ = runtime.quality.ObserveProbe(rtt)
	snapshot, notify := runtime.snapshotIfNotifyDueLocked()
	runtime.mu.Unlock()
	if notify {
		runtime.notifySnapshot(snapshot)
	}
}

func (runtime *sessionRuntime) setStallPenalty(penalty time.Duration) {
	if runtime == nil || penalty < 0 {
		return
	}
	runtime.mu.Lock()
	_ = runtime.quality.SetStallPenalty(penalty)
	snapshot, notify := runtime.snapshotIfNotifyDueLocked()
	runtime.mu.Unlock()
	if notify {
		runtime.notifySnapshot(snapshot)
	}
}

func (runtime *sessionRuntime) observeDataCredit(acknowledgedBytes uint64, writeCompletedAt, acknowledgedAt time.Time, capacityEligible bool) {
	if runtime == nil || acknowledgedBytes == 0 || writeCompletedAt.IsZero() || acknowledgedAt.IsZero() || acknowledgedAt.Before(writeCompletedAt) {
		return
	}
	runtime.mu.Lock()
	runtime.eligibleAckedData = saturatingUint64(runtime.eligibleAckedData, acknowledgedBytes)
	if !capacityEligible {
		runtime.dataWindowStart = time.Time{}
		runtime.dataWindowLastACK = time.Time{}
		runtime.dataWindowBytes = 0
		snapshot, notify := runtime.snapshotIfNotifyDueLocked()
		runtime.mu.Unlock()
		if notify {
			runtime.notifySnapshot(snapshot)
		}
		return
	}
	if !runtime.dataWindowLastACK.IsZero() && runtime.dataWindowLastACK.Before(writeCompletedAt) &&
		writeCompletedAt.Sub(runtime.dataWindowLastACK) >= dataCapacityWindowTime {
		runtime.dataWindowStart = time.Time{}
		runtime.dataWindowLastACK = time.Time{}
		runtime.dataWindowBytes = 0
	}
	if runtime.dataWindowLastACK.IsZero() {
		runtime.dataWindowStart = acknowledgedAt
		runtime.dataWindowLastACK = acknowledgedAt
		snapshot, notify := runtime.snapshotIfNotifyDueLocked()
		runtime.mu.Unlock()
		if notify {
			runtime.notifySnapshot(snapshot)
		}
		return
	}
	if acknowledgedAt.Before(runtime.dataWindowLastACK) ||
		acknowledgedAt.Equal(runtime.dataWindowLastACK) && runtime.dataWindowBytes == 0 {
		snapshot, notify := runtime.snapshotIfNotifyDueLocked()
		runtime.mu.Unlock()
		if notify {
			runtime.notifySnapshot(snapshot)
		}
		return
	}
	if acknowledgedAt.After(runtime.dataWindowLastACK) {
		runtime.dataWindowLastACK = acknowledgedAt
	}
	runtime.dataWindowBytes = saturatingUint64(runtime.dataWindowBytes, acknowledgedBytes)
	if runtime.dataWindowBytes < dataCapacityWindowBytes {
		snapshot, notify := runtime.snapshotIfNotifyDueLocked()
		runtime.mu.Unlock()
		if notify {
			runtime.notifySnapshot(snapshot)
		}
		return
	}
	interval := runtime.dataWindowLastACK.Sub(runtime.dataWindowStart)
	if interval > 0 {
		_ = runtime.quality.ObserveData(0, runtime.dataWindowBytes, interval)
	}
	runtime.dataWindowStart = time.Time{}
	runtime.dataWindowLastACK = time.Time{}
	runtime.dataWindowBytes = 0
	snapshot, notify := runtime.snapshotIfNotifyDueLocked()
	runtime.mu.Unlock()
	if notify {
		runtime.notifySnapshot(snapshot)
	}
}

func (runtime *sessionRuntime) close(err error) {
	if runtime == nil {
		return
	}
	if err == nil {
		err = transport.ErrClosed
	}
	runtime.closeOnce.Do(func() {
		runtime.mu.Lock()
		runtime.closed = true
		runtime.closeErr = err
		for _, request := range runtime.requests {
			if request.state == sessionRuntimeQueued || request.state == sessionRuntimeSelected {
				runtime.finishLocked(request, err)
			}
		}
		runtime.controlQueue = nil
		runtime.dataScheduler = mustNewRuntimeScheduler()
		runtime.requests = make(map[uint64]*sessionRuntimeRequest, int(runtime.queueLimits.MaxFrames))
		runtime.selected = nil
		runtime.queuedFrames = 0
		runtime.queuedBytes = 0
		runtime.inFlightFrames = 0
		runtime.inFlightBytes = 0
		runtime.queuedData = 0
		runtime.inFlightData = 0
		runtime.updateQualityLoadLocked()
		runtime.signalSpaceLocked()
		close(runtime.done)
		runtime.cancel()
		runtime.mu.Unlock()
		runtime.notifySnapshot(runtime.snapshot())
	})
}

func (runtime *sessionRuntime) finishLocked(request *sessionRuntimeRequest, err error) {
	if request.state == sessionRuntimeTerminal {
		return
	}
	request.state = sessionRuntimeTerminal
	delete(runtime.requests, request.id)
	request.result <- err
}

func (runtime *sessionRuntime) closeErrorLocked() error {
	if runtime.closeErr != nil {
		return runtime.closeErr
	}
	return errSessionRuntimeClosed
}

func (runtime *sessionRuntime) fitsLocked(class transport.FrameClass, encodedBytes uint64) bool {
	limits := runtime.queueLimits
	acceptedFrames := runtime.queuedFrames + runtime.inFlightFrames
	acceptedBytes := runtime.queuedBytes + runtime.inFlightBytes
	if acceptedFrames >= limits.MaxFrames || acceptedBytes > limits.MaxBytes || encodedBytes > limits.MaxBytes-acceptedBytes {
		return false
	}
	if class == transport.FrameData {
		dataFrameLimit := limits.MaxFrames - limits.ReservedControlFrames
		dataByteLimit := limits.MaxBytes - limits.ReservedControlBytes
		return acceptedFrames < dataFrameLimit && acceptedBytes <= dataByteLimit &&
			encodedBytes <= dataByteLimit-acceptedBytes
	}
	return class == transport.FrameControl
}

func (runtime *sessionRuntime) signalLocked() {
	select {
	case runtime.wake <- struct{}{}:
	default:
	}
}

func (runtime *sessionRuntime) signalSpaceLocked() {
	select {
	case runtime.space <- struct{}{}:
	default:
	}
}

func (runtime *sessionRuntime) updateQualityLoadLocked() {
	runtime.quality.SetLoad(runtime.queuedData, runtime.inFlightData)
}

func (runtime *sessionRuntime) snapshot() sessionRuntimeSnapshot {
	if runtime == nil {
		return sessionRuntimeSnapshot{}
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.snapshotLocked()
}

func (runtime *sessionRuntime) snapshotLocked() sessionRuntimeSnapshot {
	dataScheduler := runtime.dataScheduler.Snapshot()
	return sessionRuntimeSnapshot{
		QueuedFrames: runtime.queuedFrames, QueuedEncodedBytes: runtime.queuedBytes,
		InFlightFrames: runtime.inFlightFrames, InFlightEncoded: runtime.inFlightBytes,
		QueuedDataPayload: runtime.queuedData, InFlightData: runtime.inFlightData,
		ScheduledData: runtime.scheduledData, WrittenData: runtime.writtenData,
		DataQueueFrames: uint32(dataScheduler.Descriptors), ActiveDataFlows: dataScheduler.Flows, Closed: runtime.closed,
		Quality: runtime.quality.SnapshotAt(runtime.now()), EligibleAckedData: runtime.eligibleAckedData,
	}
}

func mustNewRuntimeScheduler() *policy.Scheduler {
	scheduler, _ := policy.NewScheduler(policy.MaxSchedulerFlows)
	return scheduler
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}

func saturatingUint64(current, delta uint64) uint64 {
	if math.MaxUint64-current < delta {
		return math.MaxUint64
	}
	return current + delta
}
