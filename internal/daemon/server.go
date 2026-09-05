package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adrianceding/via/internal/auth"
	"github.com/adrianceding/via/internal/config"
	"github.com/adrianceding/via/internal/flow"
	"github.com/adrianceding/via/internal/policy"
	"github.com/adrianceding/via/internal/protocol"
	servercore "github.com/adrianceding/via/internal/server"
	statusapi "github.com/adrianceding/via/internal/status"
	"github.com/adrianceding/via/internal/transport"
)

type serverDaemon struct {
	configuration config.Server
	runtimeCtx    context.Context
	cancelRuntime context.CancelFunc
	dialCtx       context.Context
	cancelDials   context.CancelFunc
	listener      transport.Listener
	draining      atomic.Bool
	openCommitMu  sync.Mutex

	registry      *servercore.Registry
	limiter       *servercore.RateLimiter
	challenges    *auth.ChallengeGenerator
	verifier      *auth.Verifier
	principalKeys map[string]auth.Key
	targets       *servercore.TargetDialExecutor

	nextSession     atomic.Uint64
	nextAttachment  atomic.Uint64
	sessionSlots    chan struct{}
	authSlots       chan struct{}
	recoveringSlots chan struct{}

	sessionsMu           sync.RWMutex
	sessions             map[uint64]*wireSession
	pathGroups           map[serverPathGroupKey]*wirePathGroup
	pathGroupGenerations map[uint64]*wirePathGroup
	laneGroups           map[uint64]*wirePathGroup
	principalSessions    map[string]int
	flowsMu              sync.RWMutex
	flows                map[servercore.FlowKey]*serverFlow
	waiters              map[servercore.FlowKey]*flowWaiter
	flowChanged          chan struct{}
	wg                   sync.WaitGroup
	timerCallbacks       sync.WaitGroup
	workersMu            sync.Mutex
	workersCond          *sync.Cond
	workers              int
	workerLimit          int
	workersClosed        bool
	openWorkersMu        sync.Mutex
	openWorkersCond      *sync.Cond
	openWorkers          int
	openWorkerLimit      int
	openWorkersClosed    bool
	openWaitersMu        sync.Mutex
	openWaitersCond      *sync.Cond
	openWaiters          map[openWaiterKey]struct{}
	openWaiterLimit      int
	openWaitersClosed    bool

	statusRepository *statusapi.Repository
	statusObserver   *runtimeStatus
	statusServer     *http.Server
	statusListener   net.Listener
}

type flowWaiter struct {
	done  chan struct{}
	users int
}

type openWaiterKey struct {
	sessionGeneration uint64
	flowID            protocol.FlowID
}

type serverPathGroupKey struct {
	principal string
	id        protocol.PathGroupID
}

func RunServer(ctx context.Context, configuration config.Server) error {
	if ctx == nil {
		return ErrWireProtocol
	}
	daemon, err := newServerDaemon(configuration)
	if err != nil {
		return err
	}
	return daemon.run(ctx)
}

func newServerDaemon(configuration config.Server) (*serverDaemon, error) {
	transportRegistry, err := newTransportRegistry(configuration.Deadlines.FrameTotal, configuration.Deadlines.FrameNoProgress, configuration.Transport.WriteBufferBytes)
	if err != nil {
		return nil, err
	}
	factory, err := lookupTransportFactory(transportRegistry, configuration.Transport.Type)
	if err != nil {
		return nil, err
	}
	registry, err := servercore.NewRegistry(servercore.RegistryLimits{
		Flows: int(configuration.Limits.Flows), FlowsPerPrincipal: int(configuration.Limits.PerPrincipalFlows),
		OpeningFlows: int(configuration.Limits.OpeningFlows), TargetDials: int(configuration.Limits.TargetDials),
		Tombstones: int(configuration.Limits.Tombstones), TombstonesPerPrincipal: int(configuration.Limits.TombstonesPerPrincipal),
		FlowSendWindowBytes: configuration.Limits.FlowSendWindowBytes, FlowReceiveWindowBytes: configuration.Limits.FlowReceiveWindowBytes,
	}, rand.Reader, time.Now)
	if err != nil {
		return nil, err
	}
	limiter, err := servercore.NewRateLimiter(int(configuration.Limits.RateLimitKeys), servercore.OpenRateLimits{
		PrincipalRatePerMinute: int(configuration.Limits.OpenRatePerMinutePerPrincipal),
		PrincipalBurst:         int(configuration.Limits.OpenBurstPerPrincipal),
		GlobalRatePerMinute:    int(configuration.Limits.OpenRatePerMinuteGlobal),
		GlobalBurst:            int(configuration.Limits.OpenBurstGlobal),
	})
	if err != nil {
		return nil, err
	}
	challenges, err := auth.NewChallengeGenerator(rand.Reader)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]auth.Key, len(configuration.Principals))
	for _, principal := range configuration.Principals {
		keys[principal.ID] = auth.Key(principal.PSK)
	}
	var dummy auth.Key
	if _, err := rand.Read(dummy[:]); err != nil {
		return nil, err
	}
	verifier := auth.NewVerifier(keys, dummy)
	targets, err := servercore.NewTargetDialExecutor(net.DefaultResolver, &net.Dialer{})
	if err != nil {
		return nil, err
	}
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	dialCtx, cancelDials := context.WithCancel(runtimeCtx)
	statusRepository, err := statusapi.NewRepositoryForRole(statusapi.Limits{
		Interfaces: 1, Sessions: int(configuration.Limits.Sessions), Flows: int(configuration.Limits.Flows),
	}, statusapi.RoleServer)
	if err != nil {
		cancelDials()
		cancelRuntime()
		return nil, err
	}
	statusObserver, err := newRuntimeStatus(statusRepository, int(configuration.Limits.Flows), configuration.RequiredBytes)
	if err != nil {
		cancelDials()
		cancelRuntime()
		return nil, err
	}
	listener, err := factory.NewListener(transport.ListenOptions{
		LocalEndpoint: configuration.Transport.Listen, QueueLimits: transport.QueueLimits{
			MaxFrames: uint32(configuration.Transport.OutputQueueFrames), MaxBytes: configuration.Transport.OutputQueueBytes,
			ReservedControlFrames: uint32(configuration.Transport.ControlReserveFrames), ReservedControlBytes: configuration.Transport.ControlReserveBytes,
		},
	})
	if err != nil {
		cancelDials()
		cancelRuntime()
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = listener.Close()
		}
	}()
	daemon := &serverDaemon{
		configuration: configuration, runtimeCtx: runtimeCtx, cancelRuntime: cancelRuntime,
		dialCtx: dialCtx, cancelDials: cancelDials, listener: listener,
		registry: registry, limiter: limiter, challenges: challenges, verifier: verifier, principalKeys: keys, targets: targets,
		sessionSlots:    make(chan struct{}, int(configuration.Limits.Sessions)),
		authSlots:       make(chan struct{}, int(configuration.Limits.AuthInProgress)),
		recoveringSlots: make(chan struct{}, int(configuration.Limits.RecoveringFlows)),
		sessions:        make(map[uint64]*wireSession), pathGroups: make(map[serverPathGroupKey]*wirePathGroup),
		pathGroupGenerations: make(map[uint64]*wirePathGroup), laneGroups: make(map[uint64]*wirePathGroup),
		principalSessions: make(map[string]int),
		flows:             make(map[servercore.FlowKey]*serverFlow), waiters: make(map[servercore.FlowKey]*flowWaiter),
		flowChanged:      make(chan struct{}, 1),
		workerLimit:      int(configuration.Limits.Flows) * (servercore.MaxRelayPendingSends + 8),
		openWorkerLimit:  int(configuration.Limits.OpeningFlows),
		openWaiters:      make(map[openWaiterKey]struct{}, int(configuration.Limits.Sessions)),
		openWaiterLimit:  int(configuration.Limits.Sessions),
		statusRepository: statusRepository, statusObserver: statusObserver,
	}
	daemon.workersCond = sync.NewCond(&daemon.workersMu)
	daemon.openWorkersCond = sync.NewCond(&daemon.openWorkersMu)
	daemon.openWaitersCond = sync.NewCond(&daemon.openWaitersMu)
	statusObserver.setResourceLimits(
		configuration.Limits.Sessions, configuration.Limits.Flows, 0,
		configuration.Limits.TargetDials, configuration.Limits.Tombstones,
	)
	if configuration.Status.Enabled {
		statusHandler, handlerErr := statusapi.NewHandler(statusRepository, statusBasicAuth(configuration.Status)...)
		if handlerErr != nil {
			cancelDials()
			cancelRuntime()
			return nil, handlerErr
		}
		rawListener, listenErr := net.Listen("tcp", configuration.Status.Listen)
		if listenErr != nil {
			cancelDials()
			cancelRuntime()
			return nil, listenErr
		}
		statusListener, limitErr := newLimitedListener(rawListener, statusapi.MaxStatusConnections)
		if limitErr != nil {
			_ = rawListener.Close()
			cancelDials()
			cancelRuntime()
			return nil, limitErr
		}
		daemon.statusListener = statusListener
		daemon.statusServer = &http.Server{
			Handler: statusHandler, ReadHeaderTimeout: statusReadHeaderTimeout, ReadTimeout: statusReadTimeout,
			WriteTimeout: statusWriteTimeout, IdleTimeout: statusIdleTimeout, MaxHeaderBytes: statusMaxHeaderBytes,
		}
	}
	cleanup = false
	return daemon, nil
}

func (daemon *serverDaemon) run(ctx context.Context) error {
	if daemon == nil || ctx == nil {
		if daemon != nil {
			daemon.cancelDials()
			daemon.cancelRuntime()
			if daemon.listener != nil {
				_ = daemon.listener.Close()
			}
			if daemon.statusListener != nil {
				_ = daemon.statusListener.Close()
			}
		}
		return ErrWireProtocol
	}
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		ticks := statusTicks(daemon.runtimeCtx, daemon.statusObserver.reconcileRepository)
		_ = daemon.statusRepository.Run(daemon.runtimeCtx, ticks)
	}()
	statusError := make(chan error, 1)
	if daemon.statusServer != nil {
		daemon.wg.Add(1)
		go func() {
			defer daemon.wg.Done()
			statusError <- daemon.statusServer.Serve(daemon.statusListener)
		}()
	}
	acceptError := make(chan error, 1)
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		acceptError <- daemon.acceptLoop()
	}()

	var result error
	select {
	case <-ctx.Done():
	case err := <-acceptError:
		if err != nil && !errors.Is(err, context.Canceled) {
			result = err
		}
	case err := <-statusError:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	daemon.openCommitMu.Lock()
	daemon.draining.Store(true)
	daemon.openCommitMu.Unlock()
	daemon.statusObserver.setHealthy(false)
	daemon.cancelDials()
	_ = daemon.listener.Close()
	cleanupDeadline := time.Now().Add(daemon.configuration.Deadlines.DrainCleanup)
	if daemon.statusServer != nil {
		_ = daemon.statusServer.Close()
	}
	daemon.waitForFlowDrain(time.Until(cleanupDeadline))
	daemon.closeAll()
	daemon.cancelRuntime()
	daemon.timerCallbacks.Wait()
	daemon.waitOpenWaiters()
	daemon.waitOpenWorkers()
	daemon.waitWorkers()
	daemon.wg.Wait()
	return result
}

func (daemon *serverDaemon) acceptLoop() error {
	for {
		connection, err := daemon.listener.Accept(daemon.runtimeCtx)
		if err != nil {
			return err
		}
		select {
		case daemon.sessionSlots <- struct{}{}:
		default:
			daemon.statusObserver.rejectSession()
			_ = connection.Close()
			continue
		}
		generation, ok := allocateDaemonGeneration(&daemon.nextSession)
		if !ok {
			<-daemon.sessionSlots
			_ = connection.Close()
			continue
		}
		daemon.wg.Add(1)
		go func() {
			defer daemon.wg.Done()
			defer func() { <-daemon.sessionSlots }()
			daemon.serveConnection(generation, connection)
		}()
	}
}

func (daemon *serverDaemon) serveConnection(generation uint64, connection transport.Connection) {
	session, err := newWireSession(daemon.runtimeCtx, generation, connection)
	if err != nil {
		_ = connection.Close()
		return
	}
	defer session.close()
	session.setStatusObserver(daemon.statusObserver)
	localAddress, localOK := endpointIP(connection.LocalEndpoint())
	sessionRegistered := false
	if localOK {
		daemon.statusObserver.upsertSessionObservation(runtimeSessionObservation{
			generation: generation, transportName: daemon.configuration.Transport.Type, interfaceName: "listener",
			localAddress: localAddress, localEndpoint: connection.LocalEndpoint(), remoteEndpoint: connection.RemoteEndpoint(),
			state: statusapi.SessionAuthenticating, reason: statusapi.ReasonStarted,
		})
		defer func() {
			if !sessionRegistered {
				daemon.statusObserver.removeSession(generation)
			}
		}()
	}
	source, ok := remoteAddress(connection.RemoteEndpoint())
	if !ok || !daemon.limiter.AllowAuth(time.Now(), source) {
		if ok {
			daemon.statusObserver.rejectSession()
		}
		return
	}
	select {
	case daemon.authSlots <- struct{}{}:
	default:
		return
	}
	err = authenticateServer(daemon.runtimeCtx, session, daemon.challenges, daemon.verifier)
	<-daemon.authSlots
	if err != nil {
		if localOK {
			daemon.statusObserver.upsertSessionObservation(runtimeSessionObservation{
				generation: generation, transportName: daemon.configuration.Transport.Type, interfaceName: "listener",
				localAddress: localAddress, localEndpoint: connection.LocalEndpoint(), remoteEndpoint: connection.RemoteEndpoint(),
				state: statusapi.SessionClosed, reason: statusapi.ReasonAuthenticationFailed,
			})
		}
		return
	}
	if !daemon.addSession(session) {
		return
	}
	sessionRegistered = true
	defer daemon.removeSession(session)
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		daemon.probeServerSession(session)
	}()
	for {
		// Closing a session closes the transport connection, which interrupts the
		// read. Avoid installing a redundant cancellation callback for every frame.
		message, err := session.read(context.Background())
		if err != nil {
			return
		}
		if err := daemon.dispatch(session, message); err != nil {
			return
		}
	}
}

func (daemon *serverDaemon) dispatch(session *wireSession, message protocol.Message) error {
	switch typed := message.(type) {
	case protocol.Probe:
		return session.send(session.probeACK(typed.Token))
	case protocol.ProbeACK:
		if rtt, ok := session.completeProbeACK(typed, time.Now()); ok {
			daemon.notifyServerProbeQuality(session, rtt)
		}
		return nil
	case protocol.Open:
		return daemon.handleOpen(session, typed)
	case protocol.Join:
		return daemon.handleJoin(session, typed)
	case protocol.Data, protocol.ACK, protocol.FIN, protocol.FINACK, protocol.Reset:
		flowID, ok := daemonMessageFlowID(message)
		if !ok {
			return ErrWireProtocol
		}
		attachment, ok := session.attachment(flowID)
		if !ok {
			return nil
		}
		instance := daemon.flow(servercore.FlowKey{PrincipalID: session.principal, FlowID: flowID})
		if instance == nil {
			return nil
		}
		return instance.remote(message, attachment)
	default:
		return ErrWireProtocol
	}
}

func (daemon *serverDaemon) probeServerSession(session *wireSession) {
	probe := func() bool {
		message, ok, expired, dead := session.startProbe(time.Now())
		if expired {
			daemon.notifyServerProbeStallPenalty(session, probeTimeout)
		}
		if dead {
			return false
		}
		return !ok || session.send(message) == nil
	}
	if !probe() {
		session.close()
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-session.ctx.Done():
			return
		case <-ticker.C:
			if !probe() {
				session.close()
				return
			}
		}
	}
}

func (daemon *serverDaemon) notifyServerProbeQuality(session *wireSession, rtt time.Duration) {
	_, quality, ok := daemon.pathGroupQuality(session.generation)
	if !ok {
		return
	}
	if quality.SRTT == 0 {
		quality.SRTT = rtt
	}
	for flowID, attachment := range session.allAttachments() {
		instance := daemon.flow(servercore.FlowKey{PrincipalID: session.principal, FlowID: flowID})
		if instance != nil && !instance.qualityStopped.Load() {
			_ = instance.handle(servercore.RelayEvent{
				Kind: servercore.RelaySetSessionQuality, Attachment: attachment,
				Quality: quality,
			})
		}
	}
}

func (daemon *serverDaemon) notifyServerProbeStallPenalty(session *wireSession, penalty time.Duration) {
	if daemon == nil || session == nil || penalty <= 0 {
		return
	}
	_, quality, ok := daemon.pathGroupQuality(session.generation)
	if !ok {
		return
	}
	for flowID, attachment := range session.allAttachments() {
		instance := daemon.flow(servercore.FlowKey{PrincipalID: session.principal, FlowID: flowID})
		if instance != nil && !instance.qualityStopped.Load() {
			_ = instance.handle(servercore.RelayEvent{
				Kind: servercore.RelaySetSessionQuality, Attachment: attachment,
				Quality: quality,
			})
		}
	}
}

func (daemon *serverDaemon) handleOpen(session *wireSession, request protocol.Open) error {
	if daemon.draining.Load() {
		return session.send(protocol.OpenResult{FlowID: request.FlowID, Result: protocol.OpenResourceLimit})
	}
	outcome := daemon.registry.HandleOpen(session.principal, request)
	if outcome.GenerateCapability != nil && !daemon.limiter.AllowOpen(time.Now(), session.principal) {
		daemon.statusObserver.rejectFlow(flowRejectionRateLimited)
		failure := daemon.registry.RejectOpen(
			outcome.GenerateCapability.Key, outcome.GenerateCapability.Generation, protocol.OpenResourceLimit,
		)
		daemon.statusObserver.setTombstones(uint64(daemon.registry.Snapshot().Tombstones))
		if failure.Ready {
			return session.send(failure.Result)
		}
		return session.send(protocol.OpenResult{FlowID: request.FlowID, Result: protocol.OpenResourceLimit})
	}
	if outcome.Ready {
		switch outcome.Rejection {
		case servercore.OpenRejectedOpeningCapacity:
			daemon.statusObserver.rejectFlow(flowRejectionOpeningCapacity)
		case servercore.OpenRejectedTargetDialCapacity:
			daemon.statusObserver.rejectFlow(flowRejectionTargetDialCapacity)
		case servercore.OpenRejectedCapacity:
			daemon.statusObserver.rejectFlow()
		}
		return daemon.completeOpenSession(session, request, outcome.Result)
	}
	if outcome.GenerateCapability != nil {
		if !daemon.startOpenWorker(func() {
			if err := daemon.processOpen(session, request, outcome); err != nil {
				session.close()
			}
		}) {
			daemon.statusObserver.rejectFlow(flowRejectionOpeningCapacity)
			failure := daemon.registry.RejectOpen(
				outcome.GenerateCapability.Key, outcome.GenerateCapability.Generation, protocol.OpenResourceLimit,
			)
			daemon.statusObserver.setTombstones(uint64(daemon.registry.Snapshot().Tombstones))
			if failure.Ready {
				return session.send(failure.Result)
			}
			return nil
		}
		return nil
	}
	if outcome.Operation == nil {
		return session.send(protocol.OpenResult{FlowID: request.FlowID, Result: protocol.OpenInternalFailure})
	}
	waiterKey := openWaiterKey{sessionGeneration: session.generation, flowID: request.FlowID}
	if !daemon.startOpenWaiter(waiterKey, func() {
		if err := daemon.processOpen(session, request, outcome); err != nil {
			session.close()
		}
	}) {
		return session.send(protocol.OpenResult{FlowID: request.FlowID, Result: protocol.OpenResourceLimit})
	}
	return nil
}

func (daemon *serverDaemon) processOpen(session *wireSession, request protocol.Open, outcome servercore.OpenOutcome) error {
	if outcome.GenerateCapability != nil {
		outcome = daemon.registry.GenerateCapability(*outcome.GenerateCapability)
	}
	if outcome.Dial != nil {
		daemon.statusObserver.addTargetDial(1)
		deadline := time.Now().Add(daemon.configuration.Deadlines.Dial)
		if outcome.Dial.Deadline.Before(deadline) {
			deadline = outcome.Dial.Deadline
		}
		dialCtx, cancel := context.WithDeadline(daemon.dialCtx, deadline)
		targetConnection, _ := daemon.targets.Dial(dialCtx, outcome.Dial.Target)
		daemon.statusObserver.addTargetDial(-1)
		cancel()
		outcome = daemon.completeOpenDial(request, *outcome.Dial, targetConnection)
	}
	if !outcome.Ready && outcome.Operation != nil {
		key := servercore.FlowKey{PrincipalID: session.principal, FlowID: request.FlowID}
		maximum := servercore.OpenDeadline
		generation := uint64(0)
		if entry, ok := daemon.registry.Lookup(key); ok && !entry.OpenExpiresAt.IsZero() {
			maximum = time.Until(entry.OpenExpiresAt)
			generation = entry.ActionGeneration
		}
		if maximum <= 0 {
			outcome = daemon.expireOpenWaiter(key, generation, outcome)
		} else {
			timer := time.NewTimer(maximum)
			select {
			case <-outcome.Operation.Done():
				outcome.Result, outcome.Ready = outcome.Operation.Result()
			case <-timer.C:
				outcome = daemon.expireOpenWaiter(key, generation, outcome)
			case <-daemon.runtimeCtx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return daemon.runtimeCtx.Err()
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
	if !outcome.Ready {
		if outcome.Result.FlowID == (protocol.FlowID{}) {
			outcome.Result = protocol.OpenResult{FlowID: request.FlowID, Result: protocol.OpenInternalFailure}
		}
	}
	return daemon.completeOpenSession(session, request, outcome.Result)
}

// completeOpenSession establishes the originating session as an implicit
// attachment on a successful OPEN result. The result explicitly marks that
// attachment so the client can publish its local side without a separate
// JOIN control round trip.
func (daemon *serverDaemon) completeOpenSession(session *wireSession, request protocol.Open, result protocol.OpenResult) error {
	if result.Result != protocol.OpenSuccess {
		return session.send(result)
	}
	key := servercore.FlowKey{PrincipalID: session.principal, FlowID: request.FlowID}
	instance := daemon.awaitFlow(key, time.Second)
	if instance == nil || instance.owner == nil {
		return session.send(protocol.OpenResult{FlowID: request.FlowID, Result: protocol.OpenInternalFailure})
	}
	if _, published := session.attachment(request.FlowID); published {
		result.ImplicitAttachment = true
		return session.send(result)
	}
	pending, ok := instance.beginJoin(session)
	if !ok {
		return session.send(protocol.OpenResult{FlowID: request.FlowID, Result: protocol.OpenResourceLimit})
	}
	result.ImplicitAttachment = true
	openErr := session.send(result)
	published := instance.completeJoin(session, pending, openErr == nil)
	if !published {
		if openErr == nil {
			_ = session.send(protocol.Reset{FlowID: request.FlowID, Reason: protocol.ResetCancelled})
			session.close()
		}
	}
	return openErr
}

func (daemon *serverDaemon) expireOpenWaiter(key servercore.FlowKey, generation uint64, previous servercore.OpenOutcome) servercore.OpenOutcome {
	if generation != 0 {
		if expired := daemon.registry.ExpireOpen(key, generation); expired.Ready {
			daemon.statusObserver.setTombstones(uint64(daemon.registry.Snapshot().Tombstones))
			return expired
		}
	}
	if previous.Operation != nil {
		previous.Result, previous.Ready = previous.Operation.Result()
	}
	return previous
}

func (daemon *serverDaemon) completeOpenDial(request protocol.Open, action servercore.OpenDialAction, targetConnection net.Conn) servercore.OpenOutcome {
	if targetConnection == nil {
		outcome := daemon.registry.CompleteDial(action, false)
		daemon.statusObserver.setTombstones(uint64(daemon.registry.Snapshot().Tombstones))
		return outcome
	}
	daemon.openCommitMu.Lock()
	defer daemon.openCommitMu.Unlock()
	if daemon.draining.Load() {
		_ = targetConnection.Close()
		outcome := daemon.registry.CompleteDial(action, false)
		daemon.statusObserver.setTombstones(uint64(daemon.registry.Snapshot().Tombstones))
		return outcome
	}
	entry, ok := daemon.registry.Lookup(action.Key)
	if !ok || entry.Owner == nil {
		_ = targetConnection.Close()
		return daemon.registry.CompleteDial(action, false)
	}
	instance, err := newServerFlow(
		daemon, action.Key, request.FlowID, action.Target, entry.DeliveryMode, entry.PathSelection, entry.Constraints,
		entry.Owner, targetConnection,
	)
	if err != nil || !daemon.addFlow(action.Key, instance) {
		if instance != nil {
			instance.abort()
		}
		_ = targetConnection.Close()
		outcome := daemon.registry.CompleteDial(action, false)
		daemon.statusObserver.setTombstones(uint64(daemon.registry.Snapshot().Tombstones))
		return outcome
	}
	if err := instance.start(); err != nil {
		daemon.removeFlow(action.Key, instance)
		instance.abort()
		_ = targetConnection.Close()
		outcome := daemon.registry.CompleteDial(action, false)
		daemon.statusObserver.setTombstones(uint64(daemon.registry.Snapshot().Tombstones))
		return outcome
	}
	outcome := daemon.registry.CompleteDial(action, true)
	if !outcome.Ready || outcome.Owner != entry.Owner || outcome.CloseLateTarget {
		daemon.removeFlow(action.Key, instance)
		instance.abort()
		_ = targetConnection.Close()
		return outcome
	}
	instance.activateStatus()
	return outcome
}

func (daemon *serverDaemon) handleJoin(session *wireSession, request protocol.Join) error {
	_, known := daemon.registry.Lookup(servercore.FlowKey{PrincipalID: session.principal, FlowID: request.FlowID})
	if !daemon.limiter.AllowJoin(time.Now(), session.principal, request.FlowID, known) {
		daemon.statusObserver.rejectFlow()
		return session.send(protocol.JoinResult{FlowID: request.FlowID, Result: protocol.JoinFailure})
	}
	authorization, authorized := daemon.registry.AuthorizeJoin(session.principal, request.FlowID, request.Capability)
	if !authorized || authorization.Flow == nil {
		return session.send(protocol.JoinResult{FlowID: request.FlowID, Result: protocol.JoinFailure})
	}
	key := servercore.FlowKey{PrincipalID: session.principal, FlowID: request.FlowID}
	instance := daemon.awaitFlow(key, time.Second)
	if instance == nil || instance.owner != authorization.Flow {
		return session.send(protocol.JoinResult{FlowID: request.FlowID, Result: protocol.JoinFailure})
	}
	pending, ok := instance.beginJoin(session)
	if !ok {
		return session.send(protocol.JoinResult{FlowID: request.FlowID, Result: protocol.JoinFailure})
	}
	err := session.send(protocol.JoinResult{FlowID: request.FlowID, Result: protocol.JoinSuccess})
	actuallyPublished := instance.completeJoin(session, pending, err == nil)
	if !actuallyPublished {
		if err == nil {
			// A terminal transition won while JOIN_RESULT was in flight. Make the
			// ordered success explicitly unusable and retire the shared session so
			// the client cannot retain an attachment the server never published.
			_ = session.send(protocol.Reset{FlowID: request.FlowID, Reason: protocol.ResetCancelled})
			session.close()
		}
	}
	return err
}

func allocateDaemonGeneration(counter *atomic.Uint64) (uint64, bool) {
	if counter == nil {
		return 0, false
	}
	for {
		current := counter.Load()
		if current == ^uint64(0) {
			return 0, false
		}
		if counter.CompareAndSwap(current, current+1) {
			return current + 1, true
		}
	}
}

func (daemon *serverDaemon) addSession(session *wireSession) bool {
	daemon.sessionsMu.Lock()
	defer daemon.sessionsMu.Unlock()
	if session == nil || session.generation == 0 || session.principal == "" || !protocol.ValidPathGroupID(session.pathGroupID) ||
		daemon.sessions[session.generation] != nil || daemon.laneGroups[session.generation] != nil ||
		daemon.principalSessions[session.principal] >= int(daemon.configuration.Limits.SessionsPerPrincipal) {
		return false
	}
	key := serverPathGroupKey{principal: session.principal, id: session.pathGroupID}
	group := daemon.pathGroups[key]
	if group == nil {
		var err error
		group, err = newWirePathGroup(session.generation, session.principal, session.pathGroupID)
		if err != nil {
			return false
		}
		daemon.pathGroups[key] = group
		daemon.pathGroupGenerations[group.generation] = group
	}
	if _, err := group.add(session); err != nil {
		return false
	}
	daemon.sessions[session.generation] = session
	daemon.laneGroups[session.generation] = group
	daemon.principalSessions[session.principal]++
	if local, ok := endpointIP(session.connection.LocalEndpoint()); ok {
		daemon.statusObserver.upsertSessionObservation(runtimeSessionObservation{
			generation: session.generation, transportName: daemon.configuration.Transport.Type, interfaceName: "listener",
			localAddress: local, localEndpoint: session.connection.LocalEndpoint(), remoteEndpoint: session.connection.RemoteEndpoint(),
			connectionID: session.connectionID.String(), principalID: session.principal, pathGroupID: session.pathGroupID,
			state: statusapi.SessionReady, reason: statusapi.ReasonPathAdded,
		})
	}
	return true
}

func (daemon *serverDaemon) removeSession(session *wireSession) {
	daemon.sessionsMu.Lock()
	var group *wirePathGroup
	lost := false
	if daemon.sessions[session.generation] == session {
		delete(daemon.sessions, session.generation)
		group = daemon.laneGroups[session.generation]
		delete(daemon.laneGroups, session.generation)
		if group != nil {
			lost = group.remove(session.generation)
			if lost {
				delete(daemon.pathGroups, serverPathGroupKey{principal: group.principal, id: group.id})
				delete(daemon.pathGroupGenerations, group.generation)
			}
		}
		daemon.principalSessions[session.principal]--
		if daemon.principalSessions[session.principal] == 0 {
			delete(daemon.principalSessions, session.principal)
		}
	}
	daemon.sessionsMu.Unlock()
	daemon.statusObserver.removeSession(session.generation)
	if !lost || group == nil {
		return
	}
	for flowID := range session.allAttachments() {
		if instance := daemon.flow(servercore.FlowKey{PrincipalID: session.principal, FlowID: flowID}); instance != nil {
			instance.sessionClosed(group.generation)
		}
	}
}

func (daemon *serverDaemon) session(generation uint64) *wireSession {
	daemon.sessionsMu.RLock()
	var session *wireSession
	if group := daemon.pathGroupGenerations[generation]; group != nil {
		session = group.session()
	} else {
		session = daemon.sessions[generation]
	}
	daemon.sessionsMu.RUnlock()
	return session
}

func (daemon *serverDaemon) selectSession(generation uint64, payloadBytes uint64) *wireSession {
	if daemon == nil || generation == 0 {
		return nil
	}
	daemon.sessionsMu.RLock()
	var session *wireSession
	if group := daemon.pathGroupGenerations[generation]; group != nil {
		session = group.selectSession(payloadBytes)
	} else {
		session = daemon.sessions[generation]
	}
	daemon.sessionsMu.RUnlock()
	return session
}

func (daemon *serverDaemon) releaseAttachment(generation uint64, flowID protocol.FlowID, attachment flow.AttachmentKey) {
	if daemon == nil || generation == 0 {
		return
	}
	daemon.sessionsMu.RLock()
	if group := daemon.pathGroupGenerations[generation]; group != nil {
		group.release(flowID, attachment)
	} else if session := daemon.sessions[generation]; session != nil {
		session.release(flowID, attachment)
		session.runtime.releaseFlow(flowID)
	}
	daemon.sessionsMu.RUnlock()
}

func (daemon *serverDaemon) pathGroupQuality(generation uint64) (uint64, policy.QualitySnapshot, bool) {
	if daemon == nil || generation == 0 {
		return 0, policy.QualitySnapshot{}, false
	}
	daemon.sessionsMu.RLock()
	group := daemon.pathGroupGenerations[generation]
	if group == nil {
		group = daemon.laneGroups[generation]
	}
	if group != nil {
		logicalGeneration := group.generation
		quality := group.qualitySnapshot()
		daemon.sessionsMu.RUnlock()
		return logicalGeneration, quality, true
	}
	session := daemon.sessions[generation]
	if session == nil {
		daemon.sessionsMu.RUnlock()
		return 0, policy.QualitySnapshot{}, false
	}
	quality := session.qualitySnapshot()
	daemon.sessionsMu.RUnlock()
	return generation, quality, true
}

func (daemon *serverDaemon) physicalSession(generation uint64) *wireSession {
	if daemon == nil || generation == 0 {
		return nil
	}
	daemon.sessionsMu.RLock()
	session := daemon.sessions[generation]
	daemon.sessionsMu.RUnlock()
	return session
}

func (daemon *serverDaemon) addFlow(key servercore.FlowKey, instance *serverFlow) bool {
	if instance == nil {
		return false
	}
	daemon.flowsMu.Lock()
	if daemon.draining.Load() || daemon.flows[key] != nil {
		daemon.flowsMu.Unlock()
		return false
	}
	daemon.flows[key] = instance
	if waiter := daemon.waiters[key]; waiter != nil {
		close(waiter.done)
		delete(daemon.waiters, key)
	}
	daemon.flowsMu.Unlock()
	daemon.signalFlowChanged()
	return true
}

func (daemon *serverDaemon) flow(key servercore.FlowKey) *serverFlow {
	daemon.flowsMu.RLock()
	instance := daemon.flows[key]
	daemon.flowsMu.RUnlock()
	return instance
}

func (daemon *serverDaemon) awaitFlow(key servercore.FlowKey, maximum time.Duration) *serverFlow {
	if instance := daemon.flow(key); instance != nil {
		return instance
	}
	daemon.flowsMu.Lock()
	if instance := daemon.flows[key]; instance != nil {
		daemon.flowsMu.Unlock()
		return instance
	}
	waiter := daemon.waiters[key]
	if waiter == nil {
		waiter = &flowWaiter{done: make(chan struct{})}
		daemon.waiters[key] = waiter
	}
	waiter.users++
	daemon.flowsMu.Unlock()
	defer daemon.releaseFlowWaiter(key, waiter)
	timer := time.NewTimer(maximum)
	defer timer.Stop()
	select {
	case <-waiter.done:
		return daemon.flow(key)
	case <-timer.C:
		return nil
	case <-daemon.runtimeCtx.Done():
		return nil
	}
}

func (daemon *serverDaemon) releaseFlowWaiter(key servercore.FlowKey, waiter *flowWaiter) {
	if daemon == nil || waiter == nil {
		return
	}
	daemon.flowsMu.Lock()
	if daemon.waiters[key] == waiter {
		waiter.users--
		if waiter.users <= 0 {
			delete(daemon.waiters, key)
		}
	}
	daemon.flowsMu.Unlock()
}

func (daemon *serverDaemon) removeFlow(key servercore.FlowKey, instance *serverFlow) {
	daemon.flowsMu.Lock()
	if daemon.flows[key] == instance {
		delete(daemon.flows, key)
	}
	daemon.flowsMu.Unlock()
	daemon.signalFlowChanged()
}

func (daemon *serverDaemon) closeAll() {
	daemon.flowsMu.RLock()
	flows := make([]*serverFlow, 0, len(daemon.flows))
	for _, instance := range daemon.flows {
		flows = append(flows, instance)
	}
	daemon.flowsMu.RUnlock()
	for _, instance := range flows {
		instance.close()
	}
	daemon.sessionsMu.RLock()
	sessions := make([]*wireSession, 0, len(daemon.sessions))
	for _, session := range daemon.sessions {
		sessions = append(sessions, session)
	}
	daemon.sessionsMu.RUnlock()
	for _, session := range sessions {
		session.close()
	}
}

func (daemon *serverDaemon) signalFlowChanged() {
	if daemon == nil || daemon.flowChanged == nil {
		return
	}
	select {
	case daemon.flowChanged <- struct{}{}:
	default:
	}
}

func (daemon *serverDaemon) waitForFlowDrain(maximum time.Duration) {
	if daemon == nil || maximum <= 0 {
		return
	}
	timer := time.NewTimer(maximum)
	defer timer.Stop()
	for {
		daemon.flowsMu.RLock()
		remaining := len(daemon.flows)
		daemon.flowsMu.RUnlock()
		if remaining == 0 {
			return
		}
		select {
		case <-daemon.flowChanged:
		case <-timer.C:
			return
		}
	}
}

func (daemon *serverDaemon) startWorker(work func()) bool {
	if work == nil || !daemon.reserveWorker() {
		return false
	}
	daemon.runReservedWorker(work)
	return true
}

func (daemon *serverDaemon) reserveWorker() bool {
	if daemon == nil {
		return false
	}
	daemon.workersMu.Lock()
	if daemon.workersClosed || daemon.workerLimit < 1 || daemon.workers >= daemon.workerLimit {
		daemon.workersMu.Unlock()
		return false
	}
	if daemon.workersCond == nil {
		daemon.workersCond = sync.NewCond(&daemon.workersMu)
	}
	daemon.workers++
	daemon.workersMu.Unlock()
	return true
}

func (daemon *serverDaemon) runReservedWorker(work func()) {
	go func() {
		defer daemon.releaseWorker()
		work()
	}()
}

func (daemon *serverDaemon) releaseWorker() {
	daemon.workersMu.Lock()
	daemon.workers--
	if daemon.workers == 0 {
		daemon.workersCond.Broadcast()
	}
	daemon.workersMu.Unlock()
}

func (daemon *serverDaemon) waitWorkers() {
	if daemon == nil {
		return
	}
	daemon.workersMu.Lock()
	if daemon.workersCond == nil {
		daemon.workersCond = sync.NewCond(&daemon.workersMu)
	}
	daemon.workersClosed = true
	for daemon.workers != 0 {
		daemon.workersCond.Wait()
	}
	daemon.workersMu.Unlock()
}

func (daemon *serverDaemon) startOpenWorker(work func()) bool {
	if daemon == nil || work == nil {
		return false
	}
	daemon.openWorkersMu.Lock()
	if daemon.openWorkersClosed || daemon.openWorkerLimit < 1 || daemon.openWorkers >= daemon.openWorkerLimit {
		daemon.openWorkersMu.Unlock()
		return false
	}
	if daemon.openWorkersCond == nil {
		daemon.openWorkersCond = sync.NewCond(&daemon.openWorkersMu)
	}
	daemon.openWorkers++
	daemon.openWorkersMu.Unlock()
	go func() {
		defer func() {
			daemon.openWorkersMu.Lock()
			daemon.openWorkers--
			if daemon.openWorkers == 0 {
				daemon.openWorkersCond.Broadcast()
			}
			daemon.openWorkersMu.Unlock()
		}()
		work()
	}()
	return true
}

func (daemon *serverDaemon) waitOpenWorkers() {
	if daemon == nil {
		return
	}
	daemon.openWorkersMu.Lock()
	if daemon.openWorkersCond == nil {
		daemon.openWorkersCond = sync.NewCond(&daemon.openWorkersMu)
	}
	daemon.openWorkersClosed = true
	for daemon.openWorkers != 0 {
		daemon.openWorkersCond.Wait()
	}
	daemon.openWorkersMu.Unlock()
}

func (daemon *serverDaemon) startOpenWaiter(key openWaiterKey, work func()) bool {
	if daemon == nil || key.sessionGeneration == 0 || key.flowID == (protocol.FlowID{}) || work == nil {
		return false
	}
	daemon.openWaitersMu.Lock()
	if daemon.openWaitersClosed || daemon.openWaiterLimit < 1 {
		daemon.openWaitersMu.Unlock()
		return false
	}
	if _, exists := daemon.openWaiters[key]; exists {
		daemon.openWaitersMu.Unlock()
		return true
	}
	if len(daemon.openWaiters) >= daemon.openWaiterLimit {
		daemon.openWaitersMu.Unlock()
		return false
	}
	if daemon.openWaiters == nil {
		daemon.openWaiters = make(map[openWaiterKey]struct{}, daemon.openWaiterLimit)
	}
	if daemon.openWaitersCond == nil {
		daemon.openWaitersCond = sync.NewCond(&daemon.openWaitersMu)
	}
	daemon.openWaiters[key] = struct{}{}
	daemon.openWaitersMu.Unlock()
	go func() {
		defer func() {
			daemon.openWaitersMu.Lock()
			delete(daemon.openWaiters, key)
			if len(daemon.openWaiters) == 0 {
				daemon.openWaitersCond.Broadcast()
			}
			daemon.openWaitersMu.Unlock()
		}()
		work()
	}()
	return true
}

func (daemon *serverDaemon) waitOpenWaiters() {
	if daemon == nil {
		return
	}
	daemon.openWaitersMu.Lock()
	if daemon.openWaitersCond == nil {
		daemon.openWaitersCond = sync.NewCond(&daemon.openWaitersMu)
	}
	daemon.openWaitersClosed = true
	for len(daemon.openWaiters) != 0 {
		daemon.openWaitersCond.Wait()
	}
	daemon.openWaitersMu.Unlock()
}

func remoteAddress(endpoint string) (netip.Addr, bool) {
	return endpointIP(endpoint)
}

func endpointIP(endpoint string) (netip.Addr, bool) {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return netip.Addr{}, false
	}
	address, err := netip.ParseAddr(host)
	return address.Unmap(), err == nil && address.IsValid()
}

func daemonMessageFlowID(message protocol.Message) (protocol.FlowID, bool) {
	switch typed := message.(type) {
	case protocol.Data:
		return typed.FlowID, true
	case protocol.ACK:
		return typed.FlowID, true
	case protocol.FIN:
		return typed.FlowID, true
	case protocol.FINACK:
		return typed.FlowID, true
	case protocol.Reset:
		return typed.FlowID, true
	default:
		return protocol.FlowID{}, false
	}
}

func statusTicks(ctx context.Context, reconcile func()) <-chan time.Time {
	output := make(chan time.Time, 1)
	go func() {
		defer close(output)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case value := <-ticker.C:
				if reconcile != nil {
					reconcile()
				}
				select {
				case output <- value:
				default:
				}
			}
		}
	}()
	return output
}
