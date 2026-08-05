package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sort"
	"sync"
	"time"

	"github.com/adrianceding/via/internal/auth"
	clientcore "github.com/adrianceding/via/internal/client"
	"github.com/adrianceding/via/internal/config"
	"github.com/adrianceding/via/internal/flow"
	pathcore "github.com/adrianceding/via/internal/path"
	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/socks5"
	statusapi "github.com/adrianceding/via/internal/status"
	"github.com/adrianceding/via/internal/transport"
)

type poolEvent struct {
	manager clientcore.SessionManagerEvent
	wire    *wireSession
}

type clientDaemon struct {
	configuration  config.Client
	runtimeCtx     context.Context
	cancelRuntime  context.CancelFunc
	factory        transport.Factory
	socksListener  net.Listener
	pathManager    *pathcore.Manager
	pathTicks      <-chan time.Time
	sessionManager *clientcore.SessionManager
	poolEvents     chan poolEvent

	sessionsMu sync.RWMutex
	sessions   map[uint64]*wireSession
	pending    map[uint64]*wireSession
	dialCancel map[uint64]context.CancelFunc
	backoffs   map[uint64]*time.Timer

	flowsMu sync.RWMutex
	flows   map[protocol.FlowID]*clientFlow
	actors  map[*clientFlow]struct{}
	wg      sync.WaitGroup

	sessionWorkers sync.WaitGroup

	socksSlots       chan struct{}
	socksMu          sync.Mutex
	socksConnections map[net.Conn]struct{}
	drainEvents      chan struct{}
	handshakeSlots   chan struct{}
	sourceMu         sync.Mutex
	sourceHandshakes map[string]int
	flowSlots        chan struct{}
	openingSlots     chan struct{}
	recoveringSlots  chan struct{}

	statusRepository *statusapi.Repository
	statusObserver   *runtimeStatus
	statusServer     *http.Server
	statusListener   net.Listener
}

func RunClient(ctx context.Context, configuration config.Client) error {
	if ctx == nil {
		return ErrWireProtocol
	}
	daemon, err := newClientDaemon(configuration)
	if err != nil {
		return err
	}
	return daemon.run(ctx)
}

func newClientDaemon(configuration config.Client) (*clientDaemon, error) {
	remote, err := resolveRelayAddress(configuration.Transport.Address, configuration.Deadlines.Dial)
	if err != nil {
		return nil, err
	}
	filter, err := pathcore.NewFilter(configuration.Interfaces.Include, configuration.Interfaces.Exclude)
	if err != nil {
		return nil, err
	}
	pathManager, err := pathcore.NewManager(pathcore.SystemEnumerator{}, filter, remote)
	if err != nil {
		return nil, err
	}
	transportRegistry, err := newTransportRegistry(configuration.Deadlines.FrameTotal, configuration.Deadlines.FrameNoProgress)
	if err != nil {
		return nil, err
	}
	factory, err := lookupTransportFactory(transportRegistry, configuration.Transport.Type)
	if err != nil {
		return nil, err
	}
	sessionManager, err := clientcore.NewSessionManager(clientcore.SessionManagerConfig{
		DesiredSessions: int(configuration.Limits.Sessions), AuthInProgress: int(configuration.Limits.AuthInProgress),
		RemoteEndpoint: configuration.Transport.Address, QueueLimits: transport.V1QueueLimits(),
	})
	if err != nil {
		return nil, err
	}
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	statusRepository, err := statusapi.NewRepositoryForRole(statusapi.Limits{
		Interfaces: statusapi.MaxInterfaces, Sessions: int(configuration.Limits.Sessions), Flows: int(configuration.Limits.Flows),
	}, statusapi.RoleClient)
	if err != nil {
		cancelRuntime()
		return nil, err
	}
	statusObserver, err := newRuntimeStatus(statusRepository, int(configuration.Limits.Flows), configuration.RequiredBytes)
	if err != nil {
		cancelRuntime()
		return nil, err
	}
	socksListener, err := net.Listen("tcp", configuration.SOCKSListen)
	if err != nil {
		cancelRuntime()
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = socksListener.Close()
		}
	}()
	daemon := &clientDaemon{
		configuration: configuration, runtimeCtx: runtimeCtx, cancelRuntime: cancelRuntime,
		factory: factory, socksListener: socksListener, pathManager: pathManager, sessionManager: sessionManager,
		poolEvents: make(chan poolEvent, pathcore.MaxEventsPerRefresh), sessions: make(map[uint64]*wireSession),
		pending: make(map[uint64]*wireSession), dialCancel: make(map[uint64]context.CancelFunc), backoffs: make(map[uint64]*time.Timer),
		flows:            make(map[protocol.FlowID]*clientFlow),
		actors:           make(map[*clientFlow]struct{}, int(configuration.Limits.Flows)),
		socksSlots:       make(chan struct{}, int(configuration.Limits.SOCKSConnections)),
		socksConnections: make(map[net.Conn]struct{}, int(configuration.Limits.SOCKSConnections)),
		drainEvents:      make(chan struct{}, 1),
		handshakeSlots:   make(chan struct{}, int(configuration.Limits.SOCKSHandshakes)),
		sourceHandshakes: make(map[string]int), flowSlots: make(chan struct{}, int(configuration.Limits.Flows)),
		openingSlots:     make(chan struct{}, int(configuration.Limits.OpeningFlows)),
		recoveringSlots:  make(chan struct{}, int(configuration.Limits.RecoveringFlows)),
		statusRepository: statusRepository, statusObserver: statusObserver,
	}
	statusObserver.setResourceLimits(
		configuration.Limits.Sessions, configuration.Limits.Flows, configuration.Limits.SOCKSConnections,
		0, 0,
	)
	if configuration.Status.Enabled {
		handler, handlerErr := statusapi.NewHandler(statusRepository, statusBasicAuth(configuration.Status)...)
		if handlerErr != nil {
			cancelRuntime()
			return nil, handlerErr
		}
		rawListener, listenErr := net.Listen("tcp", configuration.Status.Listen)
		if listenErr != nil {
			cancelRuntime()
			return nil, listenErr
		}
		listener, limitErr := newLimitedListener(rawListener, statusapi.MaxStatusConnections)
		if limitErr != nil {
			_ = rawListener.Close()
			cancelRuntime()
			return nil, limitErr
		}
		daemon.statusListener = listener
		daemon.statusServer = &http.Server{
			Handler: handler, ReadHeaderTimeout: statusReadHeaderTimeout, ReadTimeout: statusReadTimeout,
			WriteTimeout: statusWriteTimeout, IdleTimeout: statusIdleTimeout, MaxHeaderBytes: statusMaxHeaderBytes,
		}
	}
	cleanup = false
	return daemon, nil
}

func (daemon *clientDaemon) run(ctx context.Context) error {
	if daemon == nil || ctx == nil {
		return ErrWireProtocol
	}
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		_ = daemon.statusRepository.Run(daemon.runtimeCtx, statusTicks(daemon.runtimeCtx))
	}()
	statusError := make(chan error, 1)
	if daemon.statusServer != nil {
		daemon.wg.Add(1)
		go func() {
			defer daemon.wg.Done()
			statusError <- daemon.statusServer.Serve(daemon.statusListener)
		}()
	}
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		daemon.runSessionPool()
	}()
	acceptError := make(chan error, 1)
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		acceptError <- daemon.acceptSOCKS()
	}()

	var result error
	acceptStopped := false
	select {
	case <-ctx.Done():
	case err := <-acceptError:
		acceptStopped = true
		if err != nil && !errors.Is(err, net.ErrClosed) {
			result = err
		}
	case err := <-statusError:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	_ = daemon.socksListener.Close()
	daemon.statusObserver.setHealthy(false)
	if !acceptStopped {
		if err := <-acceptError; err != nil && !errors.Is(err, net.ErrClosed) && result == nil {
			result = err
		}
	}
	if daemon.statusServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), daemon.configuration.Deadlines.DrainCleanup)
		_ = daemon.statusServer.Shutdown(shutdownCtx)
		cancel()
	}
	daemon.waitForDrain(daemon.configuration.Deadlines.DrainCleanup)
	daemon.closeAll()
	daemon.cancelRuntime()
	daemon.wg.Wait()
	return result
}

func (daemon *clientDaemon) runSessionPool() {
	refresh := func() {
		_, snapshot, err := daemon.pathManager.Refresh()
		if err != nil {
			return
		}
		actions, err := daemon.sessionManager.Handle(clientcore.SessionManagerEvent{
			Kind: clientcore.SessionPathsChanged, Candidates: snapshot.Candidates,
		})
		if err == nil {
			daemon.statusObserver.syncInterfaces(snapshot)
			daemon.executeSessionActions(actions)
		}
	}
	refresh()
	ticks := daemon.pathTicks
	var ticker *time.Ticker
	if ticks == nil {
		ticker = time.NewTicker(pathcore.DiscoveryInterval)
		ticks = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case <-daemon.runtimeCtx.Done():
			actions, _ := daemon.sessionManager.Handle(clientcore.SessionManagerEvent{Kind: clientcore.SessionShutdown})
			daemon.executeSessionActions(actions)
			daemon.sessionWorkers.Wait()
			daemon.discardPoolEvents()
			return
		case _, open := <-ticks:
			if !open {
				ticks = nil
				continue
			}
			refresh()
		case event := <-daemon.poolEvents:
			daemon.handlePoolEvent(event)
		}
	}
}

func (daemon *clientDaemon) handlePoolEvent(event poolEvent) {
	if event.manager.Kind == clientcore.SessionDialCompleted {
		if cancel := daemon.dialCancel[event.manager.Generation]; cancel != nil {
			cancel()
			delete(daemon.dialCancel, event.manager.Generation)
		}
	}
	if event.manager.Kind == clientcore.SessionBackoffExpired {
		delete(daemon.backoffs, event.manager.Generation)
	}
	actions, err := daemon.sessionManager.Handle(event.manager)
	if err != nil {
		if event.wire != nil {
			event.wire.close()
		}
		return
	}
	if event.manager.Kind == clientcore.SessionAuthenticationCompleted && event.wire != nil {
		ready := false
		for _, action := range actions {
			if action.Kind == clientcore.SessionActionReady && action.Generation == event.manager.Generation {
				ready = true
				break
			}
		}
		if ready {
			daemon.pending[event.manager.Generation] = event.wire
		} else {
			event.wire.close()
		}
	}
	daemon.executeSessionActions(actions)
}

func (daemon *clientDaemon) executeSessionActions(actions []clientcore.SessionManagerAction) {
	defer func() {
		authenticated := make(map[uint64]*wireSession, len(daemon.pending)+len(daemon.sessions))
		for generation, session := range daemon.pending {
			authenticated[generation] = session
		}
		daemon.sessionsMu.RLock()
		for generation, session := range daemon.sessions {
			authenticated[generation] = session
		}
		daemon.sessionsMu.RUnlock()
		daemon.statusObserver.syncClientSessions(
			daemon.configuration.Transport.Type, daemon.configuration.PrincipalID,
			daemon.sessionManager.Snapshot(), authenticated,
		)
	}()
	for _, action := range actions {
		switch action.Kind {
		case clientcore.SessionActionDial:
			dialCtx, cancel := context.WithTimeout(daemon.runtimeCtx, daemon.configuration.Deadlines.Dial)
			daemon.dialCancel[action.Generation] = cancel
			daemon.wg.Add(1)
			daemon.sessionWorkers.Add(1)
			go func(action clientcore.SessionManagerAction) {
				defer daemon.wg.Done()
				defer daemon.sessionWorkers.Done()
				dialer, err := daemon.factory.NewDialer(action.Options)
				var connection transport.Connection
				if err == nil {
					connection, err = dialer.Dial(dialCtx)
				}
				select {
				case daemon.poolEvents <- poolEvent{manager: clientcore.SessionManagerEvent{
					Kind: clientcore.SessionDialCompleted, Generation: action.Generation, Connection: connection, Err: err,
				}}:
				case <-daemon.runtimeCtx.Done():
					if connection != nil {
						_ = connection.Close()
					}
				}
			}(action)
		case clientcore.SessionActionStartAuthentication:
			session, err := newWireSession(daemon.runtimeCtx, action.Generation, action.Connection)
			if err != nil {
				daemon.emitPoolEvent(poolEvent{manager: clientcore.SessionManagerEvent{
					Kind: clientcore.SessionAuthenticationCompleted, Generation: action.Generation, Err: err,
				}})
				continue
			}
			session.setStatusObserver(daemon.statusObserver)
			daemon.wg.Add(1)
			daemon.sessionWorkers.Add(1)
			go func(action clientcore.SessionManagerAction) {
				defer daemon.wg.Done()
				defer daemon.sessionWorkers.Done()
				err := authenticateClient(daemon.runtimeCtx, session, daemon.configuration.PrincipalID, auth.Key(daemon.configuration.PSK), rand.Reader)
				daemon.emitPoolEvent(poolEvent{wire: session, manager: clientcore.SessionManagerEvent{
					Kind: clientcore.SessionAuthenticationCompleted, Generation: action.Generation, Succeeded: err == nil, Err: err,
				}})
			}(action)
		case clientcore.SessionActionReady:
			session := daemon.pending[action.Generation]
			delete(daemon.pending, action.Generation)
			if session == nil {
				daemon.emitPoolEvent(poolEvent{manager: clientcore.SessionManagerEvent{Kind: clientcore.SessionConnectionLost, Generation: action.Generation}})
				continue
			}
			daemon.sessionsMu.Lock()
			daemon.sessions[action.Generation] = session
			daemon.sessionsMu.Unlock()
			daemon.notifySessionReady(action.Generation)
			daemon.wg.Add(1)
			go func() {
				defer daemon.wg.Done()
				daemon.readClientSession(session)
			}()
			daemon.wg.Add(1)
			go func() {
				defer daemon.wg.Done()
				daemon.probeClientSession(session)
			}()
		case clientcore.SessionActionLost:
			daemon.sessionsMu.Lock()
			session := daemon.sessions[action.Generation]
			delete(daemon.sessions, action.Generation)
			daemon.sessionsMu.Unlock()
			if session != nil {
				daemon.notifySessionLost(action.Generation)
			}
		case clientcore.SessionActionCloseConnection:
			if action.Connection != nil {
				_ = action.Connection.Close()
			}
			if session := daemon.pending[action.Generation]; session != nil {
				delete(daemon.pending, action.Generation)
				session.close()
			}
		case clientcore.SessionActionArmBackoff:
			generation := action.Generation
			daemon.backoffs[generation] = time.AfterFunc(action.After, func() {
				daemon.emitPoolEvent(poolEvent{manager: clientcore.SessionManagerEvent{
					Kind: clientcore.SessionBackoffExpired, Generation: generation,
				}})
			})
		case clientcore.SessionActionCancelBackoff:
			if timer := daemon.backoffs[action.Generation]; timer != nil {
				timer.Stop()
				delete(daemon.backoffs, action.Generation)
			}
		case clientcore.SessionActionCancelDial:
			if cancel := daemon.dialCancel[action.Generation]; cancel != nil {
				cancel()
				delete(daemon.dialCancel, action.Generation)
			}
		}
	}
}

func (daemon *clientDaemon) discardPoolEvents() {
	for {
		select {
		case event := <-daemon.poolEvents:
			if event.wire != nil {
				event.wire.close()
			}
			if event.manager.Connection != nil {
				_ = event.manager.Connection.Close()
			}
		default:
			return
		}
	}
}

func (daemon *clientDaemon) readClientSession(session *wireSession) {
	defer daemon.emitPoolEvent(poolEvent{manager: clientcore.SessionManagerEvent{
		Kind: clientcore.SessionConnectionLost, Generation: session.generation,
	}})
	for {
		message, err := session.read(session.ctx)
		if err != nil {
			return
		}
		switch typed := message.(type) {
		case protocol.Probe:
			if session.send(protocol.ProbeACK{Token: typed.Token}) != nil {
				return
			}
		case protocol.ProbeACK:
			if rtt, ok := session.completeProbe(typed.Token, time.Now()); ok {
				daemon.notifyProbeQuality(session, rtt)
			}
		case protocol.OpenResult:
			daemon.routeClientMessage(session, typed.FlowID, message, flowAttachment(session, typed.FlowID))
		case protocol.JoinResult:
			daemon.routeClientMessage(session, typed.FlowID, message, flowAttachment(session, typed.FlowID))
		case protocol.Data, protocol.ACK, protocol.FIN, protocol.FINACK, protocol.Reset:
			flowID, ok := daemonMessageFlowID(message)
			if !ok {
				return
			}
			attachment, published := session.attachment(flowID)
			if published {
				daemon.routeClientMessage(session, flowID, message, attachment)
			}
		default:
			return
		}
	}
}

func (daemon *clientDaemon) probeClientSession(session *wireSession) {
	probe := func() bool {
		message, ok, expired := session.startProbe(time.Now())
		if expired {
			daemon.notifyProbeStallPenalty(session, probeTimeout)
		}
		return !ok || session.send(message) == nil
	}
	if !probe() {
		daemon.emitPoolEvent(poolEvent{manager: clientcore.SessionManagerEvent{
			Kind: clientcore.SessionConnectionLost, Generation: session.generation,
		}})
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
				daemon.emitPoolEvent(poolEvent{manager: clientcore.SessionManagerEvent{
					Kind: clientcore.SessionConnectionLost, Generation: session.generation,
				}})
				return
			}
		}
	}
}

func (daemon *clientDaemon) notifyProbeQuality(session *wireSession, rtt time.Duration) {
	smoothed, stall := session.probeQuality()
	if smoothed == 0 {
		smoothed = rtt
	}
	for flowID, attachment := range session.allAttachments() {
		daemon.flowsMu.RLock()
		instance := daemon.flows[flowID]
		daemon.flowsMu.RUnlock()
		if instance != nil {
			instance.tryEmitSessionQuality(session.generation, smoothed, stall)
			instance.tryEmitQuality(clientcore.ApplicationRelayEvent{
				Kind: clientcore.ApplicationRelaySetSessionQuality, Attachment: attachment,
				Quality: session.qualitySnapshot(),
			})
		}
	}
}

func (daemon *clientDaemon) notifyProbeStallPenalty(session *wireSession, penalty time.Duration) {
	if daemon == nil || session == nil || penalty <= 0 {
		return
	}
	for flowID, attachment := range session.allAttachments() {
		daemon.flowsMu.RLock()
		instance := daemon.flows[flowID]
		daemon.flowsMu.RUnlock()
		if instance != nil {
			rtt, _ := session.probeQuality()
			instance.tryEmitSessionQuality(session.generation, rtt, penalty)
			instance.tryEmitQuality(clientcore.ApplicationRelayEvent{
				Kind: clientcore.ApplicationRelaySetSessionQuality, Attachment: attachment,
				Quality: session.qualitySnapshot(),
			})
		}
	}
}

func (daemon *clientDaemon) emitPoolEvent(event poolEvent) {
	select {
	case daemon.poolEvents <- event:
	case <-daemon.runtimeCtx.Done():
		if event.wire != nil {
			event.wire.close()
		}
	}
}

func (daemon *clientDaemon) readySessions() []*wireSession {
	daemon.sessionsMu.RLock()
	result := make([]*wireSession, 0, len(daemon.sessions))
	for _, session := range daemon.sessions {
		result = append(result, session)
	}
	daemon.sessionsMu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].generation < result[j].generation })
	return result
}

func (daemon *clientDaemon) session(generation uint64) *wireSession {
	daemon.sessionsMu.RLock()
	session := daemon.sessions[generation]
	daemon.sessionsMu.RUnlock()
	return session
}

func (daemon *clientDaemon) notifySessionReady(generation uint64) {
	var rtt, stall time.Duration
	if session := daemon.session(generation); session != nil {
		rtt, stall = session.probeQuality()
	}
	daemon.flowsMu.RLock()
	flows := make([]*clientFlow, 0, len(daemon.actors))
	for instance := range daemon.actors {
		flows = append(flows, instance)
	}
	daemon.flowsMu.RUnlock()
	for _, instance := range flows {
		instance.emit(clientFlowSessionReady{generation: generation, rtt: rtt, stall: stall})
	}
}

func (daemon *clientDaemon) notifySessionLost(generation uint64) {
	daemon.flowsMu.RLock()
	flows := make([]*clientFlow, 0, len(daemon.actors))
	for instance := range daemon.actors {
		flows = append(flows, instance)
	}
	daemon.flowsMu.RUnlock()
	for _, instance := range flows {
		instance.emit(clientFlowSessionLost{generation: generation})
	}
}

func (daemon *clientDaemon) routeClientMessage(session *wireSession, flowID protocol.FlowID, message protocol.Message, attachment flow.AttachmentKey) {
	daemon.flowsMu.RLock()
	instance := daemon.flows[flowID]
	daemon.flowsMu.RUnlock()
	if instance != nil {
		instance.emit(clientFlowRemote{message: message, sessionGeneration: session.generation, attachment: attachment})
	}
}

func flowAttachment(session *wireSession, flowID protocol.FlowID) flow.AttachmentKey {
	attachment, _ := session.attachment(flowID)
	return attachment
}

func resolveRelayAddress(endpoint string, timeout time.Duration) (netip.Addr, error) {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return netip.Addr{}, err
	}
	if address, parseErr := netip.ParseAddr(host); parseErr == nil {
		return address.Unmap(), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return netip.Addr{}, err
	}
	for _, address := range addresses {
		if address.IsValid() && !address.IsUnspecified() && address.Zone() == "" {
			return address.Unmap(), nil
		}
	}
	return netip.Addr{}, ErrWireProtocol
}

func (daemon *clientDaemon) closeAll() {
	daemon.socksMu.Lock()
	connections := make([]net.Conn, 0, len(daemon.socksConnections))
	for connection := range daemon.socksConnections {
		connections = append(connections, connection)
	}
	daemon.socksMu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
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
	daemon.flowsMu.RLock()
	flows := make([]*clientFlow, 0, len(daemon.actors))
	for instance := range daemon.actors {
		flows = append(flows, instance)
	}
	daemon.flowsMu.RUnlock()
	for _, instance := range flows {
		instance.close()
	}
}

func (daemon *clientDaemon) waitForDrain(maximum time.Duration) {
	if maximum <= 0 {
		return
	}
	timer := time.NewTimer(maximum)
	defer timer.Stop()
	for {
		daemon.socksMu.Lock()
		drained := len(daemon.socksConnections) == 0
		daemon.socksMu.Unlock()
		if drained {
			return
		}
		select {
		case <-daemon.drainEvents:
		case <-timer.C:
			return
		case <-daemon.runtimeCtx.Done():
			return
		}
	}
}

func (daemon *clientDaemon) trackSOCKS(connection net.Conn) {
	daemon.socksMu.Lock()
	daemon.socksConnections[connection] = struct{}{}
	daemon.socksMu.Unlock()
	daemon.statusObserver.addSOCKS(1)
}

func (daemon *clientDaemon) untrackSOCKS(connection net.Conn) {
	daemon.socksMu.Lock()
	delete(daemon.socksConnections, connection)
	daemon.socksMu.Unlock()
	daemon.statusObserver.addSOCKS(-1)
	select {
	case daemon.drainEvents <- struct{}{}:
	default:
	}
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func (daemon *clientDaemon) acceptSOCKS() error {
	for {
		connection, err := daemon.socksListener.Accept()
		if err != nil {
			return err
		}
		select {
		case daemon.socksSlots <- struct{}{}:
		default:
			daemon.statusObserver.rejectSOCKS()
			_ = connection.Close()
			continue
		}
		daemon.trackSOCKS(connection)
		daemon.wg.Add(1)
		go func() {
			defer daemon.wg.Done()
			defer func() {
				<-daemon.socksSlots
				daemon.untrackSOCKS(connection)
			}()
			daemon.serveSOCKS(connection)
		}()
	}
}

func (daemon *clientDaemon) serveSOCKS(connection net.Conn) {
	defer connection.Close()
	if !daemon.acquireHandshake(connection.RemoteAddr()) {
		return
	}
	handshakeHeld := true
	defer func() {
		if handshakeHeld {
			daemon.releaseHandshake(connection.RemoteAddr())
		}
	}()
	_ = connection.SetReadDeadline(time.Now().Add(daemon.configuration.Deadlines.SOCKSGreeting))
	requiredMethod := byte(socks5.MethodNoAuth)
	if daemon.configuration.SOCKSAuth != nil {
		requiredMethod = socks5.MethodUsernamePassword
	}
	accepted, err := socks5.ReadGreeting(connection, requiredMethod)
	method := socks5.MethodReply(requiredMethod, accepted && err == nil)
	_ = connection.SetWriteDeadline(time.Now().Add(sendTimeout))
	if writeAll(connection, method[:]) != nil || err != nil || !accepted {
		return
	}
	if daemon.configuration.SOCKSAuth != nil {
		_ = connection.SetReadDeadline(time.Now().Add(daemon.configuration.Deadlines.SOCKSGreeting))
		authenticated, authenticationErr := socks5.Authenticate(
			connection, daemon.configuration.SOCKSAuth.Username, daemon.configuration.SOCKSAuth.Password,
		)
		authenticationReply := socks5.AuthenticationReply(authenticated && authenticationErr == nil)
		_ = connection.SetWriteDeadline(time.Now().Add(sendTimeout))
		if writeAll(connection, authenticationReply[:]) != nil || authenticationErr != nil || !authenticated {
			return
		}
	}
	_ = connection.SetReadDeadline(time.Now().Add(daemon.configuration.Deadlines.SOCKSRequest))
	target, err := socks5.ReadRequest(connection)
	if err != nil {
		code := socks5.ReplyGeneralFailure
		if errors.Is(err, socks5.ErrCommandUnsupported) {
			code = socks5.ReplyCommandNotSupported
		} else if errors.Is(err, socks5.ErrAddressTypeUnsupported) {
			code = socks5.ReplyAddressTypeNotSupported
		}
		_ = writeSOCKSReply(connection, code)
		return
	}
	_ = connection.SetDeadline(time.Time{})
	select {
	case daemon.flowSlots <- struct{}{}:
	default:
		daemon.statusObserver.rejectFlow()
		_ = writeSOCKSReply(connection, socks5.ReplyGeneralFailure)
		return
	}
	if !daemon.acquireOpening() {
		daemon.statusObserver.rejectFlow()
		<-daemon.flowSlots
		_ = writeSOCKSReply(connection, socks5.ReplyGeneralFailure)
		return
	}
	instance, err := newClientFlow(daemon, connection, target)
	if err != nil {
		daemon.releaseOpening()
		<-daemon.flowSlots
		_ = writeSOCKSReply(connection, socks5.ReplyGeneralFailure)
		return
	}
	result := <-instance.openResult
	daemon.releaseOpening()
	if writeSOCKSReply(connection, socks5.ReplyForOpenResult(result)) != nil || result != protocol.OpenSuccess {
		instance.close()
		return
	}
	_ = connection.SetDeadline(time.Time{})
	daemon.releaseHandshake(connection.RemoteAddr())
	handshakeHeld = false
	instance.emit(clientFlowApplicationEnabled{})
	<-instance.done
}

func (daemon *clientDaemon) acquireOpening() bool {
	select {
	case daemon.openingSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (daemon *clientDaemon) releaseOpening() {
	<-daemon.openingSlots
}

func writeSOCKSReply(connection net.Conn, code socks5.ReplyCode) error {
	reply, err := socks5.Reply(code)
	if err != nil {
		return err
	}
	if err := connection.SetWriteDeadline(time.Now().Add(sendTimeout)); err != nil {
		return err
	}
	return writeAll(connection, reply[:])
}

func (daemon *clientDaemon) acquireHandshake(address net.Addr) bool {
	select {
	case daemon.handshakeSlots <- struct{}{}:
	default:
		return false
	}
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		<-daemon.handshakeSlots
		return false
	}
	daemon.sourceMu.Lock()
	if daemon.sourceHandshakes[host] >= int(daemon.configuration.Limits.SOCKSPerSource) {
		daemon.sourceMu.Unlock()
		<-daemon.handshakeSlots
		return false
	}
	daemon.sourceHandshakes[host]++
	daemon.sourceMu.Unlock()
	return true
}

func (daemon *clientDaemon) releaseHandshake(address net.Addr) {
	host, _, _ := net.SplitHostPort(address.String())
	daemon.sourceMu.Lock()
	daemon.sourceHandshakes[host]--
	if daemon.sourceHandshakes[host] == 0 {
		delete(daemon.sourceHandshakes, host)
	}
	daemon.sourceMu.Unlock()
	<-daemon.handshakeSlots
}
