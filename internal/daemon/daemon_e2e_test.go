package daemon

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/config"
)

const (
	daemonSOCKSUsername = "test-app"
	daemonSOCKSPassword = "test-password"
)

func TestClientDaemonAllowsNoAuthWhenCredentialsAreOmitted(t *testing.T) {
	relayAddress := reserveAddress(t)
	socksAddress := reserveAddress(t)
	configuration := decodeClientForDaemonTest(t, relayAddress, socksAddress)
	configuration.SOCKSAuth = nil
	client, err := newClientDaemon(configuration)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- client.run(ctx) }()

	application, err := net.DialTimeout("tcp", socksAddress, time.Second)
	if err != nil {
		cancel()
		<-result
		t.Fatal(err)
	}
	_ = application.SetDeadline(time.Now().Add(time.Second))
	if _, err := application.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	method := make([]byte, 2)
	if _, err := io.ReadFull(application, method); err != nil || method[0] != 5 || method[1] != 0 {
		t.Fatalf("no-auth method = %v, %v", method, err)
	}
	if _, err := application.Write([]byte{5, 2, 0, 1, 127, 0, 0, 1, 0, 1}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(application, reply); err != nil || reply[1] != 7 {
		t.Fatalf("no-auth request reply = %v, %v", reply, err)
	}
	_ = application.Close()
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		client.cancelRuntime()
		client.closeAll()
		t.Fatal("no-auth client did not stop")
	}
}

func TestDaemonLocalSOCKSTCPRoundTripAndGracefulShutdown(t *testing.T) {
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer targetListener.Close()
	var targetConnections atomic.Int32
	targetDone := make(chan struct{})
	go func() {
		defer close(targetDone)
		for {
			connection, err := targetListener.Accept()
			if err != nil {
				return
			}
			targetConnections.Add(1)
			go func() {
				defer connection.Close()
				_, _ = io.Copy(connection, connection)
			}()
		}
	}()

	relayAddress := reserveAddress(t)
	socksAddress := reserveAddress(t)
	serverConfiguration := decodeServerForDaemonTest(t, relayAddress)
	clientConfiguration := decodeClientForDaemonTest(t, relayAddress, socksAddress)
	server, err := newServerDaemon(serverConfiguration)
	if err != nil {
		t.Fatal(err)
	}
	client, err := newClientDaemon(clientConfiguration)
	if err != nil {
		server.cancelRuntime()
		server.closeAll()
		t.Fatal(err)
	}
	serverCtx, cancelServer := context.WithCancel(context.Background())
	clientCtx, cancelClient := context.WithCancel(context.Background())
	serverResult := make(chan error, 1)
	clientResult := make(chan error, 1)
	go func() { serverResult <- server.run(serverCtx) }()
	go func() { clientResult <- client.run(clientCtx) }()

	waitFor(t, 5*time.Second, func() bool { return len(client.readySessions()) == 1 }, "authenticated client session")

	noAuthentication, err := net.DialTimeout("tcp", socksAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = noAuthentication.SetDeadline(time.Now().Add(time.Second))
	if _, err := noAuthentication.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	method := make([]byte, 2)
	if _, err := io.ReadFull(noAuthentication, method); err != nil || method[0] != 5 || method[1] != 0xff {
		t.Fatalf("unauthenticated method = %v, %v", method, err)
	}
	_ = noAuthentication.Close()

	wrongAuthentication, err := net.DialTimeout("tcp", socksAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = wrongAuthentication.SetDeadline(time.Now().Add(time.Second))
	writeSOCKSAuthentication(t, wrongAuthentication, daemonSOCKSUsername, "wrong-password", false)
	_ = wrongAuthentication.Close()
	if targetConnections.Load() != 0 {
		t.Fatal("failed SOCKS authentication created a target connection")
	}

	application, err := net.DialTimeout("tcp", socksAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	_ = application.SetDeadline(time.Now().Add(5 * time.Second))
	writeSOCKSAuthentication(t, application, daemonSOCKSUsername, daemonSOCKSPassword, true)
	targetHost, targetPortText, _ := net.SplitHostPort(targetListener.Addr().String())
	targetIP := net.ParseIP(targetHost).To4()
	var targetPort uint16
	_, _ = fmt.Sscanf(targetPortText, "%d", &targetPort)
	request := make([]byte, 10)
	request[0], request[1], request[3] = 5, 1, 1
	copy(request[4:8], targetIP)
	binary.BigEndian.PutUint16(request[8:], targetPort)
	if _, err := application.Write(request); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(application, reply); err != nil || reply[1] != 0 {
		t.Fatalf("SOCKS reply = %v, %v", reply, err)
	}
	payload := []byte("via-runtime-round-trip")
	if _, err := application.Write(payload); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(application, received); err != nil || string(received) != string(payload) {
		t.Fatalf("round trip = %q, %v", received, err)
	}
	if got := targetConnections.Load(); got != 1 {
		t.Fatalf("target connections = %d", got)
	}
	_ = application.Close()
	cancelClient()
	cancelServer()
	for name, result := range map[string]<-chan error{"client": clientResult, "server": serverResult} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s shutdown: %v", name, err)
			}
		case <-time.After(4 * time.Second):
			t.Fatalf("%s did not shut down", name)
		}
	}
	_ = targetListener.Close()
	<-targetDone
}

func TestDaemonStatusReflectsLiveTCPFlowWithoutTargetLeak(t *testing.T) {
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer targetListener.Close()
	go func() {
		for {
			connection, acceptErr := targetListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer connection.Close()
				_, _ = io.Copy(connection, connection)
			}()
		}
	}()

	relayAddress := reserveAddress(t)
	socksAddress := reserveAddress(t)
	serverStatusAddress := reserveAddress(t)
	clientStatusAddress := reserveAddress(t)
	serverConfiguration := decodeServerStatusForDaemonTest(t, relayAddress, serverStatusAddress)
	clientConfiguration := decodeClientStatusForDaemonTest(t, relayAddress, socksAddress, clientStatusAddress)
	server, err := newServerDaemon(serverConfiguration)
	if err != nil {
		t.Fatal(err)
	}
	client, err := newClientDaemon(clientConfiguration)
	if err != nil {
		server.cancelDials()
		server.cancelRuntime()
		server.closeAll()
		t.Fatal(err)
	}
	serverCtx, cancelServer := context.WithCancel(context.Background())
	clientCtx, cancelClient := context.WithCancel(context.Background())
	serverResult := make(chan error, 1)
	clientResult := make(chan error, 1)
	go func() { serverResult <- server.run(serverCtx) }()
	go func() { clientResult <- client.run(clientCtx) }()
	defer func() {
		cancelClient()
		cancelServer()
		<-clientResult
		<-serverResult
	}()

	waitFor(t, 5*time.Second, func() bool { return len(client.readySessions()) == 1 }, "status test session")
	application, err := net.DialTimeout("tcp", socksAddress, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	_ = application.SetDeadline(time.Now().Add(5 * time.Second))
	writeSOCKSAuthentication(t, application, daemonSOCKSUsername, daemonSOCKSPassword, true)
	_, targetPortText, _ := net.SplitHostPort(targetListener.Addr().String())
	var targetPort uint16
	_, _ = fmt.Sscanf(targetPortText, "%d", &targetPort)
	name := "localhost"
	request := make([]byte, 7+len(name))
	request[0], request[1], request[3], request[4] = 5, 1, 3, byte(len(name))
	copy(request[5:], name)
	binary.BigEndian.PutUint16(request[len(request)-2:], targetPort)
	if _, err := application.Write(request); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(application, reply); err != nil || reply[1] != 0 {
		t.Fatalf("SOCKS reply = %v, %v", reply, err)
	}
	payload := []byte("status-live-flow")
	if _, err := application.Write(payload); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(application, received); err != nil || string(received) != string(payload) {
		t.Fatalf("round trip = %q, %v", received, err)
	}

	waitFor(t, 2*time.Second, func() bool {
		clientSnapshot := client.statusRepository.Snapshot()
		serverSnapshot := server.statusRepository.Snapshot()
		return len(clientSnapshot.Interfaces) != 0 && len(clientSnapshot.Sessions) == 1 && len(clientSnapshot.Flows) == 1 &&
			len(serverSnapshot.Sessions) == 1 && len(serverSnapshot.Flows) == 1 &&
			clientSnapshot.Counters.FramesSent != 0 && serverSnapshot.Counters.FramesReceived != 0 &&
			clientSnapshot.Sessions[0].Quality.SmoothedRTTMicros != 0 &&
			serverSnapshot.Sessions[0].Quality.SmoothedRTTMicros != 0
	}, "live runtime status")
	for _, address := range []string{clientStatusAddress, serverStatusAddress} {
		response, err := http.Get("http://" + address + "/api/v1/flows")
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("status response = %d, %v", response.StatusCode, readErr)
		}
		if strings.Contains(string(body), name) || len(body) > 256<<10 {
			t.Fatalf("unsafe status response: %s", body)
		}
	}
}

func decodeServerForDaemonTest(t *testing.T, relayAddress string) config.Server {
	t.Helper()
	value, err := config.DecodeServer([]byte(fmt.Sprintf(`transport: {type: tcp, listen: %q}
principals:
  - id: client-01
    psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
deadlines: {drain_cleanup: "1s"}
`, relayAddress)))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func decodeClientForDaemonTest(t *testing.T, relayAddress, socksAddress string) config.Client {
	t.Helper()
	value, err := config.DecodeClient([]byte(fmt.Sprintf(`socks_listen: %q
socks_auth: {username: %q, password: %q}
transport: {type: tcp, address: %q}
delivery: {mode: redundant}
interfaces: {include: ["lo"]}
principal_id: client-01
psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
limits: {auth_in_progress: 1}
deadlines: {drain_cleanup: "1s"}
`, socksAddress, daemonSOCKSUsername, daemonSOCKSPassword, relayAddress)))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func decodeServerStatusForDaemonTest(t *testing.T, relayAddress, statusAddress string) config.Server {
	t.Helper()
	value, err := config.DecodeServer([]byte(fmt.Sprintf(`transport: {type: tcp, listen: %q}
status: {enabled: true, listen: %q}
principals:
  - id: client-01
    psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
deadlines: {drain_cleanup: "1s"}
`, relayAddress, statusAddress)))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func decodeClientStatusForDaemonTest(t *testing.T, relayAddress, socksAddress, statusAddress string) config.Client {
	t.Helper()
	value, err := config.DecodeClient([]byte(fmt.Sprintf(`socks_listen: %q
socks_auth: {username: %q, password: %q}
transport: {type: tcp, address: %q}
delivery: {mode: redundant}
interfaces: {include: ["lo"]}
principal_id: client-01
psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
status: {enabled: true, listen: %q}
limits: {auth_in_progress: 1}
deadlines: {drain_cleanup: "1s"}
`, socksAddress, daemonSOCKSUsername, daemonSOCKSPassword, relayAddress, statusAddress)))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func writeSOCKSAuthentication(t *testing.T, connection net.Conn, username, password string, wantAccepted bool) {
	t.Helper()
	if _, err := connection.Write([]byte{5, 1, 2}); err != nil {
		t.Fatal(err)
	}
	method := make([]byte, 2)
	if _, err := io.ReadFull(connection, method); err != nil || method[0] != 5 || method[1] != 2 {
		t.Fatalf("SOCKS authentication method = %v, %v", method, err)
	}
	authentication := []byte{1, byte(len(username))}
	authentication = append(authentication, username...)
	authentication = append(authentication, byte(len(password)))
	authentication = append(authentication, password...)
	if _, err := connection.Write(authentication); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(connection, reply); err != nil || reply[0] != 1 || (reply[1] == 0) != wantAccepted {
		t.Fatalf("SOCKS authentication reply = %v, %v", reply, err)
	}
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitFor(t *testing.T, maximum time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(maximum)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
