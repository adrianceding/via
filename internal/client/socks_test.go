package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
	"github.com/adrianceding/via/internal/socks5"
)

const (
	testSOCKSUsername = "via-app"
	testSOCKSPassword = "test-password"
)

func TestSOCKSCoordinatorEnablesApplicationOnlyAfterPublishedFlowReply(t *testing.T) {
	coordinator := newSOCKSCoordinatorForTest(t)
	actions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSStart})
	if err != nil {
		t.Fatal(err)
	}
	greeting := requireSOCKSAction(t, actions, SOCKSActionReadGreeting)
	deadline := requireSOCKSAction(t, actions, SOCKSActionArmDeadline)
	actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSGreetingResult, Generation: greeting.Generation, Accepted: true})
	if err != nil || countSOCKSActions(actions, SOCKSActionCancelDeadline) != 1 {
		t.Fatalf("greeting result = %#v, %v", actions, err)
	}
	if requireSOCKSAction(t, actions, SOCKSActionCancelDeadline).Generation != deadline.Generation {
		t.Fatal("greeting deadline cancellation generation changed")
	}
	methodWrite := requireSOCKSAction(t, actions, SOCKSActionWrite)
	methodDeadline := requireSOCKSAction(t, actions, SOCKSActionArmDeadline)
	if methodDeadline.After != SOCKSWriteTimeout {
		t.Fatalf("method write deadline = %s", methodDeadline.After)
	}
	if got := methodWrite.CopyData(); !bytes.Equal(got, []byte{5, 2}) {
		t.Fatalf("method reply = %x", got)
	}
	actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: methodWrite.Generation})
	if err != nil || requireSOCKSAction(t, actions, SOCKSActionCancelDeadline).Generation != methodDeadline.Generation {
		t.Fatalf("method write completion = %#v, %v", actions, err)
	}
	authenticationRead := requireSOCKSAction(t, actions, SOCKSActionReadAuthentication)
	actions, err = coordinator.Handle(SOCKSEvent{
		Kind: SOCKSAuthenticationResult, Generation: authenticationRead.Generation, Accepted: true,
	})
	if err != nil || !bytes.Equal(requireSOCKSAction(t, actions, SOCKSActionWrite).CopyData(), []byte{1, 0}) {
		t.Fatalf("authentication result = %#v, %v", actions, err)
	}
	authenticationWrite := requireSOCKSAction(t, actions, SOCKSActionWrite)
	actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: authenticationWrite.Generation})
	if err != nil {
		t.Fatal(err)
	}
	requestRead := requireSOCKSAction(t, actions, SOCKSActionReadRequest)
	target := protocol.Target{DNSName: "example.test", Port: 443}
	actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSRequestResult, Generation: requestRead.Generation, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if requireSOCKSAction(t, actions, SOCKSActionStartFlow).Target != target {
		t.Fatalf("start target = %#v", actions)
	}
	if coordinator.Snapshot().ApplicationEnabled || countSOCKSActions(actions, SOCKSActionEnableApplication) != 0 {
		t.Fatal("application enabled before OPEN/JOIN publication")
	}

	actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSFlowResult, OpenResult: protocol.OpenSuccess})
	if err != nil {
		t.Fatal(err)
	}
	successWrite := requireSOCKSAction(t, actions, SOCKSActionWrite)
	successDeadline := requireSOCKSAction(t, actions, SOCKSActionArmDeadline)
	if successWrite.CopyData()[1] != byte(socks5.ReplySucceeded) || coordinator.Snapshot().ApplicationEnabled {
		t.Fatal("application enabled before SOCKS success was written")
	}
	actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: successWrite.Generation})
	if err != nil || countSOCKSActions(actions, SOCKSActionEnableApplication) != 1 ||
		requireSOCKSAction(t, actions, SOCKSActionCancelDeadline).Generation != successDeadline.Generation ||
		!coordinator.Snapshot().ApplicationEnabled {
		t.Fatalf("success completion = %#v, %#v, %v", actions, coordinator.Snapshot(), err)
	}
}

func TestSOCKSCoordinatorSkipsAuthenticationWhenCredentialsAreOmitted(t *testing.T) {
	coordinator, err := NewSOCKSCoordinator(5*time.Second, 5*time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	start, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSStart})
	greeting := requireSOCKSAction(t, start, SOCKSActionReadGreeting)
	actions, err := coordinator.Handle(SOCKSEvent{
		Kind: SOCKSGreetingResult, Generation: greeting.Generation, Accepted: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	method := requireSOCKSAction(t, actions, SOCKSActionWrite)
	if !bytes.Equal(method.CopyData(), []byte{5, 0}) {
		t.Fatalf("method reply = %x", method.CopyData())
	}
	actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: method.Generation})
	if err != nil || countSOCKSActions(actions, SOCKSActionReadAuthentication) != 0 ||
		countSOCKSActions(actions, SOCKSActionReadRequest) != 1 {
		t.Fatalf("no-auth continuation = %#v, %v", actions, err)
	}
}

func TestSOCKSCoordinatorFailureMappingsAndDeadlines(t *testing.T) {
	t.Run("no method", func(t *testing.T) {
		coordinator := newSOCKSCoordinatorForTest(t)
		start, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSStart})
		read := requireSOCKSAction(t, start, SOCKSActionReadGreeting)
		actions, err := coordinator.Handle(SOCKSEvent{
			Kind: SOCKSGreetingResult, Generation: read.Generation, Err: socks5.ErrNoAcceptableMethod,
		})
		if err != nil || !bytes.Equal(requireSOCKSAction(t, actions, SOCKSActionWrite).CopyData(), []byte{5, 0xff}) {
			t.Fatalf("no-method actions = %#v, %v", actions, err)
		}
	})

	t.Run("request command", func(t *testing.T) {
		coordinator, request := coordinatorAwaitingSOCKSRequest(t)
		actions, err := coordinator.Handle(SOCKSEvent{
			Kind: SOCKSRequestResult, Generation: request.Generation, Err: socks5.ErrCommandUnsupported,
		})
		if err != nil || requireSOCKSAction(t, actions, SOCKSActionWrite).CopyData()[1] != byte(socks5.ReplyCommandNotSupported) {
			t.Fatalf("command failure = %#v, %v", actions, err)
		}
	})

	t.Run("authentication failure", func(t *testing.T) {
		coordinator, authentication := coordinatorAwaitingSOCKSAuthentication(t)
		actions, err := coordinator.Handle(SOCKSEvent{
			Kind: SOCKSAuthenticationResult, Generation: authentication.Generation, Accepted: false,
		})
		if err != nil || !bytes.Equal(requireSOCKSAction(t, actions, SOCKSActionWrite).CopyData(), []byte{1, 1}) {
			t.Fatalf("authentication failure = %#v, %v", actions, err)
		}
		write := requireSOCKSAction(t, actions, SOCKSActionWrite)
		actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: write.Generation})
		if err != nil || coordinator.Snapshot().State != SOCKSClosed || countSOCKSActions(actions, SOCKSActionCloseConnection) != 1 {
			t.Fatalf("authentication close = %#v, %#v, %v", actions, coordinator.Snapshot(), err)
		}
	})

	t.Run("open failure", func(t *testing.T) {
		coordinator := coordinatorWaitingFlow(t)
		actions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSFlowResult, OpenResult: protocol.OpenConnectFailed})
		if err != nil || requireSOCKSAction(t, actions, SOCKSActionWrite).CopyData()[1] != byte(socks5.ReplyConnectionRefused) {
			t.Fatalf("OPEN failure = %#v, %v", actions, err)
		}
	})

	t.Run("stale and current deadline", func(t *testing.T) {
		coordinator := newSOCKSCoordinatorForTest(t)
		actions, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSStart})
		deadline := requireSOCKSAction(t, actions, SOCKSActionArmDeadline)
		if actions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSDeadline, Generation: deadline.Generation + 1}); err != nil || len(actions) != 0 {
			t.Fatalf("stale deadline = %#v, %v", actions, err)
		}
		actions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSDeadline, Generation: deadline.Generation})
		if err != nil || countSOCKSActions(actions, SOCKSActionCloseConnection) != 1 || coordinator.Snapshot().State != SOCKSClosed {
			t.Fatalf("current deadline = %#v, %#v, %v", actions, coordinator.Snapshot(), err)
		}
	})

	t.Run("write deadline", func(t *testing.T) {
		coordinator := newSOCKSCoordinatorForTest(t)
		start, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSStart})
		read := requireSOCKSAction(t, start, SOCKSActionReadGreeting)
		actions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSGreetingResult, Generation: read.Generation, Accepted: true})
		if err != nil {
			t.Fatal(err)
		}
		write := requireSOCKSAction(t, actions, SOCKSActionWrite)
		deadline := requireSOCKSAction(t, actions, SOCKSActionArmDeadline)
		if deadline.After != SOCKSWriteTimeout {
			t.Fatalf("write deadline = %s", deadline.After)
		}
		if stale, staleErr := coordinator.Handle(SOCKSEvent{Kind: SOCKSDeadline, Generation: deadline.Generation + 1}); staleErr != nil || len(stale) != 0 {
			t.Fatalf("stale write deadline = %#v, %v", stale, staleErr)
		}
		actions, err = coordinator.Handle(SOCKSEvent{Kind: SOCKSDeadline, Generation: deadline.Generation})
		if err != nil || countSOCKSActions(actions, SOCKSActionCloseConnection) != 1 || coordinator.Snapshot().State != SOCKSClosed {
			t.Fatalf("write deadline result = %#v, %#v, %v", actions, coordinator.Snapshot(), err)
		}
		if late, lateErr := coordinator.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: write.Generation}); lateErr != nil || len(late) != 0 {
			t.Fatalf("late write result = %#v, %v", late, lateErr)
		}
	})
}

func TestSOCKSPeerCloseCancelsDeadlineOrActiveFlow(t *testing.T) {
	t.Run("during greeting", func(t *testing.T) {
		coordinator := newSOCKSCoordinatorForTest(t)
		start, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSStart})
		deadline := requireSOCKSAction(t, start, SOCKSActionArmDeadline)
		actions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSPeerClosed})
		if err != nil || countSOCKSActions(actions, SOCKSActionCancelDeadline) != 1 ||
			requireSOCKSAction(t, actions, SOCKSActionCancelDeadline).Generation != deadline.Generation ||
			countSOCKSActions(actions, SOCKSActionCloseConnection) != 0 {
			t.Fatalf("greeting peer close = %#v, %v", actions, err)
		}
	})

	t.Run("while flow active", func(t *testing.T) {
		coordinator := coordinatorWaitingFlow(t)
		actions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSPeerClosed})
		if err != nil || countSOCKSActions(actions, SOCKSActionCancelFlow) != 1 ||
			countSOCKSActions(actions, SOCKSActionCloseConnection) != 0 {
			t.Fatalf("active peer close = %#v, %v", actions, err)
		}
	})
}

func TestSOCKSExecutorReadsOnlyHandshakeAndLeavesPipelinedPayload(t *testing.T) {
	clientSide, applicationSide := net.Pipe()
	defer clientSide.Close()
	defer applicationSide.Close()
	executor, err := NewSOCKSExecutor(clientSide, testSOCKSUsername, testSOCKSPassword)
	if err != nil {
		t.Fatal(err)
	}
	encoded := append([]byte{5, 1, 2}, encodeSOCKSAuthentication(testSOCKSUsername, testSOCKSPassword)...)
	encoded = append(encoded, encodeSOCKSRequest("example.test", 443)...)
	encoded = append(encoded, []byte("payload")...)
	writeDone := make(chan error, 1)
	go func() {
		_, err := applicationSide.Write(encoded)
		writeDone <- err
	}()

	greetingEvent, ok, err := executor.Execute(SOCKSAction{Kind: SOCKSActionReadGreeting, Generation: 1})
	if err != nil || !ok || greetingEvent.Err != nil || !greetingEvent.Accepted {
		t.Fatalf("greeting = %#v, %v, %v", greetingEvent, ok, err)
	}
	authenticationEvent, ok, err := executor.Execute(SOCKSAction{Kind: SOCKSActionReadAuthentication, Generation: 2})
	if err != nil || !ok || authenticationEvent.Err != nil || !authenticationEvent.Accepted {
		t.Fatalf("authentication = %#v, %v, %v", authenticationEvent, ok, err)
	}
	requestEvent, ok, err := executor.Execute(SOCKSAction{Kind: SOCKSActionReadRequest, Generation: 3})
	if err != nil || !ok || requestEvent.Err != nil || requestEvent.Target.DNSName != "example.test" {
		t.Fatalf("request = %#v, %v, %v", requestEvent, ok, err)
	}
	remaining := make([]byte, len("payload"))
	_ = clientSide.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := clientSide.Read(remaining); err != nil || string(remaining) != "payload" {
		t.Fatalf("remaining payload = %q, %v", remaining, err)
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pipelined application write did not complete")
	}
}

func TestSOCKSExecutorWritesShortChunksAndGuardsInvalidActions(t *testing.T) {
	connection := &shortSOCKSConnection{maximum: 1}
	executor, _ := NewSOCKSExecutor(connection, testSOCKSUsername, testSOCKSPassword)
	event, ok, err := executor.Execute(SOCKSAction{Kind: SOCKSActionWrite, Generation: 9, data: []byte{5, 0}})
	if err != nil || !ok || event.Err != nil || !bytes.Equal(connection.written, []byte{5, 0}) {
		t.Fatalf("short writes = %#v, %v, %v, %x", event, ok, err, connection.written)
	}
	if _, _, err := executor.Execute(SOCKSAction{Kind: SOCKSActionWrite, Generation: 10, data: make([]byte, 11)}); !errors.Is(err, ErrInvalidSOCKSEvent) {
		t.Fatalf("oversized write error = %v", err)
	}
}

func TestSOCKSCoordinatorArmsDeadlineBeforeBlockingIOAndCloseUnblocksIt(t *testing.T) {
	coordinator := newSOCKSCoordinatorForTest(t)
	actions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSStart})
	if err != nil || len(actions) != 2 || actions[0].Kind != SOCKSActionArmDeadline || actions[1].Kind != SOCKSActionReadGreeting {
		t.Fatalf("greeting action order = %#v, %v", actions, err)
	}

	viaSide, peerSide := net.Pipe()
	defer peerSide.Close()
	executor, err := NewSOCKSExecutor(viaSide, testSOCKSUsername, testSOCKSPassword)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan SOCKSEvent, 1)
	go func() {
		event, _, _ := executor.Execute(actions[1])
		result <- event
	}()

	deadlineActions, err := coordinator.Handle(SOCKSEvent{Kind: SOCKSDeadline, Generation: actions[0].Generation})
	if err != nil {
		t.Fatal(err)
	}
	closeAction := requireSOCKSAction(t, deadlineActions, SOCKSActionCloseConnection)
	if _, _, err := executor.Execute(closeAction); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-result:
		if event.Err == nil {
			t.Fatalf("blocked read returned without close error: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("closing SOCKS connection did not unblock greeting read")
	}
}

func newSOCKSCoordinatorForTest(t *testing.T) *SOCKSCoordinator {
	t.Helper()
	coordinator, err := NewSOCKSCoordinator(5*time.Second, 5*time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func coordinatorAwaitingSOCKSRequest(t *testing.T) (*SOCKSCoordinator, SOCKSAction) {
	t.Helper()
	coordinator, authentication := coordinatorAwaitingSOCKSAuthentication(t)
	authenticationResult, _ := coordinator.Handle(SOCKSEvent{
		Kind: SOCKSAuthenticationResult, Generation: authentication.Generation, Accepted: true,
	})
	authenticationWrite := requireSOCKSAction(t, authenticationResult, SOCKSActionWrite)
	request, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: authenticationWrite.Generation})
	return coordinator, requireSOCKSAction(t, request, SOCKSActionReadRequest)
}

func coordinatorAwaitingSOCKSAuthentication(t *testing.T) (*SOCKSCoordinator, SOCKSAction) {
	t.Helper()
	coordinator := newSOCKSCoordinatorForTest(t)
	start, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSStart})
	greeting := requireSOCKSAction(t, start, SOCKSActionReadGreeting)
	method, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSGreetingResult, Generation: greeting.Generation, Accepted: true})
	methodWrite := requireSOCKSAction(t, method, SOCKSActionWrite)
	authentication, _ := coordinator.Handle(SOCKSEvent{Kind: SOCKSWriteResult, Generation: methodWrite.Generation})
	return coordinator, requireSOCKSAction(t, authentication, SOCKSActionReadAuthentication)
}

func coordinatorWaitingFlow(t *testing.T) *SOCKSCoordinator {
	t.Helper()
	coordinator, request := coordinatorAwaitingSOCKSRequest(t)
	_, err := coordinator.Handle(SOCKSEvent{
		Kind: SOCKSRequestResult, Generation: request.Generation,
		Target: protocol.Target{DNSName: "example.test", Port: 443},
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func requireSOCKSAction(t *testing.T, actions []SOCKSAction, kind SOCKSActionKind) SOCKSAction {
	t.Helper()
	for _, action := range actions {
		if action.Kind == kind {
			return action
		}
	}
	t.Fatalf("missing SOCKS action %v in %#v", kind, actions)
	return SOCKSAction{}
}

func countSOCKSActions(actions []SOCKSAction, kind SOCKSActionKind) int {
	count := 0
	for _, action := range actions {
		if action.Kind == kind {
			count++
		}
	}
	return count
}

func encodeSOCKSRequest(name string, port uint16) []byte {
	encoded := make([]byte, 7+len(name))
	encoded[0] = 5
	encoded[1] = 1
	encoded[3] = byte(protocol.AddressDNS)
	encoded[4] = byte(len(name))
	copy(encoded[5:], name)
	binary.BigEndian.PutUint16(encoded[len(encoded)-2:], port)
	return encoded
}

func encodeSOCKSAuthentication(username, password string) []byte {
	encoded := []byte{1, byte(len(username))}
	encoded = append(encoded, username...)
	encoded = append(encoded, byte(len(password)))
	return append(encoded, password...)
}

type shortSOCKSConnection struct {
	written []byte
	maximum int
}

func (*shortSOCKSConnection) Read([]byte) (int, error) { return 0, errors.New("not scripted") }
func (connection *shortSOCKSConnection) Write(data []byte) (int, error) {
	n := min(len(data), connection.maximum)
	connection.written = append(connection.written, data[:n]...)
	return n, nil
}
func (*shortSOCKSConnection) Close() error                     { return nil }
func (*shortSOCKSConnection) LocalAddr() net.Addr              { return nil }
func (*shortSOCKSConnection) RemoteAddr() net.Addr             { return nil }
func (*shortSOCKSConnection) SetDeadline(time.Time) error      { return nil }
func (*shortSOCKSConnection) SetReadDeadline(time.Time) error  { return nil }
func (*shortSOCKSConnection) SetWriteDeadline(time.Time) error { return nil }
