package daemon

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestDaemonSOCKSProxiesTLSHTTPKeepAliveAndLargeResponses(t *testing.T) {
	payload := bytes.Repeat([]byte("via-http-response|"), 128<<10)
	var requests atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		writer.Header().Set("Content-Type", "application/octet-stream")
		writer.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = writer.Write(payload)
	}))
	defer target.Close()

	relayAddress := reserveAddress(t)
	socksAddress := reserveAddress(t)
	server, err := newServerDaemon(decodeServerForDaemonTest(t, relayAddress))
	if err != nil {
		t.Fatal(err)
	}
	client, err := newClientDaemon(decodeClientForDaemonTest(t, relayAddress, socksAddress))
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
	defer func() {
		cancelClient()
		cancelServer()
		for name, result := range map[string]<-chan error{"client": clientResult, "server": serverResult} {
			select {
			case err := <-result:
				if err != nil {
					t.Errorf("%s shutdown: %v", name, err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("%s did not stop", name)
			}
		}
	}()
	waitFor(t, 5*time.Second, func() bool { return len(client.readySessions()) == 1 }, "HTTP proxy session")

	transport := target.Client().Transport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, _, address string) (net.Conn, error) {
		return dialAuthenticatedSOCKS(ctx, socksAddress, address, daemonSOCKSUsername, daemonSOCKSPassword)
	}
	transport.DisableKeepAlives = false
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 20 * time.Second}
	for index := 0; index < 3; index++ {
		response, err := httpClient.Get(target.URL + "/page?request=" + strconv.Itoa(index))
		if err != nil {
			t.Fatalf("HTTP request %d: %v", index, err)
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || !bytes.Equal(body, payload) {
			t.Fatalf("HTTP response %d: status=%d bytes=%d/%d read=%v close=%v",
				index, response.StatusCode, len(body), len(payload), readErr, closeErr)
		}
	}
	if requests.Load() != 3 {
		t.Fatalf("target requests = %d", requests.Load())
	}
}

func dialAuthenticatedSOCKS(ctx context.Context, socksAddress, targetAddress, username, password string) (net.Conn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", socksAddress)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (net.Conn, error) {
		_ = connection.Close()
		return nil, err
	}
	deadline := time.Now().Add(10 * time.Second)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	_ = connection.SetDeadline(deadline)
	if err := writeAll(connection, []byte{5, 1, 2}); err != nil {
		return fail(err)
	}
	var reply [2]byte
	if _, err := io.ReadFull(connection, reply[:]); err != nil || reply != [2]byte{5, 2} {
		return fail(fmt.Errorf("SOCKS method reply %v: %w", reply, err))
	}
	authentication := []byte{1, byte(len(username))}
	authentication = append(authentication, username...)
	authentication = append(authentication, byte(len(password)))
	authentication = append(authentication, password...)
	if err := writeAll(connection, authentication); err != nil {
		return fail(err)
	}
	if _, err := io.ReadFull(connection, reply[:]); err != nil || reply != [2]byte{1, 0} {
		return fail(fmt.Errorf("SOCKS authentication reply %v: %w", reply, err))
	}
	request, err := socksConnectRequest(targetAddress)
	if err != nil {
		return fail(err)
	}
	if err := writeAll(connection, request); err != nil {
		return fail(err)
	}
	if err := readSOCKSConnectReply(connection); err != nil {
		return fail(err)
	}
	_ = connection.SetDeadline(time.Time{})
	return connection, nil
}

func socksConnectRequest(address string) ([]byte, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return nil, fmt.Errorf("invalid target port %q", portText)
	}
	request := []byte{5, 1, 0}
	if ip := net.ParseIP(host); ip != nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			request = append(request, 1)
			request = append(request, ipv4...)
		} else {
			request = append(request, 4)
			request = append(request, ip.To16()...)
		}
	} else {
		if len(host) < 1 || len(host) > 255 {
			return nil, fmt.Errorf("invalid target host %q", host)
		}
		request = append(request, 3, byte(len(host)))
		request = append(request, host...)
	}
	request = binary.BigEndian.AppendUint16(request, uint16(port))
	return request, nil
}

func readSOCKSConnectReply(reader io.Reader) error {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	if header[0] != 5 || header[1] != 0 || header[2] != 0 {
		return fmt.Errorf("SOCKS CONNECT reply %v", header)
	}
	addressBytes := 0
	switch header[3] {
	case 1:
		addressBytes = 4
	case 4:
		addressBytes = 16
	case 3:
		var size [1]byte
		if _, err := io.ReadFull(reader, size[:]); err != nil {
			return err
		}
		addressBytes = int(size[0])
	default:
		return fmt.Errorf("SOCKS CONNECT address type %d", header[3])
	}
	_, err := io.CopyN(io.Discard, reader, int64(addressBytes+2))
	return err
}
