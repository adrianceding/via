package daemon

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/adrianceding/via/internal/protocol"
	statusapi "github.com/adrianceding/via/internal/status"
)

type simulationTargetResult struct {
	data []byte
	err  error
}

func closeSimulationWrite(t *testing.T, connection net.Conn) {
	t.Helper()
	tcp, ok := connection.(*net.TCPConn)
	if !ok {
		t.Fatal("simulation application connection is not TCP")
	}
	if err := tcp.CloseWrite(); err != nil {
		t.Fatal(err)
	}
}

func readSimulationToEOF(t *testing.T, connection net.Conn) []byte {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(15 * time.Second))
	data, err := io.ReadAll(connection)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Time{})
	return data
}

func TestSimulatedFullChainDirectionalTargetTraffic(t *testing.T) {
	t.Run("upload-and-empty-response", func(t *testing.T) {
		result := make(chan simulationTargetResult, 1)
		harness := newSimulationHarnessWithTarget(t, protocol.DeliveryRedundant, protocol.PathNone,
			func(_ *simulationHarness, connection net.Conn) {
				data, err := io.ReadAll(connection)
				result <- simulationTargetResult{data: data, err: err}
			})
		defer harness.close()
		application := harness.openApplication()
		defer application.Close()
		harness.waitAttachments(2)
		payload := bytes.Repeat([]byte("upload-only|"), 16<<10)
		if err := writeAll(application, payload); err != nil {
			t.Fatal(err)
		}
		closeSimulationWrite(t, application)
		if response := readSimulationToEOF(t, application); len(response) != 0 {
			t.Fatalf("empty response bytes = %d", len(response))
		}
		target := <-result
		if target.err != nil || !bytes.Equal(target.data, payload) {
			t.Fatalf("target upload bytes=%d/%d err=%v", len(target.data), len(payload), target.err)
		}
		if harness.targetTotal.Load() != 1 {
			t.Fatalf("target connections = %d", harness.targetTotal.Load())
		}
	})

	t.Run("download-only", func(t *testing.T) {
		payload := bytes.Repeat([]byte("download-only|"), 16<<10)
		result := make(chan error, 1)
		harness := newSimulationHarnessWithTarget(t, protocol.DeliveryAdaptive, protocol.PathDistributed,
			func(_ *simulationHarness, connection net.Conn) {
				err := writeAll(connection, payload)
				if tcp, ok := connection.(*net.TCPConn); ok && err == nil {
					err = tcp.CloseWrite()
				}
				if _, readErr := io.Copy(io.Discard, connection); err == nil {
					err = readErr
				}
				result <- err
			})
		defer harness.close()
		application := harness.openApplication()
		defer application.Close()
		harness.waitAttachments(2)
		closeSimulationWrite(t, application)
		if received := readSimulationToEOF(t, application); !bytes.Equal(received, payload) {
			t.Fatalf("download bytes = %d/%d", len(received), len(payload))
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if harness.targetTotal.Load() != 1 {
			t.Fatalf("target connections = %d", harness.targetTotal.Load())
		}
	})
}

func TestSimulatedFullChainTargetDelayAndRate(t *testing.T) {
	t.Run("delayed-response", func(t *testing.T) {
		ready := make(chan struct{})
		release := make(chan struct{})
		payload := []byte("released response")
		harness := newSimulationHarnessWithTarget(t, protocol.DeliveryRedundant, protocol.PathNone,
			func(_ *simulationHarness, connection net.Conn) {
				close(ready)
				<-release
				_ = writeAll(connection, payload)
				if tcp, ok := connection.(*net.TCPConn); ok {
					_ = tcp.CloseWrite()
				}
				_, _ = io.Copy(io.Discard, connection)
			})
		defer harness.close()
		application := harness.openApplication()
		defer application.Close()
		harness.waitAttachments(2)
		closeSimulationWrite(t, application)
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("delayed target did not become ready")
		}
		close(release)
		if received := readSimulationToEOF(t, application); !bytes.Equal(received, payload) {
			t.Fatalf("delayed response = %q", received)
		}
	})

	t.Run("explicit-rate-budget", func(t *testing.T) {
		ready := make(chan struct{})
		permits := make(chan struct{})
		chunks := [][]byte{[]byte("first-rate-chunk"), []byte("second-rate-chunk")}
		harness := newSimulationHarnessWithTarget(t, protocol.DeliveryAdaptive, protocol.PathFastest,
			func(_ *simulationHarness, connection net.Conn) {
				close(ready)
				for _, chunk := range chunks {
					<-permits
					if writeAll(connection, chunk) != nil {
						return
					}
				}
				if tcp, ok := connection.(*net.TCPConn); ok {
					_ = tcp.CloseWrite()
				}
				_, _ = io.Copy(io.Discard, connection)
			})
		defer harness.close()
		application := harness.openApplication()
		defer application.Close()
		harness.waitAttachments(2)
		closeSimulationWrite(t, application)
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("rate target did not become ready")
		}
		permits <- struct{}{}
		first := make([]byte, len(chunks[0]))
		if _, err := io.ReadFull(application, first); err != nil || !bytes.Equal(first, chunks[0]) {
			t.Fatalf("first rate chunk = %q, %v", first, err)
		}
		permits <- struct{}{}
		second := readSimulationToEOF(t, application)
		if !bytes.Equal(second, chunks[1]) {
			t.Fatalf("second rate chunk = %q", second)
		}
	})
}

func TestSimulatedFullChainTargetResetHasScopedReasons(t *testing.T) {
	accepted := make(chan struct{})
	reset := make(chan struct{})
	harness := newSimulationHarnessWithTarget(t, protocol.DeliveryRedundant, protocol.PathNone,
		func(_ *simulationHarness, connection net.Conn) {
			close(accepted)
			<-reset
			if tcp, ok := connection.(*net.TCPConn); ok {
				_ = tcp.SetLinger(0)
			}
		})
	defer harness.close()
	defer func() {
		if t.Failed() {
			t.Logf("client status: %#v", harness.client.statusRepository.Snapshot())
			t.Logf("server status: %#v", harness.server.statusRepository.Snapshot())
			for _, address := range []netip.Addr{simulationAddressA, simulationAddressB} {
				for _, direction := range []simulatedDirection{simulatedUplink, simulatedDownlink} {
					controller := harness.network.controller(address, direction)
					t.Logf("frames address=%s direction=%d DATA=%d ACK=%d FIN=%d FIN_ACK=%d RESET=%d",
						address, direction, controller.count(protocol.TypeData), controller.count(protocol.TypeACK),
						controller.count(protocol.TypeFIN), controller.count(protocol.TypeFINACK), controller.count(protocol.TypeReset))
				}
			}
		}
	}()
	application := harness.openApplication()
	defer application.Close()
	harness.waitAttachments(2)
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("reset target was not accepted")
	}
	close(reset)
	_ = application.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, readErr := application.Read(make([]byte, 1))
	if readErr == nil {
		t.Fatal("target reset left application readable")
	}
	if !errors.Is(readErr, io.EOF) {
		var networkError net.Error
		if !errors.As(readErr, &networkError) {
			t.Fatalf("target reset application error = %v", readErr)
		}
	}
	waitFor(t, 5*time.Second, func() bool {
		client := harness.client.statusRepository.Snapshot()
		server := harness.server.statusRepository.Snapshot()
		return len(client.Terminals) == 1 && len(server.Terminals) == 1 &&
			client.Terminals[0].State == statusapi.FlowReset && client.Terminals[0].Reason == statusapi.ReasonRemoteReset &&
			server.Terminals[0].State == statusapi.FlowReset && server.Terminals[0].Reason == statusapi.ReasonLocalIOFailure
	}, "target reset terminal reasons")
	if harness.targetTotal.Load() != 1 {
		t.Fatalf("target connections = %d", harness.targetTotal.Load())
	}
}
