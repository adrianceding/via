//go:build linux

package transport

import (
	"errors"
	"net"
	"syscall"
	"testing"
)

func TestSetDialerInterfaceLinux(t *testing.T) {
	dialer := &net.Dialer{}
	setDialerInterface(dialer, "")
	if dialer.Control != nil {
		t.Fatal("empty interface installed a control callback")
	}
	setDialerInterface(dialer, "lo")
	if dialer.Control == nil {
		t.Fatal("interface did not install a control callback")
	}
	want := errors.New("raw control failed")
	if err := dialer.Control("tcp4", "127.0.0.1:1", failingRawConnection{err: want}); !errors.Is(err, want) {
		t.Fatalf("control error = %v, want %v", err, want)
	}
}

func TestTCPDialRejectsMissingBoundInterfaceLinux(t *testing.T) {
	factory, err := NewTCPFactory(DefaultTCPConfig())
	if err != nil {
		t.Fatal(err)
	}
	dialer, err := factory.NewDialer(DialOptions{
		RemoteEndpoint: "127.0.0.1:1", LocalEndpoint: "127.0.0.1:0",
		InterfaceName: "via-missing", QueueLimits: V1QueueLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := dialer.Dial(t.Context())
	if connection != nil {
		_ = connection.Close()
		t.Fatal("missing interface unexpectedly connected")
	}
	if err == nil {
		t.Fatal("missing interface was accepted")
	}
}

type failingRawConnection struct{ err error }

func (connection failingRawConnection) Control(func(uintptr)) error   { return connection.err }
func (connection failingRawConnection) Read(func(uintptr) bool) error { return connection.err }
func (connection failingRawConnection) Write(func(uintptr) bool) error {
	return connection.err
}

var _ syscall.RawConn = failingRawConnection{}
