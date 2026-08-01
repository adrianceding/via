package transport

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestValidName(t *testing.T) {
	valid := []string{"a", TCPName, "http2", "direct-tcp", "x1-y2", strings.Repeat("a", MaxRegisteredNameLength)}
	for _, name := range valid {
		if !ValidName(name) {
			t.Errorf("ValidName(%q) = false", name)
		}
	}

	invalid := []string{
		"", "TCP", "1tcp", "tcp_2", "tcp.2", "tcp/2", " tcp", "tcp ",
		"-tcp", "tcp-", "tcp--direct", "tCP", "tránsport", strings.Repeat("a", MaxRegisteredNameLength+1),
	}
	for _, name := range invalid {
		if ValidName(name) {
			t.Errorf("ValidName(%q) = true", name)
		}
	}
}

func TestRegistryRegisterLookupAndNames(t *testing.T) {
	registry := NewRegistry()
	tcp := newFakeFactory(t, 1024)
	http2 := newFakeFactory(t, 2048)
	if err := registry.Register(TCPName, tcp); err != nil {
		t.Fatalf("Register(tcp) error = %v", err)
	}
	if err := registry.Register("http2", http2); err != nil {
		t.Fatalf("Register(http2) error = %v", err)
	}

	got, err := registry.Lookup(TCPName)
	if err != nil {
		t.Fatalf("Lookup(tcp) error = %v", err)
	}
	if got != tcp {
		t.Fatalf("Lookup(tcp) = %p, want %p", got, tcp)
	}
	if names := registry.Names(); !reflect.DeepEqual(names, []string{"http2", "tcp"}) {
		t.Fatalf("Names() = %q", names)
	}
	names := registry.Names()
	names[0] = "changed"
	if got := registry.Names(); !reflect.DeepEqual(got, []string{"http2", "tcp"}) {
		t.Fatalf("Names() changed through returned slice: %q", got)
	}
}

func TestRegistryRejectsInvalidDuplicateAndUnknown(t *testing.T) {
	registry := NewRegistry()
	first := newFakeFactory(t, 1024)
	second := newFakeFactory(t, 2048)
	if err := registry.Register(TCPName, first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(TCPName, second); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate Register() error = %v, want ErrDuplicate", err)
	}
	got, err := registry.Lookup(TCPName)
	if err != nil || got != first {
		t.Fatalf("Lookup after duplicate = (%p, %v), want first factory", got, err)
	}
	if _, err := registry.Lookup("udp"); !errors.Is(err, ErrUnknown) {
		t.Fatalf("Lookup(unknown) error = %v, want ErrUnknown", err)
	}
	if _, err := registry.Lookup("UDP"); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("Lookup(invalid) error = %v, want ErrInvalidName", err)
	}
	if err := registry.Register("UDP", second); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("Register(invalid) error = %v, want ErrInvalidName", err)
	}
}

func TestRegistryRejectsNilAndInvalidFactories(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(TCPName, nil); !errors.Is(err, ErrNilFactory) {
		t.Fatalf("Register(nil) error = %v, want ErrNilFactory", err)
	}
	var typedNil *fakeFactory
	if err := registry.Register(TCPName, typedNil); !errors.Is(err, ErrNilFactory) {
		t.Fatalf("Register(typed nil) error = %v, want ErrNilFactory", err)
	}
	if err := registry.Register(TCPName, &fakeFactory{}); !errors.Is(err, ErrInvalidCapabilities) {
		t.Fatalf("Register(invalid capabilities) error = %v, want ErrInvalidCapabilities", err)
	}

	var nilRegistry *Registry
	if err := nilRegistry.Register(TCPName, newFakeFactory(t, 1)); !errors.Is(err, ErrNilRegistry) {
		t.Fatalf("nil Register() error = %v, want ErrNilRegistry", err)
	}
	if _, err := nilRegistry.Lookup(TCPName); !errors.Is(err, ErrNilRegistry) {
		t.Fatalf("nil Lookup() error = %v, want ErrNilRegistry", err)
	}
	if names := nilRegistry.Names(); names != nil {
		t.Fatalf("nil Names() = %v, want nil", names)
	}
}

func TestRegistryConcurrentRegistrationAndLookup(t *testing.T) {
	registry := NewRegistry()
	const count = 64
	var wait sync.WaitGroup
	wait.Add(count)
	for index := 0; index < count; index++ {
		index := index
		go func() {
			defer wait.Done()
			name := fmt.Sprintf("t%d", index)
			if err := registry.Register(name, newFakeFactory(t, uint32(index+1))); err != nil {
				t.Errorf("Register(%q) error = %v", name, err)
			}
		}()
	}
	wait.Wait()

	wait.Add(count)
	for index := 0; index < count; index++ {
		index := index
		go func() {
			defer wait.Done()
			name := fmt.Sprintf("t%d", index)
			factory, err := registry.Lookup(name)
			if err != nil {
				t.Errorf("Lookup(%q) error = %v", name, err)
				return
			}
			if factory.Capabilities().MaxEncodedFrame() != uint32(index+1) {
				t.Errorf("Lookup(%q) maximum frame = %d", name, factory.Capabilities().MaxEncodedFrame())
			}
		}()
	}
	wait.Wait()
	if names := registry.Names(); len(names) != count {
		t.Fatalf("len(Names()) = %d, want %d", len(names), count)
	}
}

type fakeFactory struct {
	capabilities Capabilities
}

func newFakeFactory(t *testing.T, maximum uint32) *fakeFactory {
	t.Helper()
	capabilities, err := NewCapabilities(CapabilitySpec{MaxEncodedFrame: maximum})
	if err != nil {
		t.Fatalf("NewCapabilities() error = %v", err)
	}
	return &fakeFactory{capabilities: capabilities}
}

func (factory *fakeFactory) Capabilities() Capabilities {
	return factory.capabilities
}

func (factory *fakeFactory) NewDialer(DialOptions) (Dialer, error) {
	return nil, errors.New("not implemented")
}

func (factory *fakeFactory) NewListener(ListenOptions) (Listener, error) {
	return nil, errors.New("not implemented")
}

type fakeConnection struct{}

func (fakeConnection) Capabilities() Capabilities                     { return Capabilities{} }
func (fakeConnection) QueueLimits() QueueLimits                       { return QueueLimits{} }
func (fakeConnection) LocalEndpoint() string                          { return "127.0.0.1:1" }
func (fakeConnection) RemoteEndpoint() string                         { return "127.0.0.1:2" }
func (fakeConnection) ReadFrame(context.Context) ([]byte, error)      { return nil, ErrClosed }
func (fakeConnection) WriteFrame(context.Context, WriteRequest) error { return ErrClosed }
func (fakeConnection) CloseWrite() error                              { return nil }
func (fakeConnection) Close() error                                   { return nil }

type fakeDialer struct{}

func (fakeDialer) Dial(context.Context) (Connection, error) { return fakeConnection{}, nil }

type fakeListener struct{}

func (fakeListener) Accept(context.Context) (Connection, error) { return fakeConnection{}, nil }
func (fakeListener) Close() error                               { return nil }

var (
	_ Factory    = (*fakeFactory)(nil)
	_ Connection = fakeConnection{}
	_ Dialer     = fakeDialer{}
	_ Listener   = fakeListener{}
)
