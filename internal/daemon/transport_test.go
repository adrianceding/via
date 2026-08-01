package daemon

import (
	"errors"
	"reflect"
	"testing"

	"github.com/adrianceding/via/internal/transport"
)

func TestRuntimeTransportRegistryOnlyEnablesTCPV1(t *testing.T) {
	registry, err := newTransportRegistry(transport.DefaultTCPConfig().FrameTotal, transport.DefaultTCPConfig().FrameNoProgress)
	if err != nil {
		t.Fatal(err)
	}
	if names := registry.Names(); !reflect.DeepEqual(names, []string{transport.TCPName}) {
		t.Fatalf("registered transports = %q, want [tcp]", names)
	}
	factory, err := lookupTransportFactory(registry, transport.TCPName)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := factory.(*transport.TCPFactory); !ok {
		t.Fatalf("tcp lookup returned %T", factory)
	}
	if _, err := lookupTransportFactory(registry, "http2"); !errors.Is(err, transport.ErrUnknown) {
		t.Fatalf("unregistered http2 error = %v, want ErrUnknown", err)
	}
}

func TestRuntimeTransportRegistryRejectsInvalidTCPDeadlines(t *testing.T) {
	if _, err := newTransportRegistry(0, 0); !errors.Is(err, transport.ErrInvalidTCPConfig) {
		t.Fatalf("invalid TCP deadlines error = %v, want ErrInvalidTCPConfig", err)
	}
}
