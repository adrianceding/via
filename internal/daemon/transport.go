package daemon

import (
	"time"

	"github.com/adrianceding/via/internal/transport"
)

func newTransportRegistry(frameTotal, frameNoProgress time.Duration) (*transport.Registry, error) {
	registry := transport.NewRegistry()
	tcpFactory, err := transport.NewTCPFactory(transport.TCPConfig{
		FrameTotal: frameTotal, FrameNoProgress: frameNoProgress,
	})
	if err != nil {
		return nil, err
	}
	if err := registry.Register(transport.TCPName, tcpFactory); err != nil {
		return nil, err
	}
	return registry, nil
}

func lookupTransportFactory(registry *transport.Registry, name string) (transport.Factory, error) {
	return registry.Lookup(name)
}
