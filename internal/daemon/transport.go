package daemon

import (
	"time"

	"github.com/adrianceding/via/internal/transport"
)

func newTransportRegistry(frameTotal, frameNoProgress time.Duration, writeBufferBytes ...uint64) (*transport.Registry, error) {
	bufferBytes := uint64(0)
	if len(writeBufferBytes) != 0 {
		bufferBytes = writeBufferBytes[0]
	}
	registry := transport.NewRegistry()
	tcpFactory, err := transport.NewTCPFactory(transport.TCPConfig{
		FrameTotal: frameTotal, FrameNoProgress: frameNoProgress, WriteBufferBytes: int(bufferBytes),
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
