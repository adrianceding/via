//go:build linux

package transport

import (
	"net"
	"syscall"
)

func setDialerInterface(dialer *net.Dialer, interfaceName string) {
	if dialer == nil || interfaceName == "" {
		return
	}
	dialer.Control = func(_, _ string, rawConnection syscall.RawConn) error {
		var socketErr error
		if err := rawConnection.Control(func(fileDescriptor uintptr) {
			socketErr = syscall.SetsockoptString(
				int(fileDescriptor), syscall.SOL_SOCKET, syscall.SO_BINDTODEVICE, interfaceName,
			)
		}); err != nil {
			return err
		}
		return socketErr
	}
}
