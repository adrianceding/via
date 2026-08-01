//go:build !linux

package transport

import "net"

func setDialerInterface(_ *net.Dialer, _ string) {}
