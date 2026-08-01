// Package socks5 implements the local optional authentication and CONNECT protocol boundary.
package socks5

import (
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
	"strings"

	"github.com/adrianceding/via/internal/protocol"
)

const (
	Version                    = 5
	MethodNoAuth               = 0
	MethodUsernamePassword     = 2
	MethodNoAcceptable         = 0xff
	UsernamePasswordVersion    = 1
	UsernamePasswordSucceeded  = 0
	UsernamePasswordFailed     = 1
	CommandConnect             = 1
	MaxGreetingSize            = 257
	MaxUsernamePasswordField   = 255
	MaxUsernamePasswordRequest = 513
	MaxConnectRequest          = 260
)

var (
	ErrInvalidGreeting        = errors.New("socks5: invalid greeting")
	ErrNoAcceptableMethod     = errors.New("socks5: no acceptable method")
	ErrInvalidAuthentication  = errors.New("socks5: invalid authentication")
	ErrInvalidRequest         = errors.New("socks5: invalid request")
	ErrCommandUnsupported     = errors.New("socks5: command unsupported")
	ErrAddressTypeUnsupported = errors.New("socks5: address type unsupported")
)

type ReplyCode uint8

const (
	ReplySucceeded ReplyCode = iota
	ReplyGeneralFailure
	ReplyConnectionNotAllowed
	ReplyNetworkUnreachable
	ReplyHostUnreachable
	ReplyConnectionRefused
	ReplyTTLExpired
	ReplyCommandNotSupported
	ReplyAddressTypeNotSupported
)

func ParseGreeting(encoded []byte, requiredMethod byte) (bool, error) {
	if len(encoded) < 3 || len(encoded) > MaxGreetingSize || encoded[0] != Version || !validMethod(requiredMethod) {
		return false, ErrInvalidGreeting
	}
	methodCount := int(encoded[1])
	if methodCount == 0 || len(encoded) != methodCount+2 {
		return false, ErrInvalidGreeting
	}
	for _, method := range encoded[2:] {
		if method == requiredMethod {
			return true, nil
		}
	}
	return false, ErrNoAcceptableMethod
}

func MethodReply(requiredMethod byte, accepted bool) [2]byte {
	method := byte(MethodNoAcceptable)
	if accepted && validMethod(requiredMethod) {
		method = requiredMethod
	}
	return [2]byte{Version, method}
}

func ReadGreeting(reader io.Reader, requiredMethod byte) (bool, error) {
	var encoded [MaxGreetingSize]byte
	if _, err := io.ReadFull(reader, encoded[:2]); err != nil {
		return false, err
	}
	methodCount := int(encoded[1])
	if methodCount == 0 {
		return false, ErrInvalidGreeting
	}
	if _, err := io.ReadFull(reader, encoded[2:2+methodCount]); err != nil {
		return false, err
	}
	return ParseGreeting(encoded[:2+methodCount], requiredMethod)
}

func validMethod(method byte) bool {
	return method == MethodNoAuth || method == MethodUsernamePassword
}

// AuthenticationReply returns an RFC 1929 username/password authentication reply.
func AuthenticationReply(accepted bool) [2]byte {
	status := byte(UsernamePasswordFailed)
	if accepted {
		status = UsernamePasswordSucceeded
	}
	return [2]byte{UsernamePasswordVersion, status}
}

// ValidCredential reports whether a credential fits one RFC 1929 length field.
func ValidCredential(credential string) bool {
	return len(credential) >= 1 && len(credential) <= MaxUsernamePasswordField
}

// Authenticate reads one bounded RFC 1929 request and compares both credentials
// without retaining or returning the received plaintext.
func Authenticate(reader io.Reader, expectedUsername, expectedPassword string) (bool, error) {
	if !ValidCredential(expectedUsername) || !ValidCredential(expectedPassword) {
		return false, ErrInvalidAuthentication
	}

	var username [MaxUsernamePasswordField]byte
	var password [MaxUsernamePasswordField]byte
	var expectedUsernameBuffer [MaxUsernamePasswordField]byte
	var expectedPasswordBuffer [MaxUsernamePasswordField]byte
	defer func() {
		clear(username[:])
		clear(password[:])
		clear(expectedUsernameBuffer[:])
		clear(expectedPasswordBuffer[:])
	}()
	copy(expectedUsernameBuffer[:], expectedUsername)
	copy(expectedPasswordBuffer[:], expectedPassword)

	var header [2]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil ||
		header[0] != UsernamePasswordVersion || header[1] == 0 {
		return false, ErrInvalidAuthentication
	}
	usernameLength := int(header[1])
	if _, err := io.ReadFull(reader, username[:usernameLength]); err != nil {
		return false, ErrInvalidAuthentication
	}

	var passwordLengthByte [1]byte
	if _, err := io.ReadFull(reader, passwordLengthByte[:]); err != nil || passwordLengthByte[0] == 0 {
		return false, ErrInvalidAuthentication
	}
	passwordLength := int(passwordLengthByte[0])
	if _, err := io.ReadFull(reader, password[:passwordLength]); err != nil {
		return false, ErrInvalidAuthentication
	}

	usernameMatches := subtle.ConstantTimeCompare(username[:], expectedUsernameBuffer[:]) &
		subtle.ConstantTimeEq(int32(usernameLength), int32(len(expectedUsername)))
	passwordMatches := subtle.ConstantTimeCompare(password[:], expectedPasswordBuffer[:]) &
		subtle.ConstantTimeEq(int32(passwordLength), int32(len(expectedPassword)))
	return usernameMatches&passwordMatches == 1, nil
}

func ParseRequest(encoded []byte) (protocol.Target, error) {
	if len(encoded) < 7 || len(encoded) > MaxConnectRequest || encoded[0] != Version || encoded[2] != 0 {
		return protocol.Target{}, ErrInvalidRequest
	}
	if encoded[1] != CommandConnect {
		return protocol.Target{}, ErrCommandUnsupported
	}
	var target protocol.Target
	switch protocol.AddressType(encoded[3]) {
	case protocol.AddressIPv4:
		if len(encoded) != 10 {
			return protocol.Target{}, ErrInvalidRequest
		}
		var address [4]byte
		copy(address[:], encoded[4:8])
		target.Address = netip.AddrFrom4(address)
		target.Port = binary.BigEndian.Uint16(encoded[8:])
	case protocol.AddressIPv6:
		if len(encoded) != 22 {
			return protocol.Target{}, ErrInvalidRequest
		}
		var address [16]byte
		copy(address[:], encoded[4:20])
		target.Address = netip.AddrFrom16(address).Unmap()
		target.Port = binary.BigEndian.Uint16(encoded[20:])
	case protocol.AddressDNS:
		nameLength := int(encoded[4])
		if nameLength < 1 || nameLength > 253 || len(encoded) != nameLength+7 {
			return protocol.Target{}, ErrInvalidRequest
		}
		target.DNSName = strings.ToLower(string(encoded[5 : 5+nameLength]))
		target.Port = binary.BigEndian.Uint16(encoded[len(encoded)-2:])
	default:
		return protocol.Target{}, ErrAddressTypeUnsupported
	}
	if err := protocol.ValidateTarget(target); err != nil {
		return protocol.Target{}, ErrInvalidRequest
	}
	return target, nil
}

func ReadRequest(reader io.Reader) (protocol.Target, error) {
	var encoded [MaxConnectRequest]byte
	if _, err := io.ReadFull(reader, encoded[:4]); err != nil {
		return protocol.Target{}, err
	}
	if encoded[0] != Version || encoded[2] != 0 {
		return protocol.Target{}, ErrInvalidRequest
	}
	if encoded[1] != CommandConnect {
		return protocol.Target{}, ErrCommandUnsupported
	}
	length := 0
	switch protocol.AddressType(encoded[3]) {
	case protocol.AddressIPv4:
		length = 10
	case protocol.AddressIPv6:
		length = 22
	case protocol.AddressDNS:
		if _, err := io.ReadFull(reader, encoded[4:5]); err != nil {
			return protocol.Target{}, err
		}
		length = 7 + int(encoded[4])
		if length > MaxConnectRequest {
			return protocol.Target{}, ErrInvalidRequest
		}
	default:
		return protocol.Target{}, ErrAddressTypeUnsupported
	}
	start := 4
	if protocol.AddressType(encoded[3]) == protocol.AddressDNS {
		start = 5
	}
	if _, err := io.ReadFull(reader, encoded[start:length]); err != nil {
		return protocol.Target{}, err
	}
	return ParseRequest(encoded[:length])
}

func Reply(code ReplyCode) ([10]byte, error) {
	if code > ReplyAddressTypeNotSupported {
		return [10]byte{}, ErrInvalidRequest
	}
	return [10]byte{Version, byte(code), 0, byte(protocol.AddressIPv4), 0, 0, 0, 0, 0, 0}, nil
}

func ReplyForOpenResult(result protocol.OpenResultCode) ReplyCode {
	switch result {
	case protocol.OpenSuccess:
		return ReplySucceeded
	case protocol.OpenInvalidTarget:
		return ReplyHostUnreachable
	case protocol.OpenConnectFailed:
		return ReplyConnectionRefused
	default:
		return ReplyGeneralFailure
	}
}
