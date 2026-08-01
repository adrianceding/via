package protocol

import (
	"encoding/binary"
	"net/netip"
	"strings"
)

type AddressType uint8

const (
	AddressIPv4 AddressType = 0x01
	AddressDNS  AddressType = 0x03
	AddressIPv6 AddressType = 0x04
)

type Target struct {
	Address netip.Addr
	DNSName string
	Port    uint16
}

func ValidateTarget(target Target) error {
	_, err := target.marshalBinary()
	return err
}

func (target Target) marshalBinary() ([]byte, error) {
	if target.Port == 0 {
		return nil, invalidPayload(TypeOpen, "zero target port")
	}
	if target.Address.IsValid() == (target.DNSName != "") {
		return nil, invalidPayload(TypeOpen, "target address union")
	}

	if target.Address.IsValid() {
		if target.Address.Zone() != "" {
			return nil, invalidPayload(TypeOpen, "zoned target address")
		}
		address := target.Address.Unmap()
		if address.Is4() {
			payload := make([]byte, 7)
			payload[0] = byte(AddressIPv4)
			value := address.As4()
			copy(payload[1:5], value[:])
			binary.BigEndian.PutUint16(payload[5:], target.Port)
			return payload, nil
		}
		if address.Is6() {
			payload := make([]byte, 19)
			payload[0] = byte(AddressIPv6)
			value := address.As16()
			copy(payload[1:17], value[:])
			binary.BigEndian.PutUint16(payload[17:], target.Port)
			return payload, nil
		}
		return nil, invalidPayload(TypeOpen, "target address")
	}

	if !validDNSName(target.DNSName) {
		return nil, invalidPayload(TypeOpen, "dns name")
	}
	payload := make([]byte, 1+1+len(target.DNSName)+2)
	payload[0] = byte(AddressDNS)
	payload[1] = byte(len(target.DNSName))
	copy(payload[2:], target.DNSName)
	binary.BigEndian.PutUint16(payload[len(payload)-2:], target.Port)
	return payload, nil
}

func decodeTarget(payload []byte) (Target, error) {
	if len(payload) < 1 {
		return Target{}, invalidPayload(TypeOpen, "missing target")
	}
	switch AddressType(payload[0]) {
	case AddressIPv4:
		if len(payload) != 7 {
			return Target{}, invalidPayload(TypeOpen, "ipv4 target length")
		}
		var value [4]byte
		copy(value[:], payload[1:5])
		port := binary.BigEndian.Uint16(payload[5:])
		if port == 0 {
			return Target{}, invalidPayload(TypeOpen, "zero target port")
		}
		return Target{Address: netip.AddrFrom4(value), Port: port}, nil
	case AddressIPv6:
		if len(payload) != 19 {
			return Target{}, invalidPayload(TypeOpen, "ipv6 target length")
		}
		var value [16]byte
		copy(value[:], payload[1:17])
		port := binary.BigEndian.Uint16(payload[17:])
		if port == 0 {
			return Target{}, invalidPayload(TypeOpen, "zero target port")
		}
		address := netip.AddrFrom16(value)
		if address.Is4In6() {
			return Target{}, invalidPayload(TypeOpen, "non-canonical mapped ipv4")
		}
		return Target{Address: address, Port: port}, nil
	case AddressDNS:
		if len(payload) < 5 {
			return Target{}, invalidPayload(TypeOpen, "dns target length")
		}
		nameLength := int(payload[1])
		if nameLength < 1 || len(payload) != nameLength+4 {
			return Target{}, invalidPayload(TypeOpen, "dns target length")
		}
		name := string(payload[2 : 2+nameLength])
		if !validDNSName(name) {
			return Target{}, invalidPayload(TypeOpen, "dns name")
		}
		port := binary.BigEndian.Uint16(payload[len(payload)-2:])
		if port == 0 {
			return Target{}, invalidPayload(TypeOpen, "zero target port")
		}
		return Target{DNSName: name, Port: port}, nil
	default:
		return Target{}, invalidPayload(TypeOpen, "address type")
	}
}

func validDNSName(name string) bool {
	if len(name) < 1 || len(name) > 253 || strings.HasSuffix(name, ".") {
		return false
	}
	labelStart := 0
	for index := 0; index <= len(name); index++ {
		if index < len(name) && name[index] != '.' {
			value := name[index]
			if !(value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '-') {
				return false
			}
			continue
		}
		labelLength := index - labelStart
		if labelLength < 1 || labelLength > 63 || name[labelStart] == '-' || name[index-1] == '-' {
			return false
		}
		labelStart = index + 1
	}
	return true
}
