package socks5

import (
	"bytes"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"testing"

	"github.com/adrianceding/via/internal/protocol"
)

func TestGreetingMatrix(t *testing.T) {
	tests := []struct {
		name           string
		encoded        []byte
		requiredMethod byte
		accepted       bool
		wantErr        error
	}{
		{name: "username password", encoded: []byte{5, 1, 2}, requiredMethod: 2, accepted: true},
		{name: "username password among methods", encoded: []byte{5, 3, 0, 2, 1}, requiredMethod: 2, accepted: true},
		{name: "no auth", encoded: []byte{5, 1, 0}, requiredMethod: 0, accepted: true},
		{name: "no auth among methods", encoded: []byte{5, 3, 2, 0, 1}, requiredMethod: 0, accepted: true},
		{name: "no auth rejected when credentials configured", encoded: []byte{5, 1, 0}, requiredMethod: 2, wantErr: ErrNoAcceptableMethod},
		{name: "username password rejected when credentials omitted", encoded: []byte{5, 1, 2}, requiredMethod: 0, wantErr: ErrNoAcceptableMethod},
		{name: "not offered", encoded: []byte{5, 2, 0, 1}, requiredMethod: 2, wantErr: ErrNoAcceptableMethod},
		{name: "version", encoded: []byte{4, 1, 0}, requiredMethod: 0, wantErr: ErrInvalidGreeting},
		{name: "zero methods", encoded: []byte{5, 0}, requiredMethod: 0, wantErr: ErrInvalidGreeting},
		{name: "truncated", encoded: []byte{5, 2, 2}, requiredMethod: 2, wantErr: ErrInvalidGreeting},
		{name: "trailing", encoded: []byte{5, 1, 2, 1}, requiredMethod: 2, wantErr: ErrInvalidGreeting},
		{name: "invalid required method", encoded: []byte{5, 1, 2}, requiredMethod: 1, wantErr: ErrInvalidGreeting},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accepted, err := ParseGreeting(test.encoded, test.requiredMethod)
			if accepted != test.accepted || !errors.Is(err, test.wantErr) {
				t.Fatalf("ParseGreeting = %t, %v", accepted, err)
			}
		})
	}
	if got := MethodReply(MethodUsernamePassword, true); got != [2]byte{5, 2} {
		t.Fatalf("authenticated method reply = %v", got)
	}
	if got := MethodReply(MethodNoAuth, true); got != [2]byte{5, 0} {
		t.Fatalf("no-auth method reply = %v", got)
	}
	if got := MethodReply(MethodUsernamePassword, false); got != [2]byte{5, 0xff} {
		t.Fatalf("failure method reply = %v", got)
	}
	if got := MethodReply(1, true); got != [2]byte{5, 0xff} {
		t.Fatalf("invalid method reply = %v", got)
	}
}

func TestAuthenticationFixedVectors(t *testing.T) {
	request := []byte{1, 4, 'u', 's', 'e', 'r', 6, 's', 'e', 'c', 'r', 'e', 't'}
	accepted, err := Authenticate(bytes.NewReader(request), "user", "secret")
	if err != nil || !accepted {
		t.Fatalf("Authenticate = %t, %v", accepted, err)
	}
	if got := AuthenticationReply(true); got != [2]byte{1, 0} {
		t.Fatalf("success authentication reply = %v", got)
	}
	if got := AuthenticationReply(false); got != [2]byte{1, 1} {
		t.Fatalf("failure authentication reply = %v", got)
	}
}

func TestAuthenticationReadsOneByteAtATimeAndStopsAtBoundary(t *testing.T) {
	authentication := authenticationRequest([]byte("local-user"), []byte("local-password"))
	trailing := []byte{5, 1, 0, 1}
	reader := &oneByteReader{data: append(authentication, trailing...)}
	accepted, err := Authenticate(reader, "local-user", "local-password")
	if err != nil || !accepted {
		t.Fatalf("Authenticate = %t, %v", accepted, err)
	}
	if got := reader.data[reader.offset:]; !bytes.Equal(got, trailing) {
		t.Fatalf("trailing bytes = %v, want %v", got, trailing)
	}
}

func TestAuthenticationRejectsEveryTruncatedPrefix(t *testing.T) {
	request := authenticationRequest([]byte("user"), []byte("password"))
	for length := 0; length < len(request); length++ {
		accepted, err := Authenticate(bytes.NewReader(request[:length]), "user", "password")
		if accepted || !errors.Is(err, ErrInvalidAuthentication) {
			t.Fatalf("prefix length %d = %t, %v", length, accepted, err)
		}
	}
}

func TestAuthenticationBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		username []byte
		password []byte
	}{
		{name: "minimum", username: []byte{'u'}, password: []byte{'p'}},
		{
			name:     "maximum",
			username: bytes.Repeat([]byte{'u'}, MaxUsernamePasswordField),
			password: bytes.Repeat([]byte{'p'}, MaxUsernamePasswordField),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := authenticationRequest(test.username, test.password)
			if test.name == "maximum" && len(request) != MaxUsernamePasswordRequest {
				t.Fatalf("request length = %d, want %d", len(request), MaxUsernamePasswordRequest)
			}
			accepted, err := Authenticate(&oneByteReader{data: request}, string(test.username), string(test.password))
			if err != nil || !accepted {
				t.Fatalf("Authenticate = %t, %v", accepted, err)
			}
		})
	}
}

func TestAuthenticationMalformedRequestsUseOneError(t *testing.T) {
	tests := []struct {
		name    string
		request []byte
	}{
		{name: "version", request: []byte{2, 1, 'u', 1, 'p'}},
		{name: "empty username", request: []byte{1, 0, 1, 'p'}},
		{name: "missing password length", request: []byte{1, 1, 'u'}},
		{name: "empty password", request: []byte{1, 1, 'u', 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accepted, err := Authenticate(bytes.NewReader(test.request), "u", "p")
			if accepted || !errors.Is(err, ErrInvalidAuthentication) {
				t.Fatalf("Authenticate = %t, %v", accepted, err)
			}
		})
	}
}

func TestAuthenticationCredentialFailuresAreIndistinguishable(t *testing.T) {
	request := authenticationRequest([]byte("user"), []byte("password"))
	tests := []struct {
		name     string
		username string
		password string
	}{
		{name: "wrong username", username: "other", password: "password"},
		{name: "wrong password", username: "user", password: "other"},
		{name: "both wrong", username: "other", password: "different"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accepted, err := Authenticate(bytes.NewReader(request), test.username, test.password)
			if err != nil || accepted {
				t.Fatalf("Authenticate = %t, %v", accepted, err)
			}
		})
	}
}

func TestAuthenticationRejectsInvalidExpectedCredentialLengths(t *testing.T) {
	request := authenticationRequest([]byte("u"), []byte("p"))
	tests := []struct {
		name     string
		username string
		password string
	}{
		{name: "empty username", password: "p"},
		{name: "empty password", username: "u"},
		{name: "long username", username: string(bytes.Repeat([]byte{'u'}, MaxUsernamePasswordField+1)), password: "p"},
		{name: "long password", username: "u", password: string(bytes.Repeat([]byte{'p'}, MaxUsernamePasswordField+1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accepted, err := Authenticate(bytes.NewReader(request), test.username, test.password)
			if accepted || !errors.Is(err, ErrInvalidAuthentication) {
				t.Fatalf("Authenticate = %t, %v", accepted, err)
			}
		})
	}
}

func TestValidCredentialUsesEncodedByteLength(t *testing.T) {
	if ValidCredential("") || !ValidCredential("u") || !ValidCredential(string(bytes.Repeat([]byte{'u'}, 255))) ||
		ValidCredential(string(bytes.Repeat([]byte{'u'}, 256))) {
		t.Fatal("credential byte length boundary rejected")
	}
	if !ValidCredential(string(bytes.Repeat([]byte{0xe7, 0x95, 0x8c}, 85))) ||
		ValidCredential(string(bytes.Repeat([]byte{0xe7, 0x95, 0x8c}, 86))) {
		t.Fatal("UTF-8 credential length was not measured in bytes")
	}
}

func TestParseCONNECTAddressTypes(t *testing.T) {
	tests := []struct {
		name    string
		encoded []byte
		want    protocol.Target
	}{
		{
			name:    "ipv4",
			encoded: []byte{5, 1, 0, 1, 192, 0, 2, 1, 1, 187},
			want:    protocol.Target{Address: netip.MustParseAddr("192.0.2.1"), Port: 443},
		},
		{
			name:    "ipv6",
			encoded: append([]byte{5, 1, 0, 4}, append(netip.MustParseAddr("2001:db8::1").AsSlice(), 0, 80)...),
			want:    protocol.Target{Address: netip.MustParseAddr("2001:db8::1"), Port: 80},
		},
		{
			name:    "dns normalized",
			encoded: append([]byte{5, 1, 0, 3, 11}, append([]byte("EXAMPLE.COM"), 0, 53)...),
			want:    protocol.Target{DNSName: "example.com", Port: 53},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseRequest(test.encoded)
			if err != nil || got != test.want {
				t.Fatalf("ParseRequest = %#v, %v", got, err)
			}
		})
	}
}

func TestCONNECTRejectsUnsupportedAndMalformedRequests(t *testing.T) {
	tests := []struct {
		name    string
		encoded []byte
		want    error
	}{
		{name: "bind", encoded: []byte{5, 2, 0, 1, 127, 0, 0, 1, 0, 1}, want: ErrCommandUnsupported},
		{name: "udp", encoded: []byte{5, 3, 0, 1, 127, 0, 0, 1, 0, 1}, want: ErrCommandUnsupported},
		{name: "address type", encoded: []byte{5, 1, 0, 2, 0, 0, 1}, want: ErrAddressTypeUnsupported},
		{name: "reserved", encoded: []byte{5, 1, 1, 1, 127, 0, 0, 1, 0, 1}, want: ErrInvalidRequest},
		{name: "zero port", encoded: []byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 0}, want: ErrInvalidRequest},
		{name: "unicode dns", encoded: append([]byte{5, 1, 0, 3, 2}, 0xc3, 0xa9, 0, 80), want: ErrInvalidRequest},
		{name: "underscore dns", encoded: append([]byte{5, 1, 0, 3, 3}, '_', 'b', 'a', 0, 80), want: ErrInvalidRequest},
		{name: "trailing dot", encoded: append([]byte{5, 1, 0, 3, 2}, 'a', '.', 0, 80), want: ErrInvalidRequest},
		{name: "truncated ipv4", encoded: []byte{5, 1, 0, 1, 127, 0, 0, 1, 1}, want: ErrInvalidRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseRequest(test.encoded); !errors.Is(err, test.want) {
				t.Fatalf("ParseRequest error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestStreamingReadersUseIndependentExactBuffers(t *testing.T) {
	greeting := []byte{5, 2, 0, 2}
	authentication := authenticationRequest([]byte("user"), []byte("password"))
	request := append([]byte{5, 1, 0, 3, 11}, append([]byte("example.com"), 1, 187)...)
	data := append([]byte(nil), greeting...)
	data = append(data, authentication...)
	data = append(data, request...)
	reader := &oneByteReader{data: data}
	accepted, err := ReadGreeting(reader, MethodUsernamePassword)
	if err != nil || !accepted {
		t.Fatalf("ReadGreeting = %t, %v", accepted, err)
	}
	authenticated, err := Authenticate(reader, "user", "password")
	if err != nil || !authenticated {
		t.Fatalf("Authenticate = %t, %v", authenticated, err)
	}
	target, err := ReadRequest(reader)
	if err != nil || target.DNSName != "example.com" || target.Port != 443 {
		t.Fatalf("ReadRequest = %#v, %v", target, err)
	}
	if reader.offset != len(reader.data) {
		t.Fatalf("reader consumed %d of %d", reader.offset, len(reader.data))
	}
}

func TestStreamingReadersReturnTruncation(t *testing.T) {
	if _, err := ReadGreeting(bytes.NewReader([]byte{5, 2, 0}), MethodUsernamePassword); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("greeting truncation error = %v", err)
	}
	if _, err := ReadRequest(bytes.NewReader([]byte{5, 1, 0, 4, 0})); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("request truncation error = %v", err)
	}
	oversized := append([]byte{5, 1, 0, 3, 254}, make([]byte, 256)...)
	if _, err := ReadRequest(bytes.NewReader(oversized)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("oversized DNS request error = %v", err)
	}
}

func TestReplyVectorsAndOpenResultMapping(t *testing.T) {
	want := [10]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}
	if got, err := Reply(ReplySucceeded); err != nil || got != want {
		t.Fatalf("success reply = %v, %v", got, err)
	}
	if _, err := Reply(99); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid reply error = %v", err)
	}
	tests := map[protocol.OpenResultCode]ReplyCode{
		protocol.OpenSuccess:         ReplySucceeded,
		protocol.OpenInvalidTarget:   ReplyHostUnreachable,
		protocol.OpenConnectFailed:   ReplyConnectionRefused,
		protocol.OpenResourceLimit:   ReplyGeneralFailure,
		protocol.OpenInternalFailure: ReplyGeneralFailure,
	}
	for result, want := range tests {
		if got := ReplyForOpenResult(result); got != want {
			t.Fatalf("ReplyForOpenResult(%v) = %v, want %v", result, got, want)
		}
	}
}

func TestMaximumDNSRequestBoundary(t *testing.T) {
	name := makeValidDNSName(253)
	request := append([]byte{5, 1, 0, 3, byte(len(name))}, append([]byte(name), 0, 80)...)
	if len(request) != MaxConnectRequest {
		t.Fatalf("request length = %d", len(request))
	}
	if target, err := ParseRequest(request); err != nil || target.DNSName != name {
		t.Fatalf("maximum request = %#v, %v", target, err)
	}
}

func makeValidDNSName(length int) string {
	var result []byte
	for len(result) < length {
		remaining := length - len(result)
		label := min(63, remaining)
		if len(result) != 0 {
			result = append(result, '.')
			remaining--
			label = min(63, remaining)
		}
		result = append(result, bytes.Repeat([]byte{'a'}, label)...)
	}
	return string(result)
}

func authenticationRequest(username, password []byte) []byte {
	request := make([]byte, 0, 3+len(username)+len(password))
	request = append(request, UsernamePasswordVersion, byte(len(username)))
	request = append(request, username...)
	request = append(request, byte(len(password)))
	return append(request, password...)
}

type oneByteReader struct {
	data   []byte
	offset int
}

func (reader *oneByteReader) Read(target []byte) (int, error) {
	if reader.offset == len(reader.data) {
		return 0, io.EOF
	}
	target[0] = reader.data[reader.offset]
	reader.offset++
	return 1, nil
}

func TestTargetValuesAreDetached(t *testing.T) {
	request := []byte{5, 1, 0, 3, 3, 'a', 'b', 'c', 0, 80}
	target, err := ParseRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	request[5] = 'x'
	if !reflect.DeepEqual(target, protocol.Target{DNSName: "abc", Port: 80}) {
		t.Fatalf("mutable request escaped: %#v", target)
	}
}
