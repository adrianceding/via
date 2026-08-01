package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

const (
	MaxPrincipalIDLength      = 64
	MaxDataLength             = MaxPayloadSize - 16 - 8
	MaxACKRanges              = 16
	DeliveryConstraintsSize   = 17
	MinimumDeliveryConstraint = 10 * time.Millisecond
	MaximumDeliveryConstraint = 10 * time.Second
)

type (
	FlowID     [16]byte
	OpenToken  [32]byte
	Capability [32]byte
)

type DeliveryMode uint8

const (
	DeliveryRedundant DeliveryMode = 1
	DeliveryAdaptive  DeliveryMode = 2
)

type PathSelection uint8

const (
	PathNone        PathSelection = 0
	PathFastest     PathSelection = 1
	PathDistributed PathSelection = 2
)

type DeliveryConstraintFallback uint8

const (
	DeliveryFallbackFastest DeliveryConstraintFallback = iota + 1
	DeliveryFallbackPause
)

type DeliveryConstraints struct {
	MaxDeliveryDelay time.Duration
	MaxDelayGap      time.Duration
	Fallback         DeliveryConstraintFallback
}

func ValidDeliveryPolicy(mode DeliveryMode, selection PathSelection, constraints DeliveryConstraints) bool {
	switch {
	case mode == DeliveryRedundant && selection == PathNone:
		return constraints == (DeliveryConstraints{})
	case mode == DeliveryAdaptive && selection == PathFastest:
		return constraints == (DeliveryConstraints{})
	case mode == DeliveryAdaptive && selection == PathDistributed:
		return validDeliveryConstraint(constraints.MaxDeliveryDelay) &&
			validDeliveryConstraint(constraints.MaxDelayGap) &&
			(constraints.Fallback == DeliveryFallbackFastest || constraints.Fallback == DeliveryFallbackPause)
	default:
		return false
	}
}

func validDeliveryConstraint(value time.Duration) bool {
	return value == 0 || value >= MinimumDeliveryConstraint && value <= MaximumDeliveryConstraint
}

type AuthResultCode uint8

const (
	AuthSuccess AuthResultCode = 0
	AuthFailure AuthResultCode = 1
)

type OpenResultCode uint8

const (
	OpenSuccess           OpenResultCode = 0
	OpenInvalidTarget     OpenResultCode = 1
	OpenConnectFailed     OpenResultCode = 2
	OpenResourceLimit     OpenResultCode = 3
	OpenInternalFailure   OpenResultCode = 4
	OpenUnsupportedPolicy OpenResultCode = 5
)

type JoinResultCode uint8

const (
	JoinSuccess JoinResultCode = 0
	JoinFailure JoinResultCode = 1
)

type ResetReason uint16

const (
	ResetCancelled        ResetReason = 1
	ResetProtocolConflict ResetReason = 2
	ResetResourceLimit    ResetReason = 3
	ResetLocalIOFailure   ResetReason = 4
	ResetDeadlineExceeded ResetReason = 5
	ResetInternalFailure  ResetReason = 6
)

type Message interface {
	messageType() Type
	marshalPayload() ([]byte, error)
}

type DataSource interface {
	DataLen() int
	AppendData([]byte) []byte
}

type AuthChallenge struct {
	Challenge [32]byte
}

type AuthProof struct {
	PrincipalID string
	ClientNonce [32]byte
	Proof       [32]byte
}

type AuthResult struct {
	Result AuthResultCode
}

type Probe struct {
	Token uint64
}

type ProbeACK struct {
	Token uint64
}

type Open struct {
	FlowID        FlowID
	OpenToken     OpenToken
	DeliveryMode  DeliveryMode
	PathSelection PathSelection
	Constraints   DeliveryConstraints
	Target        Target
}

type OpenResult struct {
	FlowID             FlowID
	Result             OpenResultCode
	Capability         Capability
	DeliveryMode       DeliveryMode
	PathSelection      PathSelection
	Constraints        DeliveryConstraints
	ImplicitAttachment bool
}

type Join struct {
	FlowID     FlowID
	Capability Capability
}

type JoinResult struct {
	FlowID FlowID
	Result JoinResultCode
}

type Data struct {
	FlowID FlowID
	Offset uint64
	Bytes  []byte
}

type ACKRange struct {
	Start uint64
	End   uint64
}

type ACK struct {
	FlowID     FlowID
	NextOffset uint64
	Ranges     []ACKRange
}

type FIN struct {
	FlowID      FlowID
	FinalOffset uint64
}

type FINACK struct {
	FlowID      FlowID
	FinalOffset uint64
}

type Reset struct {
	FlowID FlowID
	Reason ResetReason
}

func FrameForMessage(message Message) (Frame, error) {
	if message == nil {
		return Frame{}, fmt.Errorf("%w: nil message", ErrInvalidPayload)
	}
	payload, err := message.marshalPayload()
	if err != nil {
		return Frame{}, err
	}
	frame := Frame{Type: message.messageType(), Payload: payload}
	if err := validatePayloadLength(frame.Type, len(frame.Payload)); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func EncodeMessage(message Message) ([]byte, error) {
	if data, ok := message.(Data); ok {
		return EncodeData(data.FlowID, data.Offset, dataBytes(data.Bytes))
	}
	frame, err := FrameForMessage(message)
	if err != nil {
		return nil, err
	}
	return EncodeFrame(frame)
}

func EncodeData[T DataSource](flowID FlowID, offset uint64, source T) ([]byte, error) {
	dataLength := source.DataLen()
	if dataLength < 1 || dataLength > MaxDataLength {
		return nil, invalidPayload(TypeData, "data length")
	}
	if uint64(dataLength) > math.MaxUint64-offset {
		return nil, invalidPayload(TypeData, "offset overflow")
	}
	payloadLength := 24 + dataLength
	encoded := make([]byte, HeaderSize+24, HeaderSize+payloadLength)
	encoded[0] = 'V'
	encoded[1] = 'I'
	encoded[2] = Version
	encoded[3] = byte(TypeData)
	binary.BigEndian.PutUint32(encoded[4:8], uint32(payloadLength))
	copy(encoded[8:24], flowID[:])
	binary.BigEndian.PutUint64(encoded[24:32], offset)
	encoded = source.AppendData(encoded)
	if len(encoded) != HeaderSize+payloadLength {
		return nil, invalidPayload(TypeData, "data source length")
	}
	return encoded, nil
}

type dataBytes []byte

func (data dataBytes) DataLen() int { return len(data) }

func (data dataBytes) AppendData(destination []byte) []byte {
	return append(destination, data...)
}

func DecodeMessage(frame Frame) (Message, error) {
	if err := validatePayloadLength(frame.Type, len(frame.Payload)); err != nil {
		return nil, err
	}
	payload := frame.Payload

	switch frame.Type {
	case TypeAuthChallenge:
		var message AuthChallenge
		copy(message.Challenge[:], payload)
		return message, nil
	case TypeAuthProof:
		principalLength := int(payload[0])
		if principalLength < 1 || principalLength > MaxPrincipalIDLength || len(payload) != 65+principalLength {
			return nil, invalidPayload(frame.Type, "principal length")
		}
		principal := payload[1 : 1+principalLength]
		if !validPrincipalID(principal) {
			return nil, invalidPayload(frame.Type, "principal id")
		}
		message := AuthProof{PrincipalID: string(principal)}
		copy(message.ClientNonce[:], payload[1+principalLength:33+principalLength])
		copy(message.Proof[:], payload[33+principalLength:])
		return message, nil
	case TypeAuthResult:
		result := AuthResultCode(payload[0])
		if result != AuthSuccess && result != AuthFailure {
			return nil, invalidPayload(frame.Type, "result")
		}
		return AuthResult{Result: result}, nil
	case TypeProbe:
		return Probe{Token: binary.BigEndian.Uint64(payload)}, nil
	case TypeProbeACK:
		return ProbeACK{Token: binary.BigEndian.Uint64(payload)}, nil
	case TypeOpen:
		mode := DeliveryMode(payload[48])
		selection := PathSelection(payload[49])
		constraints, err := decodeDeliveryConstraints(frame.Type, payload[50:67])
		if err != nil || !ValidDeliveryPolicy(mode, selection, constraints) {
			return nil, invalidPayload(frame.Type, "delivery policy")
		}
		target, err := decodeTarget(payload[67:])
		if err != nil {
			return nil, err
		}
		message := Open{DeliveryMode: mode, PathSelection: selection, Constraints: constraints, Target: target}
		copy(message.FlowID[:], payload[:16])
		copy(message.OpenToken[:], payload[16:48])
		return message, nil
	case TypeOpenResult:
		result := OpenResultCode(payload[16])
		if result > OpenUnsupportedPolicy {
			return nil, invalidPayload(frame.Type, "result")
		}
		message := OpenResult{Result: result}
		copy(message.FlowID[:], payload[:16])
		if result != OpenSuccess {
			if len(payload) != 17 {
				return nil, invalidPayload(frame.Type, "failure length")
			}
			return message, nil
		}
		if len(payload) != 68 && len(payload) != 69 {
			return nil, invalidPayload(frame.Type, "success length")
		}
		copy(message.Capability[:], payload[17:49])
		message.DeliveryMode = DeliveryMode(payload[49])
		message.PathSelection = PathSelection(payload[50])
		constraints, err := decodeDeliveryConstraints(frame.Type, payload[51:68])
		if err != nil || !ValidDeliveryPolicy(message.DeliveryMode, message.PathSelection, constraints) {
			return nil, invalidPayload(frame.Type, "delivery policy")
		}
		message.Constraints = constraints
		if len(payload) == 69 {
			if payload[68] != 1 {
				return nil, invalidPayload(frame.Type, "implicit attachment")
			}
			message.ImplicitAttachment = true
		}
		return message, nil
	case TypeJoin:
		message := Join{}
		copy(message.FlowID[:], payload[:16])
		copy(message.Capability[:], payload[16:])
		return message, nil
	case TypeJoinResult:
		result := JoinResultCode(payload[16])
		if result != JoinSuccess && result != JoinFailure {
			return nil, invalidPayload(frame.Type, "result")
		}
		message := JoinResult{Result: result}
		copy(message.FlowID[:], payload[:16])
		return message, nil
	case TypeData:
		offset := binary.BigEndian.Uint64(payload[16:24])
		dataLength := len(payload) - 24
		if uint64(dataLength) > math.MaxUint64-offset {
			return nil, invalidPayload(frame.Type, "offset overflow")
		}
		message := Data{Offset: offset, Bytes: payload[24:]}
		copy(message.FlowID[:], payload[:16])
		return message, nil
	case TypeACK:
		return decodeACK(payload)
	case TypeFIN:
		message := FIN{FinalOffset: binary.BigEndian.Uint64(payload[16:])}
		copy(message.FlowID[:], payload[:16])
		return message, nil
	case TypeFINACK:
		message := FINACK{FinalOffset: binary.BigEndian.Uint64(payload[16:])}
		copy(message.FlowID[:], payload[:16])
		return message, nil
	case TypeReset:
		reason := ResetReason(binary.BigEndian.Uint16(payload[16:]))
		if reason < ResetCancelled || reason > ResetInternalFailure {
			return nil, invalidPayload(frame.Type, "reason")
		}
		message := Reset{Reason: reason}
		copy(message.FlowID[:], payload[:16])
		return message, nil
	default:
		return nil, fmt.Errorf("%w: 0x%02x", ErrUnknownType, byte(frame.Type))
	}
}

func (message AuthChallenge) messageType() Type { return TypeAuthChallenge }
func (message AuthProof) messageType() Type     { return TypeAuthProof }
func (message AuthResult) messageType() Type    { return TypeAuthResult }
func (message Probe) messageType() Type         { return TypeProbe }
func (message ProbeACK) messageType() Type      { return TypeProbeACK }
func (message Open) messageType() Type          { return TypeOpen }
func (message OpenResult) messageType() Type    { return TypeOpenResult }
func (message Join) messageType() Type          { return TypeJoin }
func (message JoinResult) messageType() Type    { return TypeJoinResult }
func (message Data) messageType() Type          { return TypeData }
func (message ACK) messageType() Type           { return TypeACK }
func (message FIN) messageType() Type           { return TypeFIN }
func (message FINACK) messageType() Type        { return TypeFINACK }
func (message Reset) messageType() Type         { return TypeReset }

func (message AuthChallenge) marshalPayload() ([]byte, error) {
	return append([]byte(nil), message.Challenge[:]...), nil
}

func (message AuthProof) marshalPayload() ([]byte, error) {
	principal := []byte(message.PrincipalID)
	if !validPrincipalID(principal) {
		return nil, invalidPayload(TypeAuthProof, "principal id")
	}
	payload := make([]byte, 1+len(principal)+64)
	payload[0] = byte(len(principal))
	copy(payload[1:], principal)
	copy(payload[1+len(principal):], message.ClientNonce[:])
	copy(payload[33+len(principal):], message.Proof[:])
	return payload, nil
}

func (message AuthResult) marshalPayload() ([]byte, error) {
	if message.Result != AuthSuccess && message.Result != AuthFailure {
		return nil, invalidPayload(TypeAuthResult, "result")
	}
	return []byte{byte(message.Result)}, nil
}

func (message Probe) marshalPayload() ([]byte, error) {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, message.Token)
	return payload, nil
}

func (message ProbeACK) marshalPayload() ([]byte, error) {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, message.Token)
	return payload, nil
}

func (message Open) marshalPayload() ([]byte, error) {
	if !ValidDeliveryPolicy(message.DeliveryMode, message.PathSelection, message.Constraints) {
		return nil, invalidPayload(TypeOpen, "delivery policy")
	}
	target, err := message.Target.marshalBinary()
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 50+DeliveryConstraintsSize+len(target))
	copy(payload[:16], message.FlowID[:])
	copy(payload[16:48], message.OpenToken[:])
	payload[48] = byte(message.DeliveryMode)
	payload[49] = byte(message.PathSelection)
	marshalDeliveryConstraints(payload[50:67], message.Constraints)
	copy(payload[67:], target)
	return payload, nil
}

func (message OpenResult) marshalPayload() ([]byte, error) {
	if message.Result > OpenUnsupportedPolicy {
		return nil, invalidPayload(TypeOpenResult, "result")
	}
	if message.Result != OpenSuccess {
		if message.Capability != (Capability{}) || message.DeliveryMode != 0 || message.PathSelection != 0 ||
			message.Constraints != (DeliveryConstraints{}) || message.ImplicitAttachment {
			return nil, invalidPayload(TypeOpenResult, "failure fields")
		}
		payload := make([]byte, 17)
		copy(payload[:16], message.FlowID[:])
		payload[16] = byte(message.Result)
		return payload, nil
	}
	if !ValidDeliveryPolicy(message.DeliveryMode, message.PathSelection, message.Constraints) {
		return nil, invalidPayload(TypeOpenResult, "delivery policy")
	}
	payloadLength := 51 + DeliveryConstraintsSize
	if message.ImplicitAttachment {
		payloadLength++
	}
	payload := make([]byte, payloadLength)
	copy(payload[:16], message.FlowID[:])
	payload[16] = byte(message.Result)
	copy(payload[17:49], message.Capability[:])
	payload[49] = byte(message.DeliveryMode)
	payload[50] = byte(message.PathSelection)
	marshalDeliveryConstraints(payload[51:68], message.Constraints)
	if message.ImplicitAttachment {
		payload[68] = 1
	}
	return payload, nil
}

func marshalDeliveryConstraints(target []byte, constraints DeliveryConstraints) {
	binary.BigEndian.PutUint64(target[:8], uint64(constraints.MaxDeliveryDelay))
	binary.BigEndian.PutUint64(target[8:16], uint64(constraints.MaxDelayGap))
	target[16] = byte(constraints.Fallback)
}

func decodeDeliveryConstraints(frameType Type, payload []byte) (DeliveryConstraints, error) {
	if len(payload) != DeliveryConstraintsSize {
		return DeliveryConstraints{}, invalidPayload(frameType, "delivery constraints length")
	}
	maxDeliveryDelay := binary.BigEndian.Uint64(payload[:8])
	maxDelayGap := binary.BigEndian.Uint64(payload[8:16])
	if maxDeliveryDelay > math.MaxInt64 || maxDelayGap > math.MaxInt64 {
		return DeliveryConstraints{}, invalidPayload(frameType, "delivery constraints overflow")
	}
	return DeliveryConstraints{
		MaxDeliveryDelay: time.Duration(maxDeliveryDelay),
		MaxDelayGap:      time.Duration(maxDelayGap),
		Fallback:         DeliveryConstraintFallback(payload[16]),
	}, nil
}

func (message Join) marshalPayload() ([]byte, error) {
	payload := make([]byte, 48)
	copy(payload[:16], message.FlowID[:])
	copy(payload[16:], message.Capability[:])
	return payload, nil
}

func (message JoinResult) marshalPayload() ([]byte, error) {
	if message.Result != JoinSuccess && message.Result != JoinFailure {
		return nil, invalidPayload(TypeJoinResult, "result")
	}
	payload := make([]byte, 17)
	copy(payload[:16], message.FlowID[:])
	payload[16] = byte(message.Result)
	return payload, nil
}

func (message Data) marshalPayload() ([]byte, error) {
	if len(message.Bytes) < 1 || len(message.Bytes) > MaxDataLength {
		return nil, invalidPayload(TypeData, "data length")
	}
	if uint64(len(message.Bytes)) > math.MaxUint64-message.Offset {
		return nil, invalidPayload(TypeData, "offset overflow")
	}
	payload := make([]byte, 24+len(message.Bytes))
	copy(payload[:16], message.FlowID[:])
	binary.BigEndian.PutUint64(payload[16:24], message.Offset)
	copy(payload[24:], message.Bytes)
	return payload, nil
}

func (message ACK) marshalPayload() ([]byte, error) {
	if err := validateACKRanges(message.NextOffset, message.Ranges); err != nil {
		return nil, err
	}
	payload := make([]byte, 25+16*len(message.Ranges))
	copy(payload[:16], message.FlowID[:])
	binary.BigEndian.PutUint64(payload[16:24], message.NextOffset)
	payload[24] = byte(len(message.Ranges))
	for index, ackRange := range message.Ranges {
		position := 25 + index*16
		binary.BigEndian.PutUint64(payload[position:position+8], ackRange.Start)
		binary.BigEndian.PutUint64(payload[position+8:position+16], ackRange.End)
	}
	return payload, nil
}

func (message FIN) marshalPayload() ([]byte, error) {
	return marshalFinalOffset(message.FlowID, message.FinalOffset), nil
}

func (message FINACK) marshalPayload() ([]byte, error) {
	return marshalFinalOffset(message.FlowID, message.FinalOffset), nil
}

func (message Reset) marshalPayload() ([]byte, error) {
	if message.Reason < ResetCancelled || message.Reason > ResetInternalFailure {
		return nil, invalidPayload(TypeReset, "reason")
	}
	payload := make([]byte, 18)
	copy(payload[:16], message.FlowID[:])
	binary.BigEndian.PutUint16(payload[16:], uint16(message.Reason))
	return payload, nil
}

func decodeACK(payload []byte) (Message, error) {
	rangeCount := int(payload[24])
	if rangeCount > MaxACKRanges || len(payload) != 25+rangeCount*16 {
		return nil, invalidPayload(TypeACK, "range count")
	}
	message := ACK{
		NextOffset: binary.BigEndian.Uint64(payload[16:24]),
		Ranges:     make([]ACKRange, rangeCount),
	}
	copy(message.FlowID[:], payload[:16])
	for index := range message.Ranges {
		position := 25 + index*16
		message.Ranges[index] = ACKRange{
			Start: binary.BigEndian.Uint64(payload[position : position+8]),
			End:   binary.BigEndian.Uint64(payload[position+8 : position+16]),
		}
	}
	if err := validateACKRanges(message.NextOffset, message.Ranges); err != nil {
		return nil, err
	}
	return message, nil
}

func validateACKRanges(nextOffset uint64, ranges []ACKRange) error {
	if len(ranges) > MaxACKRanges {
		return invalidPayload(TypeACK, "too many ranges")
	}
	previousEnd := nextOffset
	for _, ackRange := range ranges {
		if ackRange.Start <= previousEnd || ackRange.End <= ackRange.Start {
			return invalidPayload(TypeACK, "non-canonical ranges")
		}
		previousEnd = ackRange.End
	}
	return nil
}

func marshalFinalOffset(flowID FlowID, finalOffset uint64) []byte {
	payload := make([]byte, 24)
	copy(payload[:16], flowID[:])
	binary.BigEndian.PutUint64(payload[16:], finalOffset)
	return payload
}

func validPrincipalID(principal []byte) bool {
	if len(principal) < 1 || len(principal) > MaxPrincipalIDLength || !principalEdge(principal[0]) {
		return false
	}
	for _, value := range principal[1:] {
		if !principalEdge(value) && value != '.' && value != '_' && value != '-' {
			return false
		}
	}
	return true
}

func ValidPrincipalID(principal string) bool {
	return validPrincipalID([]byte(principal))
}

func principalEdge(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}

func invalidPayload(frameType Type, detail string) error {
	return fmt.Errorf("%w: type 0x%02x %s", ErrInvalidPayload, byte(frameType), detail)
}
