package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	transferHeaderBytes  = 40
	transferChunkBytes   = 32 << 10
	maximumTransfer      = 64 << 20
	transferStatusOK     = 0xa5
	maximumWindowSamples = 10_000

	transferUpload byte = iota + 1
	transferDownload
	transferBidirectional

	flagSynchronizeDownload byte = 1
)

var transferMagic = [4]byte{'V', 'N', '0', '1'}

type transferHeader struct {
	mode          byte
	flags         byte
	requestID     uint64
	seed          uint64
	uploadBytes   uint64
	downloadBytes uint64
}

type targetState struct {
	Accepted  uint64 `json:"accepted"`
	Active    uint64 `json:"active"`
	Completed uint64 `json:"completed"`
	Failures  uint64 `json:"failures"`
}

type targetStateStore struct {
	mu   sync.Mutex
	path string
	seen map[uint64]struct{}
	data targetState
}

type sessionList struct {
	Items []sessionStatus `json:"items"`
}

type flowList struct {
	Items []flowStatus `json:"items"`
}

type flowStatus struct {
	UnacknowledgedBytes uint64 `json:"unacknowledged_bytes"`
	TxAllocatedOffset   uint64 `json:"tx_allocated_offset"`
	TxAcknowledged      uint64 `json:"tx_acknowledged_offset"`
}

type flowWindowObservation struct {
	ActiveFlows           int
	FullWindowFlows       int
	MaximumUnacknowledged uint64
	AcknowledgedBytes     uint64
}

type flowWindowSummary struct {
	WindowBytes            uint64 `json:"window_bytes"`
	Samples                uint64 `json:"samples"`
	ActiveSamples          uint64 `json:"active_samples"`
	FullWindowSamples      uint64 `json:"full_window_samples"`
	MaximumUnacknowledged  uint64 `json:"maximum_unacknowledged_bytes"`
	ACKProgressSamples     uint64 `json:"ack_progress_samples"`
	OneSessionProgress     uint64 `json:"one_session_progress_samples"`
	BothSessionsProgress   uint64 `json:"both_sessions_progress_samples"`
	FullWindowNoProgress   uint64 `json:"full_window_no_ack_progress_samples"`
	LongestFullWindowStall uint64 `json:"longest_full_window_no_ack_streak"`
}

type sessionStatus struct {
	Interface      string `json:"interface"`
	RemoteEndpoint string `json:"remote_endpoint"`
	State          uint8  `json:"state"`
	Quality        struct {
		SmoothedRTTMicros       uint64 `json:"smoothed_rtt_micros"`
		WrittenDataPayloadBytes uint64 `json:"written_data_payload_bytes"`
	} `json:"quality"`
}

type stringValues []string

func (values *stringValues) String() string { return strings.Join(*values, ",") }

func (values *stringValues) Set(value string) error {
	if value == "" {
		return errors.New("empty value")
	}
	*values = append(*values, value)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: network-harness <target|app|wait-status|dump-status|wait-target> [flags]")
	}
	var err error
	switch os.Args[1] {
	case "target":
		err = runTarget(os.Args[2:])
	case "app":
		err = runApplication(os.Args[2:])
	case "wait-status":
		err = waitStatus(os.Args[2:])
	case "dump-status":
		err = dumpStatus(os.Args[2:])
	case "wait-target":
		err = waitTarget(os.Args[2:])
	case "wait-resources":
		err = waitResources(os.Args[2:])
	case "session-written":
		err = sessionWritten(os.Args[2:], os.Stdout)
	case "sample-flow-window":
		err = sampleFlowWindow(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatalf("%v", err)
	}
}

type summary struct {
	Resources struct {
		Flows              uint64 `json:"flows"`
		Sessions           uint64 `json:"sessions"`
		SOCKSConnections   uint64 `json:"socks_connections"`
		PendingTargetDials uint64 `json:"pending_target_dials"`
	} `json:"resources"`
}

func runTarget(arguments []string) error {
	flags := flag.NewFlagSet("target", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:18080", "target listen address")
	readyFile := flags.String("ready-file", "", "readiness file")
	stateFile := flags.String("state-file", "", "state file")
	deadline := flags.Duration("connection-timeout", 2*time.Minute, "per-connection timeout")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || *readyFile == "" || *stateFile == "" || *deadline <= 0 {
		return errors.New("invalid target flags")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	store := &targetStateStore{path: *stateFile, seen: make(map[uint64]struct{})}
	if err := store.writeLocked(); err != nil {
		return err
	}
	if err := createSignalFile(*readyFile); err != nil {
		return err
	}
	for {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return acceptErr
		}
		if err := store.accepted(); err != nil {
			_ = connection.Close()
			return err
		}
		go func() {
			handleErr := handleTargetConnection(connection, store, *deadline)
			if updateErr := store.finished(handleErr); updateErr != nil {
				_, _ = fmt.Fprintf(os.Stderr, "target state update: %v\n", updateErr)
			}
			if handleErr != nil {
				_, _ = fmt.Fprintf(os.Stderr, "target connection: %v\n", handleErr)
			}
		}()
	}
}

func (store *targetStateStore) accepted() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data.Accepted++
	store.data.Active++
	return store.writeLocked()
}

func (store *targetStateStore) register(requestID uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, duplicate := store.seen[requestID]; duplicate {
		return fmt.Errorf("duplicate request id %d", requestID)
	}
	store.seen[requestID] = struct{}{}
	return nil
}

func (store *targetStateStore) finished(result error) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.data.Active == 0 {
		return errors.New("target active count underflow")
	}
	store.data.Active--
	if result == nil {
		store.data.Completed++
	} else {
		store.data.Failures++
	}
	return store.writeLocked()
}

func (store *targetStateStore) writeLocked() error {
	encoded, err := json.Marshal(store.data)
	if err != nil {
		return err
	}
	temporary := store.path + ".tmp"
	if err := os.WriteFile(temporary, encoded, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, store.path)
}

func handleTargetConnection(connection net.Conn, store *targetStateStore, timeout time.Duration) error {
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	header, err := readTransferHeader(connection)
	if err != nil {
		return err
	}
	if err := store.register(header.requestID); err != nil {
		return err
	}

	progress := newTransferProgress(header.uploadBytes)
	downloadResult := make(chan error, 1)
	go func() {
		downloadResult <- writeTargetDownload(connection, header, progress)
	}()
	uploadErr := readTargetUpload(connection, header, progress)
	progress.finish(uploadErr)
	downloadErr := <-downloadResult
	if uploadErr != nil {
		return uploadErr
	}
	if downloadErr != nil {
		return downloadErr
	}
	if _, err := connection.Write([]byte{transferStatusOK}); err != nil {
		return err
	}
	if tcp, ok := connection.(*net.TCPConn); ok {
		return tcp.CloseWrite()
	}
	return nil
}

type transferProgress struct {
	mu          sync.Mutex
	condition   *sync.Cond
	uploaded    uint64
	uploadBytes uint64
	finished    bool
	err         error
}

func newTransferProgress(uploadBytes uint64) *transferProgress {
	progress := &transferProgress{uploadBytes: uploadBytes}
	progress.condition = sync.NewCond(&progress.mu)
	return progress
}

func (progress *transferProgress) add(bytes uint64) {
	progress.mu.Lock()
	progress.uploaded += bytes
	progress.condition.Broadcast()
	progress.mu.Unlock()
}

func (progress *transferProgress) finish(err error) {
	progress.mu.Lock()
	progress.finished = true
	progress.err = err
	progress.condition.Broadcast()
	progress.mu.Unlock()
}

func (progress *transferProgress) wait(required uint64) error {
	progress.mu.Lock()
	defer progress.mu.Unlock()
	for progress.uploaded < required && !progress.finished {
		progress.condition.Wait()
	}
	if progress.uploaded >= required {
		return nil
	}
	if progress.err != nil {
		return progress.err
	}
	return io.ErrUnexpectedEOF
}

func readTargetUpload(connection net.Conn, header transferHeader, progress *transferProgress) error {
	buffer := make([]byte, transferChunkBytes)
	var offset uint64
	for offset < header.uploadBytes {
		count := int(min64(uint64(len(buffer)), header.uploadBytes-offset))
		if _, err := io.ReadFull(connection, buffer[:count]); err != nil {
			return fmt.Errorf("upload read at %d: %w", offset, err)
		}
		if err := verifyPattern(buffer[:count], header.seed, offset); err != nil {
			return fmt.Errorf("upload verify at %d: %w", offset, err)
		}
		offset += uint64(count)
		progress.add(uint64(count))
	}
	var extra [1]byte
	if count, err := connection.Read(extra[:]); count != 0 || !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("missing upload EOF")
		}
		return fmt.Errorf("upload boundary: n=%d: %w", count, err)
	}
	return nil
}

func writeTargetDownload(connection net.Conn, header transferHeader, progress *transferProgress) error {
	buffer := make([]byte, transferChunkBytes)
	var offset uint64
	for offset < header.downloadBytes {
		count := int(min64(uint64(len(buffer)), header.downloadBytes-offset))
		end := offset + uint64(count)
		if header.flags&flagSynchronizeDownload != 0 && header.uploadBytes != 0 {
			required := (end*header.uploadBytes + header.downloadBytes - 1) / header.downloadBytes
			if err := progress.wait(required); err != nil {
				return fmt.Errorf("download synchronization: %w", err)
			}
		}
		fillPattern(buffer[:count], header.seed^0xd0d0d0d0d0d0d0d0, offset)
		if err := writeAll(connection, buffer[:count]); err != nil {
			return fmt.Errorf("download write at %d: %w", offset, err)
		}
		offset = end
	}
	return nil
}

func runApplication(arguments []string) error {
	flags := flag.NewFlagSet("app", flag.ContinueOnError)
	socksAddress := flags.String("socks", "127.0.0.1:11080", "SOCKS address")
	socksUsername := flags.String("socks-username", "network-test", "SOCKS username; empty with password disables authentication")
	socksPassword := flags.String("socks-password", "network-test-password", "SOCKS password; empty with username disables authentication")
	targetAddress := flags.String("target", "127.0.0.1:18080", "target address")
	modeName := flags.String("mode", "bidirectional", "upload, download, or bidirectional")
	requestID := flags.Uint64("id", 0, "unique request id")
	seed := flags.Uint64("seed", 1, "payload seed")
	uploadBytes := flags.Uint64("upload-bytes", 0, "upload byte count")
	downloadBytes := flags.Uint64("download-bytes", 0, "download byte count")
	pauseAfter := flags.Uint64("pause-after", 0, "pause upload after this many bytes")
	readyFile := flags.String("ready-file", "", "pause readiness file")
	continueFile := flags.String("continue-file", "", "pause continuation file")
	progressFile := flags.String("progress-file", "", "first verified download progress file")
	resultFile := flags.String("result-file", "", "successful transfer duration file")
	timeout := flags.Duration("timeout", 90*time.Second, "application timeout")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || *requestID == 0 || *timeout <= 0 ||
		(*socksUsername == "") != (*socksPassword == "") || len(*socksUsername) > 255 || len(*socksPassword) > 255 {
		return errors.New("invalid app flags")
	}
	mode, err := parseTransferMode(*modeName)
	if err != nil {
		return err
	}
	if *uploadBytes > maximumTransfer || *downloadBytes > maximumTransfer || (*uploadBytes == 0 && *downloadBytes == 0) {
		return errors.New("invalid transfer size")
	}
	if mode == transferUpload && (*uploadBytes == 0 || *downloadBytes != 0) ||
		mode == transferDownload && (*uploadBytes != 0 || *downloadBytes == 0) ||
		mode == transferBidirectional && (*uploadBytes == 0 || *downloadBytes == 0) {
		return errors.New("transfer sizes do not match mode")
	}
	if *pauseAfter != 0 && (*pauseAfter >= *uploadBytes || *readyFile == "" || *continueFile == "") {
		return errors.New("invalid pause configuration")
	}
	connection, err := net.DialTimeout("tcp", *socksAddress, 5*time.Second)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(*timeout)); err != nil {
		return err
	}
	if err := socksConnect(connection, *targetAddress, *socksUsername, *socksPassword); err != nil {
		return err
	}
	startedAt := time.Now()
	header := transferHeader{
		mode: mode, requestID: *requestID, seed: *seed,
		uploadBytes: *uploadBytes, downloadBytes: *downloadBytes,
	}
	if *pauseAfter != 0 && *downloadBytes != 0 {
		header.flags |= flagSynchronizeDownload
	}
	if err := writeTransferHeader(connection, header); err != nil {
		return err
	}
	downloadResult := make(chan error, 1)
	go func() {
		downloadResult <- readApplicationDownload(connection, header, *progressFile)
	}()
	uploadErr := writeApplicationUpload(connection, header, *pauseAfter, *readyFile, *continueFile, *timeout)
	if uploadErr == nil {
		if tcp, ok := connection.(*net.TCPConn); ok {
			uploadErr = tcp.CloseWrite()
		}
	}
	if uploadErr != nil {
		_ = connection.Close()
	}
	downloadErr := <-downloadResult
	if uploadErr != nil {
		return uploadErr
	}
	if downloadErr != nil {
		return downloadErr
	}
	if *resultFile != "" {
		return writeResultFile(*resultFile, time.Since(startedAt))
	}
	return nil
}

func parseTransferMode(value string) (byte, error) {
	switch value {
	case "upload":
		return transferUpload, nil
	case "download":
		return transferDownload, nil
	case "bidirectional":
		return transferBidirectional, nil
	default:
		return 0, fmt.Errorf("invalid mode %q", value)
	}
}

func writeApplicationUpload(connection net.Conn, header transferHeader, pauseAfter uint64, readyFile, continueFile string, timeout time.Duration) error {
	buffer := make([]byte, transferChunkBytes)
	var offset uint64
	paused := false
	for offset < header.uploadBytes {
		count := min64(uint64(len(buffer)), header.uploadBytes-offset)
		if pauseAfter != 0 && !paused && offset < pauseAfter && offset+count > pauseAfter {
			count = pauseAfter - offset
		}
		fillPattern(buffer[:count], header.seed, offset)
		if err := writeAll(connection, buffer[:count]); err != nil {
			return fmt.Errorf("upload write at %d: %w", offset, err)
		}
		offset += count
		if pauseAfter != 0 && !paused && offset == pauseAfter {
			if err := createSignalFile(readyFile); err != nil {
				return err
			}
			if err := waitForFile(continueFile, timeout); err != nil {
				return err
			}
			paused = true
		}
	}
	return nil
}

func readApplicationDownload(connection net.Conn, header transferHeader, progressFile string) error {
	buffer := make([]byte, transferChunkBytes)
	var offset uint64
	for offset < header.downloadBytes {
		count := int(min64(uint64(len(buffer)), header.downloadBytes-offset))
		if _, err := io.ReadFull(connection, buffer[:count]); err != nil {
			return fmt.Errorf("download read at %d: %w", offset, err)
		}
		if err := verifyPattern(buffer[:count], header.seed^0xd0d0d0d0d0d0d0d0, offset); err != nil {
			return fmt.Errorf("download verify at %d: %w", offset, err)
		}
		offset += uint64(count)
		if progressFile != "" {
			if err := createSignalFile(progressFile); err != nil {
				return err
			}
			progressFile = ""
		}
	}
	var status [1]byte
	if _, err := io.ReadFull(connection, status[:]); err != nil {
		return fmt.Errorf("target status: %w", err)
	}
	if status[0] != transferStatusOK {
		return fmt.Errorf("target status = %#x", status[0])
	}
	return nil
}

func writeResultFile(path string, duration time.Duration) error {
	if path == "" || duration <= 0 {
		return errors.New("invalid result file")
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, []byte(strconv.FormatInt(duration.Nanoseconds(), 10)+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func socksConnect(connection net.Conn, target, username, password string) error {
	method := byte(0)
	if username != "" {
		method = 2
	}
	if err := writeAll(connection, []byte{5, 1, method}); err != nil {
		return err
	}
	var greeting [2]byte
	if _, err := io.ReadFull(connection, greeting[:]); err != nil {
		return err
	}
	if greeting != [2]byte{5, method} {
		return fmt.Errorf("SOCKS greeting = %x", greeting)
	}
	if method == 2 {
		authentication := []byte{1, byte(len(username))}
		authentication = append(authentication, username...)
		authentication = append(authentication, byte(len(password)))
		authentication = append(authentication, password...)
		if err := writeAll(connection, authentication); err != nil {
			return err
		}
		var authenticationReply [2]byte
		if _, err := io.ReadFull(connection, authenticationReply[:]); err != nil {
			return err
		}
		if authenticationReply != [2]byte{1, 0} {
			return fmt.Errorf("SOCKS authentication = %x", authenticationReply)
		}
	}
	host, portValue, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	address := net.ParseIP(host).To4()
	port, err := strconv.ParseUint(portValue, 10, 16)
	if address == nil || err != nil || port == 0 {
		return errors.New("target must be a non-zero IPv4 endpoint")
	}
	request := []byte{5, 1, 0, 1, address[0], address[1], address[2], address[3], byte(port >> 8), byte(port)}
	if err := writeAll(connection, request); err != nil {
		return err
	}
	var response [10]byte
	if _, err := io.ReadFull(connection, response[:]); err != nil {
		return err
	}
	if response[0] != 5 || response[1] != 0 || response[2] != 0 || response[3] != 1 {
		return fmt.Errorf("SOCKS response = %x", response)
	}
	return nil
}

func writeTransferHeader(writer io.Writer, header transferHeader) error {
	var encoded [transferHeaderBytes]byte
	copy(encoded[:4], transferMagic[:])
	encoded[4] = header.mode
	encoded[5] = header.flags
	binary.BigEndian.PutUint64(encoded[8:16], header.requestID)
	binary.BigEndian.PutUint64(encoded[16:24], header.seed)
	binary.BigEndian.PutUint64(encoded[24:32], header.uploadBytes)
	binary.BigEndian.PutUint64(encoded[32:40], header.downloadBytes)
	return writeAll(writer, encoded[:])
}

func readTransferHeader(reader io.Reader) (transferHeader, error) {
	var encoded [transferHeaderBytes]byte
	if _, err := io.ReadFull(reader, encoded[:]); err != nil {
		return transferHeader{}, err
	}
	if !bytes.Equal(encoded[:4], transferMagic[:]) || encoded[6] != 0 || encoded[7] != 0 || encoded[5]&^flagSynchronizeDownload != 0 {
		return transferHeader{}, errors.New("invalid transfer header")
	}
	header := transferHeader{
		mode: encoded[4], flags: encoded[5], requestID: binary.BigEndian.Uint64(encoded[8:16]),
		seed: binary.BigEndian.Uint64(encoded[16:24]), uploadBytes: binary.BigEndian.Uint64(encoded[24:32]),
		downloadBytes: binary.BigEndian.Uint64(encoded[32:40]),
	}
	if header.mode < transferUpload || header.mode > transferBidirectional ||
		header.requestID == 0 || header.uploadBytes > maximumTransfer || header.downloadBytes > maximumTransfer {
		return transferHeader{}, errors.New("invalid transfer header values")
	}
	if header.mode == transferUpload && (header.uploadBytes == 0 || header.downloadBytes != 0) ||
		header.mode == transferDownload && (header.uploadBytes != 0 || header.downloadBytes == 0) ||
		header.mode == transferBidirectional && (header.uploadBytes == 0 || header.downloadBytes == 0) {
		return transferHeader{}, errors.New("invalid transfer header mode")
	}
	return header, nil
}

func waitStatus(arguments []string) error {
	flags := flag.NewFlagSet("wait-status", flag.ContinueOnError)
	url := flags.String("url", "http://127.0.0.1:18081/api/v1/sessions", "sessions URL")
	readySessions := flags.Int("ready-sessions", 2, "minimum ready sessions")
	fastestInterface := flags.String("fastest-interface", "", "required fastest ready interface")
	timeout := flags.Duration("timeout", 20*time.Second, "wait timeout")
	var readyInterfaces stringValues
	flags.Var(&readyInterfaces, "ready-interface", "required ready interface")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || *readySessions < 0 || *timeout <= 0 {
		return errors.New("invalid wait-status flags")
	}
	deadline := time.Now().Add(*timeout)
	var last string
	for time.Now().Before(deadline) {
		data, err := getURL(*url)
		if err == nil {
			last = string(data)
			var sessions sessionList
			if json.Unmarshal(data, &sessions) == nil && sessionsReady(sessions, *readySessions, readyInterfaces, *fastestInterface) {
				return nil
			}
		} else {
			last = err.Error()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("status condition timed out: %s", last)
}

func sessionsReady(sessions sessionList, minimum int, required []string, fastest string) bool {
	ready := make(map[string]uint64)
	count := 0
	for _, session := range sessions.Items {
		if session.State != 3 {
			continue
		}
		count++
		ready[session.Interface] = session.Quality.SmoothedRTTMicros
	}
	if count < minimum {
		return false
	}
	for _, name := range required {
		if _, exists := ready[name]; !exists {
			return false
		}
	}
	if fastest == "" {
		return true
	}
	fastestRTT, exists := ready[fastest]
	if !exists || fastestRTT == 0 {
		return false
	}
	for name, rtt := range ready {
		if name != fastest && (rtt == 0 || rtt <= fastestRTT) {
			return false
		}
	}
	return true
}

func sessionWritten(arguments []string, output io.Writer) error {
	flags := flag.NewFlagSet("session-written", flag.ContinueOnError)
	url := flags.String("url", "http://127.0.0.1:18081/api/v1/sessions", "sessions URL")
	interfaceName := flags.String("interface", "", "client interface name")
	remoteHost := flags.String("remote-host", "", "server session remote host")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || output == nil || (*interfaceName == "") == (*remoteHost == "") {
		return errors.New("invalid session-written flags")
	}
	data, err := getURL(*url)
	if err != nil {
		return err
	}
	var sessions sessionList
	if err := json.Unmarshal(data, &sessions); err != nil {
		return err
	}
	written, matches := selectSessionWritten(sessions, *interfaceName, *remoteHost)
	if matches != 1 {
		return fmt.Errorf("session selector matched %d sessions", matches)
	}
	_, err = fmt.Fprintln(output, written)
	return err
}

func selectSessionWritten(sessions sessionList, interfaceName, remoteHost string) (uint64, int) {
	var written uint64
	matches := 0
	for _, session := range sessions.Items {
		matched := interfaceName != "" && session.Interface == interfaceName
		if remoteHost != "" {
			host, _, err := net.SplitHostPort(session.RemoteEndpoint)
			matched = err == nil && host == remoteHost
		}
		if !matched {
			continue
		}
		matches++
		written += session.Quality.WrittenDataPayloadBytes
	}
	return written, matches
}

func observeFlowWindow(flows flowList, window uint64) (flowWindowObservation, error) {
	if window == 0 {
		return flowWindowObservation{}, errors.New("invalid flow window")
	}
	observation := flowWindowObservation{ActiveFlows: len(flows.Items)}
	for _, flow := range flows.Items {
		if flow.TxAcknowledged > flow.TxAllocatedOffset || flow.UnacknowledgedBytes != flow.TxAllocatedOffset-flow.TxAcknowledged {
			return flowWindowObservation{}, errors.New("inconsistent flow offsets")
		}
		if flow.UnacknowledgedBytes > observation.MaximumUnacknowledged {
			observation.MaximumUnacknowledged = flow.UnacknowledgedBytes
		}
		if math.MaxUint64-observation.AcknowledgedBytes < flow.TxAcknowledged {
			return flowWindowObservation{}, errors.New("flow acknowledgement overflow")
		}
		observation.AcknowledgedBytes += flow.TxAcknowledged
		if flow.UnacknowledgedBytes == window {
			observation.FullWindowFlows++
		}
	}
	return observation, nil
}

func (summary *flowWindowSummary) add(observation flowWindowObservation, acknowledgedProgress bool, sessionsProgressed int, fullWindowStreak *uint64) {
	summary.Samples++
	if observation.ActiveFlows != 0 {
		summary.ActiveSamples++
	}
	if observation.FullWindowFlows != 0 {
		summary.FullWindowSamples++
	}
	if observation.MaximumUnacknowledged > summary.MaximumUnacknowledged {
		summary.MaximumUnacknowledged = observation.MaximumUnacknowledged
	}
	if acknowledgedProgress {
		summary.ACKProgressSamples++
	}
	switch sessionsProgressed {
	case 1:
		summary.OneSessionProgress++
	case 2:
		summary.BothSessionsProgress++
	}
	if observation.FullWindowFlows != 0 && !acknowledgedProgress {
		summary.FullWindowNoProgress++
		*fullWindowStreak++
		if *fullWindowStreak > summary.LongestFullWindowStall {
			summary.LongestFullWindowStall = *fullWindowStreak
		}
	} else {
		*fullWindowStreak = 0
	}
}

func sampleFlowWindow(arguments []string) error {
	flags := flag.NewFlagSet("sample-flow-window", flag.ContinueOnError)
	url := flags.String("url", "http://127.0.0.1:18082/api/v1/flows", "flows URL")
	sessionsURL := flags.String("sessions-url", "http://127.0.0.1:18082/api/v1/sessions", "sessions URL")
	stopFile := flags.String("stop-file", "", "file that stops sampling")
	resultFile := flags.String("result-file", "", "summary result file")
	window := flags.Uint64("window-bytes", 320<<10, "full Flow send window")
	interval := flags.Duration("interval", 50*time.Millisecond, "sample interval")
	timeout := flags.Duration("timeout", 80*time.Second, "sampling timeout")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || *url == "" || *sessionsURL == "" || *stopFile == "" || *resultFile == "" ||
		*window == 0 || *interval < 10*time.Millisecond || *interval > time.Second || *timeout <= 0 || *timeout > 90*time.Second ||
		*timeout / *interval > maximumWindowSamples {
		return errors.New("invalid sample-flow-window flags")
	}
	summary := flowWindowSummary{WindowBytes: *window}
	var previousAcknowledged uint64
	previousSessions := make(map[string]uint64, 2)
	var fullWindowStreak uint64
	havePrevious := false
	deadline := time.Now().Add(*timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(*stopFile); err == nil {
			return writeJSONFile(*resultFile, summary)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		data, err := getURL(*url)
		sessionData, sessionErr := getURL(*sessionsURL)
		if err == nil && sessionErr == nil {
			var flows flowList
			if err := json.Unmarshal(data, &flows); err != nil {
				return err
			}
			var sessions sessionList
			if err := json.Unmarshal(sessionData, &sessions); err != nil {
				return err
			}
			observation, err := observeFlowWindow(flows, *window)
			if err != nil {
				return err
			}
			progressed := 0
			currentSessions := make(map[string]uint64, len(sessions.Items))
			for _, session := range sessions.Items {
				currentSessions[session.RemoteEndpoint] = session.Quality.WrittenDataPayloadBytes
				if previous, exists := previousSessions[session.RemoteEndpoint]; havePrevious && exists && session.Quality.WrittenDataPayloadBytes > previous {
					progressed++
				}
			}
			acknowledgedProgress := havePrevious && observation.AcknowledgedBytes > previousAcknowledged
			summary.add(observation, acknowledgedProgress, progressed, &fullWindowStreak)
			previousAcknowledged = observation.AcknowledgedBytes
			previousSessions = currentSessions
			havePrevious = true
		}
		time.Sleep(*interval)
	}
	if err := writeJSONFile(*resultFile, summary); err != nil {
		return err
	}
	return errors.New("flow window sampling timed out")
}

func writeJSONFile(path string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(encoded, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func dumpStatus(arguments []string) error {
	flags := flag.NewFlagSet("dump-status", flag.ContinueOnError)
	base := flags.String("base-url", "http://127.0.0.1:18081", "status base URL")
	var rejected stringValues
	flags.Var(&rejected, "reject-value", "sensitive value that must not appear in status responses")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return errors.New("invalid dump-status flags")
	}
	return inspectStatus(*base, rejected, os.Stdout)
}

func inspectStatus(base string, rejected []string, output io.Writer) error {
	if strings.TrimSpace(base) == "" || output == nil {
		return errors.New("invalid status inspection")
	}
	var result error
	for _, route := range []string{"health", "summary", "interfaces", "sessions", "flows"} {
		data, err := getURL(strings.TrimRight(base, "/") + "/api/v1/" + route)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("%s: %w", route, err))
			continue
		}
		if !json.Valid(data) {
			result = errors.Join(result, fmt.Errorf("%s: invalid JSON response", route))
			continue
		}
		containsRejected := false
		for _, value := range rejected {
			if bytes.Contains(data, []byte(value)) {
				result = errors.Join(result, fmt.Errorf("%s: rejected value present", route))
				containsRejected = true
			}
		}
		if containsRejected {
			continue
		}
		_, _ = fmt.Fprintf(output, "%s: %s\n", route, data)
	}
	return result
}

func getURL(url string) ([]byte, error) {
	client := http.Client{Timeout: time.Second}
	response, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP status %d", response.StatusCode)
	}
	return io.ReadAll(io.LimitReader(response.Body, 1<<20))
}

func waitTarget(arguments []string) error {
	flags := flag.NewFlagSet("wait-target", flag.ContinueOnError)
	stateFile := flags.String("state-file", "", "target state file")
	accepted := flags.Uint64("accepted", 0, "expected accepted connections")
	completed := flags.Uint64("completed", 0, "expected completed connections")
	timeout := flags.Duration("timeout", 15*time.Second, "wait timeout")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || *stateFile == "" || *timeout <= 0 {
		return errors.New("invalid wait-target flags")
	}
	deadline := time.Now().Add(*timeout)
	var last targetState
	for time.Now().Before(deadline) {
		encoded, err := os.ReadFile(*stateFile)
		if err == nil && json.Unmarshal(encoded, &last) == nil {
			if last.Failures != 0 || last.Accepted > *accepted || last.Completed > *completed {
				return fmt.Errorf("target state violated: %+v", last)
			}
			if last.Accepted == *accepted && last.Completed == *completed && last.Active == 0 {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("target state timed out: %+v", last)
}

func waitResources(arguments []string) error {
	flags := flag.NewFlagSet("wait-resources", flag.ContinueOnError)
	url := flags.String("url", "", "summary URL")
	flows := flags.Uint64("flows", 0, "expected flows")
	sessions := flags.Uint64("sessions", 0, "expected sessions")
	socks := flags.Uint64("socks-connections", 0, "expected SOCKS connections")
	targetDials := flags.Uint64("target-dials", 0, "expected pending target dials")
	timeout := flags.Duration("timeout", 20*time.Second, "wait timeout")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || *url == "" || *timeout <= 0 {
		return errors.New("invalid wait-resources flags")
	}
	deadline := time.Now().Add(*timeout)
	var last summary
	for time.Now().Before(deadline) {
		encoded, err := getURL(*url)
		if err == nil && json.Unmarshal(encoded, &last) == nil &&
			last.Resources.Flows == *flows && last.Resources.Sessions == *sessions &&
			last.Resources.SOCKSConnections == *socks && last.Resources.PendingTargetDials == *targetDials {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("resource state timed out: %+v", last.Resources)
}

func fillPattern(buffer []byte, seed, offset uint64) {
	for index := range buffer {
		position := offset + uint64(index)
		value := position*0x9e3779b97f4a7c15 + seed*0xbf58476d1ce4e5b9
		value ^= value >> 30
		value *= 0xbf58476d1ce4e5b9
		value ^= value >> 27
		buffer[index] = byte(value ^ (value >> 31))
	}
}

func verifyPattern(buffer []byte, seed, offset uint64) error {
	expected := make([]byte, len(buffer))
	fillPattern(expected, seed, offset)
	if bytes.Equal(buffer, expected) {
		return nil
	}
	for index := range buffer {
		if buffer[index] != expected[index] {
			return fmt.Errorf("byte %d = %#x, want %#x", offset+uint64(index), buffer[index], expected[index])
		}
	}
	return errors.New("payload mismatch")
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		count, err := writer.Write(data)
		if count < 0 || count > len(data) {
			return io.ErrShortWrite
		}
		data = data[count:]
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func createSignalFile(path string) error {
	if path == "" {
		return errors.New("empty signal file")
	}
	return os.WriteFile(path, []byte("ready\n"), 0o600)
}

func waitForFile(path string, maximum time.Duration) error {
	deadline := time.Now().Add(maximum)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", path)
}

func min64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}

func fatalf(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "network-harness: "+format+"\n", values...)
	os.Exit(1)
}
