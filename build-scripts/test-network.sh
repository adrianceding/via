#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${VIA_NETWORK_TIMEOUT_ACTIVE:-}" != "1" ]]; then
	export VIA_NETWORK_TIMEOUT_ACTIVE=1
	exec timeout --foreground "${VIA_NETWORK_TIMEOUT:-12m}" "$0" "$@"
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

for command_name in awk cat go ip tc sudo timeout setpriv sysctl; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		echo "test-network: 缺少命令: $command_name" >&2
		exit 1
	fi
done
if [[ "$(uname -s)" != "Linux" ]]; then
	echo "test-network: 只支持 Linux" >&2
	exit 1
fi
if ! sudo -n true; then
	echo "test-network: 需要可用的 sudo -n 网络管理权限" >&2
	exit 1
fi

umask 077
RUN_UID="$(id -u)"
RUN_GID="$(id -g)"
RUN_TOKEN="${RUN_UID}-${BASHPID}"
CLIENT_NS="via-c-${RUN_TOKEN}"
SERVER_NS="via-s-${RUN_TOKEN}"
HOST_CLIENT_A="vca${BASHPID}"
HOST_SERVER_A="vsa${BASHPID}"
HOST_CLIENT_B="vcb${BASHPID}"
HOST_SERVER_B="vsb${BASHPID}"
CLIENT_A="vianet-a"
CLIENT_B="vianet-b"
SERVER_A="viasrv-a"
SERVER_B="viasrv-b"
RELAY_ADDRESS="10.200.0.1"
CLIENT_A_ADDRESS="10.201.0.2"
SERVER_A_ADDRESS="10.201.0.1"
CLIENT_B_ADDRESS="10.202.0.2"
SERVER_B_ADDRESS="10.202.0.1"
RELAY_ENDPOINT="${RELAY_ADDRESS}:19443"
SOCKS_ENDPOINT="127.0.0.1:11080"
TARGET_ENDPOINT="127.0.0.1:18080"
STATUS_ENDPOINT="http://127.0.0.1:18081"

if [[ ${#HOST_CLIENT_A} -gt 15 || ${#HOST_SERVER_A} -gt 15 || ${#HOST_CLIENT_B} -gt 15 || ${#HOST_SERVER_B} -gt 15 ]]; then
	echo "test-network: 运行标识过长，无法创建 veth" >&2
	exit 1
fi

ARTIFACT_DIR="${VIA_NETWORK_ARTIFACT_DIR:-}"
if [[ -z "$ARTIFACT_DIR" ]]; then
	ARTIFACT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/via-network-${RUN_TOKEN}.XXXXXX")"
else
	mkdir -p "$ARTIFACT_DIR"
fi
VIA_BINARY="$ARTIFACT_DIR/via"
HARNESS_BINARY="$ARTIFACT_DIR/network-harness"
CLIENT_CONFIG="$ARTIFACT_DIR/client.yml"
SERVER_CONFIG="$ARTIFACT_DIR/server.yml"
TARGET_READY="$ARTIFACT_DIR/target.ready"
TARGET_STATE="$ARTIFACT_DIR/target-state.json"
TARGET_LOG="$ARTIFACT_DIR/target.log"
SERVER_LOG="$ARTIFACT_DIR/server.log"
CLIENT_STATUS_LOG="$ARTIFACT_DIR/client-status.log"
SERVER_STATUS_LOG="$ARTIFACT_DIR/server-status.log"
TC_LOG="$ARTIFACT_DIR/tc.log"
NETWORK_TEST_PSK="MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
CLIENT_LOG=""
CLIENT_LAUNCH_PID=""
TARGET_LAUNCH_PID=""
SERVER_LAUNCH_PID=""
EXPECTED_TARGET_CONNECTIONS=0
NEXT_REQUEST_ID=1000

kill_namespace_processes() {
	local namespace="$1"
	local signal_name="$2"
	local pid
	while read -r pid; do
		[[ -n "$pid" ]] && sudo -n kill "-$signal_name" "$pid" >/dev/null 2>&1 || true
	done < <(sudo -n ip netns pids "$namespace" 2>/dev/null || true)
}

delete_namespace() {
	local namespace="$1"
	if sudo -n ip netns list | awk '{print $1}' | awk -v wanted="$namespace" '$0 == wanted { found = 1 } END { exit !found }'; then
		kill_namespace_processes "$namespace" TERM
		for _ in {1..30}; do
			[[ -z "$(sudo -n ip netns pids "$namespace" 2>/dev/null)" ]] && break
			sleep 0.1
		done
		kill_namespace_processes "$namespace" KILL
		sudo -n ip netns delete "$namespace" >/dev/null 2>&1 || true
	fi
}

delete_host_links() {
	local interface_name
	for interface_name in "$HOST_CLIENT_A" "$HOST_SERVER_A" "$HOST_CLIENT_B" "$HOST_SERVER_B"; do
		sudo -n ip link delete "$interface_name" >/dev/null 2>&1 || true
	done
}

run_in_namespace() {
	local namespace="$1"
	shift
	sudo -n ip netns exec "$namespace" setpriv --reuid="$RUN_UID" --regid="$RUN_GID" --clear-groups "$@"
}

run_in_namespace_timeout() {
	local maximum="$1"
	local namespace="$2"
	shift 2
	timeout --foreground "$maximum" sudo -n ip netns exec "$namespace" \
		setpriv --reuid="$RUN_UID" --regid="$RUN_GID" --clear-groups "$@"
}

dump_diagnostics() {
	set +e
	echo "test-network: 诊断目录: $ARTIFACT_DIR" >&2
	for logfile in "$SERVER_LOG" "$CLIENT_LOG" "$TARGET_LOG" "$TC_LOG" "$CLIENT_STATUS_LOG" "$SERVER_STATUS_LOG"; do
		if [[ -n "$logfile" && -f "$logfile" ]]; then
			echo "===== $logfile =====" >&2
			tail -n 200 "$logfile" >&2
		fi
	done
	if sudo -n ip netns list | awk '{print $1}' | awk -v wanted="$CLIENT_NS" '$0 == wanted { found = 1 } END { exit !found }'; then
		echo "===== client addr/rule/route/qdisc =====" >&2
		sudo -n ip -n "$CLIENT_NS" -details address show >&2
		sudo -n ip -n "$CLIENT_NS" rule show >&2
		sudo -n ip -n "$CLIENT_NS" route show table all >&2
		sudo -n ip netns exec "$CLIENT_NS" tc -s qdisc show >&2
		run_in_namespace "$CLIENT_NS" "$HARNESS_BINARY" dump-status --base-url "$STATUS_ENDPOINT" >&2 || true
	fi
	if sudo -n ip netns list | awk '{print $1}' | awk -v wanted="$SERVER_NS" '$0 == wanted { found = 1 } END { exit !found }'; then
		echo "===== server addr/route/qdisc =====" >&2
		sudo -n ip -n "$SERVER_NS" -details address show >&2
		sudo -n ip -n "$SERVER_NS" route show table all >&2
		sudo -n ip netns exec "$SERVER_NS" tc -s qdisc show >&2
		run_in_namespace "$SERVER_NS" "$HARNESS_BINARY" dump-status --base-url "http://127.0.0.1:18082" >&2 || true
	fi
	if [[ -f "$TARGET_STATE" ]]; then
		echo "===== target state =====" >&2
		tr -d '\n' <"$TARGET_STATE" >&2
		echo >&2
	fi
}

cleanup() {
	local exit_status=$?
	trap - EXIT INT TERM
	set +e
	if [[ $exit_status -ne 0 ]]; then
		dump_diagnostics
	fi
	delete_namespace "$CLIENT_NS"
	delete_namespace "$SERVER_NS"
	delete_host_links
	for launch_pid in "$CLIENT_LAUNCH_PID" "$TARGET_LAUNCH_PID" "$SERVER_LAUNCH_PID"; do
		[[ -n "$launch_pid" ]] && wait "$launch_pid" >/dev/null 2>&1 || true
	done
	if [[ $exit_status -eq 0 && "${VIA_KEEP_NETWORK_ARTIFACTS:-0}" != "1" ]]; then
		rm -rf "$ARTIFACT_DIR"
	else
		echo "test-network: 已保留诊断: $ARTIFACT_DIR" >&2
	fi
	exit "$exit_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_file() {
	local path="$1"
	local attempts="${2:-200}"
	for ((attempt = 0; attempt < attempts; attempt++)); do
		[[ -f "$path" ]] && return 0
		sleep 0.05
	done
	echo "test-network: 等待文件超时: $path" >&2
	return 1
}

configure_client_route_a() {
	sudo -n ip -n "$CLIENT_NS" route replace table 201 10.201.0.0/30 dev "$CLIENT_A" scope link src "$CLIENT_A_ADDRESS"
	sudo -n ip -n "$CLIENT_NS" route replace table 201 "$RELAY_ADDRESS"/32 via "$SERVER_A_ADDRESS" dev "$CLIENT_A" src "$CLIENT_A_ADDRESS"
}

configure_client_route_b() {
	sudo -n ip -n "$CLIENT_NS" route replace table 202 10.202.0.0/30 dev "$CLIENT_B" scope link src "$CLIENT_B_ADDRESS"
	sudo -n ip -n "$CLIENT_NS" route replace table 202 "$RELAY_ADDRESS"/32 via "$SERVER_B_ADDRESS" dev "$CLIENT_B" src "$CLIENT_B_ADDRESS"
}

create_topology() {
	delete_namespace "$CLIENT_NS"
	delete_namespace "$SERVER_NS"
	delete_host_links
	sudo -n ip netns add "$CLIENT_NS"
	sudo -n ip netns add "$SERVER_NS"

	sudo -n ip link add "$HOST_CLIENT_A" type veth peer name "$HOST_SERVER_A"
	sudo -n ip link add "$HOST_CLIENT_B" type veth peer name "$HOST_SERVER_B"
	sudo -n ip link set "$HOST_CLIENT_A" netns "$CLIENT_NS"
	sudo -n ip link set "$HOST_CLIENT_B" netns "$CLIENT_NS"
	sudo -n ip link set "$HOST_SERVER_A" netns "$SERVER_NS"
	sudo -n ip link set "$HOST_SERVER_B" netns "$SERVER_NS"
	sudo -n ip -n "$CLIENT_NS" link set "$HOST_CLIENT_A" name "$CLIENT_A"
	sudo -n ip -n "$CLIENT_NS" link set "$HOST_CLIENT_B" name "$CLIENT_B"
	sudo -n ip -n "$SERVER_NS" link set "$HOST_SERVER_A" name "$SERVER_A"
	sudo -n ip -n "$SERVER_NS" link set "$HOST_SERVER_B" name "$SERVER_B"

	sudo -n ip -n "$CLIENT_NS" link set lo up
	sudo -n ip -n "$SERVER_NS" link set lo up
	sudo -n ip -n "$CLIENT_NS" address add "$CLIENT_A_ADDRESS"/30 dev "$CLIENT_A"
	sudo -n ip -n "$CLIENT_NS" address add "$CLIENT_B_ADDRESS"/30 dev "$CLIENT_B"
	sudo -n ip -n "$SERVER_NS" address add "$SERVER_A_ADDRESS"/30 dev "$SERVER_A"
	sudo -n ip -n "$SERVER_NS" address add "$SERVER_B_ADDRESS"/30 dev "$SERVER_B"
	sudo -n ip -n "$SERVER_NS" address add "$RELAY_ADDRESS"/32 dev lo
	sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_A" up
	sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_B" up
	sudo -n ip -n "$SERVER_NS" link set "$SERVER_A" up
	sudo -n ip -n "$SERVER_NS" link set "$SERVER_B" up

	sudo -n ip -n "$CLIENT_NS" rule add priority 201 from "$CLIENT_A_ADDRESS"/32 table 201
	sudo -n ip -n "$CLIENT_NS" rule add priority 202 from "$CLIENT_B_ADDRESS"/32 table 202
	configure_client_route_a
	configure_client_route_b

	for namespace in "$CLIENT_NS" "$SERVER_NS"; do
		sudo -n ip netns exec "$namespace" sysctl -qw net.ipv4.conf.all.rp_filter=0
		sudo -n ip netns exec "$namespace" sysctl -qw net.ipv4.conf.default.rp_filter=0
	done
	for interface_name in "$CLIENT_A" "$CLIENT_B"; do
		sudo -n ip netns exec "$CLIENT_NS" sysctl -qw "net.ipv4.conf.${interface_name}.rp_filter=0"
	done
	for interface_name in "$SERVER_A" "$SERVER_B"; do
		sudo -n ip netns exec "$SERVER_NS" sysctl -qw "net.ipv4.conf.${interface_name}.rp_filter=0"
	done
}

clear_netem_device() {
	local namespace="$1"
	local device="$2"
	sudo -n ip netns exec "$namespace" tc qdisc delete dev "$device" root >/dev/null 2>&1 || true
}

clear_all_netem() {
	clear_netem_device "$CLIENT_NS" "$CLIENT_A"
	clear_netem_device "$CLIENT_NS" "$CLIENT_B"
	clear_netem_device "$SERVER_NS" "$SERVER_A"
	clear_netem_device "$SERVER_NS" "$SERVER_B"
}

set_netem() {
	local namespace="$1"
	local device="$2"
	shift 2
	sudo -n ip netns exec "$namespace" tc qdisc replace dev "$device" root netem "$@"
}

set_path_a_netem() {
	set_netem "$CLIENT_NS" "$CLIENT_A" "$@"
	set_netem "$SERVER_NS" "$SERVER_A" "$@"
}

set_path_b_netem() {
	set_netem "$CLIENT_NS" "$CLIENT_B" "$@"
	set_netem "$SERVER_NS" "$SERVER_B" "$@"
}

record_qdisc() {
	local label="$1"
	{
		echo "===== $label ====="
		sudo -n ip netns exec "$CLIENT_NS" tc -s qdisc show
		sudo -n ip netns exec "$SERVER_NS" tc -s qdisc show
	} >>"$TC_LOG" 2>&1
}

wait_for_drop() {
	local namespace="$1"
	local device="$2"
	local output
	for _ in {1..200}; do
		output="$(sudo -n ip netns exec "$namespace" tc -s qdisc show dev "$device" 2>/dev/null || true)"
		if [[ "$output" =~ dropped[[:space:]]+([1-9][0-9]*) ]]; then
			return 0
		fi
		sleep 0.05
	done
	echo "test-network: $namespace/$device 未观察到内核丢包" >&2
	return 1
}

wait_for_packets() {
	local namespace="$1"
	local device="$2"
	local output
	for _ in {1..100}; do
		output="$(sudo -n ip netns exec "$namespace" tc -s qdisc show dev "$device" 2>/dev/null || true)"
		if [[ "$output" =~ Sent[[:space:]]+[^[:space:]]+[[:space:]]+bytes[[:space:]]+([1-9][0-9]*)[[:space:]]+pkt ]]; then
			return 0
		fi
		sleep 0.05
	done
	echo "test-network: $namespace/$device 的 netem 未经过真实数据包" >&2
	return 1
}

qdisc_bytes() {
	local namespace="$1"
	local device="$2"
	local output
	output="$(sudo -n ip netns exec "$namespace" tc -s qdisc show dev "$device")"
	if [[ "$output" =~ Sent[[:space:]]+([0-9]+)[[:space:]]+bytes ]]; then
		printf '%s\n' "${BASH_REMATCH[1]}"
		return 0
	fi
	echo "test-network: 无法读取 $namespace/$device 的 qdisc 字节计数" >&2
	return 1
}

wait_for_byte_increase() {
	local namespace="$1"
	local device="$2"
	local baseline="$3"
	local minimum_increase="$4"
	local current
	for _ in {1..200}; do
		current="$(qdisc_bytes "$namespace" "$device")"
		if ((current >= baseline + minimum_increase)); then
			return 0
		fi
		sleep 0.05
	done
	echo "test-network: $namespace/$device 未观察到至少 $minimum_increase 字节的备用线路传输" >&2
	return 1
}

interface_tx_bytes() {
	local namespace="$1"
	local device="$2"
	sudo -n ip netns exec "$namespace" cat "/sys/class/net/$device/statistics/tx_bytes"
}

wait_for_interface_tx_increase() {
	local namespace="$1"
	local device="$2"
	local baseline="$3"
	local minimum_increase="$4"
	local current
	for _ in {1..200}; do
		current="$(interface_tx_bytes "$namespace" "$device")"
		if ((current >= baseline + minimum_increase)); then
			return 0
		fi
		sleep 0.05
	done
	echo "test-network: $namespace/$device 未观察到至少 $minimum_increase 字节的接口发送" >&2
	return 1
}

write_server_config() {
	cat >"$SERVER_CONFIG" <<EOF
transport:
  type: tcp
  listen: "0.0.0.0:19443"
status:
  enabled: true
  listen: "127.0.0.1:18082"
principals:
  - id: network-test
    psk: "$NETWORK_TEST_PSK"
EOF
	chmod 0600 "$SERVER_CONFIG"
}

write_client_config() {
	local delivery="$1"
	local lanes_per_path="${2:-1}"
	local interface_pattern="${3:-vianet-*}"
	cat >"$CLIENT_CONFIG" <<EOF
socks_listen: "$SOCKS_ENDPOINT"
socks_auth:
  username: network-test
  password: network-test-password
transport: {type: tcp, address: "$RELAY_ENDPOINT", lanes_per_path: $lanes_per_path}
$delivery
interfaces: {include: ["$interface_pattern"], exclude: ["vianet-z"]}
principal_id: network-test
psk: "$NETWORK_TEST_PSK"
status:
  enabled: true
  listen: "127.0.0.1:18081"
EOF
	chmod 0600 "$CLIENT_CONFIG"
}

start_services() {
	rm -f "$TARGET_READY" "$TARGET_STATE"
	run_in_namespace "$SERVER_NS" "$HARNESS_BINARY" target \
		--listen "$TARGET_ENDPOINT" --ready-file "$TARGET_READY" --state-file "$TARGET_STATE" \
		>"$TARGET_LOG" 2>&1 &
	TARGET_LAUNCH_PID=$!
	wait_for_file "$TARGET_READY"
	run_in_namespace "$SERVER_NS" "$VIA_BINARY" server --config "$SERVER_CONFIG" >"$SERVER_LOG" 2>&1 &
	SERVER_LAUNCH_PID=$!
}

stop_client() {
	if [[ -z "$CLIENT_LAUNCH_PID" ]]; then
		return
	fi
	kill_namespace_processes "$CLIENT_NS" TERM
	for _ in {1..100}; do
		if [[ -z "$(sudo -n ip netns pids "$CLIENT_NS" 2>/dev/null)" ]]; then
			break
		fi
		sleep 0.1
	done
	kill_namespace_processes "$CLIENT_NS" KILL
	wait "$CLIENT_LAUNCH_PID" >/dev/null 2>&1 || true
	CLIENT_LAUNCH_PID=""
}

start_client() {
	local name="$1"
	local delivery="$2"
	stop_client
	write_client_config "$delivery"
	CLIENT_LOG="$ARTIFACT_DIR/client-${name}.log"
	run_in_namespace "$CLIENT_NS" "$VIA_BINARY" client --config "$CLIENT_CONFIG" >"$CLIENT_LOG" 2>&1 &
	CLIENT_LAUNCH_PID=$!
	run_in_namespace_timeout 30s "$CLIENT_NS" "$HARNESS_BINARY" wait-status \
		--url "$STATUS_ENDPOINT/api/v1/sessions" --ready-sessions 2 \
		--ready-interface "$CLIENT_A" --ready-interface "$CLIENT_B" --timeout 25s
}

start_multilane_client() {
	local name="$1"
	local lanes="$2"
	stop_client
	write_client_config $'delivery:\n  mode: adaptive\n  path_selection: fastest' "$lanes" "$CLIENT_A"
	CLIENT_LOG="$ARTIFACT_DIR/client-${name}.log"
	run_in_namespace "$CLIENT_NS" "$VIA_BINARY" client --config "$CLIENT_CONFIG" >"$CLIENT_LOG" 2>&1 &
	CLIENT_LAUNCH_PID=$!
	run_in_namespace_timeout 15s "$CLIENT_NS" "$HARNESS_BINARY" wait-status \
		--url "$STATUS_ENDPOINT/api/v1/sessions" --ready-sessions "$lanes" \
		--ready-interface "$CLIENT_A" --timeout 10s
}

wait_client_ready() {
	local count="$1"
	shift
	local arguments=(--url "$STATUS_ENDPOINT/api/v1/sessions" --ready-sessions "$count" --timeout 25s)
	local interface_name
	for interface_name in "$@"; do
		arguments+=(--ready-interface "$interface_name")
	done
	run_in_namespace_timeout 30s "$CLIENT_NS" "$HARNESS_BINARY" wait-status "${arguments[@]}"
}

wait_server_ready() {
	local count="$1"
	run_in_namespace_timeout 30s "$SERVER_NS" "$HARNESS_BINARY" wait-status \
		--url "http://127.0.0.1:18082/api/v1/sessions" --ready-sessions "$count" --timeout 25s
}

wait_fastest_a() {
	run_in_namespace_timeout 30s "$CLIENT_NS" "$HARNESS_BINARY" wait-status \
		--url "$STATUS_ENDPOINT/api/v1/sessions" --ready-sessions 2 \
		--ready-interface "$CLIENT_A" --ready-interface "$CLIENT_B" \
		--fastest-interface "$CLIENT_A" --timeout 25s
}

next_request_id() {
	NEXT_REQUEST_ID=$((NEXT_REQUEST_ID + 1))
}

wait_target_exact() {
	"$HARNESS_BINARY" wait-target --state-file "$TARGET_STATE" \
		--accepted "$EXPECTED_TARGET_CONNECTIONS" --completed "$EXPECTED_TARGET_CONNECTIONS" --timeout 20s
}

wait_client_resources() {
	run_in_namespace_timeout 80s "$CLIENT_NS" "$HARNESS_BINARY" wait-resources \
		--url "$STATUS_ENDPOINT/api/v1/summary" --flows 0 --sessions 2 \
		--socks-connections 0 --target-dials 0 --timeout 75s
}

wait_server_resources() {
	local sessions="$1"
	run_in_namespace_timeout 80s "$SERVER_NS" "$HARNESS_BINARY" wait-resources \
		--url "http://127.0.0.1:18082/api/v1/summary" --flows 0 --sessions "$sessions" \
		--socks-connections 0 --target-dials 0 --timeout 75s
}

run_transfer() {
	local label="$1"
	local mode="$2"
	local size="${3:-524288}"
	local upload=0
	local download=0
	case "$mode" in
		upload) upload="$size" ;;
		download) download="$size" ;;
		bidirectional) upload="$size"; download="$size" ;;
		*) echo "test-network: 非法测试方向: $mode" >&2; return 1 ;;
	esac
	next_request_id
	EXPECTED_TARGET_CONNECTIONS=$((EXPECTED_TARGET_CONNECTIONS + 1))
	echo "test-network: $label ($mode, $size bytes)"
	local log="$ARTIFACT_DIR/app-${NEXT_REQUEST_ID}-${label}.log"
	if ! run_in_namespace_timeout 90s "$CLIENT_NS" "$HARNESS_BINARY" app \
		--socks "$SOCKS_ENDPOINT" --target "$TARGET_ENDPOINT" --mode "$mode" \
		--id "$NEXT_REQUEST_ID" --seed "$((NEXT_REQUEST_ID * 17))" \
		--upload-bytes "$upload" --download-bytes "$download" --timeout 80s >"$log" 2>&1; then
		cat "$log" >&2
		return 1
	fi
	wait_target_exact
}

LAST_TRANSFER_NANOS=0

run_timed_transfer() {
	local label="$1"
	local mode="$2"
	local size="$3"
	local upload=0
	local download=0
	case "$mode" in
		upload) upload="$size" ;;
		download) download="$size" ;;
		*) echo "test-network: 非法计时方向: $mode" >&2; return 1 ;;
	esac
	next_request_id
	EXPECTED_TARGET_CONNECTIONS=$((EXPECTED_TARGET_CONNECTIONS + 1))
	local log="$ARTIFACT_DIR/app-${NEXT_REQUEST_ID}-${label}.log"
	local result="$ARTIFACT_DIR/app-${NEXT_REQUEST_ID}-${label}.duration"
	rm -f "$result"
	echo "test-network: $label ($mode, $size bytes, timed)"
	if ! run_in_namespace_timeout 90s "$CLIENT_NS" "$HARNESS_BINARY" app \
		--socks "$SOCKS_ENDPOINT" --target "$TARGET_ENDPOINT" --mode "$mode" \
		--id "$NEXT_REQUEST_ID" --seed "$((NEXT_REQUEST_ID * 17))" \
		--upload-bytes "$upload" --download-bytes "$download" --timeout 80s \
		--result-file "$result" >"$log" 2>&1; then
		cat "$log" >&2
		return 1
	fi
	wait_target_exact
	LAST_TRANSFER_NANOS="$(cat "$result")"
	if [[ ! "$LAST_TRANSFER_NANOS" =~ ^[1-9][0-9]*$ ]]; then
		echo "test-network: 非法传输耗时: $LAST_TRANSFER_NANOS" >&2
		return 1
	fi
}

server_session_written() {
	local remote_host="$1"
	run_in_namespace_timeout 10s "$SERVER_NS" "$HARNESS_BINARY" session-written \
		--url "http://127.0.0.1:18082/api/v1/sessions" --remote-host "$remote_host"
}

assert_distributed_aggregation() {
	local size=$((16 << 20))
	clear_all_netem
	set_path_a_netem rate 5mbit
	set_path_b_netem rate 5mbit

	sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_B" down
	wait_client_ready 1 "$CLIENT_A"
	run_timed_transfer aggregate-single-a download "$size"
	local single_a="$LAST_TRANSFER_NANOS"
	sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_B" up
	configure_client_route_b
	wait_server_ready 2
	wait_client_ready 2 "$CLIENT_A" "$CLIENT_B"

	sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_A" down
	wait_client_ready 1 "$CLIENT_B"
	run_timed_transfer aggregate-single-b download "$size"
	local single_b="$LAST_TRANSFER_NANOS"
	sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_A" up
	configure_client_route_a
	wait_server_ready 2
	wait_client_ready 2 "$CLIENT_A" "$CLIENT_B"
	start_client adaptive-distributed-aggregation-dual $'delivery:\n  mode: adaptive\n  path_selection: distributed'

	local before_a before_b after_a after_b delta_a delta_b total fastest
	before_a="$(server_session_written "$CLIENT_A_ADDRESS")"
	before_b="$(server_session_written "$CLIENT_B_ADDRESS")"
	local window_summary="$ARTIFACT_DIR/aggregate-dual-window.json"
	rm -f "$window_summary"
	run_in_namespace_timeout 90s "$SERVER_NS" "$HARNESS_BINARY" sample-flow-window \
		--url "http://127.0.0.1:18082/api/v1/flows" \
		--sessions-url "http://127.0.0.1:18082/api/v1/sessions" \
		--stop-file "$ARTIFACT_DIR/app-$((NEXT_REQUEST_ID + 1))-aggregate-dual.duration" \
		--result-file "$window_summary" --window-bytes $((320 << 10)) --interval 50ms --timeout 80s &
	local window_sampler_pid=$!
	run_timed_transfer aggregate-dual download "$size"
	wait "$window_sampler_pid"
	echo "test-network: 双路 Flow 窗口 $(cat "$window_summary")"
	local dual="$LAST_TRANSFER_NANOS"
	# The transfer completion can precede the throttled read-only session
	# snapshot. Wait for diagnostics to catch up before asserting DATA shares.
	for _ in {1..100}; do
		after_a="$(server_session_written "$CLIENT_A_ADDRESS")"
		after_b="$(server_session_written "$CLIENT_B_ADDRESS")"
		delta_a=$((after_a - before_a))
		delta_b=$((after_b - before_b))
		total=$((delta_a + delta_b))
		((total >= size)) && break
		sleep 0.05
	done
	fastest="$single_a"
	((single_b < fastest)) && fastest="$single_b"
	echo "test-network: 聚合耗时 A=${single_a}ns B=${single_b}ns dual=${dual}ns，DATA A=${delta_a} B=${delta_b}"
	if ((dual * 100 > fastest * 75)); then
		echo "test-network: 双路耗时超过最快单路的 75%" >&2
		return 1
	fi
	if ((total < size || delta_a * 100 < total * 30 || delta_b * 100 < total * 30)); then
		echo "test-network: 双路 DATA 分配未达到每路至少 30%" >&2
		return 1
	fi
	clear_all_netem
}

assert_multilane_aggregation() {
	local size=$((4 << 20))
	clear_all_netem
	sudo -n ip netns exec "$SERVER_NS" tc qdisc replace dev "$SERVER_A" root fq maxrate 5mbit

	start_multilane_client adaptive-fastest-one-lane 1
	run_timed_transfer multilane-single download "$size"
	local single="$LAST_TRANSFER_NANOS"

	start_multilane_client adaptive-fastest-four-lanes 4
	run_timed_transfer multilane-four download "$size"
	local four="$LAST_TRANSFER_NANOS"

	echo "test-network: 单 interface 聚合耗时 one=${single}ns four=${four}ns"
	if ((four * 100 > single * 60)); then
		echo "test-network: 四 lane 耗时超过单 lane 的 60%" >&2
		return 1
	fi
	clear_all_netem
}

assert_fastest_capacity_shift() {
	local initial_size=$((8 << 20))
	local learning_size=$((4 << 20))
	local followup_size=$((4 << 20))
	clear_all_netem
	set_path_a_netem delay 2ms rate 10mbit
	set_path_b_netem delay 12ms rate 5mbit
	wait_fastest_a
	run_transfer fastest-capacity-warmup download "$learning_size"
	wait_fastest_a

	local before_a before_b after_a after_b delta_a delta_b total
	before_a="$(server_session_written "$CLIENT_A_ADDRESS")"
	before_b="$(server_session_written "$CLIENT_B_ADDRESS")"
	run_transfer fastest-capacity-initial download "$initial_size"
	after_a="$(server_session_written "$CLIENT_A_ADDRESS")"
	after_b="$(server_session_written "$CLIENT_B_ADDRESS")"
	delta_a=$((after_a - before_a))
	delta_b=$((after_b - before_b))
	total=$((delta_a + delta_b))
	echo "test-network: fastest 10/5 Mbit DATA A=${delta_a} B=${delta_b}"
	if ((total < initial_size || delta_a * 100 < total * 75)); then
		echo "test-network: 10 Mbit/s 路径未承载至少 75% DATA" >&2
		return 1
	fi

	set_path_a_netem delay 2ms rate 2mbit
	run_transfer fastest-capacity-learning download "$learning_size"
	before_a="$(server_session_written "$CLIENT_A_ADDRESS")"
	before_b="$(server_session_written "$CLIENT_B_ADDRESS")"
	run_transfer fastest-capacity-after-shift download "$followup_size"
	after_a="$(server_session_written "$CLIENT_A_ADDRESS")"
	after_b="$(server_session_written "$CLIENT_B_ADDRESS")"
	delta_a=$((after_a - before_a))
	delta_b=$((after_b - before_b))
	total=$((delta_a + delta_b))
	echo "test-network: fastest 2/5 Mbit 后续 DATA A=${delta_a} B=${delta_b}"
	if ((total < followup_size || delta_b <= delta_a)); then
		echo "test-network: 原 5 Mbit/s 路径未在容量重学习后成为主要路径" >&2
		return 1
	fi
	clear_all_netem
}

assert_single_session_flow_fairness() {
	local size=$((2 << 20))
	clear_all_netem
	set_path_a_netem rate 2mbit
	sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_B" down
	wait_client_ready 1 "$CLIENT_A"

	local pids=()
	local logs=()
	local progress_files=()
	local result_files=()
	local index request_id log progress result
	for index in 0 1; do
		next_request_id
		request_id="$NEXT_REQUEST_ID"
		EXPECTED_TARGET_CONNECTIONS=$((EXPECTED_TARGET_CONNECTIONS + 1))
		log="$ARTIFACT_DIR/app-${request_id}-single-session-fairness.log"
		progress="$ARTIFACT_DIR/app-${request_id}.progress"
		result="$ARTIFACT_DIR/app-${request_id}.duration"
		rm -f "$progress" "$result"
		logs+=("$log")
		progress_files+=("$progress")
		result_files+=("$result")
		run_in_namespace_timeout 90s "$CLIENT_NS" "$HARNESS_BINARY" app \
			--socks "$SOCKS_ENDPOINT" --target "$TARGET_ENDPOINT" --mode download \
			--id "$request_id" --seed "$((request_id * 17))" \
			--upload-bytes 0 --download-bytes "$size" --timeout 80s \
			--progress-file "$progress" --result-file "$result" >"$log" 2>&1 &
		pids+=("$!")
	done

	for ((attempt = 0; attempt < 400; attempt++)); do
		if [[ -f "${progress_files[0]}" && -f "${progress_files[1]}" ]]; then
			break
		fi
		if [[ -f "${result_files[0]}" || -f "${result_files[1]}" ]]; then
			echo "test-network: 一个 Flow 完成后另一个才首次推进" >&2
			return 1
		fi
		sleep 0.05
	done
	if [[ ! -f "${progress_files[0]}" || ! -f "${progress_files[1]}" ]]; then
		echo "test-network: 单 session 双 Flow 未在 watchdog 内同时推进" >&2
		return 1
	fi
	local failed=0
	for index in "${!pids[@]}"; do
		if ! wait "${pids[index]}"; then
			cat "${logs[index]}" >&2
			failed=1
		fi
	done
	[[ $failed -eq 0 ]]
	wait_target_exact

	sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_B" up
	configure_client_route_b
	wait_server_ready 2
	wait_client_ready 2 "$CLIENT_A" "$CLIENT_B"
	clear_all_netem
}

reject_socks_access() {
	local label="$1"
	local username="$2"
	local password="$3"
	local log="$ARTIFACT_DIR/app-rejected-${label}.log"
	if run_in_namespace_timeout 10s "$CLIENT_NS" "$HARNESS_BINARY" app \
		--socks "$SOCKS_ENDPOINT" --socks-username "$username" --socks-password "$password" \
		--target "$TARGET_ENDPOINT" --mode upload --id 900001 --seed 1 \
		--upload-bytes 1 --download-bytes 0 --timeout 5s >"$log" 2>&1; then
		echo "test-network: SOCKS 拒绝场景意外成功: $label" >&2
		return 1
	fi
	wait_target_exact
	wait_client_resources
}

run_multiple_flows() {
	local label="$1"
	local flows="$2"
	local pids=()
	local logs=()
	local modes=(upload download bidirectional)
	local index mode upload download request_id log
	echo "test-network: $label ($flows 个并发数据流)"
	for ((index = 0; index < flows; index++)); do
		next_request_id
		request_id="$NEXT_REQUEST_ID"
		EXPECTED_TARGET_CONNECTIONS=$((EXPECTED_TARGET_CONNECTIONS + 1))
		mode="${modes[index % ${#modes[@]}]}"
		upload=0
		download=0
		case "$mode" in
			upload) upload=262144 ;;
			download) download=262144 ;;
			bidirectional) upload=262144; download=262144 ;;
		esac
		log="$ARTIFACT_DIR/app-${request_id}-${label}.log"
		logs+=("$log")
		run_in_namespace_timeout 90s "$CLIENT_NS" "$HARNESS_BINARY" app \
			--socks "$SOCKS_ENDPOINT" --target "$TARGET_ENDPOINT" --mode "$mode" \
			--id "$request_id" --seed "$((request_id * 17))" \
			--upload-bytes "$upload" --download-bytes "$download" --timeout 80s >"$log" 2>&1 &
		pids+=("$!")
	done
	local failed=0
	for index in "${!pids[@]}"; do
		if ! wait "${pids[index]}"; then
			cat "${logs[index]}" >&2
			failed=1
		fi
	done
	[[ $failed -eq 0 ]]
	wait_target_exact
}

PAUSED_PID=""
PAUSED_LOG=""
PAUSED_READY=""
PAUSED_CONTINUE=""

start_paused_transfer() {
	local label="$1"
	next_request_id
	EXPECTED_TARGET_CONNECTIONS=$((EXPECTED_TARGET_CONNECTIONS + 1))
	PAUSED_READY="$ARTIFACT_DIR/pause-${NEXT_REQUEST_ID}.ready"
	PAUSED_CONTINUE="$ARTIFACT_DIR/pause-${NEXT_REQUEST_ID}.continue"
	PAUSED_LOG="$ARTIFACT_DIR/app-${NEXT_REQUEST_ID}-${label}.log"
	rm -f "$PAUSED_READY" "$PAUSED_CONTINUE"
	run_in_namespace_timeout 100s "$CLIENT_NS" "$HARNESS_BINARY" app \
		--socks "$SOCKS_ENDPOINT" --target "$TARGET_ENDPOINT" --mode bidirectional \
		--id "$NEXT_REQUEST_ID" --seed "$((NEXT_REQUEST_ID * 17))" \
		--upload-bytes 2097152 --download-bytes 2097152 --pause-after 65536 \
		--ready-file "$PAUSED_READY" --continue-file "$PAUSED_CONTINUE" --timeout 90s \
		>"$PAUSED_LOG" 2>&1 &
	PAUSED_PID=$!
	wait_for_file "$PAUSED_READY" 400
}

continue_paused_transfer() {
	printf 'continue\n' >"$PAUSED_CONTINUE"
}

finish_paused_transfer() {
	if ! wait "$PAUSED_PID"; then
		cat "$PAUSED_LOG" >&2
		return 1
	fi
	PAUSED_PID=""
	wait_target_exact
}

run_single_path_fault() {
	local fault_name="$1"
	local mode="$2"
	shift 2
	clear_all_netem
	set_path_a_netem "$@"
	run_transfer "single-${fault_name}" "$mode"
	wait_for_packets "$CLIENT_NS" "$CLIENT_A"
	wait_for_packets "$SERVER_NS" "$SERVER_A"
	if [[ "$fault_name" == "loss" ]]; then
		wait_for_drop "$CLIENT_NS" "$CLIENT_A"
		wait_for_drop "$SERVER_NS" "$SERVER_A"
	fi
	record_qdisc "single-${fault_name}"
	clear_all_netem
}

run_dual_path_fault() {
	local fault_name="$1"
	local mode="$2"
	local size=524288
	shift 2
	if [[ "$fault_name" == "loss" ]]; then
		size=1048576
	fi
	clear_all_netem
	set_path_a_netem "$@"
	set_path_b_netem "$@"
	run_transfer "dual-${fault_name}" "$mode" "$size"
	for endpoint in "$CLIENT_NS:$CLIENT_A" "$CLIENT_NS:$CLIENT_B" "$SERVER_NS:$SERVER_A" "$SERVER_NS:$SERVER_B"; do
		wait_for_packets "${endpoint%%:*}" "${endpoint##*:}"
	done
	record_qdisc "dual-${fault_name}"
	clear_all_netem
}

echo "test-network: 构建 Via 与网络验收工具"
go build -trimpath -o "$VIA_BINARY" ./cmd/via
go build -trimpath -o "$HARNESS_BINARY" ./build-scripts/network-harness
write_server_config
create_topology

# 先确认当前内核确实支持 netem，再启动被测进程。
set_netem "$CLIENT_NS" "$CLIENT_A" delay 1ms
clear_netem_device "$CLIENT_NS" "$CLIENT_A"
start_services

if [[ "${VIA_NETWORK_SCENARIO:-full}" == "multilane" ]]; then
	echo "test-network: 单 interface 多 TCP lane 聚合"
	assert_multilane_aggregation
	wait_target_exact
	echo "test-network: PASS，单 interface 四 lane 聚合通过"
	exit 0
fi

echo "test-network: 冗余发送与内核故障矩阵"
start_client redundant $'delivery:\n  mode: redundant'
reject_socks_access no-auth "" ""
reject_socks_access wrong-username wrong-user network-test-password
reject_socks_access wrong-password network-test wrong-password
run_transfer redundant-baseline bidirectional
run_multiple_flows redundant-multiple 6

run_single_path_fault loss upload loss random 20%
run_dual_path_fault loss bidirectional loss random 2%
run_single_path_fault delay download delay 25ms
run_dual_path_fault delay upload delay 20ms
run_single_path_fault jitter bidirectional delay 18ms 7ms 25% distribution normal
run_dual_path_fault jitter download delay 15ms 5ms 20% distribution normal
run_single_path_fault rate upload rate 4mbit
run_dual_path_fault rate bidirectional rate 6mbit
run_single_path_fault reorder download delay 12ms reorder 30% 50%
run_dual_path_fault reorder bidirectional delay 10ms reorder 20% 40%

echo "test-network: 双路同向全黑洞后的有界重传"
start_paused_transfer all-path-blackhole
set_netem "$CLIENT_NS" "$CLIENT_A" loss 100%
set_netem "$CLIENT_NS" "$CLIENT_B" loss 100%
continue_paused_transfer
wait_for_drop "$CLIENT_NS" "$CLIENT_A"
wait_for_drop "$CLIENT_NS" "$CLIENT_B"
record_qdisc all-path-blackhole
clear_netem_device "$CLIENT_NS" "$CLIENT_A"
clear_netem_device "$CLIENT_NS" "$CLIENT_B"
finish_paused_transfer

echo "test-network: 自适应最快优先、单向黑洞和链路上下线"
clear_all_netem
set_path_b_netem delay 35ms
start_client adaptive-fastest $'delivery:\n  mode: adaptive\n  path_selection: fastest'
wait_fastest_a
run_transfer fastest-upload upload
run_transfer fastest-download download

start_paused_transfer one-way-blackhole
backup_bytes="$(qdisc_bytes "$SERVER_NS" "$SERVER_B")"
set_netem "$SERVER_NS" "$SERVER_A" loss 100%
continue_paused_transfer
wait_for_drop "$SERVER_NS" "$SERVER_A"
wait_for_byte_increase "$SERVER_NS" "$SERVER_B" "$backup_bytes" 32768
finish_paused_transfer
run_transfer fastest-new-during-blackhole bidirectional
record_qdisc one-way-blackhole
clear_netem_device "$SERVER_NS" "$SERVER_A"
wait_server_ready 2
wait_client_ready 2 "$CLIENT_A" "$CLIENT_B"
run_transfer fastest-after-blackhole download

start_paused_transfer link-down
sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_A" down
continue_paused_transfer
wait_client_ready 1 "$CLIENT_B"
finish_paused_transfer
run_transfer fastest-new-while-link-down upload
sudo -n ip -n "$CLIENT_NS" link set "$CLIENT_A" up
configure_client_route_a
wait_server_ready 2
wait_client_ready 2 "$CLIENT_A" "$CLIENT_B"
run_transfer fastest-after-link-up bidirectional
clear_all_netem

echo "test-network: 5/10 Mbit/s fastest 选择与运行中降速"
start_client adaptive-fastest-capacity $'delivery:\n  mode: adaptive\n  path_selection: fastest'
assert_fastest_capacity_shift

echo "test-network: 无约束自适应分散发送性能与公平"
start_client adaptive-distributed-aggregation $'delivery:\n  mode: adaptive\n  path_selection: distributed'
echo "test-network: 5 Mbit/s 单路基线与双路聚合阈值"
assert_distributed_aggregation
echo "test-network: 单限速 session 的双 Flow 公平进度"
assert_single_session_flow_fairness

echo "test-network: 单 interface 多 TCP lane 聚合"
assert_multilane_aggregation

echo "test-network: 带时延约束的自适应分散发送、地址删除与恢复"
start_client adaptive-distributed-constrained $'delivery:\n  mode: adaptive\n  path_selection: distributed\n  constraints:\n    max_delivery_delay: 80ms\n    max_delay_gap: 30ms\n    constraint_fallback: fastest'
run_transfer distributed-baseline download
run_multiple_flows distributed-multiple 8

start_paused_transfer address-delete
sudo -n ip -n "$CLIENT_NS" address delete "$CLIENT_B_ADDRESS"/30 dev "$CLIENT_B"
wait_client_ready 1 "$CLIENT_A"
remaining_path_bytes="$(interface_tx_bytes "$CLIENT_NS" "$CLIENT_A")"
continue_paused_transfer
wait_for_interface_tx_increase "$CLIENT_NS" "$CLIENT_A" "$remaining_path_bytes" 32768
sudo -n ip -n "$CLIENT_NS" address add "$CLIENT_B_ADDRESS"/30 dev "$CLIENT_B"
configure_client_route_b
wait_server_ready 2
wait_client_ready 2 "$CLIENT_A" "$CLIENT_B"
finish_paused_transfer

sudo -n ip -n "$CLIENT_NS" address delete "$CLIENT_B_ADDRESS"/30 dev "$CLIENT_B"
wait_client_ready 1 "$CLIENT_A"
run_transfer distributed-new-without-address bidirectional
sudo -n ip -n "$CLIENT_NS" address add "$CLIENT_B_ADDRESS"/30 dev "$CLIENT_B"
configure_client_route_b
wait_server_ready 2
wait_client_ready 2 "$CLIENT_A" "$CLIENT_B"
run_transfer distributed-after-address-restore upload

wait_target_exact
wait_client_resources
wait_server_resources 2
run_in_namespace_timeout 10s "$CLIENT_NS" "$HARNESS_BINARY" dump-status \
	--base-url "$STATUS_ENDPOINT" --reject-value "$NETWORK_TEST_PSK" \
	--reject-value network-test --reject-value "$TARGET_ENDPOINT" >"$CLIENT_STATUS_LOG"
stop_client
wait_server_resources 0
run_in_namespace_timeout 10s "$SERVER_NS" "$HARNESS_BINARY" dump-status \
	--base-url "http://127.0.0.1:18082" --reject-value "$NETWORK_TEST_PSK" \
	--reject-value network-test --reject-value "$TARGET_ENDPOINT" >"$SERVER_STATUS_LOG"
echo "test-network: PASS，目标连接=$EXPECTED_TARGET_CONNECTIONS，字节校验、故障恢复和资源收敛均通过"
