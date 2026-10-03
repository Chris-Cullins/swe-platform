#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEMP="$(mktemp -d)"
trap 'rm -rf "$TEMP"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# Execute the actual helper with isolated temporary paths and a FIFO scheduling
# barrier immediately before the background command's redirection. The command,
# redirection, readiness loop and assertions remain the source's actual code.
# No cluster or real Go RPC is used.
sed -n '/^manage_observation_listener() {/,/^}/p' "$ROOT/hack/e2e.sh" |
	awk '/^\tkubectl .* port-forward .* &$/ {
		count++
		print "\t{"
		print "\t\t: >\"$TEMP/blocked\""
		print "\t\tread -r _ <\"$TEMP/start\""
		sub(/ &$/, "")
		print
		print "\t} &"
		next
	} { print } END { if (count != 1) exit 1 }' |
	sed "s|/tmp/swe-platform-|$TEMP/swe-platform-|g" >"$TEMP/helper.sh"
cat >"$TEMP/run.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
TEMP="$1" MODE="$2"
SYSTEM_NAMESPACE=system PROJECT_NAMESPACE=project SANDBOXD_PORT_FORWARD_PID=""
source "$TEMP/helper.sh"
cleanup() {
	if [[ -n "$SANDBOXD_PORT_FORWARD_PID" ]]; then
		kill "$SANDBOXD_PORT_FORWARD_PID" 2>/dev/null || true
		wait "$SANDBOXD_PORT_FORWARD_PID" 2>/dev/null || true
	fi
}
trap cleanup EXIT
wait_marker() { while [[ ! -e "$TEMP/$1" ]]; do command sleep .001; done; }

kubectl() {
	case "$*" in
		*' port-forward '*)
			echo launch >>"$TEMP/launches"
			printf '%s\n' 'SENSITIVE_TOKEN SENSITIVE_URL SENSITIVE_ARGS' >&2
			: >"$TEMP/started"
			case "$MODE" in
				failure) exit 19 ;;
				stale) ;;
				*)
					read -r _ <"$TEMP/ready-signal"
					printf '%s\n' 'Forwarding from fixture SENSITIVE_ADDRESS'
					: >"$TEMP/ready"
					;;
			esac
			exec sleep 60
			;;
		*' get secret '*) printf fixture | base64 ;;
		*' get pod '*) printf fixture ;;
		*' delete pod swe-sandboxd-relay '*) echo delete >>"$TEMP/relay" ;;
		*' run swe-sandboxd-relay '*) echo run >>"$TEMP/relay" ;;
		*' wait --for=condition=Ready pod/swe-sandboxd-relay '*) echo wait >>"$TEMP/relay" ;;
		*) exit 90 ;;
	esac
}
grep() {
	wait_marker blocked
	command grep "$@"
}
sleep() {
	[[ "$1" == 1 ]] || exit 91
	POLLS=$((POLLS + 1))
	if [[ "$POLLS" == 1 ]]; then
		echo start >"$TEMP/start"
		wait_marker started
	fi
	if [[ "$MODE" != stale && "$MODE" != failure && "$POLLS" == "$DELAY" ]]; then
		echo ready >"$TEMP/ready-signal"
		wait_marker ready
	fi
	echo "$POLLS" >"$TEMP/polls"
}
go() {
	if [[ ! -e "$TEMP/ready" ]]; then
		: >"$TEMP/premature"
		return 42
	fi
	[[ "$*" == *'127.0.0.1:15052'* && "$*" == *'owner role'* ]] || return 92
	echo rpc >>"$TEMP/rpc"
}
mkfifo "$TEMP/start" "$TEMP/ready-signal"
LAUNCHES=1
[[ "$MODE" != repeated ]] || LAUNCHES=3
for ((launch = 1; launch <= LAUNCHES; launch++)); do
	rm -f "$TEMP/blocked" "$TEMP/started" "$TEMP/ready"
	POLLS=0 DELAY=3
	[[ "$MODE" != success ]] || DELAY=1
	# This represents the previous invocation's readiness, not the new child.
	printf '%s\n' 'Forwarding from old-launch SENSITIVE_OLD_LOG' >"$TEMP/swe-platform-observation-port-forward.log"
	manage_observation_listener service-start pod owner role 3000
	[[ "$SANDBOXD_PORT_FORWARD_PID" == "" && "$POLLS" == "$DELAY" ]] || exit 93
	[[ ! -e "$TEMP/swe-platform-observation-cert-$$" && ! -e "$TEMP/swe-platform-observation-process-token-$$" ]] || exit 94
done
EOF

for mode in delayed stale failure success repeated; do
	rm -f "$TEMP/start" "$TEMP/ready-signal" "$TEMP/premature" "$TEMP/polls" "$TEMP/rpc" "$TEMP/relay" "$TEMP/launches"
	status=0
	timeout 10 bash "$TEMP/run.sh" "$TEMP" "$mode" >"$TEMP/out" 2>"$TEMP/err" || status=$?
	[[ ! -e "$TEMP/premature" ]] || fail "$mode accepted stale readiness before the new child"
	count=1
	[[ "$mode" != repeated ]] || count=3
	[[ -f "$TEMP/launches" && "$(wc -l <"$TEMP/launches")" == "$count" ]] || fail "$mode forward launch count changed"
	if [[ "$mode" == stale || "$mode" == failure ]]; then
		[[ "$status" == 1 && "$(cat "$TEMP/polls")" == 30 && ! -e "$TEMP/rpc" ]] || fail "$mode changed timeout or invoked RPC"
		[[ "$(cat "$TEMP/err")" == 'FAIL: sandboxd observation port-forward did not become ready' ]] || fail "$mode emitted unexpected failure text"
	else
		[[ "$status" == 0 && ! -s "$TEMP/err" ]] || fail "$mode failed: status=$status"
		[[ "$(wc -l <"$TEMP/rpc")" == "$count" ]] || fail "$mode RPC count changed"
		[[ "$(command grep -c '^delete$' "$TEMP/relay")" == "$((count * 2))" ]] || fail "$mode relay cleanup changed"
	fi
	[[ ! -s "$TEMP/out" ]] || fail "$mode emitted stdout"
	! command grep -q SENSITIVE "$TEMP/out" "$TEMP/err" || fail "$mode exposed raw log content"
done
echo 'PASS: actual observation helper ignores stale logs, waits for each launch and preserves bounded safe failures'
