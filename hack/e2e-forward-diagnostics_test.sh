#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEMP="$(mktemp -d)"
trap 'rm -rf "$TEMP"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

# Execute the real initialization, cleanup and EXIT trap, without starting kind.
sed '/^contains_e2e_key()/,$d' "$ROOT/hack/e2e.sh" >"$TEMP/acceptance.sh"
cp "$ROOT/hack/e2e-forward-diagnostics.sh" "$TEMP/"
cat >>"$TEMP/acceptance.sh" <<'EOF'
KEEP_CLUSTER=true
CONTROL_PLANE_FORWARD_LOG="$1"
MARKER="$2"
kubectl() { echo called >>"$MARKER"; echo SENSITIVE_KUBE_ERROR >&2; return 91; }
case "$3" in
	alive)
		sleep 60 & PORT_FORWARD_PID=$!
		echo "$PORT_FORWARD_PID" >"$MARKER.pid"
		# Wait for exec, not the transient forked shell and its inherited traps.
		while [[ "$(ps -o comm= -p "$PORT_FORWARD_PID")" != sleep ]]; do :; done
		;;
	dead) (exit 0) & PORT_FORWARD_PID=$!; wait "$PORT_FORWARD_PID" ;;
	missing) PORT_FORWARD_PID="" ;;
	command-failure) (exit "$4") ;;
	recovered-failure)
		set +e
		(exit 29)
		handled_status=$?
		set -e
		[[ "$handled_status" == 29 ]]
		true
		;;
	diagnostic-failure) e2e_forward_diagnostics() { echo SENSITIVE_DIAGNOSTIC_ERROR >&2; exit 92; } ;;
esac
exit "$4"
EOF

run() {
	local mode="$1" status="$2" log="$3" actual=0
	rm -f "$TEMP/cleanup" "$TEMP/cleanup.pid"
	timeout 5 bash "$TEMP/acceptance.sh" "$log" "$TEMP/cleanup" "$mode" "$status" >"$TEMP/out" 2>"$TEMP/err" || actual=$?
	[[ "$actual" == "$status" ]] || fail "exit changed for $mode: $actual"
	[[ ! -s "$TEMP/out" ]] || fail 'unexpected stdout'
	[[ "$(wc -l <"$TEMP/cleanup")" -eq 3 ]] || fail 'real cleanup did not finish after Kubernetes failures'
	if [[ -f "$TEMP/cleanup.pid" ]] && kill -0 "$(cat "$TEMP/cleanup.pid")" 2>/dev/null; then
		fail 'cleanup left the forward alive'
	fi
	[[ "$(wc -c <"$TEMP/err")" -le 320 ]] || fail 'output cap exceeded'
	! grep -q SENSITIVE "$TEMP/err" || fail 'sensitive sentinel leaked'
}
has() { grep -Eq "$1" "$TEMP/err" || fail "missing fixed field: $1 (observed $(grep -oE 'line=[0-9]+' "$TEMP/err"))"; }

printf '%s\n' 'SENSITIVE_TOKEN lost connection to pod' 'SENSITIVE_URL connection refused' \
	'unable to listen on any of the requested ports SENSITIVE_ARGS' 'error upgrading connection SENSITIVE_BODY' >"$TEMP/log"
run alive 37 "$TEMP/log"
has '^e2e_forward exit=37 line=[0-9]+ configured=true alive=true log_readable=true '
has 'line=0 '
has 'lost_connection=true connection_refused=true bind_failure=true upgrade_failure=true$'
[[ "$(wc -l <"$TEMP/err")" -eq 1 ]] || fail 'unexpected output lines'
run dead 23 "$TEMP/log"
has 'configured=true alive=false'
run missing 19 "$TEMP/missing"
has 'configured=false alive=false log_readable=false'

printf 'SENSITIVE_UNKNOWN_MESSAGE\000SENSITIVE_BINARY\n' >"$TEMP/log"
run missing 17 "$TEMP/log"
has 'log_readable=true'
has 'lost_connection=false connection_refused=false bind_failure=false upgrade_failure=false$'
# A recognized pattern past the byte bound must not be classified.
head -c 65536 /dev/zero | tr '\000' x >"$TEMP/log"
printf 'connection refused SENSITIVE_TAIL' >>"$TEMP/log"
run missing 18 "$TEMP/log"
has 'connection_refused=false'
# The same pattern within the bound must be classified.
printf 'connection refused' >"$TEMP/log"
head -c 1048576 /dev/zero >>"$TEMP/log"
run missing 18 "$TEMP/log"
has 'connection_refused=true'
mkfifo "$TEMP/fifo"
run missing 20 "$TEMP/fifo"
has 'log_readable=false'
run command-failure 43 "$TEMP/log"
has "exit=43 line=$(grep -n 'command-failure)' "$TEMP/acceptance.sh" | cut -d: -f1) "
run recovered-failure 37 "$TEMP/log"
has 'exit=37 line=0 '
run alive 0 "$TEMP/log"
[[ ! -s "$TEMP/err" ]] || fail 'success emitted diagnostics'
run diagnostic-failure 41 "$TEMP/log"
[[ ! -s "$TEMP/err" ]] || fail 'diagnostic failure leaked text'
echo 'PASS: actual E2E EXIT diagnostics preserve failure, cleanup, privacy and bounds'
