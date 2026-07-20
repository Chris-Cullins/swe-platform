#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HELPER="$ROOT/hack/argocd-port-forward.sh"
TEST_ROOT="$(mktemp -d)"
helper_pid=""

cleanup() {
	if [[ -n "$helper_pid" ]] && kill -0 "$helper_pid" 2>/dev/null; then
		kill -TERM "$helper_pid" 2>/dev/null || true
		wait "$helper_pid" 2>/dev/null || true
	fi
	rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

wait_for() {
	local description="$1"
	shift
	for _ in {1..100}; do
		if "$@"; then
			return 0
		fi
		sleep 0.05
	done
	fail "timed out waiting for $description"
}

mkdir -p "$TEST_ROOT/bin" "$TEST_ROOT/runtime"
cat >"$TEST_ROOT/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"$FAKE_KUBECTL_LOG"
count=0
if [[ -r "$FAKE_COUNT_FILE" ]]; then
	read -r count <"$FAKE_COUNT_FILE"
fi
count=$((count + 1))
printf '%s\n' "$count" >"$FAKE_COUNT_FILE"
if [[ "${FAKE_EXIT_FIRST:-false}" == true && "$count" -eq 1 ]]; then
	exit 42
fi

printf '%s\n' "$$" >"$FAKE_CHILD_PID_FILE"
trap 'printf "%s\n" "$$" >"$FAKE_TERM_FILE"; exit 0' INT TERM
while true; do
	sleep 1
done
EOF
chmod +x "$TEST_ROOT/bin/kubectl"

export PATH="$TEST_ROOT/bin:$PATH"
export TMPDIR="$TEST_ROOT/runtime"
export FAKE_KUBECTL_LOG="$TEST_ROOT/kubectl.log"
export FAKE_COUNT_FILE="$TEST_ROOT/count"
export FAKE_CHILD_PID_FILE="$TEST_ROOT/child.pid"
export FAKE_TERM_FILE="$TEST_ROOT/terminated.pid"
export FAKE_EXIT_FIRST=true

"$HELPER" >"$TEST_ROOT/helper.log" 2>&1 &
helper_pid=$!

wait_for "port-forward retry" test -s "$FAKE_CHILD_PID_FILE"
[[ "$(cat "$FAKE_COUNT_FILE")" -ge 2 ]] || fail "kubectl was not restarted after its first exit"
expected="--context kind-swe-argo --namespace swe-platform-system port-forward --address 127.0.0.1 service/swe-platform-swe-platform-control-plane 18080:80"
grep -Fx -- "$expected" "$FAKE_KUBECTL_LOG" >/dev/null || fail "kubectl did not receive the scoped context, namespace, address, Service, and port"

if "$HELPER" >"$TEST_ROOT/duplicate.log" 2>&1; then
	fail "a duplicate helper unexpectedly started"
fi
grep -F "lock already exists" "$TEST_ROOT/duplicate.log" >/dev/null || fail "duplicate helper failure was not explained"

child_pid="$(cat "$FAKE_CHILD_PID_FILE")"
kill -TERM "$helper_pid"
wait "$helper_pid"
helper_pid=""
wait_for "kubectl child signal cleanup" test -s "$FAKE_TERM_FILE"
[[ "$(cat "$FAKE_TERM_FILE")" == "$child_pid" ]] || fail "the helper did not terminate its own kubectl child"

rm -f "$FAKE_CHILD_PID_FILE" "$FAKE_TERM_FILE"
export FAKE_EXIT_FIRST=false
"$HELPER" >"$TEST_ROOT/reacquired.log" 2>&1 &
helper_pid=$!
wait_for "helper lock reacquisition" test -s "$FAKE_CHILD_PID_FILE"
child_pid="$(cat "$FAKE_CHILD_PID_FILE")"
kill -TERM "$helper_pid"
wait "$helper_pid"
helper_pid=""
wait_for "reacquired kubectl child signal cleanup" test -s "$FAKE_TERM_FILE"
[[ "$(cat "$FAKE_TERM_FILE")" == "$child_pid" ]] || fail "the reacquired helper did not terminate its own kubectl child"

if ARGO_UI_PORT=not-a-port "$HELPER" >"$TEST_ROOT/invalid.log" 2>&1; then
	fail "an invalid local port was accepted"
fi

echo "argocd port-forward helper tests passed"
