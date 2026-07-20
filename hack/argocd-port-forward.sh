#!/usr/bin/env bash
# Keep the Argo mirror's control-plane Service reachable across pod rollouts.
set -euo pipefail

CLUSTER="${KIND_ARGO_CLUSTER:-swe-argo}"
LOCAL_PORT="${ARGO_UI_PORT:-18080}"
CONTEXT="kind-$CLUSTER"
NAMESPACE="swe-platform-system"
SERVICE="swe-platform-swe-platform-control-plane"
LOCK_ROOT="${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}"
LOCK_KEY="${CONTEXT//[^a-zA-Z0-9_.-]/_}-$LOCAL_PORT"
LOCK_DIR="$LOCK_ROOT/swe-platform-argocd-port-forward-$LOCK_KEY.lock"
child_pid=""
lock_owned=false
stopping=false

case "$LOCAL_PORT" in
	''|*[!0-9]*)
		echo "ARGO_UI_PORT must be a numeric TCP port" >&2
		exit 2
		;;
esac
if ((LOCAL_PORT < 1 || LOCAL_PORT > 65535)); then
	echo "ARGO_UI_PORT must be between 1 and 65535" >&2
	exit 2
fi
command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required" >&2; exit 1; }

cleanup() {
	if [[ -n "$child_pid" ]]; then
		kill "$child_pid" 2>/dev/null || true
		wait "$child_pid" 2>/dev/null || true
		child_pid=""
	fi
	if [[ "$lock_owned" == true ]]; then
		rm -f "$LOCK_DIR/pid"
		rmdir "$LOCK_DIR" 2>/dev/null || true
		lock_owned=false
	fi
}

request_stop() {
	stopping=true
	if [[ -n "$child_pid" ]]; then
		kill "$child_pid" 2>/dev/null || true
	fi
}

acquire_lock() {
	if ! mkdir "$LOCK_DIR" 2>/dev/null; then
		local existing_pid=""
		if [[ -r "$LOCK_DIR/pid" ]]; then
			read -r existing_pid <"$LOCK_DIR/pid" || true
		fi
		echo "an Argo console port-forward helper lock already exists for $CONTEXT on port $LOCAL_PORT${existing_pid:+ (pid $existing_pid)}" >&2
		echo "if no helper is running, remove the stale lock: $LOCK_DIR" >&2
		exit 1
	fi
	printf '%s\n' "$$" >"$LOCK_DIR/pid"
	lock_owned=true
}

trap request_stop INT TERM
trap cleanup EXIT
acquire_lock

echo "==> console URL: http://127.0.0.1:$LOCAL_PORT/"
echo "==> forwarding $CONTEXT/$NAMESPACE/service/$SERVICE (Ctrl-C to stop)"

while [[ "$stopping" == false ]]; do
	kubectl --context "$CONTEXT" --namespace "$NAMESPACE" port-forward \
		--address 127.0.0.1 "service/$SERVICE" "$LOCAL_PORT:80" &
	child_pid=$!
	if [[ "$stopping" == true ]]; then
		break
	fi

	set +e
	wait "$child_pid"
	status=$?
	set -e
	child_pid=""
	if [[ "$stopping" == true ]]; then
		break
	fi

	echo "==> port-forward exited with status $status; retrying in 1 second" >&2
	sleep 1 || true
done

echo "==> stopped Argo console port-forward"
