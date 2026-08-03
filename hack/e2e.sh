#!/usr/bin/env bash
# End-to-end acceptance: build everything, spin up kind, install CRDs, run the
# operator through Helm, and drive a real environment through the swe CLI.
#
# No registries or cloud credentials required: env-base is built locally and
# loaded into the kind cluster directly.
#
# Prerequisites: go, docker, kind, kubectl, helm.
# Env: KIND_CLUSTER (default swe-e2e), KEEP_CLUSTER=true to skip teardown,
# E2E_USE_EXISTING_CLUSTER=true to test a bootstrapped cluster, and
# E2E_RUNTIME_CLASS (for example gvisor) to select its environment runtime. Existing
# cluster mode expects a fresh `make kind-up` cluster without an installed platform.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-swe-e2e}"
SYSTEM_NAMESPACE="swe-platform-system"
PROJECT_NAMESPACE="swe-e2e-project"
PROJECT_NAME="e2e"
INSTALLATION_NAME="swe-platform-swe-platform"
ENV_IMAGE="ghcr.io/chris-cullins/swe-platform/env-base:dev"
E2E_ENV_IMAGE="ghcr.io/chris-cullins/swe-platform/env-base:e2e-credentials"
OPERATOR_IMAGE="ghcr.io/chris-cullins/swe-platform/operator:dev"
CONTROL_PLANE_IMAGE="ghcr.io/chris-cullins/swe-platform/control-plane:dev"
E2E_AGENT_API_KEY='!!SWE-E2E-AGENT-API-KEY-DO-NOT-USE!!'
E2E_ROTATED_AGENT_API_KEY='!!SWE-E2E-ROTATED-AGENT-API-KEY-DO-NOT-USE!!'
E2E_AMP_API_KEY='!!SWE-E2E-AMP-API-KEY-DO-NOT-USE!!'
PORT_FORWARD_PID=""
SANDBOXD_PORT_FORWARD_PID=""
POSTGRES_PORT_FORWARD_PID=""
STREAM_PID=""
CLI_STREAM_PID=""
WEB_TERMINAL_CLIENT=""
WEB_TERMINAL_SOURCE=""
PROJECT_REPO=""
PROJECT_WORKTREE=""
FAKE_ENV_CONTEXT=""
E2E_KUBECONFIG=""
SESSION_KEYRING_FILE=""
E2E_SESSION_FIXTURE=""
SELECTOR_ENV_NAME=""
LEGACY_CRD_ACTIVE="false"
LEGACY_ENV_NAMES=""

cleanup() {
	if [[ "$LEGACY_CRD_ACTIVE" == "true" ]]; then
		kubectl apply --server-side --force-conflicts -f charts/swe-platform/crds >/dev/null 2>&1 || true
		LEGACY_CRD_ACTIVE="false"
	fi
	if [[ -n "$LEGACY_ENV_NAMES" ]]; then
		for legacy_env in $LEGACY_ENV_NAMES; do
			kubectl -n "$PROJECT_NAMESPACE" patch secret "env-$legacy_env-sandboxd" --type=merge \
				-p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
		done
		kubectl -n "$PROJECT_NAMESPACE" delete environment $LEGACY_ENV_NAMES --wait=false >/dev/null 2>&1 || true
	fi
	if [[ -n "$SELECTOR_ENV_NAME" ]]; then
		kubectl -n "$PROJECT_NAMESPACE" delete environment "$SELECTOR_ENV_NAME" --wait=false >/dev/null 2>&1 || true
	fi
	if [[ -n "$STREAM_PID" ]]; then
		kill "$STREAM_PID" >/dev/null 2>&1 || true
		wait "$STREAM_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$CLI_STREAM_PID" ]]; then
		kill "$CLI_STREAM_PID" >/dev/null 2>&1 || true
		wait "$CLI_STREAM_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$PORT_FORWARD_PID" ]]; then
		kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
		wait "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$SANDBOXD_PORT_FORWARD_PID" ]]; then
		kill "$SANDBOXD_PORT_FORWARD_PID" >/dev/null 2>&1 || true
		wait "$SANDBOXD_PORT_FORWARD_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$POSTGRES_PORT_FORWARD_PID" ]]; then
		kill "$POSTGRES_PORT_FORWARD_PID" >/dev/null 2>&1 || true
		wait "$POSTGRES_PORT_FORWARD_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$WEB_TERMINAL_CLIENT" ]]; then
		rm -f "$WEB_TERMINAL_CLIENT"
	fi
	if [[ -n "$WEB_TERMINAL_SOURCE" ]]; then
		rm -f "$WEB_TERMINAL_SOURCE"
	fi
	if [[ -n "$PROJECT_REPO" ]]; then
		rm -rf "$PROJECT_REPO"
	fi
	if [[ -n "$PROJECT_WORKTREE" ]]; then
		rm -rf "$PROJECT_WORKTREE"
	fi
	if [[ -n "$FAKE_ENV_CONTEXT" ]]; then
		rm -rf "$FAKE_ENV_CONTEXT"
	fi
	if [[ -n "$E2E_KUBECONFIG" ]]; then
		rm -f "$E2E_KUBECONFIG"
	fi
	if [[ -n "$SESSION_KEYRING_FILE" ]]; then
		rm -f "$SESSION_KEYRING_FILE"
	fi
	if [[ -n "$E2E_SESSION_FIXTURE" ]]; then
		rm -rf "$E2E_SESSION_FIXTURE"
	fi
	rm -f /tmp/swe-platform-sandboxd-cert-"$$" /tmp/swe-platform-sandboxd-token-"$$" \
		/tmp/swe-platform-observation-cert-"$$" /tmp/swe-platform-observation-process-token-"$$"
	if [[ "${KEEP_CLUSTER:-false}" != "true" && "${E2E_USE_EXISTING_CLUSTER:-false}" != "true" ]]; then
		kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

contains_e2e_key() {
	grep -aFq -- "$E2E_AGENT_API_KEY" "$1" || grep -aFq -- "$E2E_ROTATED_AGENT_API_KEY" "$1" || \
		grep -aFq -- "$E2E_AMP_API_KEY" "$1"
}

wait_for_resource_quota_observation() {
	local namespace="$1"
	for _ in $(seq 1 300); do
		if kubectl -n "$namespace" get resourcequota swe-project -o json | \
			jq -e '(.status.hard // {}) == .spec.hard and ((.status.used // {}) | has("count/environments.swe.dev"))' >/dev/null; then
			return 0
		fi
		sleep 1
	done
	echo "FAIL: ResourceQuota in $namespace did not observe configured hard limits and Environment usage" >&2
	kubectl -n "$namespace" get resourcequota swe-project -o yaml >&2 || true
	return 1
}

start_control_plane_port_forward() {
	if [[ -n "$PORT_FORWARD_PID" ]]; then
		kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
		wait "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
	fi
	: > /tmp/swe-platform-port-forward.log
	kubectl -n "$SYSTEM_NAMESPACE" port-forward service/swe-platform-swe-platform-control-plane 18080:80 \
		>/tmp/swe-platform-port-forward.log 2>&1 &
	PORT_FORWARD_PID=$!
	for _ in $(seq 1 30); do
		if kill -0 "$PORT_FORWARD_PID" >/dev/null 2>&1 && \
			curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null; then
			return 0
		fi
		sleep 1
	done
	echo "FAIL: control-plane port-forward did not become ready" >&2
	cat /tmp/swe-platform-port-forward.log >&2
	return 1
}

replace_control_plane_pod() {
	local deployment="$INSTALLATION_NAME-control-plane"
	local ready_pods old_name old_uid
	ready_pods=$(kubectl -n "$SYSTEM_NAMESPACE" get pod \
		-l 'app.kubernetes.io/component=control-plane' -o json | \
		jq -r '.items[] | select(.metadata.deletionTimestamp == null and any(.status.conditions[]?; .type == "Ready" and .status == "True")) | [.metadata.name, .metadata.uid] | @tsv')
	if [[ "$(wc -l <<<"$ready_pods")" != "1" ]]; then
		echo "FAIL: expected exactly one ready control-plane pod before replacement" >&2
		kubectl -n "$SYSTEM_NAMESPACE" get pods -l 'app.kubernetes.io/component=control-plane' >&2 || true
		return 1
	fi
	IFS=$'\t' read -r old_name old_uid <<<"$ready_pods"
	kubectl -n "$SYSTEM_NAMESPACE" delete pod "$old_name" --wait=true >/dev/null
	for _ in $(seq 1 120); do
		if kubectl -n "$SYSTEM_NAMESPACE" get pod \
			-l 'app.kubernetes.io/component=control-plane' -o json | \
			jq -e --arg old "$old_uid" '.items | any(.metadata.deletionTimestamp == null and .metadata.uid != $old and any(.status.conditions[]?; .type == "Ready" and .status == "True"))' >/dev/null; then
			kubectl -n "$SYSTEM_NAMESPACE" rollout status deployment/"$deployment" --timeout=30s >/dev/null
			start_control_plane_port_forward
			return 0
		fi
		sleep 1
	done
	echo "FAIL: control-plane pod was not replaced with a ready pod" >&2
	kubectl -n "$SYSTEM_NAMESPACE" get pods -l 'app.kubernetes.io/component=control-plane' >&2 || true
	return 1
}

browser_session_count() {
	kubectl -n "$SYSTEM_NAMESPACE" exec deployment/postgres -- \
		psql -U swe -d swe -tAc 'SELECT count(*) FROM browser_sessions' | tr -d '[:space:]'
}

check_sandboxd_process() {
	local pod_name="$1"
	local run_uid="$2"
	local expected_key="$3"
	local secret_name identity
	secret_name=$(kubectl -n "$PROJECT_NAMESPACE" get pod "$pod_name" -o jsonpath='{.metadata.annotations.swe\.dev/sandboxd-secret-name}')
	identity=$(kubectl -n "$PROJECT_NAMESPACE" get pod "$pod_name" -o jsonpath='{.metadata.annotations.swe\.dev/sandboxd-identity}')
	kubectl -n "$PROJECT_NAMESPACE" get secret "$secret_name" -o jsonpath='{.data.tls\.crt}' | base64 --decode > /tmp/swe-platform-sandboxd-cert-"$$"
	kubectl -n "$PROJECT_NAMESPACE" get secret "$secret_name" -o jsonpath='{.data.process-token}' | base64 --decode > /tmp/swe-platform-sandboxd-token-"$$"
	kubectl -n "$PROJECT_NAMESPACE" port-forward pod/"$pod_name" 15051:50051 >/tmp/swe-platform-sandboxd-port-forward.log 2>&1 &
	SANDBOXD_PORT_FORWARD_PID=$!
	for _ in $(seq 1 30); do
		if grep -q 'Forwarding from' /tmp/swe-platform-sandboxd-port-forward.log; then
			break
		fi
		sleep 1
	done
	printf '%s' "$expected_key" | go run ./hack/e2e-process-check \
		127.0.0.1:15051 "$identity" /tmp/swe-platform-sandboxd-cert-"$$" /tmp/swe-platform-sandboxd-token-"$$" "$run_uid"
	kill "$SANDBOXD_PORT_FORWARD_PID" >/dev/null 2>&1 || true
	wait "$SANDBOXD_PORT_FORWARD_PID" >/dev/null 2>&1 || true
	SANDBOXD_PORT_FORWARD_PID=""
	rm -f /tmp/swe-platform-sandboxd-cert-"$$" /tmp/swe-platform-sandboxd-token-"$$"
}

manage_observation_listener() {
	local action="$1"
	local pod_name="$2"
	local owner="$3"
	local role="$4"
	local port="${5:-}"
	local secret_name identity
	secret_name=$(kubectl -n "$PROJECT_NAMESPACE" get pod "$pod_name" -o jsonpath='{.metadata.annotations.swe\.dev/sandboxd-secret-name}')
	identity=$(kubectl -n "$PROJECT_NAMESPACE" get pod "$pod_name" -o jsonpath='{.metadata.annotations.swe\.dev/sandboxd-identity}')
	kubectl -n "$PROJECT_NAMESPACE" get secret "$secret_name" -o jsonpath='{.data.tls\.crt}' | base64 --decode > /tmp/swe-platform-observation-cert-"$$"
	kubectl -n "$PROJECT_NAMESPACE" get secret "$secret_name" -o jsonpath='{.data.process-token}' | base64 --decode > /tmp/swe-platform-observation-process-token-"$$"
	kubectl -n "$PROJECT_NAMESPACE" port-forward pod/"$pod_name" 15052:50051 >/tmp/swe-platform-observation-port-forward.log 2>&1 &
	SANDBOXD_PORT_FORWARD_PID=$!
	for _ in $(seq 1 30); do
		if grep -q 'Forwarding from' /tmp/swe-platform-observation-port-forward.log; then
			break
		fi
		sleep 1
	done
	if ! grep -q 'Forwarding from' /tmp/swe-platform-observation-port-forward.log; then
		cat /tmp/swe-platform-observation-port-forward.log >&2
		echo "FAIL: sandboxd observation port-forward did not become ready" >&2
		return 1
	fi
	if [[ "$action" == "service-start" || "$action" == "service-state" ]]; then
		go run ./hack/e2e-process-check "$action" 127.0.0.1:15052 "$identity" /tmp/swe-platform-observation-cert-"$$" /tmp/swe-platform-observation-process-token-"$$" "$owner" "$role" "$port"
	else
		go run ./hack/e2e-process-check "$action" 127.0.0.1:15052 "$identity" /tmp/swe-platform-observation-cert-"$$" /tmp/swe-platform-observation-process-token-"$$" "$owner" "$role"
	fi
	kill "$SANDBOXD_PORT_FORWARD_PID" >/dev/null 2>&1 || true
	wait "$SANDBOXD_PORT_FORWARD_PID" >/dev/null 2>&1 || true
	SANDBOXD_PORT_FORWARD_PID=""
	rm -f /tmp/swe-platform-observation-cert-"$$" /tmp/swe-platform-observation-process-token-"$$"
}

wait_service_observation() {
	local environment="$1"
	local service="$2"
	local revision="$3"
	local state="$4"
	local execution_mode="${5:-present}"
	for _ in $(seq 1 60); do
		if kubectl -n "$PROJECT_NAMESPACE" get environment "$environment" -o json | jq -e \
			--arg service "$service" --argjson revision "$revision" --arg state "$state" --arg execution_mode "$execution_mode" '
			.status.serviceObservations as $envelope |
			$envelope.observedGeneration == .metadata.generation and
			any($envelope.records[]?; .name == $service and .declarationRevision == $revision and .state == $state) and
			(if $execution_mode == "absent" then ($envelope.executionGeneration == null)
			 else ($envelope.executionGeneration == .status.executionGeneration) end)' >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	echo "FAIL: service $service revision $revision did not reach $state ($execution_mode execution)" >&2
	kubectl -n "$PROJECT_NAMESPACE" get environment "$environment" -o yaml >&2 || true
	return 1
}

cd "$(dirname "$0")/.."

echo "==> building binaries"
make build

if [[ "${E2E_USE_EXISTING_CLUSTER:-false}" == "true" ]]; then
	echo "==> using existing kind cluster '$CLUSTER'"
	E2E_KUBECONFIG="$(mktemp /tmp/swe-e2e-kubeconfig-XXXXXX)"
	kubectl config view --raw > "$E2E_KUBECONFIG"
	export KUBECONFIG="$E2E_KUBECONFIG"
	kubectl cluster-info --context "kind-$CLUSTER" >/dev/null
	kubectl config use-context "kind-$CLUSTER" >/dev/null
	if kubectl get crd runs.swe.dev >/dev/null 2>&1; then
		echo "FAIL: existing-cluster E2E requires a fresh make kind-up cluster without swe-platform CRDs" >&2
		exit 1
	fi
else
	echo "==> creating kind cluster '$CLUSTER'"
	kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
	kind create cluster --name "$CLUSTER"
fi

echo "==> building platform images"
make docker-build >/dev/null

echo "==> building fake Claude credential-acceptance image"
FAKE_ENV_CONTEXT="$(mktemp -d /tmp/swe-e2e-env-image-XXXXXX)"
cat > "$FAKE_ENV_CONTEXT/claude" <<'EOF'
#!/bin/sh
if [ -z "${ANTHROPIC_API_KEY+x}" ] || [ -z "$ANTHROPIC_API_KEY" ]; then
	exit 42
fi
printf '%s\n' credential-present >> /workspace/agent-credential-marker
printf '%s\n' '{"type":"system","subtype":"init","session_id":"fake-e2e"}'
printf '%s\n' '{"type":"assistant","session_id":"fake-e2e","message":{"id":"msg-fake-e2e","type":"message","role":"assistant","model":"claude-e2e","content":[{"type":"text","text":"fake Claude Code is working"},{"type":"tool_use","id":"tool-fake-e2e","name":"Read","input":{"file_path":"README.md"}}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}},"parent_tool_use_id":null}'
WAIT_FOR_BROWSER=false
for arg in "$@"; do
	if [ "$arg" = 'resume credential smoke test' ]; then WAIT_FOR_BROWSER=true; fi
done
if [ "$WAIT_FOR_BROWSER" = true ]; then
	attempt=0
	while [ "$attempt" -lt 60 ]; do
		if [ -f /workspace/browser-terminal-opened ]; then break; fi
		attempt=$((attempt + 1))
		sleep 1
	done
else
	sleep 5
fi
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"fake Claude Code completed"}'
EOF
chmod 0755 "$FAKE_ENV_CONTEXT/claude"
cat > "$FAKE_ENV_CONTEXT/amp" <<'EOF'
#!/bin/sh
set -eu
test "$#" -eq 4
test "$2" = '--stream-json'
test "$3" = '--no-ide'
test "$4" = '--no-notifications'
case "$1" in
	'--execute=fake Amp lifecycle smoke test')
		test "${AMP_API_KEY:-}" = '!!SWE-E2E-AMP-API-KEY-DO-NOT-USE!!'
		printf '%s\n' amp-credential-present
		;;
	'--execute=fake Amp credentialless lifecycle smoke test')
		test -z "${AMP_API_KEY+x}"
		;;
	*) exit 45 ;;
esac
printf '%s\n' '{"type":"assistant","message":{"content":[{"type":"text","text":"fake-amp-stdout-marker"}]}}'
printf '%s\n' 'fake-amp-stderr-marker' >&2
sleep 5
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"fake Amp completed"}'
EOF
chmod 0755 "$FAKE_ENV_CONTEXT/amp"
cat > "$FAKE_ENV_CONTEXT/codex" <<'EOF'
#!/bin/sh
set -eu
test "$#" -eq 12
test "$1" = exec
test "$2" = --json
test "$3" = --ephemeral
test "$4" = --ignore-user-config
test "$5" = --ignore-rules
test "$6" = --sandbox
test "$7" = workspace-write
test "$8" = --color
test "$9" = never
test "${10}" = --skip-git-repo-check
test "${11}" = --
test -z "${CODEX_API_KEY+x}"
printf '%s\n' '{"type":"thread.started","thread_id":"fake-codex-thread"}'
printf '%s\n' 'fake-codex-stderr-marker' >&2
if [ "${12}" = 'fake Codex failure smoke test' ]; then
	printf '%s\n' '{"type":"turn.failed","error":{"message":"fake Codex failed"}}'
	exit 0
fi
test "${12}" = 'fake Codex lifecycle smoke test'
sleep 5
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":2}}'
EOF
chmod 0755 "$FAKE_ENV_CONTEXT/codex"
cat > "$FAKE_ENV_CONTEXT/pi" <<'EOF'
#!/bin/sh
set -eu
test "$#" -eq 5
test "$1" = --mode; test "$2" = json; test "$3" = --no-session; test "$4" = -p
test -z "${ANTHROPIC_API_KEY+x}"; test -z "${OPENAI_API_KEY+x}"; test -z "${GEMINI_API_KEY+x}"
printf '%s\n' '{"type":"message_end","message":{"role":"assistant","stopReason":"stop"}}'
printf '%s\n' 'fake-pi-stderr-marker' >&2
if [ "$5" = 'fake Pi failure smoke test' ]; then reason=error; else test "$5" = 'fake Pi lifecycle smoke test'; reason=stop; sleep 5; fi
printf '%s\n' "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"stopReason\":\"$reason\"}]}"
EOF
chmod 0755 "$FAKE_ENV_CONTEXT/pi"
cat > "$FAKE_ENV_CONTEXT/Dockerfile" <<'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
COPY claude /usr/local/bin/claude
COPY amp /usr/local/bin/amp
COPY codex /usr/local/bin/codex
COPY pi /usr/local/bin/pi
USER swe
EOF
docker build --build-arg BASE_IMAGE="$ENV_IMAGE" -t "$E2E_ENV_IMAGE" "$FAKE_ENV_CONTEXT" >/dev/null
rm -rf "$FAKE_ENV_CONTEXT"
FAKE_ENV_CONTEXT=""

echo "==> verifying terminal drain against patched env-base runtime"
docker build --target terminal-test -t swe-platform-terminal-test -f images/env-base/Dockerfile . >/dev/null
docker run --rm -e SWE_REQUIRE_PATCHED_TMUX=1 swe-platform-terminal-test \
	-test.run '^TestTerminalDrains(OutputWhenShellExits|ImmediateOutputAfterFirstOpen)$' -test.count=1 -test.v

echo "==> loading images into kind"
kind load docker-image "$ENV_IMAGE" "$E2E_ENV_IMAGE" "$OPERATOR_IMAGE" "$CONTROL_PLANE_IMAGE" --name "$CLUSTER"

echo "==> simulating a plain-Helm CRD upgrade from the pre-scoped-credentials schema"
PRE_SCOPED_CREDENTIALS_SHA=d76e694521b18f1b3921311c7886f53e5a3c8806
git fetch --depth=1 origin "$PRE_SCOPED_CREDENTIALS_SHA"
for resource in environments environmenttemplates projects runs; do
	git show FETCH_HEAD:"config/crd/bases/swe.dev_${resource}.yaml" | kubectl create -f -
done

# Exercise the immediate pre-execution-generation schema with persisted nested
# activity. The new nested fields must remain optional so this live object can
# survive structural-schema replacement; controller handling fails closed.
PRE_EXECUTION_GENERATION_SHA=c115faf13ab62aead857e44c135fae6c2777f38e
git fetch --depth=1 origin "$PRE_EXECUTION_GENERATION_SHA"
for resource in environments runs; do
	git show "$PRE_EXECUTION_GENERATION_SHA:config/crd/bases/swe.dev_${resource}.yaml" | kubectl apply --server-side --force-conflicts -f -
done
cat <<'EOF' | kubectl create -f -
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: legacy-execution-generation-migration
spec:
  templateRef: small
  lifecycle:
    activity:
    - source: Terminal
      id: legacy-terminal-activity
      environmentUID: legacy-environment-uid
      holdPolicyRevision: 0
EOF
kubectl patch environment legacy-execution-generation-migration --subresource=status --type=merge -p \
	'{"status":{"lifecycle":{"activityReceipts":[{"source":"Terminal","requestID":"legacy-terminal-activity"}]}}}'

# Exercise the exact pre-portal service schema with a declaration persisted
# before instanceID existed. The upgraded schema must leave it writable and
# permit only a revision-advancing missing-to-valid migration.
PRE_SERVICE_INSTANCE_ID_SHA=5654ba351a6a80c71d9bdcfa3f123fe39c060bea
git fetch --depth=1 origin "$PRE_SERVICE_INSTANCE_ID_SHA"
git show "$PRE_SERVICE_INSTANCE_ID_SHA:config/crd/bases/swe.dev_environments.yaml" | kubectl apply --server-side --force-conflicts -f -
cat <<'EOF' | kubectl create -f -
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: legacy-service-instance-id-migration
spec:
  templateRef: small
  services:
  - name: web
    revision: 1
    protocol: HTTP
    targetPort: 3000
    visibility: Project
    readiness: TCPConnect
EOF
# Upgrade to the immediate pre-#162 schema and persist representative flat
# recovery states in the eventual Project namespace. They remain outside
# operator scope only until that precreated namespace is adopted below.
PRE_NESTED_RECOVERY_SHA=f0fea8ba44fc002e342bfae316cf1d6a6d15fa6f
git fetch --depth=1 origin "$PRE_NESTED_RECOVERY_SHA"
git show "$PRE_NESTED_RECOVERY_SHA:config/crd/bases/swe.dev_environments.yaml" | kubectl apply --server-side --force-conflicts -f -
kubectl create namespace "$PROJECT_NAMESPACE"
for name in legacy-recovery-existing legacy-recovery-missing legacy-recovery-exhausted; do
	kubectl -n "$PROJECT_NAMESPACE" create -f - <<EOF
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata: {name: $name}
spec: {templateRef: small}
EOF
done
LEGACY_ENV_UID=$(kubectl -n "$PROJECT_NAMESPACE" get environment legacy-recovery-existing -o jsonpath='{.metadata.uid}')
cat <<'EOF' | kubectl -n "$PROJECT_NAMESPACE" create -f -
apiVersion: v1
kind: Pod
metadata:
  name: env-legacy-recovery-existing
  annotations: {swe.dev/execution-generation: "1"}
spec:
  restartPolicy: Never
  containers: [{name: environment, image: busybox, command: [sh, -c, "exit 1"]}]
EOF
kubectl -n "$PROJECT_NAMESPACE" wait --for=jsonpath='{.status.phase}'=Failed pod/env-legacy-recovery-existing --timeout=1m
kubectl -n "$PROJECT_NAMESPACE" patch pod env-legacy-recovery-existing --type=merge -p \
	"{\"metadata\":{\"ownerReferences\":[{\"apiVersion\":\"swe.dev/v1alpha1\",\"kind\":\"Environment\",\"name\":\"legacy-recovery-existing\",\"uid\":\"$LEGACY_ENV_UID\",\"controller\":true,\"blockOwnerDeletion\":true}]}}"
LEGACY_POD_UID=$(kubectl -n "$PROJECT_NAMESPACE" get pod env-legacy-recovery-existing -o jsonpath='{.metadata.uid}')
LEGACY_DEADLINE=2099-01-01T00:00:00Z
kubectl -n "$PROJECT_NAMESPACE" patch environment legacy-recovery-existing --subresource=status --type=merge -p \
	"{\"status\":{\"executionGeneration\":1,\"podRecoveryAttempts\":1,\"podRecoveryUID\":\"$LEGACY_POD_UID\",\"podRecoveryNextAttemptAt\":\"$LEGACY_DEADLINE\"}}"
kubectl -n "$PROJECT_NAMESPACE" patch environment legacy-recovery-missing --subresource=status --type=merge -p \
	"{\"status\":{\"executionGeneration\":1,\"podRecoveryAttempts\":2,\"podRecoveryUID\":\"missing-pod-uid\",\"podRecoveryNextAttemptAt\":\"$LEGACY_DEADLINE\"}}"
kubectl -n "$PROJECT_NAMESPACE" patch environment legacy-recovery-exhausted --subresource=status --type=merge -p \
	'{"status":{"executionGeneration":1,"podRecoveryAttempts":3,"podRecoveryExhausted":true,"podRecoveryUID":"exhausted-pod-uid"}}'
kubectl apply --server-side --force-conflicts -f charts/swe-platform/crds
kubectl get crd agentcredentialprofiles.swe.dev >/dev/null
kubectl get environment legacy-execution-generation-migration -o json | jq -e \
	'.spec.lifecycle.activity[0].executionGeneration == null and .status.lifecycle.activityReceipts[0].executionGeneration == null' >/dev/null
kubectl patch environment legacy-service-instance-id-migration --subresource=status --type=merge -p '{"status":{"phase":"Creating"}}'
kubectl patch environment legacy-service-instance-id-migration --type=json -p \
	'[{"op":"add","path":"/spec/projectRef","value":"legacy-migration"}]'
kubectl get environment legacy-service-instance-id-migration -o json | jq -e \
	'.status.phase == "Creating" and .spec.projectRef == "legacy-migration" and .spec.services[0].targetPort == 3000 and .spec.services[0].revision == 1 and .spec.services[0].instanceID == null' >/dev/null
if kubectl create -f - >/tmp/new-service-missing-instance-id.out 2>&1 <<'EOF'; then
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: new-service-missing-instance-id
spec:
  templateRef: small
  services:
  - name: web
    revision: 1
    protocol: HTTP
    targetPort: 3000
    visibility: Project
    readiness: TCPConnect
EOF
	echo "FAIL: upgraded admission accepted a new service without instanceID"
	exit 1
fi
if ! grep -q 'new services require instanceID; a legacy missing instanceID may be added only with a higher revision and is then immutable' /tmp/new-service-missing-instance-id.out; then
	echo "FAIL: new missing instanceID was not rejected by the intended migration rule"
	cat /tmp/new-service-missing-instance-id.out
	exit 1
fi
if kubectl patch environment legacy-service-instance-id-migration --type=json -p \
	'[{"op":"add","path":"/spec/services/-","value":{"name":"admin","revision":1,"protocol":"HTTP","targetPort":3001,"visibility":"Project","readiness":"TCPConnect"}}]' \
	>/tmp/legacy-service-new-name-missing-instance-id.out 2>&1; then
	echo "FAIL: upgraded admission accepted a new service name without instanceID"
	exit 1
fi
if ! grep -q 'new services require instanceID; a legacy missing instanceID may be added only with a higher revision and is then immutable' /tmp/legacy-service-new-name-missing-instance-id.out; then
	echo "FAIL: update-side new name was not rejected by the intended migration rule"
	cat /tmp/legacy-service-new-name-missing-instance-id.out
	exit 1
fi
if kubectl patch environment legacy-service-instance-id-migration --type=json -p \
	'[{"op":"add","path":"/spec/services/0/instanceID","value":"aaaaaaaaaaaaaaaaaaaa"}]' \
	>/tmp/legacy-service-instance-id-without-revision.out 2>&1; then
	echo "FAIL: upgraded admission accepted legacy instanceID backfill without a higher revision"
	exit 1
fi
if ! grep -q 'new services require instanceID; a legacy missing instanceID may be added only with a higher revision and is then immutable' /tmp/legacy-service-instance-id-without-revision.out; then
	echo "FAIL: unrevisioned legacy backfill was not rejected by the intended migration rule"
	cat /tmp/legacy-service-instance-id-without-revision.out
	exit 1
fi
kubectl get environment legacy-service-instance-id-migration -o json | jq -e \
	'.spec.services | length == 1 and .[0].revision == 1 and .[0].instanceID == null' >/dev/null
bin/swe --namespace default environment services update legacy-service-instance-id-migration web --target-port 3000
LEGACY_SERVICE_INSTANCE_ID=$(kubectl get environment legacy-service-instance-id-migration -o jsonpath='{.spec.services[0].instanceID}')
if [[ ! "$LEGACY_SERVICE_INSTANCE_ID" =~ ^[a-z0-9]{20,63}$ ]] ||
	[[ "$(kubectl get environment legacy-service-instance-id-migration -o jsonpath='{.spec.services[0].revision}')" != "2" ]]; then
	echo "FAIL: CLI did not backfill the legacy service instanceID at a higher revision"
	exit 1
fi
if kubectl patch environment legacy-service-instance-id-migration --type=json -p \
	'[{"op":"replace","path":"/spec/services/0/instanceID","value":"aaaaaaaaaaaaaaaaaaaa"},{"op":"replace","path":"/spec/services/0/revision","value":3}]' \
	>/tmp/legacy-service-instance-id-replacement.out 2>&1; then
	echo "FAIL: admission accepted replacement of a migrated service instanceID"
	exit 1
fi
if ! grep -q 'new services require instanceID; a legacy missing instanceID may be added only with a higher revision and is then immutable' /tmp/legacy-service-instance-id-replacement.out; then
	echo "FAIL: migrated instanceID replacement was not rejected by the intended immutability rule"
	cat /tmp/legacy-service-instance-id-replacement.out
	exit 1
fi
kubectl -n "$PROJECT_NAMESPACE" get environments legacy-recovery-existing legacy-recovery-missing legacy-recovery-exhausted -o json | jq -e --arg uid "$LEGACY_POD_UID" --arg deadline "$LEGACY_DEADLINE" '
  (.items[] | select(.metadata.name=="legacy-recovery-existing") | .status | .podRecoveryAttempts==1 and .podRecoveryUID==$uid and .podRecoveryNextAttemptAt==$deadline) and
  (.items[] | select(.metadata.name=="legacy-recovery-missing") | .status | .podRecoveryAttempts==2 and .podRecoveryUID=="missing-pod-uid" and .podRecoveryNextAttemptAt==$deadline) and
  (.items[] | select(.metadata.name=="legacy-recovery-exhausted") | .status | .podRecoveryAttempts==3 and .podRecoveryExhausted==true)' >/dev/null

# Exercise the immediate pre-service-source schema with a durable API-owned
# declaration. Defaulting and the one-way legacy migration must preserve its
# identity and intent through both status and ordinary spec writes.
PRE_SERVICE_SOURCE_SHA=28c9d396a9960ebbbd1b4feea3626de7c258a383
git fetch --depth=1 origin "$PRE_SERVICE_SOURCE_SHA"
git show "$PRE_SERVICE_SOURCE_SHA:config/crd/bases/swe.dev_environments.yaml" | kubectl apply --server-side --force-conflicts -f -
cat <<'EOF' | kubectl create -f -
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: legacy-service-source-migration
spec:
  templateRef: small
  services:
  - name: legacy-api
    instanceID: legacyapiabcdefghijkl
    revision: 3
    protocol: HTTP
    targetPort: 4321
    visibility: Project
    readiness: TCPConnect
EOF
kubectl apply --server-side --force-conflicts -f charts/swe-platform/crds
kubectl get environment legacy-service-source-migration -o json | jq -e \
	'.spec.services == [{"instanceID":"legacyapiabcdefghijkl","name":"legacy-api","protocol":"HTTP","readiness":"TCPConnect","revision":3,"source":"API","targetPort":4321,"visibility":"Project"}]' >/dev/null
kubectl patch environment legacy-service-source-migration --subresource=status --type=merge -p '{"status":{"phase":"Creating"}}'
kubectl patch environment legacy-service-source-migration --type=merge -p \
	'{"spec":{"lifecycle":{"hold":{"enabled":false,"revision":1}}}}'
kubectl get environment legacy-service-source-migration -o json | jq -e \
	'.status.phase == "Creating" and .spec.lifecycle.hold == {"enabled":false,"revision":1} and .spec.services == [{"instanceID":"legacyapiabcdefghijkl","name":"legacy-api","protocol":"HTTP","readiness":"TCPConnect","revision":3,"source":"API","targetPort":4321,"visibility":"Project"}]' >/dev/null
LEGACY_SOURCE_ERROR=/tmp/swe-platform-legacy-service-source-error
if kubectl patch environment legacy-service-source-migration --type=merge -p \
	'{"spec":{"services":[{"name":"legacy-api","instanceID":"legacyapiabcdefghijkl","revision":4,"source":"Repository","launch":{"argv":["serve"]},"protocol":"HTTP","targetPort":4321,"visibility":"Project","readiness":"TCPConnect"}]}}' \
	>/dev/null 2>"$LEGACY_SOURCE_ERROR"; then
	echo "FAIL: admission allowed a legacy API service to become Repository-owned"
	exit 1
fi
if ! grep -Fq 'only legacy declarations may adopt API ownership' "$LEGACY_SOURCE_ERROR"; then
	echo "FAIL: legacy source takeover was not rejected by the source-immutability rule"
	cat "$LEGACY_SOURCE_ERROR"
	exit 1
fi
kubectl get environment legacy-service-source-migration -o json | jq -e \
	'.spec.services[0].source == "API" and .spec.services[0].revision == 3 and (.spec.services[0] | has("launch") | not)' >/dev/null
kubectl delete environment legacy-execution-generation-migration --wait=false
kubectl delete environment legacy-service-instance-id-migration --wait=false
kubectl delete environment legacy-service-source-migration --wait=false

echo "==> verifying trusted-admin preset and release-scoped cluster RBAC names"
TRUSTED_RENDER=$(helm template swe-platform charts/swe-platform --namespace preset-check --values charts/swe-platform/values-kind.yaml)
grep -q -- '--tenancy-mode=trusted-admin' <<<"$TRUSTED_RENDER"
grep -q 'SWE_TENANCY_MODE' <<<"$TRUSTED_RENDER"
grep -q '^kind: ClusterRoleBinding$' <<<"$TRUSTED_RENDER"
for preset in kind argocd k3s gke eks; do
	helm template swe-platform charts/swe-platform --namespace preset-check --values "charts/swe-platform/values-$preset.yaml" \
		| awk '/^kind: Deployment$/{deployment=1; next} deployment && /^  name: swe-platform-swe-platform$/{operator=1} operator && /^    type: Recreate$/{found=1} /^---$/{if (operator) exit !found; deployment=operator=found=0} END{if (operator) exit !found}'
done
RBAC_A=$(helm template swe-platform charts/swe-platform --namespace render-system-a --values charts/swe-platform/values-kind.yaml --set tenancy.mode=scoped | awk '/^kind: ClusterRole(Binding)?$/{k=$2} k && /^  name:/{print k "/" $2; k=""}' | sort -u)
RBAC_B=$(helm template swe-platform charts/swe-platform --namespace render-system-b --values charts/swe-platform/values-kind.yaml --set tenancy.mode=scoped | awk '/^kind: ClusterRole(Binding)?$/{k=$2} k && /^  name:/{print k "/" $2; k=""}' | sort -u)
if comm -12 <(printf '%s\n' "$RBAC_A") <(printf '%s\n' "$RBAC_B") | grep -q .; then
	echo "FAIL: identical release names in distinct system namespaces produced colliding cluster RBAC names"
	exit 1
fi

echo "==> installing scoped catalog-only platform through Helm with upgraded CRDs"
E2E_BOOTSTRAP_TOKEN="$(openssl rand -hex 32)"
kubectl create namespace "$SYSTEM_NAMESPACE"
kubectl -n "$SYSTEM_NAMESPACE" create secret generic swe-platform-bootstrap --from-literal=token="$E2E_BOOTSTRAP_TOKEN"
echo "==> provisioning PostgreSQL 17 and administrator-owned durable-session Secrets"
POSTGRES_PASSWORD="$(openssl rand -hex 24)"
kubectl -n "$SYSTEM_NAMESPACE" create secret generic swe-platform-postgres \
	--from-literal=url="postgres://swe:${POSTGRES_PASSWORD}@postgres:5432/swe?sslmode=disable" \
	--from-literal=password="$POSTGRES_PASSWORD"
SESSION_KEYRING_FILE="$(mktemp /tmp/swe-session-keyring-XXXXXX.json)"
jq -n --arg key "$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n')" \
	'{version:1,activeKeyId:"e2e",keys:[{id:"e2e",masterKey:$key}]}' > "$SESSION_KEYRING_FILE"
kubectl -n "$SYSTEM_NAMESPACE" create secret generic swe-platform-session-keyring \
	--from-file=keyring.json="$SESSION_KEYRING_FILE"
unset POSTGRES_PASSWORD
cat <<'EOF' | kubectl -n "$SYSTEM_NAMESPACE" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  replicas: 1
  selector:
    matchLabels: {app: swe-e2e-postgres}
  template:
    metadata:
      labels: {app: swe-e2e-postgres}
    spec:
      containers:
        - name: postgres
          image: postgres:17
          env:
            - {name: POSTGRES_USER, value: swe}
            - {name: POSTGRES_PASSWORD, valueFrom: {secretKeyRef: {name: swe-platform-postgres, key: password}}}
            - {name: POSTGRES_DB, value: swe}
          readinessProbe:
            exec: {command: [pg_isready, -U, swe]}
            initialDelaySeconds: 2
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  selector: {app: swe-e2e-postgres}
  ports: [{port: 5432, targetPort: 5432}]
EOF
kubectl -n "$SYSTEM_NAMESPACE" rollout status deployment/postgres --timeout=2m
HELM_ARGS=(
	upgrade --install swe-platform charts/swe-platform
	--namespace "$SYSTEM_NAMESPACE" --values charts/swe-platform/values-kind.yaml
	--set tenancy.mode=scoped --set-json 'tenancy.namespaces=[]'
	--set controlPlane.auth.bootstrapTokenSecret.name=swe-platform-bootstrap
	--set controlPlane.portal.enabled=true --set-string controlPlane.portal.suffix=portal.test
	--set controlPlane.portal.scheme=http
	--set controlPlane.sessions.backend=postgres
	--set controlPlane.sessions.keyringSecret.name=swe-platform-session-keyring
	--set controlPlane.transcripts.postgresSecret.name=swe-platform-postgres
	--set-string "environmentTemplates[0].spec.image=$E2E_ENV_IMAGE"
	--wait --timeout 2m
)
if [[ -n "${E2E_RUNTIME_CLASS:-}" ]]; then
	HELM_ARGS+=(--set-string "environmentTemplates[0].spec.runtimeClass=$E2E_RUNTIME_CLASS")
fi
helm "${HELM_ARGS[@]}"

# Installation must not prune compatibility status before Project adoption.
kubectl -n "$PROJECT_NAMESPACE" get environments legacy-recovery-existing legacy-recovery-missing legacy-recovery-exhausted -o json | jq -e \
	'[.items[].status.podRecoveryAttempts] | sort == [1,2,3]' >/dev/null

CATALOG_TEMPLATE=$(kubectl -n "$SYSTEM_NAMESPACE" get environmenttemplates -o json | jq -r '.items[] | select(.metadata.annotations["swe.dev/catalog-name"] == "small") | .metadata.name' | head -1)
if [[ -z "$CATALOG_TEMPLATE" ]] || kubectl -n "$SYSTEM_NAMESPACE" get environmenttemplate small >/dev/null 2>&1; then
	echo "FAIL: chart catalog source is absent or a runnable small Template exists in the system namespace"
	exit 1
fi

echo "==> onboarding the Project before enabling workload scope"
# These generous quota values are explicit E2E test capacity, not platform defaults.
QUOTA_ARGS=(--quota-hard requests.cpu=16 --quota-hard requests.memory=32Gi --quota-hard requests.storage=100Gi
	--quota-hard persistentvolumeclaims=20 --quota-hard pods=100 --quota-hard secrets=100
	--quota-hard count/runs.swe.dev=100 --quota-hard count/environments.swe.dev=20
	--quota-hard count/agentcredentialprofiles.swe.dev=20)
ONBOARD_ARGS=(--namespace "$PROJECT_NAMESPACE" project onboard "$PROJECT_NAME"
	--system-namespace "$SYSTEM_NAMESPACE" --installation "$INSTALLATION_NAME"
	--repository "git://e2e-git-server.$PROJECT_NAMESPACE.svc.cluster.local/e2e.git"
	--default-template small --template small
	"${QUOTA_ARGS[@]}")
if bin/swe --namespace "" project onboard implicit-refusal --system-namespace "$SYSTEM_NAMESPACE" --installation "$INSTALLATION_NAME" --repository git://unused --default-template small --template small "${QUOTA_ARGS[@]}" >/tmp/swe-e2e-implicit.out 2>&1; then
	echo "FAIL: project onboarding accepted an implicit/empty CLI namespace"
	exit 1
fi
bin/swe --namespace "$PROJECT_NAMESPACE" project onboard "$PROJECT_NAME" --adopt \
	--system-namespace "$SYSTEM_NAMESPACE" --installation "$INSTALLATION_NAME" \
	--repository "git://e2e-git-server.$PROJECT_NAMESPACE.svc.cluster.local/e2e.git" \
	--default-template small --template small "${QUOTA_ARGS[@]}"

INSTALLATION_UID=$(kubectl -n "$SYSTEM_NAMESPACE" get installation "$INSTALLATION_NAME" -o jsonpath='{.metadata.uid}')
PROJECT_UID=$(kubectl -n "$PROJECT_NAMESPACE" get project "$PROJECT_NAME" -o jsonpath='{.metadata.uid}')
SOURCE_UID=$(kubectl -n "$SYSTEM_NAMESPACE" get environmenttemplate "$CATALOG_TEMPLATE" -o jsonpath='{.metadata.uid}')
SOURCE_REVISION=$(kubectl -n "$SYSTEM_NAMESPACE" get environmenttemplate "$CATALOG_TEMPLATE" -o jsonpath='{.metadata.annotations.swe\.dev/catalog-revision}')
CLAIM=$(kubectl get namespace "$PROJECT_NAMESPACE" -o json)
jq -e --arg iu "$INSTALLATION_UID" --arg pu "$PROJECT_UID" '.metadata.annotations["swe.dev/installation-uid"]==$iu and .metadata.annotations["swe.dev/project-uid"]==$pu and .metadata.annotations["swe.dev/project-namespace-lifecycle"]=="active"' <<<"$CLAIM" >/dev/null
LOCAL_TEMPLATE=$(kubectl -n "$PROJECT_NAMESPACE" get environmenttemplate small -o json)
jq -e --arg iu "$INSTALLATION_UID" --arg pu "$PROJECT_UID" --arg su "$SOURCE_UID" --arg rev "$SOURCE_REVISION" '.metadata.uid != "" and .metadata.annotations["swe.dev/installation-uid"]==$iu and .metadata.annotations["swe.dev/project-uid"]==$pu and .metadata.annotations["swe.dev/catalog-source-uid"]==$su and .metadata.annotations["swe.dev/catalog-revision"]==$rev' <<<"$LOCAL_TEMPLATE" >/dev/null
kubectl -n "$PROJECT_NAMESPACE" get resourcequota/swe-project serviceaccount/swe-environment rolebinding/swe-platform-operator rolebinding/swe-platform-control-plane networkpolicy/swe-default-deny-ingress >/dev/null
[[ "$(kubectl -n "$PROJECT_NAMESPACE" get serviceaccount swe-environment -o jsonpath='{.automountServiceAccountToken}')" == "false" ]]
kubectl -n "$PROJECT_NAMESPACE" get networkpolicy swe-default-deny-ingress -o json | jq -e '.spec.policyTypes == ["Ingress"] and (.spec.ingress | length == 0)' >/dev/null
wait_for_resource_quota_observation "$PROJECT_NAMESPACE"

echo "==> verifying scoped portal status RBAC and legacy Environment upgrade fencing"
CONTROL_PLANE_SA="$INSTALLATION_NAME-control-plane"
CONTROL_PLANE_CAN_UPDATE_STATUS=$(kubectl auth can-i update environments.swe.dev --subresource=status \
	--namespace "$PROJECT_NAMESPACE" --as="system:serviceaccount:${SYSTEM_NAMESPACE}:${CONTROL_PLANE_SA}")
if [[ "$CONTROL_PLANE_CAN_UPDATE_STATUS" != "yes" ]]; then
	echo "FAIL: portal-enabled control-plane ServiceAccount cannot update gateway-owned Environment portal status"
	exit 1
fi
STATUS_POLICY_PROBE="status-policy-probe-$RANDOM"
cat <<EOF | kubectl -n "$PROJECT_NAMESPACE" create -f - >/dev/null
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: ${STATUS_POLICY_PROBE}
spec:
  templateRef: small
  lifecycle:
    hold:
      enabled: true
      revision: 1
EOF
CONTROL_PLANE_SA_TOKEN=$(kubectl -n "$SYSTEM_NAMESPACE" create token "$CONTROL_PLANE_SA" --duration=10m)
KUBE_API=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.server}')
STATUS_URL="$KUBE_API/apis/swe.dev/v1alpha1/namespaces/${PROJECT_NAMESPACE}/environments/${STATUS_POLICY_PROBE}/status?dryRun=All"
for _ in {1..5}; do
	kubectl -n "$PROJECT_NAMESPACE" get environment "$STATUS_POLICY_PROBE" -o json |
		jq '.status.phase="Failed"' >/tmp/swe-e2e-status-policy-probe.json
	STATUS_PATCH_CODE=$(curl --silent --output /tmp/swe-e2e-status-policy.out --write-out '%{http_code}' \
		--cacert <(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 --decode) \
		-H "Authorization: Bearer ${CONTROL_PLANE_SA_TOKEN}" -H 'Content-Type: application/json' \
		-X PUT --data-binary @/tmp/swe-e2e-status-policy-probe.json "$STATUS_URL")
	[[ "$STATUS_PATCH_CODE" == "409" ]] || break
done
unset CONTROL_PLANE_SA_TOKEN
if [[ "$STATUS_PATCH_CODE" != "422" ]] || ! grep -q 'portal gateway may not change status.phase' /tmp/swe-e2e-status-policy.out; then
	echo "FAIL: control-plane bearer-token non-portal status update returned $STATUS_PATCH_CODE without the field fence"
	cat /tmp/swe-e2e-status-policy.out
	exit 1
fi
kubectl -n "$PROJECT_NAMESPACE" delete environment "$STATUS_POLICY_PROBE" --wait=false >/dev/null

# Install the exact pre-#164 storage schema while this namespace is deliberately
# outside every controller watch. This permits realistic persisted legacy objects
# without allowing the new controller to partially migrate them during setup.
PRE_164_SHA=fe6cd74815f52155aed51f31b081302e989db0b8
git fetch --depth=1 origin "$PRE_164_SHA"
git show "$PRE_164_SHA:config/crd/bases/swe.dev_environments.yaml" | kubectl apply --server-side --force-conflicts -f -
LEGACY_CRD_ACTIVE="true"
LEGACY_ACTIVE=legacy-upgrade-active
LEGACY_HELD=legacy-upgrade-held
LEGACY_EMPTY=legacy-explicit-empty
LEGACY_ENV_NAMES="$LEGACY_ACTIVE $LEGACY_HELD $LEGACY_EMPTY"
LEGACY_DISK_SIZE=$(kubectl -n "$PROJECT_NAMESPACE" get environmenttemplate small -o jsonpath='{.spec.diskSize}')
cat <<EOF | kubectl -n "$PROJECT_NAMESPACE" create -f - >/dev/null
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: $LEGACY_ACTIVE
spec:
  templateRef: small
  projectRef: $PROJECT_NAME
---
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: $LEGACY_HELD
spec:
  templateRef: small
  projectRef: $PROJECT_NAME
  paused: true
---
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: $LEGACY_EMPTY
  annotations:
    swe.dev/e2e-persisted-empty: original
spec:
  templateRef: small
  projectRef: ""
EOF

for legacy_env in "$LEGACY_ACTIVE" "$LEGACY_HELD"; do
	legacy_uid=$(kubectl -n "$PROJECT_NAMESPACE" get environment "$legacy_env" -o jsonpath='{.metadata.uid}')
	cat <<EOF | kubectl -n "$PROJECT_NAMESPACE" create -f - >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: env-$legacy_env
  ownerReferences:
  - apiVersion: swe.dev/v1alpha1
    kind: Environment
    name: $legacy_env
    uid: $legacy_uid
    controller: true
    blockOwnerDeletion: true
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: $LEGACY_DISK_SIZE
---
apiVersion: v1
kind: Secret
metadata:
  name: env-$legacy_env-sandboxd
  finalizers: [swe.dev/e2e-observe-deletion]
  ownerReferences:
  - apiVersion: swe.dev/v1alpha1
    kind: Environment
    name: $legacy_env
    uid: $legacy_uid
    controller: true
    blockOwnerDeletion: true
stringData:
  legacy: credential
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: env-$legacy_env-sandboxd
  ownerReferences:
  - apiVersion: swe.dev/v1alpha1
    kind: Environment
    name: $legacy_env
    uid: $legacy_uid
    controller: true
    blockOwnerDeletion: true
spec:
  podSelector:
    matchLabels:
      swe.dev/environment: $legacy_env
  policyTypes: [Ingress]
---
apiVersion: v1
kind: Pod
metadata:
  name: env-$legacy_env
  labels:
    swe.dev/environment: $legacy_env
  ownerReferences:
  - apiVersion: swe.dev/v1alpha1
    kind: Environment
    name: $legacy_env
    uid: $legacy_uid
    controller: true
    blockOwnerDeletion: true
spec:
  containers:
  - name: environment
    image: $E2E_ENV_IMAGE
    command: [sh, -c, 'git -C /workspace init -q; git -C /workspace config --local swe.setup-complete true; printf "%s\\n" "$legacy_env" > /workspace/pre-164-marker; exec sleep 3600']
    resources:
      requests: {cpu: 100m, memory: 128Mi}
    volumeMounts:
    - {name: workspace, mountPath: /workspace}
  volumes:
  - name: workspace
    persistentVolumeClaim: {claimName: env-$legacy_env}
EOF
	kubectl -n "$PROJECT_NAMESPACE" annotate environment "$legacy_env" \
		swe.dev/e2e-legacy-pvc-uid="$(kubectl -n "$PROJECT_NAMESPACE" get pvc "env-$legacy_env" -o jsonpath='{.metadata.uid}')" \
		swe.dev/e2e-legacy-policy-uid="$(kubectl -n "$PROJECT_NAMESPACE" get networkpolicy "env-$legacy_env-sandboxd" -o jsonpath='{.metadata.uid}')" >/dev/null
	kubectl -n "$PROJECT_NAMESPACE" wait --for=condition=Ready pod/"env-$legacy_env" --timeout=2m
	kubectl -n "$PROJECT_NAMESPACE" patch environment "$legacy_env" --subresource=status --type=merge \
		-p "{\"status\":{\"phase\":\"Ready\",\"executionGeneration\":1,\"podName\":\"env-${legacy_env}\"}}" >/dev/null
done

kubectl apply --server-side --force-conflicts -f charts/swe-platform/crds
LEGACY_CRD_ACTIVE="false"
# Existing explicit-empty values are grandfathered for metadata and status
# writes, but admission must reject creation of another such object.
kubectl -n "$PROJECT_NAMESPACE" annotate environment "$LEGACY_EMPTY" swe.dev/e2e-persisted-empty=updated --overwrite >/dev/null
kubectl -n "$PROJECT_NAMESPACE" patch environment "$LEGACY_EMPTY" --subresource=status --type=merge \
	-p '{"status":{"phase":"Paused"}}' >/dev/null
kubectl -n "$PROJECT_NAMESPACE" get environment "$LEGACY_EMPTY" -o json | jq -e \
	'.spec.projectRef == "" and .metadata.annotations["swe.dev/e2e-persisted-empty"] == "updated" and .status.phase == "Paused"' >/dev/null
kubectl -n "$PROJECT_NAMESPACE" delete environment "$LEGACY_EMPTY" --wait=true >/dev/null
LEGACY_ENV_NAMES="$LEGACY_ACTIVE $LEGACY_HELD"
if cat <<'EOF' | kubectl -n "$PROJECT_NAMESPACE" create -f - >/dev/null 2>&1; then
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: rejected-explicit-empty
spec:
  templateRef: small
  projectRef: ""
EOF
	echo "FAIL: current CRD admitted a new Environment with explicit empty projectRef"
	kubectl -n "$PROJECT_NAMESPACE" delete environment rejected-explicit-empty --wait=false >/dev/null 2>&1 || true
	exit 1
fi

echo "==> verifying fail-closed namespace adoption and ownership"
ADOPT_NAMESPACE=swe-e2e-adopt
kubectl create namespace "$ADOPT_NAMESPACE"
if bin/swe --namespace "$ADOPT_NAMESPACE" project onboard "$PROJECT_NAME" --system-namespace "$SYSTEM_NAMESPACE" --installation "$INSTALLATION_NAME" --repository git://unused --default-template small --template small "${QUOTA_ARGS[@]}" >/dev/null 2>&1; then
	echo "FAIL: existing unclaimed Namespace was onboarded without --adopt"
	exit 1
fi
bin/swe --namespace "$ADOPT_NAMESPACE" project onboard "$PROJECT_NAME" --adopt --system-namespace "$SYSTEM_NAMESPACE" --installation "$INSTALLATION_NAME" --repository git://unused --default-template small --template small "${QUOTA_ARGS[@]}"
if bin/swe --namespace "$ADOPT_NAMESPACE" project onboard ownership-collision --adopt --system-namespace "$SYSTEM_NAMESPACE" --installation "$INSTALLATION_NAME" --repository git://unused --default-template small --template small "${QUOTA_ARGS[@]}" >/dev/null 2>&1; then
	echo "FAIL: Namespace accepted a second Project ownership claim"
	exit 1
fi
echo "==> adding the onboarded Project namespace to scoped controllers"
HELM_ARGS+=(--set-string "tenancy.namespaces[0]=$PROJECT_NAMESPACE")
if ! helm "${HELM_ARGS[@]}"; then
	echo "FAIL: scoped-controller Helm upgrade failed; collecting operator rollout diagnostics" >&2
	kubectl -n "$SYSTEM_NAMESPACE" get deployment "$INSTALLATION_NAME" -o json | jq '{metadata: {name: .metadata.name, generation: .metadata.generation}, spec: {replicas: .spec.replicas, strategy: .spec.strategy}, status: .status}' >&2 || true
	kubectl -n "$SYSTEM_NAMESPACE" get replicasets \
		-l 'app.kubernetes.io/component=operator' -o wide >&2 || true
	OPERATOR_PODS=$(kubectl -n "$SYSTEM_NAMESPACE" get pods \
		-l 'app.kubernetes.io/component=operator' -o name 2>/dev/null || true)
	kubectl -n "$SYSTEM_NAMESPACE" get pods \
		-l 'app.kubernetes.io/component=operator' -o json | jq '{items: [.items[] | {metadata: {name: .metadata.name, uid: .metadata.uid, deletionTimestamp: .metadata.deletionTimestamp, finalizers: .metadata.finalizers}, spec: {terminationGracePeriodSeconds: .spec.terminationGracePeriodSeconds}, status: {phase: .status.phase, reason: .status.reason, message: .status.message, conditions: .status.conditions, containerStatuses: .status.containerStatuses}}]}' >&2 || true
	kubectl -n "$SYSTEM_NAMESPACE" describe deployment "$INSTALLATION_NAME" >&2 || true
	for pod in $OPERATOR_PODS; do
		pod=${pod#pod/}
		kubectl -n "$SYSTEM_NAMESPACE" describe pod "$pod" >&2 || true
		kubectl -n "$SYSTEM_NAMESPACE" logs "$pod" --container operator --tail=200 >&2 || true
		kubectl -n "$SYSTEM_NAMESPACE" logs "$pod" --container operator --previous --tail=200 >&2 || true
		kubectl -n "$SYSTEM_NAMESPACE" get events --field-selector "involvedObject.name=$pod" --sort-by=.lastTimestamp >&2 || true
	done
	exit 1
fi
kubectl -n "$SYSTEM_NAMESPACE" rollout status deployment/"$INSTALLATION_NAME" --timeout=2m
kubectl -n "$SYSTEM_NAMESPACE" rollout status deployment/"$INSTALLATION_NAME-control-plane" --timeout=2m
kubectl config set-context --current --namespace="$PROJECT_NAMESPACE" >/dev/null

echo "==> verifying migration and enforcement of upgraded flat Pod recovery state"
for _ in $(seq 1 60); do
	RECOVERY_FIXTURES=$(kubectl get environments legacy-recovery-existing legacy-recovery-missing legacy-recovery-exhausted -o json)
	if jq -e '
		(.items[] | select(.metadata.name=="legacy-recovery-existing") | .status.conditions[]? | select(.type=="Ready" and .reason=="PodRecoveryPending")) and
		(.items[] | select(.metadata.name=="legacy-recovery-missing") | .status.conditions[]? | select(.type=="Ready" and .reason=="PodRecoveryPending")) and
		(.items[] | select(.metadata.name=="legacy-recovery-exhausted") | .status.conditions[]? | select(.type=="Ready" and .reason=="PodRecoveryExhausted"))' <<<"$RECOVERY_FIXTURES" >/dev/null; then
		break
	fi
	sleep 1
done
[[ "$(kubectl get pod env-legacy-recovery-existing -o jsonpath='{.metadata.uid}')" == "$LEGACY_POD_UID" ]]
if kubectl get pod env-legacy-recovery-missing >/dev/null 2>&1 || kubectl get pod env-legacy-recovery-exhausted >/dev/null 2>&1; then
	echo "FAIL: controller replaced a Pod blocked by upgraded legacy recovery state" >&2
	exit 1
fi
jq -e --arg deadline "$LEGACY_DEADLINE" '
	(.items[] | select(.metadata.name=="legacy-recovery-existing") | .status |
		(.recovery | .attempts==1 and .executionGeneration==1 and .nextAttemptAt==$deadline) and
		(has("podRecoveryAttempts")|not) and (has("podRecoveryExhausted")|not) and (has("podRecoveryUID")|not) and (has("podRecoveryNextAttemptAt")|not) and
		(.conditions[] | select(.type=="Ready") | .status=="False" and .reason=="PodRecoveryPending")) and
	(.items[] | select(.metadata.name=="legacy-recovery-missing") | .status |
		(.recovery | .attempts==2 and (.executionGeneration // 0)==0 and .nextAttemptAt==$deadline) and
		(has("podRecoveryAttempts")|not) and (has("podRecoveryExhausted")|not) and (has("podRecoveryUID")|not) and (has("podRecoveryNextAttemptAt")|not) and
		(.conditions[] | select(.type=="Ready") | .status=="False" and .reason=="PodRecoveryPending")) and
	(.items[] | select(.metadata.name=="legacy-recovery-exhausted") | .status |
		(.recovery | .attempts==3 and .exhausted==true and (.executionGeneration // 0)==0) and
		(has("podRecoveryAttempts")|not) and (has("podRecoveryExhausted")|not) and (has("podRecoveryUID")|not) and (has("podRecoveryNextAttemptAt")|not) and
		(.conditions[] | select(.type=="Ready") | .status=="False" and .reason=="PodRecoveryExhausted"))' <<<"$RECOVERY_FIXTURES" >/dev/null
kubectl delete environments legacy-recovery-existing legacy-recovery-missing legacy-recovery-exhausted --wait=false >/dev/null
kubectl delete pod env-legacy-recovery-existing --ignore-not-found --wait=false >/dev/null

echo "==> verifying legacy Environments migrate through teardown, snapshot, and resume"
for legacy_env in "$LEGACY_ACTIVE" "$LEGACY_HELD"; do
	for _ in $(seq 1 120); do
		if ! kubectl get pod "env-$legacy_env" >/dev/null 2>&1; then
			break
		fi
		sleep 1
	done
	if kubectl get pod "env-$legacy_env" >/dev/null 2>&1; then
		echo "FAIL: legacy $legacy_env did not remove its Pod"
		exit 1
	fi
	for _ in $(seq 1 120); do
		if [[ -n "$(kubectl get secret "env-$legacy_env-sandboxd" -o jsonpath='{.metadata.deletionTimestamp}')" ]]; then
			break
		fi
		sleep 1
	done
	if [[ -z "$(kubectl get secret "env-$legacy_env-sandboxd" -o jsonpath='{.metadata.deletionTimestamp}')" ]] || \
		[[ "$(kubectl get environment "$legacy_env" -o jsonpath='{.status.provisioning}')" != "" ]]; then
		echo "FAIL: legacy $legacy_env did not fence Pod before requesting credential deletion and snapshot"
		exit 1
	fi
	kubectl patch secret "env-$legacy_env-sandboxd" --type=json \
		-p '[{"op":"remove","path":"/metadata/finalizers"}]' >/dev/null
	for _ in $(seq 1 120); do
		if ! kubectl get secret "env-$legacy_env-sandboxd" >/dev/null 2>&1; then
			break
		fi
		sleep 1
	done
	if kubectl get secret "env-$legacy_env-sandboxd" >/dev/null 2>&1; then
		echo "FAIL: legacy $legacy_env did not revoke credentials"
		exit 1
	fi
	if ! kubectl get pvc "env-$legacy_env" >/dev/null 2>&1; then
		echo "FAIL: legacy $legacy_env migration removed its workspace PVC"
		exit 1
	fi
	if [[ "$(kubectl get pvc "env-$legacy_env" -o jsonpath='{.metadata.uid}')" != \
		"$(kubectl get environment "$legacy_env" -o jsonpath='{.metadata.annotations.swe\.dev/e2e-legacy-pvc-uid}')" ]] || \
		[[ "$(kubectl get networkpolicy "env-$legacy_env-sandboxd" -o jsonpath='{.metadata.uid}')" != \
		"$(kubectl get environment "$legacy_env" -o jsonpath='{.metadata.annotations.swe\.dev/e2e-legacy-policy-uid}')" ]]; then
		echo "FAIL: legacy $legacy_env migration replaced its retained PVC or NetworkPolicy"
		exit 1
	fi
	for _ in $(seq 1 120); do
		if kubectl get environment "$legacy_env" -o json | jq -e \
			'.status.provisioning.templateVerified == true and .status.provisioning.projectVerified == true' >/dev/null; then
			break
		fi
		sleep 1
	done
	legacy_pvc_uid=$(kubectl get environment "$legacy_env" -o jsonpath='{.metadata.annotations.swe\.dev/e2e-legacy-pvc-uid}')
	kubectl get environment "$legacy_env" -o json | jq -e --arg pvc_uid "$legacy_pvc_uid" \
		'.status.provisioning.templateVerified == true and .status.provisioning.projectVerified == true and .status.provisioning.legacyWorkspacePVCUID == $pvc_uid' >/dev/null || {
		echo "FAIL: legacy $legacy_env did not receive a verified provisioning snapshot"
		kubectl get environment "$legacy_env" -o yaml >&2
		exit 1
	}
done
kubectl wait --for=jsonpath='{.status.phase}'=Ready environment/"$LEGACY_ACTIVE" --timeout=2m
kubectl wait --for=condition=Ready pod/"env-$LEGACY_ACTIVE" --timeout=2m
if [[ "$(kubectl get pod "env-$LEGACY_ACTIVE" -o jsonpath='{.metadata.creationTimestamp}')" == "" ]] || \
	! kubectl exec "env-$LEGACY_ACTIVE" -- grep -qx "$LEGACY_ACTIVE" /workspace/pre-164-marker; then
	echo "FAIL: active legacy Environment did not resume on its durable workspace"
	exit 1
fi
if [[ "$(kubectl get environment "$LEGACY_HELD" -o jsonpath='{.status.phase}')" != "Paused" ]] || \
	kubectl get pod "env-$LEGACY_HELD" >/dev/null 2>&1; then
	echo "FAIL: held legacy Environment did not remain Paused and podless after migration"
	exit 1
fi
bin/swe --namespace "$PROJECT_NAMESPACE" environment release "$LEGACY_HELD"
kubectl wait --for=jsonpath='{.status.phase}'=Ready environment/"$LEGACY_HELD" --timeout=2m
kubectl wait --for=condition=Ready pod/"env-$LEGACY_HELD" --timeout=2m
if ! kubectl exec "env-$LEGACY_HELD" -- grep -qx "$LEGACY_HELD" /workspace/pre-164-marker; then
	echo "FAIL: released legacy Environment did not retain its durable workspace marker"
	exit 1
fi
kubectl delete environment "$LEGACY_ACTIVE" "$LEGACY_HELD" --wait=true >/dev/null
LEGACY_ENV_NAMES=""

echo "==> waiting for local warm environment"
if ! kubectl wait --for=jsonpath='{.status.warmPoolReady}'=1 environmenttemplate/small --timeout=2m; then
	echo "FAIL: local warm environment did not become ready" >&2
	kubectl get environmenttemplate small -o yaml >&2 || true
	kubectl get environments,pods -o wide >&2 || true
	kubectl get events --sort-by=.lastTimestamp >&2 || true
	kubectl -n "$SYSTEM_NAMESPACE" logs deployment/"$INSTALLATION_NAME" --tail=200 >&2 || true
	exit 1
fi
WARM_ENV_NAME=$(kubectl get environments -l swe.dev/warm-pool=small -o jsonpath='{.items[0].metadata.name}')
if [[ -z "$WARM_ENV_NAME" ]]; then
	echo "FAIL: warm pool did not create an environment"
	exit 1
fi
WARM_POD_UID=$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.metadata.uid}')
if [[ "$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.spec.serviceAccountName}')" != "swe-environment" || \
	"$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.spec.automountServiceAccountToken}')" != "false" ]]; then
	echo "FAIL: Environment pod did not use the tokenless onboarding ServiceAccount"
	exit 1
fi
if [[ -n "${E2E_RUNTIME_CLASS:-}" ]]; then
	WARM_RUNTIME_CLASS=$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.spec.runtimeClassName}')
	if [[ "$WARM_RUNTIME_CLASS" != "$E2E_RUNTIME_CLASS" ]]; then
		echo "FAIL: warm Environment uses RuntimeClass '$WARM_RUNTIME_CLASS', expected '$E2E_RUNTIME_CLASS'"
		exit 1
	fi
	WARM_RUNTIME_CLASS_UID=$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.metadata.annotations.swe\.dev/runtime-class-uid}')
	EXPECTED_RUNTIME_CLASS_UID=$(kubectl get runtimeclass "$E2E_RUNTIME_CLASS" -o jsonpath='{.metadata.uid}')
	if [[ "$WARM_RUNTIME_CLASS_UID" != "$EXPECTED_RUNTIME_CLASS_UID" ]]; then
		echo "FAIL: warm Environment pins RuntimeClass UID '$WARM_RUNTIME_CLASS_UID', expected '$EXPECTED_RUNTIME_CLASS_UID'"
		exit 1
	fi
fi
if [[ "${E2E_USE_EXISTING_CLUSTER:-false}" == "true" ]]; then
	WARM_PVC_NAME=$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.spec.volumes[?(@.name=="workspace")].persistentVolumeClaim.claimName}')
	WARM_STORAGE_CLASS=$(kubectl get pvc "$WARM_PVC_NAME" -o jsonpath='{.spec.storageClassName}')
	if [[ "$WARM_STORAGE_CLASS" != "csi-hostpath-sc" ]]; then
		echo "FAIL: warm Environment workspace uses StorageClass '$WARM_STORAGE_CLASS', expected 'csi-hostpath-sc'"
		exit 1
	fi
fi

echo "==> synchronizing catalog revision drift in place"
LOCAL_TEMPLATE_UID=$(kubectl get environmenttemplate small -o jsonpath='{.metadata.uid}')
WARM_ENV_UID=$(kubectl get environment "$WARM_ENV_NAME" -o jsonpath='{.metadata.uid}')
kubectl -n "$SYSTEM_NAMESPACE" patch environmenttemplate "$CATALOG_TEMPLATE" --type=merge \
	-p '{"metadata":{"annotations":{"swe.dev/catalog-revision":"e2e-revision-2"}},"spec":{"idleTimeout":"14m"}}' >/dev/null
bin/swe --namespace "$PROJECT_NAMESPACE" "${ONBOARD_ARGS[@]:2}"
if [[ "$(kubectl get environmenttemplate small -o jsonpath='{.metadata.uid}')" != "$LOCAL_TEMPLATE_UID" || \
	"$(kubectl get environmenttemplate small -o jsonpath='{.metadata.annotations.swe\.dev/catalog-revision}')" != "e2e-revision-2" || \
	"$(kubectl get environmenttemplate small -o jsonpath='{.spec.idleTimeout}')" != "14m0s" || \
	"$(kubectl get environment "$WARM_ENV_NAME" -o jsonpath='{.metadata.uid}')" != "$WARM_ENV_UID" ]]; then
	echo "FAIL: explicit catalog sync did not preserve local Template/warm-pool identity while applying drift"
	exit 1
fi
kubectl -n "$SYSTEM_NAMESPACE" delete environmenttemplate "$CATALOG_TEMPLATE" --wait=true >/dev/null
if [[ "$(kubectl get environmenttemplate small -o jsonpath='{.metadata.uid}')" != "$LOCAL_TEMPLATE_UID" ]]; then
	echo "FAIL: deleting a catalog source implicitly removed its managed Project copy"
	exit 1
fi

echo "==> verifying exact-UID two-release isolation and retained offboarding"
PEER_SYSTEM_NAMESPACE=swe-platform-peer-system
PEER_PROJECT_NAMESPACE=swe-e2e-peer-project
PEER_PROJECT_NAME=e2e
PEER_INSTALLATION_NAME=peer-swe-platform
kubectl create namespace "$PEER_SYSTEM_NAMESPACE"
PEER_HELM_ARGS=(upgrade --install peer charts/swe-platform --namespace "$PEER_SYSTEM_NAMESPACE"
	--values charts/swe-platform/values-kind.yaml --set tenancy.mode=scoped --set-json 'tenancy.namespaces=[]'
	--set controlPlane.enabled=false --set-string "environmentTemplates[0].spec.image=$E2E_ENV_IMAGE"
	--wait --timeout 2m)
if [[ -n "${E2E_RUNTIME_CLASS:-}" ]]; then
	PEER_HELM_ARGS+=(--set-string "environmentTemplates[0].spec.runtimeClass=$E2E_RUNTIME_CLASS")
fi
helm "${PEER_HELM_ARGS[@]}"
bin/swe --namespace "$PEER_PROJECT_NAMESPACE" project onboard "$PEER_PROJECT_NAME" \
	--system-namespace "$PEER_SYSTEM_NAMESPACE" --installation "$PEER_INSTALLATION_NAME" \
	--repository git://unused --default-template small --template small "${QUOTA_ARGS[@]}"
wait_for_resource_quota_observation "$PEER_PROJECT_NAMESPACE"
PEER_HELM_ARGS+=(--set-string "tenancy.namespaces[0]=$PEER_PROJECT_NAMESPACE")
helm "${PEER_HELM_ARGS[@]}"
kubectl -n "$PEER_SYSTEM_NAMESPACE" rollout status deployment/"$PEER_INSTALLATION_NAME" --timeout=2m
kubectl -n "$PEER_PROJECT_NAMESPACE" wait --for=jsonpath='{.status.warmPoolReady}'=1 environmenttemplate/small --timeout=2m
PEER_INSTALLATION_UID=$(kubectl -n "$PEER_SYSTEM_NAMESPACE" get installation "$PEER_INSTALLATION_NAME" -o jsonpath='{.metadata.uid}')
PEER_PROJECT_UID=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get project "$PEER_PROJECT_NAME" -o jsonpath='{.metadata.uid}')
PEER_CLAIM=$(kubectl get namespace "$PEER_PROJECT_NAMESPACE" -o json)
if [[ "$PEER_INSTALLATION_UID" == "$INSTALLATION_UID" ]]; then
	echo "FAIL: independent releases did not receive independent Installation identities"
	exit 1
fi
jq -e --arg iu "$PEER_INSTALLATION_UID" --arg pu "$PEER_PROJECT_UID" '.metadata.annotations["swe.dev/installation-uid"]==$iu and .metadata.annotations["swe.dev/project-uid"]==$pu' <<<"$PEER_CLAIM" >/dev/null
MAIN_OPERATOR_ROLE=$(kubectl -n "$PROJECT_NAMESPACE" get rolebinding swe-platform-operator -o jsonpath='{.roleRef.name}')
PEER_OPERATOR_ROLE=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get rolebinding swe-platform-operator -o jsonpath='{.roleRef.name}')
if [[ "$MAIN_OPERATOR_ROLE" == "$PEER_OPERATOR_ROLE" || \
	"$(kubectl -n "$PEER_PROJECT_NAMESPACE" get rolebinding swe-platform-operator -o jsonpath='{.subjects[0].namespace}')" != "$PEER_SYSTEM_NAMESPACE" ]]; then
	echo "FAIL: independent releases share workload authority"
	exit 1
fi
printf 'peer-e2e-retained-key' | bin/swe --namespace "$PEER_PROJECT_NAMESPACE" credentials create retained-peer --agent claude-code --api-key-stdin
PEER_PROFILE_UID=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get agentcredentialprofile retained-peer -o jsonpath='{.metadata.uid}')
PEER_SECRET_NAME=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get secrets -o json | jq -r --arg uid "$PEER_PROFILE_UID" '.items[] | select(any(.metadata.ownerReferences[]?; .uid == $uid)) | .metadata.name' | head -1)
if [[ -z "$PEER_SECRET_NAME" ]]; then
	echo "FAIL: retained credential profile has no exact UID-owned Secret"
	exit 1
fi
PEER_TEMPLATE_UID=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get environmenttemplate small -o jsonpath='{.metadata.uid}')
PEER_ENVIRONMENT=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get environments -l swe.dev/warm-pool=small -o jsonpath='{.items[0].metadata.name}')
PEER_POD=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get environment "$PEER_ENVIRONMENT" -o jsonpath='{.status.podName}')
PEER_PVC=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get pod "$PEER_POD" -o jsonpath='{.spec.volumes[?(@.name=="workspace")].persistentVolumeClaim.claimName}')
PEER_PVC_UID=$(kubectl -n "$PEER_PROJECT_NAMESPACE" get pvc "$PEER_PVC" -o jsonpath='{.metadata.uid}')
bin/swe --namespace "$PEER_PROJECT_NAMESPACE" project offboard "$PEER_PROJECT_NAME" \
	--system-namespace "$PEER_SYSTEM_NAMESPACE" --installation "$PEER_INSTALLATION_NAME" --timeout 3m
if [[ "$(kubectl get namespace "$PEER_PROJECT_NAMESPACE" -o jsonpath='{.metadata.annotations.swe\.dev/project-namespace-lifecycle}')" != "fenced" || \
	"$(kubectl -n "$PEER_PROJECT_NAMESPACE" get environmenttemplate small -o jsonpath='{.metadata.uid}')" != "$PEER_TEMPLATE_UID" || \
	"$(kubectl -n "$PEER_PROJECT_NAMESPACE" get pvc "$PEER_PVC" -o jsonpath='{.metadata.uid}')" != "$PEER_PVC_UID" || \
	"$(kubectl -n "$PEER_PROJECT_NAMESPACE" get secret "$PEER_SECRET_NAME" -o jsonpath='{.metadata.ownerReferences[0].uid}')" != "$PEER_PROFILE_UID" ]]; then
	echo "FAIL: retained offboarding removed or replaced claimed resources"
	exit 1
fi
if [[ -n "$(kubectl -n "$PEER_PROJECT_NAMESPACE" get environment "$PEER_ENVIRONMENT" -o jsonpath='{.status.podName}')" ]] || \
	kubectl -n "$PEER_PROJECT_NAMESPACE" get pod "$PEER_POD" >/dev/null 2>&1; then
	echo "FAIL: retained offboarding left a warm Environment pod running"
	exit 1
fi
helm upgrade peer charts/swe-platform --namespace "$PEER_SYSTEM_NAMESPACE" \
	--values charts/swe-platform/values-kind.yaml --set tenancy.mode=scoped --set-json 'tenancy.namespaces=[]' \
	--set controlPlane.enabled=false --set-string "environmentTemplates[0].spec.image=$E2E_ENV_IMAGE" --wait --timeout 2m
kubectl -n "$PEER_SYSTEM_NAMESPACE" rollout status deployment/"$PEER_INSTALLATION_NAME" --timeout=2m

echo "==> creating project configuration"
PROJECT_REPO="$(mktemp -d /tmp/swe-e2e-project-XXXXXX)"
PROJECT_WORKTREE="$(mktemp -d /tmp/swe-e2e-worktree-XXXXXX)"
git -C "$PROJECT_WORKTREE" init -b main >/dev/null
git -C "$PROJECT_WORKTREE" config user.name "swe e2e"
git -C "$PROJECT_WORKTREE" config user.email "swe-e2e@example.invalid"
mkdir -p "$PROJECT_WORKTREE/.agents" "$PROJECT_WORKTREE/.swe"
cat > "$PROJECT_WORKTREE/.agents/setup" <<'EOF'
if [ -n "${ANTHROPIC_API_KEY+x}" ] || [ -n "${AMP_API_KEY+x}" ] || [ -n "${PORT+x}" ] || [ -n "${PUBLIC_URL+x}" ]; then exit 43; fi
printf '%s\n' credential-absent >> setup-result
EOF
cat > "$PROJECT_WORKTREE/.agents/resume" <<'EOF'
if [ -n "${ANTHROPIC_API_KEY+x}" ] || [ -n "${AMP_API_KEY+x}" ] || [ -n "${PORT+x}" ] || [ -n "${PUBLIC_URL+x}" ]; then exit 44; fi
printf '%s\n' credential-absent >> resume-result
EOF
cat > "$PROJECT_WORKTREE/.swe/services.yaml" <<'EOF'
version: 1
services:
  repository-web:
    command: ["node", ".swe/service.js", "v1"]
EOF
cat > "$PROJECT_WORKTREE/.swe/service.js" <<'EOF'
const http = require("http");
const crypto = require("crypto");
const marker = process.argv[2];
const boot = `${Date.now()}-${process.pid}-${crypto.randomBytes(8).toString("hex")}`;
const forbidden = [
  "ANTHROPIC_API_KEY", "AMP_API_KEY", "CODEX_API_KEY", "SWE_CONTROL_PLANE_TOKEN",
  "POD_NAME", "POD_UID", "SANDBOXD_TOKEN", "KUBERNETES_TOKEN",
].filter((name) => Object.prototype.hasOwnProperty.call(process.env, name));
const server = http.createServer((request, response) => {
  response.setHeader("Content-Type", "application/json");
  if (request.url === "/crash") {
    response.end(JSON.stringify({marker, boot, crashing: true}));
    response.on("finish", () => process.exit(17));
    return;
  }
  response.end(JSON.stringify({
    marker, boot, port: process.env.PORT, publicURL: process.env.PUBLIC_URL,
    forbidden, authorization: request.headers.authorization || "",
  }));
});
server.listen(Number(process.env.PORT), "127.0.0.1");
EOF
git -C "$PROJECT_WORKTREE" add .agents/setup .agents/resume .swe/services.yaml .swe/service.js
git -C "$PROJECT_WORKTREE" commit -m "Add e2e lifecycle hooks and declared service" >/dev/null
git -C "$PROJECT_WORKTREE" bundle create "$PROJECT_REPO/repo.bundle" main
kubectl create configmap e2e-git-repo --from-file="$PROJECT_REPO/repo.bundle"
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: e2e-git-server
  labels:
    app: e2e-git-server
spec:
  securityContext:
    fsGroup: 10001
  initContainers:
    - name: prepare-repository
      image: $ENV_IMAGE
      command: [/bin/sh, -c]
      args:
        - git clone --bare /seed/repo.bundle /repos/e2e.git && git -C /repos/e2e.git symbolic-ref HEAD refs/heads/main
      resources:
        requests: {cpu: 10m, memory: 32Mi}
      volumeMounts:
        - {name: seed, mountPath: /seed}
        - {name: repositories, mountPath: /repos}
  containers:
    - name: git
      image: $ENV_IMAGE
      command: [git, daemon, --reuseaddr, --base-path=/repos, --export-all, --verbose]
      ports:
        - {name: git, containerPort: 9418}
      resources:
        requests: {cpu: 10m, memory: 32Mi}
      volumeMounts:
        - {name: repositories, mountPath: /repos}
  volumes:
    - name: seed
      configMap: {name: e2e-git-repo}
    - name: repositories
      emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: e2e-git-server
spec:
  selector: {app: e2e-git-server}
  ports:
    - {name: git, port: 9418, targetPort: git}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: e2e-git-server-ingress
spec:
  podSelector:
    matchLabels: {app: e2e-git-server}
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector: {}
      ports:
        - {protocol: TCP, port: 9418}
EOF
kubectl wait --for=condition=Ready pod/e2e-git-server --timeout=1m
rm -rf "$PROJECT_REPO" "$PROJECT_WORKTREE"
PROJECT_REPO=""
PROJECT_WORKTREE=""
echo "==> creating project environment + run intent via swe"
printf '%s' "$E2E_AGENT_API_KEY" | bin/swe --namespace "$PROJECT_NAMESPACE" credentials create e2e-claude --agent claude-code --api-key-stdin
bin/swe --namespace "$PROJECT_NAMESPACE" run "end-to-end smoke test" --project "$PROJECT_NAME" --credential-profile e2e-claude --wait=false
RUN_NAME=$(kubectl get runs -o jsonpath='{.items[0].metadata.name}')
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$RUN_NAME" --timeout=3m
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$RUN_NAME" --timeout=3m
RUN_UID=$(kubectl get run "$RUN_NAME" -o jsonpath='{.metadata.uid}')
PROFILE_UID=$(kubectl get agentcredentialprofile e2e-claude -o jsonpath='{.metadata.uid}')
BOUND_PROFILE_NAME=$(kubectl get run "$RUN_NAME" -o jsonpath='{.status.credentialProfileRef.name}')
BOUND_PROFILE_UID=$(kubectl get run "$RUN_NAME" -o jsonpath='{.status.credentialProfileRef.uid}')
if [[ "$BOUND_PROFILE_NAME" != "e2e-claude" || -z "$PROFILE_UID" || "$BOUND_PROFILE_UID" != "$PROFILE_UID" ]]; then
	echo "FAIL: Run did not retain the exact selected credential profile identity"
	exit 1
fi
RUN_ENV_NAME=$(kubectl get run "$RUN_NAME" -o jsonpath='{.status.environmentRef.name}')
RUN_ENV_OWNERSHIP=$(kubectl get run "$RUN_NAME" -o jsonpath='{.status.environmentRef.ownership}')
RUN_STARTED_AT=$(kubectl get run "$RUN_NAME" -o jsonpath='{.status.startedAt}')
RUN_FINISHED_AT=$(kubectl get run "$RUN_NAME" -o jsonpath='{.status.finishedAt}')
if [[ -z "$RUN_STARTED_AT" || -z "$RUN_FINISHED_AT" ]]; then
	echo "FAIL: Run lifecycle timestamps missing (startedAt=$RUN_STARTED_AT finishedAt=$RUN_FINISHED_AT)"
	exit 1
fi
if [[ "$RUN_ENV_NAME" != "$WARM_ENV_NAME" || "$RUN_ENV_OWNERSHIP" != "Claimed" ]]; then
	echo "FAIL: Run allocated $RUN_ENV_NAME ($RUN_ENV_OWNERSHIP), expected claimed warm environment $WARM_ENV_NAME"
	exit 1
fi
RUN_POD_UID=$(kubectl get pod "env-${RUN_ENV_NAME}" -o jsonpath='{.metadata.uid}')
RUN_POD_PROJECT=$(kubectl get pod "env-${RUN_ENV_NAME}" -o jsonpath='{.metadata.annotations.swe\.dev/project}')
if [[ "$RUN_POD_UID" == "$WARM_POD_UID" || "$RUN_POD_PROJECT" != "e2e" ]]; then
	echo "FAIL: Run reached a terminal state before its claimed warm pod was replaced and configured for the Project"
	exit 1
fi
RUN_POD_NAME=$(kubectl get environment "$RUN_ENV_NAME" -o jsonpath='{.status.podName}')
if ! kubectl exec "$RUN_POD_NAME" -- sh -c \
	'test "$(cat /workspace/setup-result)" = credential-absent && test "$(cat /workspace/agent-credential-marker)" = credential-present && test -z "${ANTHROPIC_API_KEY+x}" && ! tr "\000" "\n" < /proc/1/environ | grep -q "^ANTHROPIC_API_KEY="'; then
	echo "FAIL: API key was not confined to the selected fake Claude process"
	exit 1
fi
check_sandboxd_process "$RUN_POD_NAME" "$RUN_UID" "$E2E_AGENT_API_KEY"
for _ in $(seq 1 60); do
	CLAIM_UID=$(kubectl get environment "$RUN_ENV_NAME" -o jsonpath='{.status.claimedBy.uid}' 2>/dev/null || true)
	if [[ -z "$CLAIM_UID" ]]; then
		break
	fi
	sleep 1
done
if [[ -n "${CLAIM_UID:-}" ]]; then
	echo "FAIL: terminal Run did not release its warm Environment claim"
	exit 1
fi
kubectl wait --for=jsonpath='{.status.phase}'=Ready environment/"$RUN_ENV_NAME" --timeout=3m
ENV_NAME=$RUN_ENV_NAME
ENV_UID=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.metadata.uid}')
for _ in $(seq 1 60); do
	REPLACEMENT_NAME=$(kubectl get environments -l swe.dev/warm-pool=small -o jsonpath='{range .items[*]}{.metadata.name}{end}' 2>/dev/null || true)
	REPLACEMENT_PHASE=$(kubectl get environments -l swe.dev/warm-pool=small -o jsonpath='{range .items[*]}{.status.phase}{end}' 2>/dev/null || true)
	if [[ -n "$REPLACEMENT_NAME" && "$REPLACEMENT_NAME" != "$ENV_NAME" && "$REPLACEMENT_PHASE" == "Ready" ]]; then
		break
	fi
	sleep 1
done
if [[ -z "${REPLACEMENT_NAME:-}" || "$REPLACEMENT_NAME" == "$ENV_NAME" || "${REPLACEMENT_PHASE:-}" != "Ready" ]]; then
	echo "FAIL: warm pool was not replenished after claim"
	exit 1
fi

echo "==> verifying state"
kubectl get environments
kubectl get pods -l app.kubernetes.io/managed-by=swe-platform
STATUS_POD_NAME=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.podName}')
ENV_IMAGE_ID=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.imageID}')
POD_IMAGE_ID=$(kubectl get pod "$STATUS_POD_NAME" -o jsonpath='{.status.containerStatuses[?(@.name=="environment")].imageID}')
if [[ -z "$ENV_IMAGE_ID" || "$ENV_IMAGE_ID" != "$POD_IMAGE_ID" ]]; then
	echo "FAIL: environment image ID '${ENV_IMAGE_ID:-<empty>}' does not match pod image ID '${POD_IMAGE_ID:-<empty>}'"
	exit 1
fi

echo "==> configuring a run-scoped transcript producer"
cat <<EOF | kubectl apply -f -
apiVersion: swe.dev/v1alpha1
kind: Run
metadata:
  name: auth-scope-run-b
spec:
  environmentRef: ${ENV_NAME}
  agent: e2e
  prompt: authorization scope test
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: e2e-transcript-producer
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: e2e-transcript-producer
rules:
  - apiGroups: ["swe.dev"]
    resources: ["runs/transcript"]
    resourceNames: ["${RUN_NAME}"]
    verbs: ["update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: e2e-transcript-producer
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: e2e-transcript-producer
subjects:
  - kind: ServiceAccount
    name: e2e-transcript-producer
    namespace: ${PROJECT_NAMESPACE}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: e2e-transcript-reader
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: e2e-transcript-reader
rules:
  - apiGroups: ["swe.dev"]
    resources: ["runs/transcript"]
    resourceNames: ["${RUN_NAME}"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: e2e-transcript-reader
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: e2e-transcript-reader
subjects:
  - kind: ServiceAccount
    name: e2e-transcript-reader
    namespace: ${PROJECT_NAMESPACE}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: e2e-terminal
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: e2e-terminal
rules:
  - apiGroups: ["swe.dev"]
    resources: ["environments/terminal"]
    resourceNames: ["${ENV_NAME}"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: e2e-terminal
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: e2e-terminal
subjects:
  - kind: ServiceAccount
    name: e2e-terminal
    namespace: ${PROJECT_NAMESPACE}
EOF
PRODUCER_TOKEN=$(kubectl create token e2e-transcript-producer --audience=swe-platform)
READER_TOKEN=$(kubectl create token e2e-transcript-reader --audience=swe-platform)
TERMINAL_TOKEN=$(kubectl create token e2e-terminal --audience=swe-platform)

echo "==> configuring a console API user"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: e2e-console
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: e2e-console
rules:
  - apiGroups: ["swe.dev"]
    resources: ["runs"]
    verbs: ["create", "get", "list", "update", "watch"]
  - apiGroups: ["swe.dev"]
    resources: ["environments"]
    verbs: ["get"]
  - apiGroups: ["swe.dev"]
    resources: ["environments/terminal"]
    verbs: ["get"]
  - apiGroups: ["swe.dev"]
    resources: ["environments/portal"]
    resourceNames: ["${ENV_NAME}"]
    verbs: ["get"]
  - apiGroups: ["swe.dev"]
    resources: ["environmentservices/portal"]
    resourceNames: ["${ENV_NAME}.web", "${ENV_NAME}.manual-api", "${ENV_NAME}.repository-web"]
    verbs: ["get"]
  - apiGroups: ["swe.dev"]
    resources: ["projects"]
    resourceNames: ["${PROJECT_NAME}"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: e2e-console
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: e2e-console
subjects:
  - kind: ServiceAccount
    name: e2e-console
    namespace: ${PROJECT_NAMESPACE}
EOF
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Namespace
metadata:
  name: e2e-console-other
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: e2e-console
  namespace: e2e-console-other
rules:
  - apiGroups: ["swe.dev"]
    resources: ["runs"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: e2e-console
  namespace: e2e-console-other
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: e2e-console
subjects:
  - kind: ServiceAccount
    name: e2e-console
    namespace: ${PROJECT_NAMESPACE}
---
apiVersion: swe.dev/v1alpha1
kind: Run
metadata:
  name: e2e-other-namespace-run
  namespace: e2e-console-other
spec:
  templateRef: unavailable
  agent: e2e
  prompt: namespace navigation acceptance
EOF
CONSOLE_TOKEN=$(kubectl create token e2e-console --audience=swe-platform)

echo "==> verifying live transcript stream through the control plane"
start_control_plane_port_forward
UNLISTED_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${ADOPT_NAMESPACE}/runs")
UNLISTED_ENVIRONMENTS=$(kubectl -n "$ADOPT_NAMESPACE" get environments -o json | jq '.items | length')
if [[ "$UNLISTED_STATUS" != "403" || "$UNLISTED_ENVIRONMENTS" != "0" ]]; then
	echo "FAIL: active but unlisted claimed Namespace status=${UNLISTED_STATUS} environments=${UNLISTED_ENVIRONMENTS}, expected 403/0"
	exit 1
fi
# Direct test-admin cleanup: retention/purge semantics are deliberately not involved.
kubectl delete namespace "$ADOPT_NAMESPACE" --wait=false

WEB_TERMINAL_SOURCE="$(mktemp /tmp/swe-browser-terminal-XXXXXX.go)"
WEB_TERMINAL_CLIENT="${WEB_TERMINAL_SOURCE%.go}"
cat > "$WEB_TERMINAL_SOURCE" <<'EOF'
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	if len(os.Args) != 6 {
		panic("usage: browser-terminal BASE_URL TOKEN PATH COMMAND MARKER")
	}
	base, token, path, command, marker := os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5]
	request, err := http.NewRequest(http.MethodPost, base+"/api/v1/session", nil)
	if err != nil {
		panic(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		panic(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || len(response.Cookies()) == 0 {
		panic(fmt.Sprintf("session exchange returned %s", response.Status))
	}
	parsed, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	parsed.Scheme = "ws"
	parsed.Path = path
	header := http.Header{"Origin": []string{base}}
	for _, cookie := range response.Cookies() {
		header.Add("Cookie", cookie.Name+"="+cookie.Value)
	}
	connection, dialResponse, err := websocket.DefaultDialer.Dial(parsed.String(), header)
	if err != nil {
		if dialResponse != nil {
			panic(fmt.Sprintf("terminal handshake returned %s", dialResponse.Status))
		}
		panic(err)
	}
	defer connection.Close()
	if err := connection.WriteJSON(map[string]any{"type": "open", "cols": 80, "rows": 24}); err != nil {
		panic(err)
	}
	if err := connection.WriteMessage(websocket.BinaryMessage, []byte(command)); err != nil {
		panic(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(30 * time.Second))
	var output strings.Builder
	for !strings.Contains(output.String(), marker) {
		messageType, payload, err := connection.ReadMessage()
		if err != nil {
			panic(err)
		}
		if messageType == websocket.BinaryMessage {
			output.Write(payload)
		}
	}
}
EOF
go build -o "$WEB_TERMINAL_CLIENT" "$WEB_TERMINAL_SOURCE"
rm -f "$WEB_TERMINAL_SOURCE"
WEB_TERMINAL_SOURCE=""

echo "==> verifying terminal console authenticated client path"
SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$CONSOLE_TOKEN" \
	bin/swe tui --check --namespace "$PROJECT_NAMESPACE" > /tmp/swe-platform-tui-check.out
if ! grep -Fq "terminal console API ready for namespace $PROJECT_NAMESPACE" /tmp/swe-platform-tui-check.out || \
	grep -Fq "$CONSOLE_TOKEN" /tmp/swe-platform-tui-check.out; then
	echo "FAIL: swe tui --check did not validate the namespaced Run API safely"
	cat /tmp/swe-platform-tui-check.out
	exit 1
fi

echo "==> verifying embedded operations console through the control-plane Service"
ROOT_STATUS=$(curl --silent --dump-header /tmp/swe-platform-console-root.headers \
	--output /tmp/swe-platform-console-root.html --write-out '%{http_code}' \
	http://127.0.0.1:18080/)
SPA_STATUS=$(curl --silent --dump-header /tmp/swe-platform-console-spa.headers \
	--output /tmp/swe-platform-console-spa.html --write-out '%{http_code}' \
	"http://127.0.0.1:18080/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/overview")
OTHER_NAMESPACE_SPA_STATUS=$(curl --silent --output /tmp/swe-platform-console-other-spa.html \
	--write-out '%{http_code}' http://127.0.0.1:18080/namespaces/e2e-console-other/runs)
ASSET_PATH=$(grep -oE 'src="/assets/[^"]+"' /tmp/swe-platform-console-root.html | head -1 | cut -d'"' -f2 || true)
if [[ "$ROOT_STATUS" != "200" || "$SPA_STATUS" != "200" || "$OTHER_NAMESPACE_SPA_STATUS" != "200" || -z "$ASSET_PATH" ]] || \
	! cmp -s /tmp/swe-platform-console-root.html /tmp/swe-platform-console-spa.html || \
	! cmp -s /tmp/swe-platform-console-root.html /tmp/swe-platform-console-other-spa.html || \
	! tr -d '\r' < /tmp/swe-platform-console-root.headers | grep -Eiq '^Cache-Control: no-store$' || \
	! grep -Eiq '^Content-Security-Policy: ' /tmp/swe-platform-console-root.headers; then
	echo "FAIL: control-plane image did not serve the secured SPA entry point and client-route fallback"
	exit 1
fi
ASSET_STATUS=$(curl --silent --dump-header /tmp/swe-platform-console-asset.headers \
	--output /tmp/swe-platform-console-asset --write-out '%{http_code}' \
	"http://127.0.0.1:18080${ASSET_PATH}")
if [[ "$ASSET_STATUS" != "200" || ! -s /tmp/swe-platform-console-asset ]] || \
	! tr -d '\r' < /tmp/swe-platform-console-asset.headers | grep -Eiq '^Cache-Control: public, max-age=31536000, immutable$'; then
	echo "FAIL: control-plane image did not serve the immutable Vite asset ${ASSET_PATH}"
	exit 1
fi
UNKNOWN_API_STATUS=$(curl --silent --output /tmp/swe-platform-console-unknown-api \
	--write-out '%{http_code}' http://127.0.0.1:18080/api/not-a-console-route)
if [[ "$UNKNOWN_API_STATUS" != "404" ]] || grep -q 'SWE Operations' /tmp/swe-platform-console-unknown-api; then
	echo "FAIL: unknown API route was swallowed by the SPA fallback"
	exit 1
fi

echo "==> verifying browser session and typed resource APIs"
COOKIE_JAR=/tmp/swe-platform-console-cookies
rm -f "$COOKIE_JAR"
SESSION_STATUS=$(curl --silent --output /tmp/swe-platform-session.json --write-out '%{http_code}' \
	--cookie-jar "$COOKIE_JAR" -X POST -H "Authorization: Bearer ${CONSOLE_TOKEN}" \
	http://127.0.0.1:18080/api/v1/session)
if [[ "$SESSION_STATUS" != "200" ]] || ! grep -q '"authenticated":true' /tmp/swe-platform-session.json; then
	echo "FAIL: session exchange returned ${SESSION_STATUS}: $(cat /tmp/swe-platform-session.json)"
	exit 1
fi
if grep -Fq "$CONSOLE_TOKEN" "$COOKIE_JAR"; then
	echo "FAIL: session cookie contains the Kubernetes bearer token"
	exit 1
fi
echo "==> verifying PostgreSQL browser session survives control-plane pod replacement"
replace_control_plane_pod
SESSION_REPLACEMENT_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" http://127.0.0.1:18080/api/v1/session)
if [[ "$SESSION_REPLACEMENT_STATUS" != "200" ]]; then
	echo "FAIL: durable session status after pod replacement was ${SESSION_REPLACEMENT_STATUS}, expected 200"
	exit 1
fi

# Stock kube-apiserver reports every declined non-empty bearer with TokenReview
# status.error, which is intentionally indeterminate here and must retain the
# session. Use the bootstrap credential as stored session material instead: it
# is definitively rejected before TokenReview because bootstrap credentials are
# explicit-bearer-only. Focused tests cover TokenReview authenticated:false.
echo "==> verifying definitive bootstrap-cookie rejection is durably revoked"
kubectl -n "$SYSTEM_NAMESPACE" port-forward service/postgres 15432:5432 >/tmp/swe-platform-postgres-port-forward.log 2>&1 &
POSTGRES_PORT_FORWARD_PID=$!
for _ in $(seq 1 30); do
	if kill -0 "$POSTGRES_PORT_FORWARD_PID" >/dev/null 2>&1 && (echo >/dev/tcp/127.0.0.1/15432) >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
E2E_SESSION_FIXTURE=$(mktemp -d ./e2e-session-fixture-XXXXXX)
cat >"$E2E_SESSION_FIXTURE/main.go" <<'EOF'
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
)

func main() {
	ctx := context.Background()
	db, err := controlplane.NewPostgresDatabase(ctx, os.Getenv("E2E_POSTGRES_URL"))
	if err != nil { panic(err) }
	defer db.Close()
	keyring, err := controlplane.LoadSessionKeyring(os.Getenv("E2E_SESSION_KEYRING_FILE"))
	if err != nil { panic(err) }
	store, err := controlplane.NewPostgresSessionStore(ctx, db, keyring, controlplane.PostgresSessionStoreOptions{})
	if err != nil { panic(err) }
	cookie, err := store.Create(ctx, os.Getenv("E2E_REJECTED_TOKEN"))
	if err != nil { panic(err) }
	fmt.Print(cookie)
}
EOF
REJECTION_SESSION_COUNT_BEFORE=$(browser_session_count)
E2E_SESSION_POSTGRES_URL=$(kubectl -n "$SYSTEM_NAMESPACE" get secret swe-platform-postgres \
	-o jsonpath='{.data.url}' | base64 -d)
E2E_SESSION_POSTGRES_URL=${E2E_SESSION_POSTGRES_URL/@postgres:5432/@127.0.0.1:15432}
REJECTION_COOKIE=$(E2E_POSTGRES_URL="$E2E_SESSION_POSTGRES_URL" \
	E2E_SESSION_KEYRING_FILE="$SESSION_KEYRING_FILE" E2E_REJECTED_TOKEN="$E2E_BOOTSTRAP_TOKEN" \
	go run "$E2E_SESSION_FIXTURE/main.go")
unset E2E_SESSION_POSTGRES_URL
REJECTION_SESSION_COUNT_AFTER=$(browser_session_count)
rm -rf "$E2E_SESSION_FIXTURE"
E2E_SESSION_FIXTURE=""
rm -f "$SESSION_KEYRING_FILE"
SESSION_KEYRING_FILE=""
kill "$POSTGRES_PORT_FORWARD_PID" >/dev/null 2>&1 || true
wait "$POSTGRES_PORT_FORWARD_PID" >/dev/null 2>&1 || true
POSTGRES_PORT_FORWARD_PID=""
if [[ "$REJECTION_SESSION_COUNT_AFTER" != "$((REJECTION_SESSION_COUNT_BEFORE + 1))" ]]; then
	echo "FAIL: invalid-token fixture did not add exactly one durable session"
	exit 1
fi
REJECTION_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Cookie: swe-platform-session=${REJECTION_COOKIE}" http://127.0.0.1:18080/api/v1/session)
REJECTION_SESSION_COUNT_REVOKED=$(browser_session_count)
if [[ "$REJECTION_STATUS" != "401" || "$REJECTION_SESSION_COUNT_REVOKED" != "$REJECTION_SESSION_COUNT_BEFORE" ]]; then
	echo "FAIL: definitive browser-session rejection status=${REJECTION_STATUS}, session counts before=${REJECTION_SESSION_COUNT_BEFORE} after-fixture=${REJECTION_SESSION_COUNT_AFTER} after-rejection=${REJECTION_SESSION_COUNT_REVOKED}; expected 401 and durable deletion"
	exit 1
fi
replace_control_plane_pod
REJECTION_REPLAY_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Cookie: swe-platform-session=${REJECTION_COOKIE}" http://127.0.0.1:18080/api/v1/session)
unset REJECTION_COOKIE
if [[ "$REJECTION_REPLAY_STATUS" != "401" ]]; then
	echo "FAIL: rejected-token session replay after replacement was ${REJECTION_REPLAY_STATUS}, expected 401"
	exit 1
fi
SESSION_GET_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" http://127.0.0.1:18080/api/v1/session)
RUN_LIST_STATUS=$(curl --silent --output /tmp/swe-platform-runs.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" "http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs?limit=200")
OTHER_RUN_LIST_STATUS=$(curl --silent --output /tmp/swe-platform-other-runs.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" 'http://127.0.0.1:18080/api/v1/namespaces/e2e-console-other/runs?limit=200')
ENV_GET_STATUS=$(curl --silent --output /tmp/swe-platform-environment.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" "http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/environments/${ENV_NAME}")
if [[ "$SESSION_GET_STATUS" != "200" || "$RUN_LIST_STATUS" != "200" || "$OTHER_RUN_LIST_STATUS" != "403" || "$ENV_GET_STATUS" != "200" ]]; then
	echo "FAIL: typed read API statuses session=${SESSION_GET_STATUS} runs=${RUN_LIST_STATUS} other-runs=${OTHER_RUN_LIST_STATUS} environment=${ENV_GET_STATUS}"
	exit 1
fi
if grep -Fq '"name":"e2e-other-namespace-run"' /tmp/swe-platform-runs.json || \
	! grep -Fq "\"name\":\"${RUN_NAME}\"" /tmp/swe-platform-runs.json; then
	echo "FAIL: browser-session Run feed crossed the claimed namespace boundary"
	exit 1
fi
if ! grep -Fq '"credentialProfile":"e2e-claude"' /tmp/swe-platform-runs.json || \
	grep -Fq "$PROFILE_UID" /tmp/swe-platform-runs.json || \
	contains_e2e_key /tmp/swe-platform-runs.json || contains_e2e_key /tmp/swe-platform-environment.json; then
	echo "FAIL: typed APIs did not expose only the selected credential profile name"
	exit 1
fi

echo "==> verifying authenticated typed Run watch"
curl --fail --silent --cookie "$COOKIE_JAR" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs?limit=200&view=summary" \
	> /tmp/swe-platform-run-watch-snapshot.json
RUN_WATCH_RV=$(python3 -c 'import json; print(json.load(open("/tmp/swe-platform-run-watch-snapshot.json"))["resourceVersion"])')
DENIED_WATCH_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -H 'Accept: text/event-stream' \
	"http://127.0.0.1:18080/api/v1/namespaces/e2e-console-other/runs?watch=true&view=summary&resourceVersion=${RUN_WATCH_RV}")
if [[ "$DENIED_WATCH_STATUS" != "403" ]]; then
	echo "FAIL: unauthorized namespace Run watch status was ${DENIED_WATCH_STATUS}, expected 403"
	exit 1
fi
curl --silent --no-buffer --max-time 30 --cookie "$COOKIE_JAR" -H 'Accept: text/event-stream' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs?watch=true&view=summary&resourceVersion=${RUN_WATCH_RV}" \
	> /tmp/swe-platform-run-watch.out &
RUN_WATCH_PID=$!
cat <<'EOF' | kubectl apply -f -
apiVersion: swe.dev/v1alpha1
kind: Run
metadata:
  name: e2e-watch-replacement
spec:
  templateRef: unavailable
  agent: e2e
  prompt: public Run watch acceptance
EOF
OLD_WATCH_UID=$(kubectl get run e2e-watch-replacement -o jsonpath='{.metadata.uid}')
kubectl patch run e2e-watch-replacement --subresource=status --type=merge -p '{"status":{"state":"Running"}}'
kubectl patch run e2e-watch-replacement --type=merge -p '{"metadata":{"finalizers":[]}}'
kubectl delete run e2e-watch-replacement --wait=true
cat <<'EOF' | kubectl apply -f -
apiVersion: swe.dev/v1alpha1
kind: Run
metadata:
  name: e2e-watch-replacement
spec:
  templateRef: unavailable
  agent: e2e
  prompt: replacement Run watch acceptance
EOF
NEW_WATCH_UID=$(kubectl get run e2e-watch-replacement -o jsonpath='{.metadata.uid}')
for _ in $(seq 1 30); do
	if grep -Fq '"type":"ADDED"' /tmp/swe-platform-run-watch.out && \
		grep -Fq '"type":"MODIFIED"' /tmp/swe-platform-run-watch.out && \
		grep -Fq '"type":"DELETED"' /tmp/swe-platform-run-watch.out && \
		grep -Fq "\"uid\":\"${OLD_WATCH_UID}\"" /tmp/swe-platform-run-watch.out && \
		grep -Fq "\"uid\":\"${NEW_WATCH_UID}\"" /tmp/swe-platform-run-watch.out; then
		break
	fi
	sleep 1
done
kill "$RUN_WATCH_PID" 2>/dev/null || true
wait "$RUN_WATCH_PID" 2>/dev/null || true
if [[ "$OLD_WATCH_UID" == "$NEW_WATCH_UID" ]] || \
	! grep -Fq '"type":"ADDED"' /tmp/swe-platform-run-watch.out || \
	! grep -Fq '"type":"MODIFIED"' /tmp/swe-platform-run-watch.out || \
	! grep -Fq '"type":"DELETED"' /tmp/swe-platform-run-watch.out || \
	! grep -Fq "\"uid\":\"${OLD_WATCH_UID}\"" /tmp/swe-platform-run-watch.out || \
	! grep -Fq "\"uid\":\"${NEW_WATCH_UID}\"" /tmp/swe-platform-run-watch.out; then
	echo "FAIL: public Run watch did not preserve ADDED/MODIFIED and replacement UID fencing"
	cat /tmp/swe-platform-run-watch.out
	exit 1
fi
API_RUN_BODY='{"name":"e2e-api-run","selector":{"template":"small"},"agent":"e2e","prompt":"resource API acceptance"}'
API_CREATE_STATUS=$(curl --silent --output /tmp/swe-platform-api-run.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -H 'Origin: http://127.0.0.1:18080' -H 'Content-Type: application/json' \
	-d "$API_RUN_BODY" "http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs")
API_RETRY_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -H 'Origin: http://127.0.0.1:18080' -H 'Content-Type: application/json' \
	-d "$API_RUN_BODY" "http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs")
API_RUN_UID=$(python3 -c 'import json; print(json.load(open("/tmp/swe-platform-api-run.json"))["uid"])')
CSRF_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -X POST "http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/e2e-api-run/cancel")
API_CANCEL_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -X POST -H 'Origin: http://127.0.0.1:18080' -H 'Content-Type: application/json' \
	-d "{\"runUID\":\"${API_RUN_UID}\"}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/e2e-api-run/cancel")
if [[ "$API_CREATE_STATUS" != "201" || "$API_RETRY_STATUS" != "200" || "$CSRF_STATUS" != "403" || "$API_CANCEL_STATUS" != "200" ]]; then
	echo "FAIL: typed mutation API statuses create=${API_CREATE_STATUS} retry=${API_RETRY_STATUS} csrf=${CSRF_STATUS} cancel=${API_CANCEL_STATUS}"
	exit 1
fi
LOGOUT_REPLAY_COOKIE_JAR=/tmp/swe-platform-logout-replay-cookies
cp "$COOKIE_JAR" "$LOGOUT_REPLAY_COOKIE_JAR"
SESSION_DELETE_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" --cookie-jar "$COOKIE_JAR" -X DELETE -H 'Origin: http://127.0.0.1:18080' \
	http://127.0.0.1:18080/api/v1/session)
if [[ "$SESSION_DELETE_STATUS" != "204" ]]; then
	echo "FAIL: session delete status was ${SESSION_DELETE_STATUS}, expected 204"
	exit 1
fi
LOGOUT_REPLAY_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$LOGOUT_REPLAY_COOKIE_JAR" http://127.0.0.1:18080/api/v1/session)
if [[ "$LOGOUT_REPLAY_STATUS" != "401" ]]; then
	echo "FAIL: logged-out session replay status was ${LOGOUT_REPLAY_STATUS}, expected 401"
	exit 1
fi
replace_control_plane_pod
LOGOUT_REPLACEMENT_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$LOGOUT_REPLAY_COOKIE_JAR" http://127.0.0.1:18080/api/v1/session)
if [[ "$LOGOUT_REPLACEMENT_STATUS" != "401" ]]; then
	echo "FAIL: logged-out session replay after replacement was ${LOGOUT_REPLACEMENT_STATUS}, expected 401"
	exit 1
fi

ANONYMOUS_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript")
if [[ "$ANONYMOUS_STATUS" != "401" ]]; then
	echo "FAIL: anonymous transcript status was ${ANONYMOUS_STATUS}, expected 401"
	exit 1
fi
curl --fail --silent --no-buffer --max-time 10 \
	-H "Authorization: Bearer ${READER_TOKEN}" \
	-H "SWE-Run-UID: ${RUN_UID}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript" > /tmp/swe-platform-transcript.out &
STREAM_PID=$!
SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$READER_TOKEN" \
	timeout 10 bin/swe --namespace "$PROJECT_NAMESPACE" logs --run "$RUN_NAME" --run-uid "$RUN_UID" > /tmp/swe-platform-cli-transcript.out &
CLI_STREAM_PID=$!
sleep 1
APPEND_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${PRODUCER_TOKEN}" \
	-H 'Content-Type: application/json' \
	-H "SWE-Run-UID: ${RUN_UID}" \
	-d '{"type":"output","data":{"text":"e2e transcript event"}}' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript")
if [[ "$APPEND_STATUS" != "202" ]]; then
	echo "FAIL: run-scoped producer append status was ${APPEND_STATUS}, expected 202"
	exit 1
fi
DENIED_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${PRODUCER_TOKEN}" \
	-H 'Content-Type: application/json' \
	-H "SWE-Run-UID: ${RUN_UID}" \
	-d '{"type":"output","data":{"text":"forged"}}' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/auth-scope-run-b/transcript")
if [[ "$DENIED_STATUS" != "403" ]]; then
	echo "FAIL: cross-run producer append status was ${DENIED_STATUS}, expected 403"
	exit 1
fi
STALE_UID_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${PRODUCER_TOKEN}" \
	-H 'Content-Type: application/json' \
	-H 'SWE-Run-UID: stale-uid-not-current' \
	-d '{"type":"output","data":{"text":"stale"}}' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript")
if [[ "$STALE_UID_STATUS" != "409" ]]; then
	echo "FAIL: stale UID producer append status was ${STALE_UID_STATUS}, expected 409"
	exit 1
fi
MISSING_UID_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${PRODUCER_TOKEN}" \
	-H 'Content-Type: application/json' \
	-d '{"type":"output","data":{"text":"unfenced"}}' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript")
if [[ "$MISSING_UID_STATUS" != "428" ]]; then
	echo "FAIL: missing UID producer append status was ${MISSING_UID_STATUS}, expected 428"
	exit 1
fi
MISSING_READ_UID_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript")
OVERLONG_READ_UID_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	-H "SWE-Run-UID: $(printf 'x%.0s' $(seq 1 129))" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript")
STALE_READ_UID_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	-H 'SWE-Run-UID: stale-uid-not-current' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript")
DENIED_READ_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${PRODUCER_TOKEN}" \
	-H "SWE-Run-UID: $(printf 'x%.0s' $(seq 1 129))" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}/transcript")
READER_BASE_RUN_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${READER_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RUN_NAME}")
if [[ "$MISSING_READ_UID_STATUS" != "428" || "$OVERLONG_READ_UID_STATUS" != "400" || \
	"$STALE_READ_UID_STATUS" != "409" || "$DENIED_READ_STATUS" != "403" || "$READER_BASE_RUN_STATUS" != "403" ]]; then
	echo "FAIL: transcript read identity statuses missing=${MISSING_READ_UID_STATUS} overlong=${OVERLONG_READ_UID_STATUS} stale=${STALE_READ_UID_STATUS} denied=${DENIED_READ_STATUS} base-run=${READER_BASE_RUN_STATUS}"
	exit 1
fi
UNKNOWN_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	-H 'SWE-Run-UID: unknown-run-uid' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/unknown-run/transcript")
if [[ "$UNKNOWN_STATUS" != "404" ]]; then
	echo "FAIL: unknown Run transcript status was ${UNKNOWN_STATUS}, expected 404"
	exit 1
fi
for _ in $(seq 1 30); do
	if grep -q 'e2e transcript event' /tmp/swe-platform-transcript.out && \
		grep -q 'e2e transcript event' /tmp/swe-platform-cli-transcript.out; then
		break
	fi
	sleep 1
done
kill "$STREAM_PID" >/dev/null 2>&1 || true
wait "$STREAM_PID" >/dev/null 2>&1 || true
STREAM_PID=""
kill "$CLI_STREAM_PID" >/dev/null 2>&1 || true
wait "$CLI_STREAM_PID" >/dev/null 2>&1 || true
CLI_STREAM_PID=""
if ! grep -q 'e2e transcript event' /tmp/swe-platform-transcript.out; then
	echo "FAIL: transcript event was not received from the SSE stream"
	cat /tmp/swe-platform-transcript.out
	exit 1
fi
if ! grep -F '"event":"transcript"' /tmp/swe-platform-cli-transcript.out | \
	grep -Fq 'e2e transcript event'; then
	echo "FAIL: swe logs --run did not emit the opaque transcript SSE envelope as NDJSON"
	cat /tmp/swe-platform-cli-transcript.out
	exit 1
fi
echo "==> verifying local authenticated MCP stdio tools"
{
	printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"swe-e2e","version":"1"}}}'
	sleep 1
	printf '%s\n' \
		'{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}' \
		'{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
		"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"read_transcript\",\"arguments\":{\"runName\":\"${RUN_NAME}\",\"runUID\":\"${RUN_UID}\",\"maxEvents\":100,\"waitMilliseconds\":1000}}}"
	sleep 2
} | \
	SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$READER_TOKEN" \
	timeout 10 bin/swe --namespace "$PROJECT_NAMESPACE" mcp > /tmp/swe-platform-mcp.out || {
	echo "FAIL: local MCP stdio process did not complete cleanly"
	cat /tmp/swe-platform-mcp.out
	exit 1
}
if ! grep -F '"id":2' /tmp/swe-platform-mcp.out | grep -Fq '"name":"create_run"' || \
	! grep -F '"id":2' /tmp/swe-platform-mcp.out | grep -Fq '"name":"read_transcript"' || \
	! grep -F '"id":3' /tmp/swe-platform-mcp.out | grep -Fq "\"runUID\":\"${RUN_UID}\"" || \
	! grep -F '"id":3' /tmp/swe-platform-mcp.out | grep -Fq '"event":"transcript"' || \
	! grep -F '"id":3' /tmp/swe-platform-mcp.out | grep -Fq '"nextCursor"' || \
	! grep -F '"id":3' /tmp/swe-platform-mcp.out | grep -Fq 'e2e transcript event' || \
	grep -Fq "$READER_TOKEN" /tmp/swe-platform-mcp.out; then
	echo "FAIL: local MCP stdio server did not list tools and return a UID-fenced transcript batch"
	cat /tmp/swe-platform-mcp.out
	exit 1
fi
if ! grep -F '"source":"claude-code"' /tmp/swe-platform-transcript.out | \
	grep -F '"type":"claude-code.process-output"' \
	>/tmp/swe-platform-claude-envelopes.out; then
	echo "FAIL: deployed transcript transport did not retain the exact Claude Code source/type envelope"
	exit 1
fi
if ! grep -oE '"data":"[A-Za-z0-9+/=]+"' /tmp/swe-platform-claude-envelopes.out | \
	sed 's/^"data":"//; s/"$//' | \
	while IFS= read -r encoded; do printf '%s' "$encoded" | base64 --decode || exit 1; done \
	>/tmp/swe-platform-process-output.out; then
	echo "FAIL: Claude Code process output was not valid base64 in the deployed transcript transport"
	exit 1
fi
if ! grep -Fq '"type":"assistant"' /tmp/swe-platform-process-output.out || \
	! grep -Fq '"text":"fake Claude Code is working"' /tmp/swe-platform-process-output.out; then
	echo "FAIL: deployed transcript transport did not retain the realistic Claude assistant record"
	cat /tmp/swe-platform-process-output.out
	exit 1
fi
if contains_e2e_key /tmp/swe-platform-transcript.out || contains_e2e_key /tmp/swe-platform-process-output.out; then
	echo "FAIL: transcript transport exposed the agent API key"
	exit 1
fi
kubectl exec "$RUN_POD_NAME" -- tar -C /workspace -cf - . > /tmp/swe-platform-workspace.tar
if contains_e2e_key /tmp/swe-platform-workspace.tar; then
	echo "FAIL: retained workspace contains the agent API key"
	exit 1
fi

echo "==> rotating the profile without restarting the existing process"
printf '%s' "$E2E_ROTATED_AGENT_API_KEY" | bin/swe --namespace "$PROJECT_NAMESPACE" credentials rotate e2e-claude --api-key-stdin

kubectl delete run "$RUN_NAME" --wait=true >/dev/null
if ! kubectl get environment "$RUN_ENV_NAME" >/dev/null 2>&1; then
	echo "FAIL: deleting Run removed its claimed Environment"
	exit 1
fi

echo "==> verifying shared terminal through swe attach"
MISSING_TERMINAL_UID_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${TERMINAL_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/environments/${ENV_NAME}/terminal")
UNAUTHORIZED_TERMINAL_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${READER_TOKEN}" \
	-H 'SWE-Environment-UID: stale-environment-uid' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/environments/${ENV_NAME}/terminal")
if [[ "$MISSING_TERMINAL_UID_STATUS" != "400" || "$UNAUTHORIZED_TERMINAL_STATUS" != "403" ]]; then
	echo "FAIL: direct terminal auth/identity ordering statuses missing=${MISSING_TERMINAL_UID_STATUS} unauthorized=${UNAUTHORIZED_TERMINAL_STATUS}"
	exit 1
fi
printf 'printf terminal-e2e-ok; if [ -n "${ANTHROPIC_API_KEY+x}" ]; then printf credential-present; else printf credential-absent; fi; exit\n' | \
	SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$TERMINAL_TOKEN" \
	bin/swe --namespace "$PROJECT_NAMESPACE" attach "$ENV_NAME" --environment-uid "$ENV_UID" > /tmp/swe-platform-terminal.out
if ! grep -q 'terminal-e2e-ok' /tmp/swe-platform-terminal.out; then
	echo "FAIL: terminal output was not received through swe attach"
	cat /tmp/swe-platform-terminal.out
	exit 1
fi
if ! grep -q 'credential-absent' /tmp/swe-platform-terminal.out; then
	echo "FAIL: shared terminal inherited the agent API key"
	cat /tmp/swe-platform-terminal.out
	exit 1
fi
POD_NAME=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.podName}')
PVC_NAME=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.volumes[?(@.name=="workspace")].persistentVolumeClaim.claimName}')
if ! kubectl exec "$POD_NAME" -- sh -c 'test "$(cat /workspace/setup-result)" = credential-absent'; then
	echo "FAIL: project repository checkout or .agents/setup did not complete"
	exit 1
fi

echo "==> verifying Environment selector admission and mutable services contract"
SELECTOR_ENV_NAME="selector-admission-$RANDOM"
cat <<EOF | kubectl create -f - >/dev/null
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: ${SELECTOR_ENV_NAME}
spec:
  templateRef: small
  lifecycle:
    hold:
      enabled: true
      revision: 1
EOF
if kubectl patch environment "$SELECTOR_ENV_NAME" --type=merge -p '{"spec":{"templateRef":"unavailable"}}' >/dev/null 2>&1; then
	echo "FAIL: admission accepted an Environment templateRef change"
	exit 1
fi
if kubectl patch environment "$SELECTOR_ENV_NAME" --type=merge -p '{"spec":{"backend":"pod"}}' >/dev/null 2>&1; then
	echo "FAIL: admission accepted an Environment backend presence transition"
	exit 1
fi
kubectl patch environment "$SELECTOR_ENV_NAME" --type=merge -p "{\"spec\":{\"projectRef\":\"${PROJECT_NAME}\"}}" >/dev/null
if kubectl patch environment "$SELECTOR_ENV_NAME" --type=merge -p '{"spec":{"projectRef":"replacement"}}' >/dev/null 2>&1; then
	echo "FAIL: admission accepted an Environment projectRef change after promotion"
	exit 1
fi
if kubectl patch environment "$SELECTOR_ENV_NAME" --type=json -p '[{"op":"remove","path":"/spec/projectRef"}]' >/dev/null 2>&1; then
	echo "FAIL: admission accepted clearing a promoted Environment projectRef"
	exit 1
fi
kubectl patch environment "$SELECTOR_ENV_NAME" --type=merge -p '{"spec":{"services":[{"name":"admission","instanceID":"admissionabcdefghijkl","revision":1,"protocol":"HTTP","targetPort":3000,"visibility":"Project","readiness":"TCPConnect"}]}}' >/dev/null
kubectl delete environment "$SELECTOR_ENV_NAME" --wait=false >/dev/null
SELECTOR_ENV_NAME=""

PROVISIONING_SNAPSHOT=$(kubectl get environment "$ENV_NAME" -o json | jq -cS '.status.provisioning')
LIVE_PROJECT_UID=$(kubectl get project "$PROJECT_NAME" -o jsonpath='{.metadata.uid}')
if [[ "$PROVISIONING_SNAPSHOT" == "null" ]] || ! jq -e \
	--arg templateUID "$LOCAL_TEMPLATE_UID" --arg projectUID "$LIVE_PROJECT_UID" \
	'.template.name == "small" and .template.uid == $templateUID and (.template.generation > 0) and
	 .project.name == "e2e" and .project.uid == $projectUID and (.project.generation > 0) and
	 (.image | length > 0) and (.project.repository | length > 0)' <<<"$PROVISIONING_SNAPSHOT" >/dev/null; then
	echo "FAIL: active Environment has no complete provisioning snapshot"
	kubectl get environment "$ENV_NAME" -o yaml >&2
	exit 1
fi

echo "==> verifying setup runs only once when the pod is recreated"
kubectl delete pod "$POD_NAME" --wait=true >/dev/null
for _ in $(seq 1 30); do
	if kubectl get pod "$POD_NAME" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
kubectl wait --for=condition=Ready pod/"$POD_NAME" --timeout=2m
if [[ "$(kubectl get environment "$ENV_NAME" -o json | jq -cS '.status.provisioning')" != "$PROVISIONING_SNAPSHOT" ]]; then
	echo "FAIL: active Pod recreation changed the provisioning snapshot"
	exit 1
fi
RECREATED_POD=$(kubectl get pod "$POD_NAME" -o json)
if ! jq -e --argjson snapshot "$PROVISIONING_SNAPSHOT" '
	([.spec.containers[] | select(.name == "environment")] | length == 1 and
	 .[0].image == $snapshot.image and .[0].resources.requests == $snapshot.resources and
	 .[0].resources.limits == $snapshot.resources) and
	([.spec.initContainers[] | select(.name == "project-setup")] | length == 1 and
	 .[0].image == $snapshot.image and .[0].resources.requests == $snapshot.resources and
	 .[0].resources.limits == $snapshot.resources and
	 ([.[0].env[] | select(.name == "SWE_REPOSITORY") | .value] | first) == $snapshot.project.repository)' <<<"$RECREATED_POD" >/dev/null ||
	[[ "$(jq -r '.spec.runtimeClassName // ""' <<<"$RECREATED_POD")" != "$(jq -r '.runtimeClassName' <<<"$PROVISIONING_SNAPSHOT")" ]]; then
	echo "FAIL: recreated Pod does not match the provisioning image/runtime/resources/repository"
	exit 1
fi
if ! kubectl exec "$POD_NAME" -- sh -c 'test "$(wc -l < /workspace/setup-result)" -eq 1'; then
	echo "FAIL: .agents/setup ran again for an initialized workspace"
	exit 1
fi
if ! kubectl exec "$POD_NAME" -- sh -c \
	'test "$(wc -l < /workspace/resume-result)" -eq 1 && ! grep -vx credential-absent /workspace/resume-result'; then
	echo "FAIL: active Pod recreation did not run exactly one credential-free resume hook"
	exit 1
fi

echo "==> verifying repository service ingestion, supervision, and authenticated portal"
bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" manual-api --target-port 3999
for _ in $(seq 1 90); do
	REPOSITORY_SERVICE=$(kubectl get environment "$ENV_NAME" -o json | jq -c '.spec.services[]? | select(.name == "repository-web")')
	if jq -e '.source == "Repository" and .revision == 1 and .targetPort >= 49152 and .targetPort <= 65535 and .launch.argv == ["node", ".swe/service.js", "v1"]' <<<"$REPOSITORY_SERVICE" >/dev/null 2>&1 &&
		[[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="manual-api")].source}')" == "API" ]]; then
		break
	fi
	sleep 1
done
if [[ -z "${REPOSITORY_SERVICE:-}" ]] || ! jq -e '.source == "Repository" and .revision == 1' <<<"$REPOSITORY_SERVICE" >/dev/null 2>&1; then
	echo "FAIL: .swe/services.yaml did not converge beside the API-owned declaration"
	kubectl get environment "$ENV_NAME" -o yaml
	kubectl -n "$SYSTEM_NAMESPACE" logs deployment/"$INSTALLATION_NAME" --since=10m | grep -E 'repository service|declared-service' || true
	exit 1
fi
REPOSITORY_PORT=$(jq -r '.targetPort' <<<"$REPOSITORY_SERVICE")
wait_service_observation "$ENV_NAME" repository-web 1 Healthy
REPOSITORY_PORTAL_URL=$(SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$CONSOLE_TOKEN" \
	bin/swe --namespace "$PROJECT_NAMESPACE" portal "$ENV_NAME" repository-web)
REPOSITORY_PORTAL_HOST=${REPOSITORY_PORTAL_URL#http://}
for _ in $(seq 1 60); do
	REPOSITORY_BODY=$(curl --silent --fail -H "Host: $REPOSITORY_PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/ 2>/dev/null || true)
	if jq -e --arg port "$REPOSITORY_PORT" --arg url "$REPOSITORY_PORTAL_URL" \
		'.marker == "v1" and .port == $port and .publicURL == $url and .forbidden == [] and .authorization == "" and (.boot | length > 0)' <<<"$REPOSITORY_BODY" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if ! jq -e --arg port "$REPOSITORY_PORT" --arg url "$REPOSITORY_PORTAL_URL" \
	'.marker == "v1" and .port == $port and .publicURL == $url and .forbidden == [] and .authorization == ""' <<<"${REPOSITORY_BODY:-}" >/dev/null 2>&1; then
	echo "FAIL: repository service did not receive exact PORT/PUBLIC_URL through the authenticated portal: ${REPOSITORY_BODY:-<empty>}"
	exit 1
fi
REPOSITORY_BOOT=$(jq -r '.boot' <<<"$REPOSITORY_BODY")
ENVIRONMENT_PUBLIC_JSON=$(kubectl get environment "$ENV_NAME" -o json)
POD_PUBLIC_JSON=$(kubectl get pod "$POD_NAME" -o json)
if [[ "$ENVIRONMENT_PUBLIC_JSON" == *"$REPOSITORY_PORTAL_URL"* || "$ENVIRONMENT_PUBLIC_JSON" == *'PUBLIC_URL'* ||
	"$POD_PUBLIC_JSON" == *"$REPOSITORY_PORTAL_URL"* || "$POD_PUBLIC_JSON" == *'PUBLIC_URL'* ]] ||
	kubectl exec "$POD_NAME" -- sh -c 'tr "\000" "\n" < /proc/1/environ | grep -Eq "^(PORT|PUBLIC_URL)="'; then
	echo "FAIL: repository service URL/environment leaked into public status or sandboxd Pod state"
	exit 1
fi

echo "==> verifying repository service crash restart"
curl --silent --fail -H "Host: $REPOSITORY_PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/crash >/dev/null
for _ in $(seq 1 60); do
	RESTARTED_BODY=$(curl --silent --fail -H "Host: $REPOSITORY_PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/ 2>/dev/null || true)
	if jq -e --arg old "$REPOSITORY_BOOT" '.marker == "v1" and .boot != $old' <<<"$RESTARTED_BODY" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if ! jq -e --arg old "$REPOSITORY_BOOT" '.marker == "v1" and .boot != $old' <<<"${RESTARTED_BODY:-}" >/dev/null 2>&1; then
	echo "FAIL: sandboxd did not restart the crashed repository service"
	exit 1
fi
REPOSITORY_BOOT=$(jq -r '.boot' <<<"$RESTARTED_BODY")
wait_service_observation "$ENV_NAME" repository-web 1 Healthy

echo "==> verifying malformed and API-colliding repository config fail closed"
printf '%s' $'version: 1\nservices:\n  repository-web:\n    command: invalid\n' |
	kubectl exec -i "$POD_NAME" -- sh -c 'cat > /workspace/.swe/services.yaml'
sleep 25
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="repository-web")].revision}')" != "1" ]] ||
	! curl --silent --fail -H "Host: $REPOSITORY_PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/ >/dev/null; then
	echo "FAIL: malformed repository service config replaced the last admitted intent"
	exit 1
fi
printf '%s' $'version: 1\nservices:\n  repository-web:\n    command: ["node", ".swe/service.js", "v1"]\n  manual-api:\n    command: ["node", ".swe/service.js", "collision"]\n' |
	kubectl exec -i "$POD_NAME" -- sh -c 'cat > /workspace/.swe/services.yaml'
sleep 25
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="manual-api")].source}')" != "API" ||
	"$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="repository-web")].revision}')" != "1" ]]; then
	echo "FAIL: repository/API same-name collision did not preserve canonical intent"
	exit 1
fi
printf '%s' $'version: 1\nservices:\n  repository-web:\n    command: ["node", ".swe/service.js", "v1"]\n' |
	kubectl exec -i "$POD_NAME" -- sh -c 'cat > /workspace/.swe/services.yaml'

echo "==> verifying bounded durable Environment service declarations"
bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" web --target-port 3000
bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" web-alias --target-port 3000
SERVICE_LIST=$(bin/swe --namespace "$PROJECT_NAMESPACE" environment services list "$ENV_NAME")
if ! grep -Fq $'web\tAPI\t1\tHTTP\t3000\tProject\tTCPConnect' <<<"$SERVICE_LIST" ||
	! grep -Fq $'web-alias\tAPI\t1\tHTTP\t3000\tProject\tTCPConnect' <<<"$SERVICE_LIST" ||
	! grep -Fq $'repository-web\tRepository\t1\tHTTP' <<<"$SERVICE_LIST"; then
	echo "FAIL: service list did not report durable declarations and duplicate-port aliases"
	exit 1
fi
if grep -Eiq 'https?://|portal|url' <<<"$SERVICE_LIST"; then
	echo "FAIL: service list exposed or implied a portal URL"
	exit 1
fi
echo "==> verifying real stateless service observations and duplicate-port correlation"
OBSERVATION_OWNER=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.metadata.uid}')
REPOSITORY_PROCESS_OWNER="repository-services/$OBSERVATION_OWNER"
OBSERVATION_SECRET=$(kubectl get pod "$POD_NAME" -o jsonpath='{.metadata.annotations.swe\.dev/sandboxd-secret-name}')
OBSERVATION_TOKEN=$(kubectl get secret "$OBSERVATION_SECRET" -o jsonpath='{.data.service-observation-token}' | base64 --decode)
OBSERVATION_POD_JSON=$(kubectl get pod "$POD_NAME" -o json)
if [[ -z "$OBSERVATION_TOKEN" || "$OBSERVATION_POD_JSON" == *"$OBSERVATION_TOKEN"* ]] ||
	kubectl exec "$POD_NAME" -- test -e /var/run/swe-platform/sandboxd/service-observation-token; then
	echo "FAIL: observation token was absent from its Secret or exposed to the Environment pod"
	exit 1
fi
unset OBSERVATION_TOKEN OBSERVATION_POD_JSON
manage_observation_listener service-start "$POD_NAME" "$OBSERVATION_OWNER" observation-listener-a 3000
wait_service_observation "$ENV_NAME" web 1 Healthy
wait_service_observation "$ENV_NAME" web-alias 1 Healthy
manage_observation_listener service-stop "$POD_NAME" "$OBSERVATION_OWNER" observation-listener-a
wait_service_observation "$ENV_NAME" web 1 Unhealthy
wait_service_observation "$ENV_NAME" web-alias 1 Unhealthy
manage_observation_listener service-start "$POD_NAME" "$OBSERVATION_OWNER" observation-listener-b 3000
wait_service_observation "$ENV_NAME" web 1 Healthy
wait_service_observation "$ENV_NAME" web-alias 1 Healthy
bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" web --target-port 3000
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="web")].revision}')" != "1" ]]; then
	echo "FAIL: exact service declare retry was not idempotent"
	exit 1
fi
if bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" web --target-port 3001 >/dev/null 2>&1; then
	echo "FAIL: declare accepted different configuration for an existing service"
	exit 1
fi
bin/swe --namespace "$PROJECT_NAMESPACE" environment services update "$ENV_NAME" web --target-port 3001
bin/swe --namespace "$PROJECT_NAMESPACE" environment services update "$ENV_NAME" web --target-port 3001
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="web")].revision}')" != "2" ||
	"$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="web")].targetPort}')" != "3001" ]]; then
	echo "FAIL: service update did not strictly increment revision once"
	exit 1
fi
if bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" sandboxd --target-port 50051 >/dev/null 2>&1; then
	echo "FAIL: service declaration accepted sandboxd control port 50051"
	exit 1
fi
INVALID_SERVICE_REVISION_PATCH=$(kubectl get environment "$ENV_NAME" -o json | jq -c \
	'.spec.services |= map(if .name == "web" then .targetPort = 3002 else . end) | {spec:{services:.spec.services}}')
if kubectl patch environment "$ENV_NAME" --type=merge -p "$INVALID_SERVICE_REVISION_PATCH" >/tmp/invalid-service-revision.out 2>&1; then
	echo "FAIL: admission accepted changed same-name service configuration without a higher revision"
	exit 1
fi
if ! grep -q 'revision must increase when an existing service declaration changes' /tmp/invalid-service-revision.out; then
	echo "FAIL: unchanged service revision was not rejected by the intended revision rule"
	cat /tmp/invalid-service-revision.out
	exit 1
fi
bin/swe --namespace "$PROJECT_NAMESPACE" environment services remove "$ENV_NAME" web-alias
bin/swe --namespace "$PROJECT_NAMESPACE" environment services remove "$ENV_NAME" web-alias
if [[ -n "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="web-alias")].name}')" ]]; then
	echo "FAIL: service removal did not durably remove desired state"
	exit 1
fi
manage_observation_listener service-stop "$POD_NAME" "$OBSERVATION_OWNER" observation-listener-b
bin/swe --namespace "$PROJECT_NAMESPACE" environment services remove "$ENV_NAME" web
bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" web --target-port 3002
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="web")].revision}')" != "1" ]]; then
	echo "FAIL: same-name service re-add did not create a fresh declaration"
	exit 1
fi
manage_observation_listener service-start "$POD_NAME" "$OBSERVATION_OWNER" observation-listener-c 3002
wait_service_observation "$ENV_NAME" web 1 Healthy
echo "==> verifying authenticated portal discovery and real sandboxd HTTP tunnel"
PORTAL_URL=$(SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$CONSOLE_TOKEN" \
	bin/swe --namespace "$PROJECT_NAMESPACE" portal "$ENV_NAME" web)
PORTAL_HOST=${PORTAL_URL#http://}
if [[ "$PORTAL_HOST" != *.portal.test ]]; then
	echo "FAIL: portal CLI returned unexpected URL $PORTAL_URL"
	exit 1
fi
PORTAL_BODY=$(curl --silent --fail -H "Host: $PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" \
	-H 'Cookie: swe-platform-session=must-not-forward; application=visible' \
	-H 'X-Portal-Check: accepted' http://127.0.0.1:18080/portal-check)
if ! jq -e '.marker == "portal-listener" and .authorization == "" and .cookie == "application=visible" and .portalHeader == "accepted"' <<<"$PORTAL_BODY" >/dev/null; then
	echo "FAIL: portal did not proxy the real body or strip platform credentials: $PORTAL_BODY"
	exit 1
fi
set +e
curl --silent --include --http1.1 --max-time 2 -H "Host: $PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" \
	-H 'Connection: Upgrade' -H 'Upgrade: websocket' -H 'Sec-WebSocket-Version: 13' \
	-H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' http://127.0.0.1:18080/socket > /tmp/swe-platform-portal-websocket.out
PORTAL_WEBSOCKET_STATUS=$?
set -e
if [[ "$PORTAL_WEBSOCKET_STATUS" != "0" && "$PORTAL_WEBSOCKET_STATUS" != "28" ]] || ! grep -q '101 Switching Protocols' /tmp/swe-platform-portal-websocket.out; then
	echo "FAIL: portal WebSocket upgrade did not traverse the real sandboxd tunnel"
	cat /tmp/swe-platform-portal-websocket.out || true
	exit 1
fi
PORTAL_NO_AUTH=$(curl --silent --output /tmp/portal-no-auth-"$$" --write-out '%{http_code}' -H "Host: $PORTAL_HOST" http://127.0.0.1:18080/)
PORTAL_UNKNOWN=$(curl --silent --output /tmp/portal-unknown-"$$" --write-out '%{http_code}' -H 'Host: aaaaaaaaaaaaaaaaaaaa.portal.test' http://127.0.0.1:18080/)
if [[ "$PORTAL_NO_AUTH" != "404" || "$PORTAL_UNKNOWN" != "404" ]] || ! cmp -s /tmp/portal-no-auth-"$$" /tmp/portal-unknown-"$$"; then
	echo "FAIL: unauthenticated and unknown portal locators were not uniform 404s"
	exit 1
fi
rm -f /tmp/portal-no-auth-"$$" /tmp/portal-unknown-"$$"
PRE_PAUSE_OBSERVATION_EXECUTION=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.serviceObservations.executionGeneration}')

echo "==> verifying pause retains the workspace and resume runs its hook"
bin/swe --namespace "$PROJECT_NAMESPACE" environment hold "$ENV_NAME"
wait_service_observation "$ENV_NAME" web 1 Unavailable absent
for _ in $(seq 1 60); do
	PHASE=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.phase}')
	if [[ "$PHASE" == "Paused" ]] && ! kubectl get pod "$POD_NAME" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if [[ "${PHASE:-}" != "Paused" ]] || kubectl get pod "$POD_NAME" >/dev/null 2>&1; then
	echo "FAIL: environment did not pause and remove its pod"
	exit 1
fi
if [[ -n "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.imageID}')" ]]; then
	echo "FAIL: paused environment retained a stale image ID"
	exit 1
fi
if ! kubectl get pvc "$PVC_NAME" >/dev/null 2>&1; then
	echo "FAIL: pause removed the workspace PVC"
	exit 1
fi
if [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' -H "Host: $PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/)" != "404" ]]; then
	echo "FAIL: explicit hold did not uniformly deny portal routing"
	exit 1
fi
if [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' -H "Host: $REPOSITORY_PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/)" != "404" ]]; then
	echo "FAIL: explicit hold did not fence the repository service portal"
	exit 1
fi
if [[ "$(kubectl get environment "$ENV_NAME" -o json | jq -cS '.status.provisioning')" != "$PROVISIONING_SNAPSHOT" ]]; then
	echo "FAIL: pause changed the provisioning snapshot"
	exit 1
fi
if [[ "$(kubectl get pvc "$PVC_NAME" -o json | jq -r '.spec.resources.requests.storage')" != "$(jq -r '.diskSize' <<<"$PROVISIONING_SNAPSHOT")" ]]; then
	echo "FAIL: retained PVC requested storage does not match the provisioning diskSize"
	exit 1
fi
HOLD_REVISION=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle.hold.revision}')
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle.hold.enabled}')" != "true" || -z "$HOLD_REVISION" ]]; then
	echo "FAIL: swe environment hold did not publish enabled revisioned policy"
	exit 1
fi
bin/swe --namespace "$PROJECT_NAMESPACE" environment release "$ENV_NAME"
RELEASE_REVISION=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle.hold.revision}')
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle.hold.enabled}')" != "false" || "$RELEASE_REVISION" -le "$HOLD_REVISION" ]]; then
	echo "FAIL: swe environment release did not publish a newer disabled policy"
	exit 1
fi
kubectl wait --for=jsonpath='{.status.phase}'=Ready environment/"$ENV_NAME" --timeout=2m
kubectl wait --for=condition=Ready pod/"$POD_NAME" --timeout=2m
wait_service_observation "$ENV_NAME" web 1 Unhealthy
wait_service_observation "$ENV_NAME" repository-web 1 Healthy
for _ in $(seq 1 60); do
	RESUMED_REPOSITORY_BODY=$(curl --silent --fail -H "Host: $REPOSITORY_PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/ 2>/dev/null || true)
	if jq -e --arg old "$REPOSITORY_BOOT" --arg url "$REPOSITORY_PORTAL_URL" \
		'.marker == "v1" and .boot != $old and .publicURL == $url' <<<"$RESUMED_REPOSITORY_BODY" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if ! jq -e --arg old "$REPOSITORY_BOOT" --arg url "$REPOSITORY_PORTAL_URL" \
	'.marker == "v1" and .boot != $old and .publicURL == $url' <<<"${RESUMED_REPOSITORY_BODY:-}" >/dev/null 2>&1; then
	echo "FAIL: pause/resume did not launch a fresh repository process behind the stable gateway URL"
	exit 1
fi
REPOSITORY_BOOT=$(jq -r '.boot' <<<"$RESUMED_REPOSITORY_BODY")
POST_RESUME_OBSERVATION_EXECUTION=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.serviceObservations.executionGeneration}')
if [[ -z "$POST_RESUME_OBSERVATION_EXECUTION" || "$POST_RESUME_OBSERVATION_EXECUTION" -le "$PRE_PAUSE_OBSERVATION_EXECUTION" ]]; then
	echo "FAIL: resumed service observation did not require a fresh execution"
	exit 1
fi
manage_observation_listener service-start "$POD_NAME" "$OBSERVATION_OWNER" observation-listener-d 3002
wait_service_observation "$ENV_NAME" web 1 Healthy
bin/swe --namespace "$PROJECT_NAMESPACE" environment services remove "$ENV_NAME" web
if [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' -H "Host: $PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/)" != "404" ]]; then
	echo "FAIL: removed declaration retained its old portal locator"
	exit 1
fi
bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" web --target-port 3002
NEW_PORTAL_URL=$(SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$CONSOLE_TOKEN" \
	bin/swe --namespace "$PROJECT_NAMESPACE" portal "$ENV_NAME" web)
if [[ "$NEW_PORTAL_URL" == "$PORTAL_URL" ]]; then
	echo "FAIL: same-name service re-add reused a tombstoned portal locator"
	exit 1
fi
bin/swe --namespace "$PROJECT_NAMESPACE" environment services remove "$ENV_NAME" web
for _ in $(seq 1 30); do
	if ! kubectl get environment "$ENV_NAME" -o json | jq -e 'any(.status.serviceObservations.records[]?; .name == "web")' >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if kubectl get environment "$ENV_NAME" -o json | jq -e 'any(.status.serviceObservations.records[]?; .name == "web")' >/dev/null 2>&1; then
	echo "FAIL: removed service retained an observation record"
	exit 1
fi

echo "==> verifying repository command change rotates route and removes stale process intent"
printf '%s' $'version: 1\nservices:\n  repository-web:\n    command: ["node", ".swe/service.js", "v2"]\n' |
	kubectl exec -i "$POD_NAME" -- sh -c 'cat > /workspace/.swe/services.yaml'
for _ in $(seq 1 90); do
	if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="repository-web")].revision}')" == "2" ]]; then
		break
	fi
	sleep 1
done
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="repository-web")].revision}')" != "2" ||
	"$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="manual-api")].source}')" != "API" ]]; then
	echo "FAIL: repository command change did not converge at a higher revision while preserving API intent"
	exit 1
fi
wait_service_observation "$ENV_NAME" repository-web 2 Healthy
if [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' -H "Host: $REPOSITORY_PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/)" != "404" ]]; then
	echo "FAIL: repository command change retained the stale route generation"
	exit 1
fi
UPDATED_REPOSITORY_URL=$(SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$CONSOLE_TOKEN" \
	bin/swe --namespace "$PROJECT_NAMESPACE" portal "$ENV_NAME" repository-web)
UPDATED_REPOSITORY_HOST=${UPDATED_REPOSITORY_URL#http://}
if [[ "$UPDATED_REPOSITORY_URL" == "$REPOSITORY_PORTAL_URL" ]]; then
	echo "FAIL: repository command change reused its old revision-fenced URL"
	exit 1
fi
for _ in $(seq 1 60); do
	UPDATED_REPOSITORY_BODY=$(curl --silent --fail -H "Host: $UPDATED_REPOSITORY_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/ 2>/dev/null || true)
	if jq -e --arg url "$UPDATED_REPOSITORY_URL" '.marker == "v2" and .publicURL == $url and .forbidden == []' <<<"$UPDATED_REPOSITORY_BODY" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if ! jq -e --arg url "$UPDATED_REPOSITORY_URL" '.marker == "v2" and .publicURL == $url and .forbidden == []' <<<"${UPDATED_REPOSITORY_BODY:-}" >/dev/null 2>&1; then
	echo "FAIL: changed repository process did not start with the new gateway-owned URL"
	exit 1
fi
UPDATED_ROUTE_GENERATION=$(kubectl get environment "$ENV_NAME" -o json | jq -r '.status.portalRoutes[] | select(.name == "repository-web" and .active == true and .declarationRevision == 2) | .generation')
manage_observation_listener service-state "$POD_NAME" "$REPOSITORY_PROCESS_OWNER" repository-web running

echo "==> verifying repository removal stops process and tombstones URL"
printf '%s' $'version: 1\nservices: {}\n' |
	kubectl exec -i "$POD_NAME" -- sh -c 'cat > /workspace/.swe/services.yaml'
for _ in $(seq 1 90); do
	if [[ -z "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="repository-web")].name}')" ]]; then
		break
	fi
	sleep 1
done
if [[ -n "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="repository-web")].name}')" ||
	"$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.services[?(@.name=="manual-api")].source}')" != "API" ]]; then
	echo "FAIL: repository removal did not preserve only API-owned service intent"
	exit 1
fi
for _ in $(seq 1 60); do
	if manage_observation_listener service-state "$POD_NAME" "$REPOSITORY_PROCESS_OWNER" repository-web stopped >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if ! manage_observation_listener service-state "$POD_NAME" "$REPOSITORY_PROCESS_OWNER" repository-web stopped; then
	echo "FAIL: repository removal did not stop its managed process"
	exit 1
fi
if [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' -H "Host: $UPDATED_REPOSITORY_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" http://127.0.0.1:18080/)" != "404" ]]; then
	echo "FAIL: repository removal retained its last portal URL"
	exit 1
fi
for _ in $(seq 1 30); do
	if kubectl get environment "$ENV_NAME" -o json | jq -e --argjson generation "$UPDATED_ROUTE_GENERATION" \
		'any(.status.portalRoutes[]?; .generation == $generation and .active == false)' >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if ! kubectl get environment "$ENV_NAME" -o json | jq -e --argjson generation "$UPDATED_ROUTE_GENERATION" \
	'any(.status.portalRoutes[]?; .generation == $generation and .active == false)' >/dev/null 2>&1; then
	echo "FAIL: gateway did not persist a denial tombstone for the removed repository route"
	exit 1
fi
if [[ "$(kubectl get environment "$ENV_NAME" -o json | jq -cS '.status.provisioning')" != "$PROVISIONING_SNAPSHOT" ]]; then
	echo "FAIL: Ready resume changed the provisioning snapshot"
	exit 1
fi
RESUMED_IMAGE_ID=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.imageID}')
RESUMED_POD_IMAGE_ID=$(kubectl get pod "$POD_NAME" -o jsonpath='{.status.containerStatuses[?(@.name=="environment")].imageID}')
if [[ -z "$RESUMED_IMAGE_ID" || "$RESUMED_IMAGE_ID" != "$RESUMED_POD_IMAGE_ID" ]]; then
	echo "FAIL: resumed environment image ID '${RESUMED_IMAGE_ID:-<empty>}' does not match pod image ID '${RESUMED_POD_IMAGE_ID:-<empty>}'"
	exit 1
fi
if ! kubectl exec "$POD_NAME" -- sh -c \
	'test "$(wc -l < /workspace/resume-result)" -eq 2 && ! grep -vx credential-absent /workspace/resume-result && test "$(wc -l < /workspace/agent-credential-marker)" -eq 1 && test -z "${ANTHROPIC_API_KEY+x}" && ! tr "\000" "\n" < /proc/1/environ | grep -q "^ANTHROPIC_API_KEY="'; then
	echo "FAIL: resume hook or fresh sandboxd received the agent API key before agent launch"
	exit 1
fi
if ! kubectl exec "$POD_NAME" -- sh -c 'test "$(wc -l < /workspace/setup-result)" -eq 1'; then
	echo "FAIL: .agents/setup ran again while resuming"
	exit 1
fi

echo "==> verifying the rotated key is materialized only for a fresh agent launch"
RESUME_RUN_NAME=e2e-resume-credential-run
bin/swe --namespace "$PROJECT_NAMESPACE" run "resume credential smoke test" --name "$RESUME_RUN_NAME" --environment "$ENV_NAME" \
	--credential-profile e2e-claude --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$RESUME_RUN_NAME" --timeout=3m
RESUME_RUN_UID=$(kubectl get run "$RESUME_RUN_NAME" -o jsonpath='{.metadata.uid}')
RESUME_ENV_UID=$(kubectl get run "$RESUME_RUN_NAME" -o jsonpath='{.status.environmentRef.uid}')
manage_observation_listener service-start "$POD_NAME" "$OBSERVATION_OWNER" console-portal-listener 3999
wait_service_observation "$ENV_NAME" manual-api 1 Healthy
CONSOLE_SESSION_REFRESH_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' -X POST \
	-H "Authorization: Bearer ${CONSOLE_TOKEN}" --cookie-jar "$COOKIE_JAR" http://127.0.0.1:18080/api/v1/session)
if [[ "$CONSOLE_SESSION_REFRESH_STATUS" != "200" ]]; then
	echo "FAIL: console session refresh returned ${CONSOLE_SESSION_REFRESH_STATUS}, expected 200"
	exit 1
fi
CONSOLE_PORTAL_URL=$(SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$CONSOLE_TOKEN" \
	bin/swe --namespace "$PROJECT_NAMESPACE" portal "$ENV_NAME" manual-api)
CONSOLE_PORTAL_HOST=${CONSOLE_PORTAL_URL#http://}
echo "==> verifying authenticated console portal discovery and host-local session handoff"
PORTAL_LIST_PATH="/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RESUME_RUN_NAME}/portals/${RESUME_RUN_UID}/${RESUME_ENV_UID}"
PORTAL_LIST_STATUS=$(curl --silent --output /tmp/swe-platform-console-portals.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" "http://127.0.0.1:18080${PORTAL_LIST_PATH}")
PORTAL_LIST=$(cat /tmp/swe-platform-console-portals.json)
if [[ "$PORTAL_LIST_STATUS" != "200" ]]; then
	echo "FAIL: console portal list returned ${PORTAL_LIST_STATUS}: $PORTAL_LIST"
	exit 1
fi
PORTAL_OPEN_PATH=$(jq -r '.items[] | select(.name == "manual-api" and .targetPort == 3999 and .status == "Ready") | .openURL' <<<"$PORTAL_LIST")
if [[ "$PORTAL_OPEN_PATH" != "${PORTAL_LIST_PATH}/manual-api/open" ]] || ! jq -e --arg url "$CONSOLE_PORTAL_URL" 'any(.items[]; .name == "manual-api" and .url == $url)' <<<"$PORTAL_LIST" >/dev/null; then
	echo "FAIL: console portal list was not exact, ready, and stable: $PORTAL_LIST"
	exit 1
fi
PORTAL_HANDOFF_STATUS=$(curl --silent --output /tmp/swe-platform-console-portal-handoff.html --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -X POST -H 'Origin: http://127.0.0.1:18080' "http://127.0.0.1:18080${PORTAL_OPEN_PATH}")
PORTAL_HANDOFF_HTML=$(cat /tmp/swe-platform-console-portal-handoff.html)
if [[ "$PORTAL_HANDOFF_STATUS" != "200" ]]; then
	echo "FAIL: console portal opener returned ${PORTAL_HANDOFF_STATUS}: $PORTAL_HANDOFF_HTML"
	exit 1
fi
PORTAL_HANDOFF_CODE=$(sed -n 's/.*name="code" value="\([^"]*\)".*/\1/p' <<<"$PORTAL_HANDOFF_HTML")
if [[ -z "$PORTAL_HANDOFF_CODE" ]] || grep -Fq "$CONSOLE_TOKEN" <<<"$PORTAL_HANDOFF_HTML"; then
	echo "FAIL: console portal opener did not return a credential-free one-time handoff"
	exit 1
fi
PORTAL_COOKIE_JAR=/tmp/swe-platform-portal-cookies
rm -f "$PORTAL_COOKIE_JAR"
HANDOFF_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--resolve "${CONSOLE_PORTAL_HOST}:18080:127.0.0.1" --cookie-jar "$PORTAL_COOKIE_JAR" \
	-H 'Origin: http://127.0.0.1:18080' -X POST --data-urlencode "code=${PORTAL_HANDOFF_CODE}" \
	"http://${CONSOLE_PORTAL_HOST}:18080/.swe/session-handoff")
if [[ "$HANDOFF_STATUS" != "200" ]]; then
	echo "FAIL: portal host-local session handoff returned ${HANDOFF_STATUS}, expected 200"
	exit 1
fi
PORTAL_SESSION_STATUS=$(curl --silent --output /tmp/swe-platform-console-portal-session.json --write-out '%{http_code}' \
	--resolve "${CONSOLE_PORTAL_HOST}:18080:127.0.0.1" --cookie "$PORTAL_COOKIE_JAR" \
	-H 'X-Portal-Check: browser-session' "http://${CONSOLE_PORTAL_HOST}:18080/portal-check")
PORTAL_SESSION_BODY=$(cat /tmp/swe-platform-console-portal-session.json)
if [[ "$PORTAL_SESSION_STATUS" != "200" ]]; then
	echo "FAIL: authorized console portal request returned ${PORTAL_SESSION_STATUS}: $PORTAL_SESSION_BODY"
	exit 1
fi
if ! jq -e '.marker == "portal-listener" and .authorization == "" and .portalHeader == "browser-session"' <<<"$PORTAL_SESSION_BODY" >/dev/null; then
	echo "FAIL: console portal handoff did not establish an authorized host-local session: $PORTAL_SESSION_BODY"
	exit 1
fi
manage_observation_listener service-stop "$POD_NAME" "$OBSERVATION_OWNER" console-portal-listener
RUN_TERMINAL_PATH="/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RESUME_RUN_NAME}/terminal/${RESUME_RUN_UID}/${RESUME_ENV_UID}"
echo "==> verifying exact browser Run terminal through a same-origin session"
"$WEB_TERMINAL_CLIENT" http://127.0.0.1:18080 "$CONSOLE_TOKEN" "$RUN_TERMINAL_PATH" \
	$'touch /workspace/browser-terminal-opened; printf browser-run-terminal-e2e-ok; exit\n' browser-run-terminal-e2e-ok
STALE_RUN_ENVIRONMENT_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${CONSOLE_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RESUME_RUN_NAME}/terminal/${RESUME_RUN_UID}/stale-environment-uid")
if [[ "$STALE_RUN_ENVIRONMENT_STATUS" != "409" ]]; then
	echo "FAIL: browser Run terminal stale Environment status was ${STALE_RUN_ENVIRONMENT_STATUS}, expected 409"
	exit 1
fi
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$RESUME_RUN_NAME" --timeout=3m
for _ in $(seq 1 60); do
	RELEASED_CLAIM_UID=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.claimedBy.uid}' 2>/dev/null || true)
	if [[ -z "$RELEASED_CLAIM_UID" || "$RELEASED_CLAIM_UID" != "$RESUME_RUN_UID" ]]; then
		break
	fi
	sleep 1
done
if [[ "${RELEASED_CLAIM_UID:-}" == "$RESUME_RUN_UID" ]]; then
	echo "FAIL: completed Run retained its Environment claim"
	exit 1
fi
RELEASED_RUN_TERMINAL_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${CONSOLE_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RESUME_RUN_NAME}/terminal/${RESUME_RUN_UID}/${RESUME_ENV_UID}")
if [[ "$RELEASED_RUN_TERMINAL_STATUS" != "409" ]]; then
	echo "FAIL: released browser Run terminal status was ${RELEASED_RUN_TERMINAL_STATUS}, expected 409"
	exit 1
fi
if ! kubectl exec "$POD_NAME" -- sh -c \
	'test "$(wc -l < /workspace/agent-credential-marker)" -eq 2 && test -z "${ANTHROPIC_API_KEY+x}" && ! tr "\000" "\n" < /proc/1/environ | grep -q "^ANTHROPIC_API_KEY="'; then
	echo "FAIL: fresh agent launch did not receive the rotated profile in isolation"
	exit 1
fi
check_sandboxd_process "$POD_NAME" "$RESUME_RUN_UID" "$E2E_ROTATED_AGENT_API_KEY"
set +e
curl --silent --no-buffer --max-time 2 \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	-H "SWE-Run-UID: ${RESUME_RUN_UID}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RESUME_RUN_NAME}/transcript" \
	> /tmp/swe-platform-resume-transcript.out
RESUME_TRANSCRIPT_STATUS=$?
set -e
if [[ "$RESUME_TRANSCRIPT_STATUS" != "0" && "$RESUME_TRANSCRIPT_STATUS" != "28" ]]; then
	echo "FAIL: resumed Run transcript read failed with curl status ${RESUME_TRANSCRIPT_STATUS}"
	exit 1
fi
grep -F '"source":"claude-code"' /tmp/swe-platform-resume-transcript.out | \
	grep -F '"type":"claude-code.process-output"' | \
	grep -oE '"data":"[A-Za-z0-9+/=]+"' | sed 's/^"data":"//; s/"$//' | \
	while IFS= read -r encoded; do printf '%s' "$encoded" | base64 --decode || exit 1; done \
	> /tmp/swe-platform-resume-process-output.out
if contains_e2e_key /tmp/swe-platform-resume-transcript.out || \
	contains_e2e_key /tmp/swe-platform-resume-process-output.out; then
	echo "FAIL: resumed Run transcript exposed the rotated agent API key"
	exit 1
fi
kubectl exec "$POD_NAME" -- tar -C /workspace -cf - . > /tmp/swe-platform-resumed-workspace.tar
if contains_e2e_key /tmp/swe-platform-resumed-workspace.tar; then
	echo "FAIL: resumed workspace contains an agent API key"
	exit 1
fi

echo "==> verifying credentialed fake Amp process scope without network"
AMP_RUN_NAME=e2e-fake-amp-run
printf '%s' "$E2E_AMP_API_KEY" | bin/swe --namespace "$PROJECT_NAMESPACE" credentials create e2e-amp --agent amp --api-key-stdin
bin/swe --namespace "$PROJECT_NAMESPACE" run "fake Amp lifecycle smoke test" --name "$AMP_RUN_NAME" --environment "$ENV_NAME" \
	--agent amp --credential-profile e2e-amp --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$AMP_RUN_NAME" --timeout=3m
REASSIGNED_RUN_TERMINAL_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${CONSOLE_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${RESUME_RUN_NAME}/terminal/${RESUME_RUN_UID}/${RESUME_ENV_UID}")
if [[ "$REASSIGNED_RUN_TERMINAL_STATUS" != "409" ]]; then
	echo "FAIL: reassigned browser Run terminal status was ${REASSIGNED_RUN_TERMINAL_STATUS}, expected 409"
	exit 1
fi
kubectl delete run "$RESUME_RUN_NAME" --wait=true >/dev/null
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$AMP_RUN_NAME" --timeout=3m
AMP_RUN_UID=$(kubectl get run "$AMP_RUN_NAME" -o jsonpath='{.metadata.uid}')
AMP_POD_NAME=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.podName}')
if ! kubectl exec "$AMP_POD_NAME" -- sh -c \
	'test -z "${AMP_API_KEY+x}" && ! tr "\000" "\n" < /proc/1/environ | grep -q "^AMP_API_KEY="'; then
	echo "FAIL: AMP_API_KEY was not confined to the selected fake Amp process"
	exit 1
fi
check_sandboxd_process "$AMP_POD_NAME" "$AMP_RUN_UID" "$E2E_AMP_API_KEY"
set +e
curl --silent --no-buffer --max-time 2 \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	-H "SWE-Run-UID: ${AMP_RUN_UID}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${AMP_RUN_NAME}/transcript" \
	> /tmp/swe-platform-amp-transcript.out
AMP_TRANSCRIPT_STATUS=$?
set -e
if [[ "$AMP_TRANSCRIPT_STATUS" != "0" && "$AMP_TRANSCRIPT_STATUS" != "28" ]]; then
	echo "FAIL: Amp transcript read failed with curl status ${AMP_TRANSCRIPT_STATUS}"
	exit 1
fi
grep -F '"source":"amp"' /tmp/swe-platform-amp-transcript.out | \
	grep -F '"type":"amp.process-output"' | \
	grep -oE '"data":"[A-Za-z0-9+/=]+"' | sed 's/^"data":"//; s/"$//' | \
	while IFS= read -r encoded; do printf '%s' "$encoded" | base64 --decode || exit 1; done \
	> /tmp/swe-platform-amp-process-output.out
if ! grep -Fq 'fake-amp-stdout-marker' /tmp/swe-platform-amp-process-output.out || \
	! grep -Fq 'fake-amp-stderr-marker' /tmp/swe-platform-amp-process-output.out || \
	! grep -Fq 'amp-credential-present' /tmp/swe-platform-amp-process-output.out; then
	echo "FAIL: Amp transcript did not retain both output stream markers"
	cat /tmp/swe-platform-amp-process-output.out
	exit 1
fi
kubectl get run "$AMP_RUN_NAME" -o yaml > /tmp/swe-platform-amp-run.yaml
kubectl -n "$SYSTEM_NAMESPACE" logs -l app.kubernetes.io/component=control-plane --all-containers --prefix --tail=-1 > /tmp/swe-platform-amp-control-plane.log
kubectl -n "$SYSTEM_NAMESPACE" logs -l app.kubernetes.io/component=operator --all-containers --prefix --tail=-1 > /tmp/swe-platform-amp-operator.log
kubectl logs "$AMP_POD_NAME" -c environment --tail=-1 > /tmp/swe-platform-amp-environment.log
kubectl exec "$AMP_POD_NAME" -- tar -C /workspace -cf - . > /tmp/swe-platform-amp-workspace.tar
for artifact in /tmp/swe-platform-amp-transcript.out /tmp/swe-platform-amp-process-output.out \
	/tmp/swe-platform-amp-run.yaml /tmp/swe-platform-amp-control-plane.log \
	/tmp/swe-platform-amp-operator.log /tmp/swe-platform-amp-environment.log \
	/tmp/swe-platform-amp-workspace.tar; do
	if contains_e2e_key "$artifact"; then
		echo "FAIL: Amp API key leaked through $artifact"
		exit 1
	fi
done
kubectl delete run "$AMP_RUN_NAME" --wait=true >/dev/null

echo "==> verifying credentialless fake Amp retains the plain managed-process path"
AMP_CREDENTIALLESS_RUN_NAME=e2e-fake-amp-credentialless-run
bin/swe --namespace "$PROJECT_NAMESPACE" run "fake Amp credentialless lifecycle smoke test" --name "$AMP_CREDENTIALLESS_RUN_NAME" \
	--environment "$ENV_NAME" --agent amp --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$AMP_CREDENTIALLESS_RUN_NAME" --timeout=3m
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$AMP_CREDENTIALLESS_RUN_NAME" --timeout=3m
kubectl delete run "$AMP_CREDENTIALLESS_RUN_NAME" --wait=true >/dev/null

echo "==> verifying fake Codex real Run lifecycle without credentials or network"
CODEX_RUN_NAME=e2e-fake-codex-run
bin/swe --namespace "$PROJECT_NAMESPACE" run "fake Codex lifecycle smoke test" --name "$CODEX_RUN_NAME" --environment "$ENV_NAME" --agent codex --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$CODEX_RUN_NAME" --timeout=3m
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$CODEX_RUN_NAME" --timeout=3m
CODEX_RUN_UID=$(kubectl get run "$CODEX_RUN_NAME" -o jsonpath='{.metadata.uid}')
set +e
curl --silent --no-buffer --max-time 2 -H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" -H "SWE-Run-UID: ${CODEX_RUN_UID}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${CODEX_RUN_NAME}/transcript" > /tmp/swe-platform-codex-transcript.out
CODEX_TRANSCRIPT_STATUS=$?
set -e
if [[ "$CODEX_TRANSCRIPT_STATUS" != "0" && "$CODEX_TRANSCRIPT_STATUS" != "28" ]]; then echo "FAIL: Codex transcript read failed"; exit 1; fi
grep -F '"source":"codex"' /tmp/swe-platform-codex-transcript.out | grep -F '"type":"codex.process-output"' | \
	grep -oE '"data":"[A-Za-z0-9+/=]+"' | sed 's/^"data":"//; s/"$//' | while IFS= read -r encoded; do printf '%s' "$encoded" | base64 --decode || exit 1; done > /tmp/swe-platform-codex-process-output.out
for marker in fake-codex-thread fake-codex-stderr-marker turn.completed; do grep -Fq "$marker" /tmp/swe-platform-codex-process-output.out || { echo "FAIL: missing Codex marker $marker"; exit 1; }; done
kubectl delete run "$CODEX_RUN_NAME" --wait=true >/dev/null

echo "==> verifying fake Codex terminal failure through the real controller"
CODEX_FAILED_RUN_NAME=e2e-fake-codex-failed-run
bin/swe --namespace "$PROJECT_NAMESPACE" run "fake Codex failure smoke test" --name "$CODEX_FAILED_RUN_NAME" --environment "$ENV_NAME" --agent codex --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Failed run/"$CODEX_FAILED_RUN_NAME" --timeout=3m
kubectl delete run "$CODEX_FAILED_RUN_NAME" --wait=true >/dev/null

echo "==> verifying fake Pi success, opaque output, and terminal error"
PI_RUN_NAME=e2e-fake-pi-run
bin/swe --namespace "$PROJECT_NAMESPACE" run "fake Pi lifecycle smoke test" --name "$PI_RUN_NAME" --environment "$ENV_NAME" --agent pi --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$PI_RUN_NAME" --timeout=3m
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$PI_RUN_NAME" --timeout=3m
PI_RUN_UID=$(kubectl get run "$PI_RUN_NAME" -o jsonpath='{.metadata.uid}')
set +e
curl --silent --no-buffer --max-time 2 -H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" -H "SWE-Run-UID: ${PI_RUN_UID}" \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/runs/${PI_RUN_NAME}/transcript" > /tmp/swe-platform-pi-transcript.out
PI_TRANSCRIPT_STATUS=$?
set -e
if [[ "$PI_TRANSCRIPT_STATUS" != "0" && "$PI_TRANSCRIPT_STATUS" != "28" ]]; then echo "FAIL: Pi transcript read failed"; exit 1; fi
grep -F '"source":"pi"' /tmp/swe-platform-pi-transcript.out | grep -F '"type":"pi.process-output"' | \
	grep -oE '"data":"[A-Za-z0-9+/=]+"' | sed 's/^"data":"//; s/"$//' | while IFS= read -r encoded; do printf '%s' "$encoded" | base64 --decode || exit 1; done > /tmp/swe-platform-pi-process-output.out
for marker in agent_end fake-pi-stderr-marker; do grep -Fq "$marker" /tmp/swe-platform-pi-process-output.out || { echo "FAIL: missing Pi marker $marker"; exit 1; }; done
kubectl delete run "$PI_RUN_NAME" --wait=true >/dev/null
PI_FAILED_RUN_NAME=e2e-fake-pi-failed-run
bin/swe --namespace "$PROJECT_NAMESPACE" run "fake Pi failure smoke test" --name "$PI_FAILED_RUN_NAME" --environment "$ENV_NAME" --agent pi --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Failed run/"$PI_FAILED_RUN_NAME" --timeout=3m
kubectl delete run "$PI_FAILED_RUN_NAME" --wait=true >/dev/null

echo "==> verifying idle pause and terminal wake through the control plane"
bin/swe --namespace "$PROJECT_NAMESPACE" environment services declare "$ENV_NAME" web --target-port 3002
IDLE_PORTAL_URL=$(SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$CONSOLE_TOKEN" \
	bin/swe --namespace "$PROJECT_NAMESPACE" portal "$ENV_NAME" web)
IDLE_PORTAL_HOST=${IDLE_PORTAL_URL#http://}
PRE_IDLE_POD_UID=$(kubectl get pod "$POD_NAME" -o jsonpath='{.metadata.uid}')
kubectl patch environmenttemplate small --type=merge -p '{"spec":{"idleTimeout":"5s"}}' >/dev/null
for _ in $(seq 1 30); do
	PHASE=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.phase}')
	if [[ "$PHASE" == "Paused" ]] && ! kubectl get pod "$POD_NAME" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if [[ "${PHASE:-}" != "Paused" ]] || kubectl get pod "$POD_NAME" >/dev/null 2>&1; then
	echo "FAIL: idle environment did not pause and remove its pod"
	exit 1
fi
echo "==> verifying a portal request queues across Idle wake and fresh listener proof"
curl --silent --fail --max-time 130 -H "Host: $IDLE_PORTAL_HOST" -H "Authorization: Bearer $CONSOLE_TOKEN" \
	http://127.0.0.1:18080/idle-wake > /tmp/swe-platform-portal-idle-wake.out &
PORTAL_WAKE_PID=$!
WOKEN_PORTAL_POD=""
for _ in $(seq 1 180); do
	WOKEN_PORTAL_POD=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.podName}' 2>/dev/null || true)
	if [[ -n "$WOKEN_PORTAL_POD" ]] && kubectl get pod "$WOKEN_PORTAL_POD" >/dev/null 2>&1 && \
		[[ -n "$(kubectl get pod "$WOKEN_PORTAL_POD" -o jsonpath='{.metadata.annotations.swe\.dev/sandboxd-secret-name}' 2>/dev/null || true)" ]]; then
		break
	fi
	sleep 0.5
done
if [[ -z "$WOKEN_PORTAL_POD" ]]; then
	kill "$PORTAL_WAKE_PID" >/dev/null 2>&1 || true
	echo "FAIL: portal request did not create a fresh pod for Idle wake"
	exit 1
fi
if ! kubectl -n "$PROJECT_NAMESPACE" wait --for=condition=Ready pod/"$WOKEN_PORTAL_POD" --timeout=90s; then
	kill "$PORTAL_WAKE_PID" >/dev/null 2>&1 || true
	echo "FAIL: portal wake replacement pod did not become ready"
	exit 1
fi
manage_observation_listener service-start "$WOKEN_PORTAL_POD" "$OBSERVATION_OWNER" portal-idle-listener 3002
if ! wait "$PORTAL_WAKE_PID" || ! jq -e '.marker == "portal-listener"' /tmp/swe-platform-portal-idle-wake.out >/dev/null; then
	echo "FAIL: queued portal request did not route after fresh listener proof"
	cat /tmp/swe-platform-portal-idle-wake.out || true
	exit 1
fi
for _ in $(seq 1 30); do
	PHASE=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.phase}')
	if [[ "$PHASE" == "Paused" ]] && ! kubectl get pod "$WOKEN_PORTAL_POD" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
if [[ "${PHASE:-}" != "Paused" ]]; then
	echo "FAIL: portal-woken environment did not return to Idle suspension for terminal wake coverage"
	exit 1
fi
printf 'printf web-terminal-e2e-ok; exit\n' | \
	SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$TERMINAL_TOKEN" \
	bin/swe --namespace "$PROJECT_NAMESPACE" attach "$ENV_NAME" --environment-uid "$ENV_UID" > /tmp/swe-platform-web-terminal.out
if ! grep -q 'web-terminal-e2e-ok' /tmp/swe-platform-web-terminal.out; then
	echo "FAIL: terminal output was not received through the control-plane websocket"
	cat /tmp/swe-platform-web-terminal.out
	exit 1
fi
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.paused}')" == "true" ]]; then
	echo "FAIL: terminal request did not wake the idle environment"
	exit 1
fi
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.phase}')" != "Ready" ]]; then
	echo "FAIL: woken environment did not become ready"
	exit 1
fi
POST_WAKE_POD_UID=$(kubectl get pod "$POD_NAME" -o jsonpath='{.metadata.uid}')
if [[ "$POST_WAKE_POD_UID" == "$PRE_IDLE_POD_UID" ]]; then
	echo "FAIL: terminal request connected without recreating the paused pod"
	exit 1
fi

echo "==> verifying direct attach rejects a same-name Environment replacement without side effects"
kubectl patch environmenttemplate small --type=merge -p '{"spec":{"idleTimeout":"15m"}}' >/dev/null
kubectl delete environment "$ENV_NAME" --wait=true >/dev/null
cat <<EOF | kubectl apply -f -
apiVersion: swe.dev/v1alpha1
kind: Environment
metadata:
  name: ${ENV_NAME}
spec:
  projectRef: e2e
  templateRef: small
EOF
kubectl wait --for=jsonpath='{.status.phase}'=Ready environment/"$ENV_NAME" --timeout=3m
REPLACEMENT_ENV_UID=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.metadata.uid}')
REPLACEMENT_LIFECYCLE_BEFORE=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle}')
STALE_REPLACEMENT_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${TERMINAL_TOKEN}" \
	-H "SWE-Environment-UID: ${ENV_UID}" \
	-H 'Connection: Upgrade' -H 'Upgrade: websocket' \
	"http://127.0.0.1:18080/api/v1/namespaces/${PROJECT_NAMESPACE}/environments/${ENV_NAME}/terminal")
REPLACEMENT_LIFECYCLE_AFTER=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle}')
if [[ "$REPLACEMENT_ENV_UID" == "$ENV_UID" || "$STALE_REPLACEMENT_STATUS" != "409" || "$REPLACEMENT_LIFECYCLE_AFTER" != "$REPLACEMENT_LIFECYCLE_BEFORE" ]]; then
	echo "FAIL: replacement terminal fence uid=${REPLACEMENT_ENV_UID} status=${STALE_REPLACEMENT_STATUS} lifecycle-before=${REPLACEMENT_LIFECYCLE_BEFORE} lifecycle-after=${REPLACEMENT_LIFECYCLE_AFTER}"
	exit 1
fi
POD_NAME=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.podName}')
POD_PHASE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.status.phase}')
if [[ "$POD_PHASE" != "Running" ]]; then
	echo "FAIL: pod ${POD_NAME} is ${POD_PHASE}, expected Running"
	echo "--- operator log ---"
	kubectl -n "$SYSTEM_NAMESPACE" logs deployment/swe-platform-swe-platform --tail=50
	exit 1
fi

echo "==> sandboxd logs from the environment pod"
kubectl logs "$POD_NAME" -c environment | head -3

echo "E2E OK"
