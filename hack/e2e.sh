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
ENV_IMAGE="ghcr.io/chris-cullins/swe-platform/env-base:dev"
E2E_ENV_IMAGE="ghcr.io/chris-cullins/swe-platform/env-base:e2e-credentials"
OPERATOR_IMAGE="ghcr.io/chris-cullins/swe-platform/operator:dev"
CONTROL_PLANE_IMAGE="ghcr.io/chris-cullins/swe-platform/control-plane:dev"
E2E_AGENT_API_KEY='!!SWE-E2E-AGENT-API-KEY-DO-NOT-USE!!'
E2E_ROTATED_AGENT_API_KEY='!!SWE-E2E-ROTATED-AGENT-API-KEY-DO-NOT-USE!!'
PORT_FORWARD_PID=""
SANDBOXD_PORT_FORWARD_PID=""
WEB_TERMINAL_CLIENT=""
PROJECT_REPO=""
PROJECT_WORKTREE=""
FAKE_ENV_CONTEXT=""
E2E_KUBECONFIG=""

cleanup() {
	if [[ -n "$PORT_FORWARD_PID" ]]; then
		kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
		wait "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$SANDBOXD_PORT_FORWARD_PID" ]]; then
		kill "$SANDBOXD_PORT_FORWARD_PID" >/dev/null 2>&1 || true
		wait "$SANDBOXD_PORT_FORWARD_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$WEB_TERMINAL_CLIENT" ]]; then
		rm -f "$WEB_TERMINAL_CLIENT"
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
	rm -f /tmp/swe-platform-sandboxd-cert-"$$" /tmp/swe-platform-sandboxd-token-"$$"
	if [[ "${KEEP_CLUSTER:-false}" != "true" && "${E2E_USE_EXISTING_CLUSTER:-false}" != "true" ]]; then
		kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

contains_e2e_key() {
	grep -aFq -- "$E2E_AGENT_API_KEY" "$1" || grep -aFq -- "$E2E_ROTATED_AGENT_API_KEY" "$1"
}

check_sandboxd_process() {
	local pod_name="$1"
	local run_uid="$2"
	local expected_key="$3"
	local secret_name identity
	secret_name=$(kubectl get pod "$pod_name" -o jsonpath='{.metadata.annotations.swe\.dev/sandboxd-secret-name}')
	identity=$(kubectl get pod "$pod_name" -o jsonpath='{.metadata.annotations.swe\.dev/sandboxd-identity}')
	kubectl get secret "$secret_name" -o jsonpath='{.data.tls\.crt}' | base64 --decode > /tmp/swe-platform-sandboxd-cert-"$$"
	kubectl get secret "$secret_name" -o jsonpath='{.data.process-token}' | base64 --decode > /tmp/swe-platform-sandboxd-token-"$$"
	kubectl port-forward pod/"$pod_name" 15051:50051 >/tmp/swe-platform-sandboxd-port-forward.log 2>&1 &
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
sleep 5
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"fake Claude Code completed"}'
EOF
chmod 0755 "$FAKE_ENV_CONTEXT/claude"
cat > "$FAKE_ENV_CONTEXT/amp" <<'EOF'
#!/bin/sh
set -eu
test "$#" -eq 4
test "$1" = '--execute=fake Amp lifecycle smoke test'
test "$2" = '--stream-json'
test "$3" = '--no-ide'
test "$4" = '--no-notifications'
test -z "${AMP_API_KEY+x}"
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
cat > "$FAKE_ENV_CONTEXT/Dockerfile" <<'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
COPY claude /usr/local/bin/claude
COPY amp /usr/local/bin/amp
COPY codex /usr/local/bin/codex
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
kubectl apply --server-side --force-conflicts -f charts/swe-platform/crds
kubectl get crd agentcredentialprofiles.swe.dev >/dev/null

echo "==> installing operator and kind template through Helm with upgraded CRDs"
E2E_BOOTSTRAP_TOKEN="$(openssl rand -hex 32)"
kubectl create secret generic swe-platform-bootstrap --from-literal=token="$E2E_BOOTSTRAP_TOKEN"
HELM_ARGS=(
	upgrade --install swe-platform charts/swe-platform
	--namespace default --values charts/swe-platform/values-kind.yaml
	--set controlPlane.auth.bootstrapTokenSecret.name=swe-platform-bootstrap
	--set-string "environmentTemplates[0].spec.image=$E2E_ENV_IMAGE"
	--wait --timeout 2m
)
if [[ -n "${E2E_RUNTIME_CLASS:-}" ]]; then
	HELM_ARGS+=(--set-string "environmentTemplates[0].spec.runtimeClass=$E2E_RUNTIME_CLASS")
fi
helm "${HELM_ARGS[@]}"

echo "==> waiting for warm environment"
kubectl wait --for=jsonpath='{.status.warmPoolReady}'=1 environmenttemplate/small --timeout=2m
WARM_ENV_NAME=$(kubectl get environments -l swe.dev/warm-pool=small -o jsonpath='{.items[0].metadata.name}')
if [[ -z "$WARM_ENV_NAME" ]]; then
	echo "FAIL: warm pool did not create an environment"
	exit 1
fi
WARM_POD_UID=$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.metadata.uid}')
if [[ -n "${E2E_RUNTIME_CLASS:-}" ]]; then
	WARM_RUNTIME_CLASS=$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.spec.runtimeClassName}')
	if [[ "$WARM_RUNTIME_CLASS" != "$E2E_RUNTIME_CLASS" ]]; then
		echo "FAIL: warm Environment uses RuntimeClass '$WARM_RUNTIME_CLASS', expected '$E2E_RUNTIME_CLASS'"
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

echo "==> creating project configuration"
PROJECT_REPO="$(mktemp -d /tmp/swe-e2e-project-XXXXXX)"
PROJECT_WORKTREE="$(mktemp -d /tmp/swe-e2e-worktree-XXXXXX)"
git -C "$PROJECT_WORKTREE" init -b main >/dev/null
git -C "$PROJECT_WORKTREE" config user.name "swe e2e"
git -C "$PROJECT_WORKTREE" config user.email "swe-e2e@example.invalid"
mkdir -p "$PROJECT_WORKTREE/.agents"
cat > "$PROJECT_WORKTREE/.agents/setup" <<'EOF'
if [ -n "${ANTHROPIC_API_KEY+x}" ]; then exit 43; fi
printf '%s\n' credential-absent >> setup-result
EOF
cat > "$PROJECT_WORKTREE/.agents/resume" <<'EOF'
if [ -n "${ANTHROPIC_API_KEY+x}" ]; then exit 44; fi
printf '%s\n' credential-absent >> resume-result
EOF
git -C "$PROJECT_WORKTREE" add .agents/setup .agents/resume
git -C "$PROJECT_WORKTREE" commit -m "Add e2e lifecycle hooks" >/dev/null
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
      volumeMounts:
        - {name: seed, mountPath: /seed}
        - {name: repositories, mountPath: /repos}
  containers:
    - name: git
      image: $ENV_IMAGE
      command: [git, daemon, --reuseaddr, --base-path=/repos, --export-all, --verbose]
      ports:
        - {name: git, containerPort: 9418}
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
EOF
kubectl wait --for=condition=Ready pod/e2e-git-server --timeout=1m
rm -rf "$PROJECT_REPO" "$PROJECT_WORKTREE"
PROJECT_REPO=""
PROJECT_WORKTREE=""
kubectl apply -f - <<'EOF'
apiVersion: swe.dev/v1alpha1
kind: Project
metadata:
  name: e2e
spec:
  repositories:
    - git://e2e-git-server/e2e.git
  templateRef: small
EOF

echo "==> creating project environment + run intent via swe"
printf '%s' "$E2E_AGENT_API_KEY" | bin/swe credentials create e2e-claude --agent claude-code --api-key-stdin
bin/swe run "end-to-end smoke test" --project e2e --credential-profile e2e-claude --wait=false
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
    namespace: default
EOF
PRODUCER_TOKEN=$(kubectl create token e2e-transcript-producer --audience=swe-platform)

echo "==> configuring a console API user"
cat <<'EOF' | kubectl apply -f -
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
    verbs: ["create", "get", "list", "update"]
  - apiGroups: ["swe.dev"]
    resources: ["environments"]
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
    namespace: default
EOF
cat <<'EOF' | kubectl apply -f -
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
    namespace: default
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
kubectl port-forward service/swe-platform-swe-platform-control-plane 18080:80 >/tmp/swe-platform-port-forward.log 2>&1 &
PORT_FORWARD_PID=$!
for _ in $(seq 1 30); do
	if curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null; then
		break
	fi
	sleep 1
done

echo "==> verifying embedded operations console through the control-plane Service"
ROOT_STATUS=$(curl --silent --dump-header /tmp/swe-platform-console-root.headers \
	--output /tmp/swe-platform-console-root.html --write-out '%{http_code}' \
	http://127.0.0.1:18080/)
SPA_STATUS=$(curl --silent --dump-header /tmp/swe-platform-console-spa.headers \
	--output /tmp/swe-platform-console-spa.html --write-out '%{http_code}' \
	"http://127.0.0.1:18080/namespaces/default/runs/${RUN_NAME}/overview")
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
SESSION_GET_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" http://127.0.0.1:18080/api/v1/session)
RUN_LIST_STATUS=$(curl --silent --output /tmp/swe-platform-runs.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" 'http://127.0.0.1:18080/api/v1/namespaces/default/runs?limit=200')
OTHER_RUN_LIST_STATUS=$(curl --silent --output /tmp/swe-platform-other-runs.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" 'http://127.0.0.1:18080/api/v1/namespaces/e2e-console-other/runs?limit=200')
ENV_GET_STATUS=$(curl --silent --output /tmp/swe-platform-environment.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" "http://127.0.0.1:18080/api/v1/namespaces/default/environments/${ENV_NAME}")
if [[ "$SESSION_GET_STATUS" != "200" || "$RUN_LIST_STATUS" != "200" || "$OTHER_RUN_LIST_STATUS" != "200" || "$ENV_GET_STATUS" != "200" ]]; then
	echo "FAIL: typed read API statuses session=${SESSION_GET_STATUS} runs=${RUN_LIST_STATUS} other-runs=${OTHER_RUN_LIST_STATUS} environment=${ENV_GET_STATUS}"
	exit 1
fi
if ! grep -Fq '"name":"e2e-other-namespace-run"' /tmp/swe-platform-other-runs.json || \
	grep -Fq '"name":"e2e-other-namespace-run"' /tmp/swe-platform-runs.json || \
	! grep -Fq "\"name\":\"${RUN_NAME}\"" /tmp/swe-platform-runs.json || \
	grep -Fq "\"name\":\"${RUN_NAME}\"" /tmp/swe-platform-other-runs.json; then
	echo "FAIL: browser-session Run feeds were not isolated by namespace"
	exit 1
fi
if ! grep -Fq '"credentialProfile":"e2e-claude"' /tmp/swe-platform-runs.json || \
	grep -Fq "$PROFILE_UID" /tmp/swe-platform-runs.json || \
	contains_e2e_key /tmp/swe-platform-runs.json || contains_e2e_key /tmp/swe-platform-environment.json; then
	echo "FAIL: typed APIs did not expose only the selected credential profile name"
	exit 1
fi
API_RUN_BODY='{"name":"e2e-api-run","selector":{"template":"small"},"agent":"e2e","prompt":"resource API acceptance"}'
API_CREATE_STATUS=$(curl --silent --output /tmp/swe-platform-api-run.json --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -H 'Origin: http://127.0.0.1:18080' -H 'Content-Type: application/json' \
	-d "$API_RUN_BODY" http://127.0.0.1:18080/api/v1/namespaces/default/runs)
API_RETRY_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -H 'Origin: http://127.0.0.1:18080' -H 'Content-Type: application/json' \
	-d "$API_RUN_BODY" http://127.0.0.1:18080/api/v1/namespaces/default/runs)
CSRF_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -X POST http://127.0.0.1:18080/api/v1/namespaces/default/runs/e2e-api-run/cancel)
API_CANCEL_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" -X POST -H 'Origin: http://127.0.0.1:18080' \
	http://127.0.0.1:18080/api/v1/namespaces/default/runs/e2e-api-run/cancel)
if [[ "$API_CREATE_STATUS" != "201" || "$API_RETRY_STATUS" != "200" || "$CSRF_STATUS" != "403" || "$API_CANCEL_STATUS" != "200" ]]; then
	echo "FAIL: typed mutation API statuses create=${API_CREATE_STATUS} retry=${API_RETRY_STATUS} csrf=${CSRF_STATUS} cancel=${API_CANCEL_STATUS}"
	exit 1
fi
SESSION_DELETE_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" --cookie-jar "$COOKIE_JAR" -X DELETE -H 'Origin: http://127.0.0.1:18080' \
	http://127.0.0.1:18080/api/v1/session)
if [[ "$SESSION_DELETE_STATUS" != "204" ]]; then
	echo "FAIL: session delete status was ${SESSION_DELETE_STATUS}, expected 204"
	exit 1
fi
LOGOUT_REPLAY_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	--cookie "$COOKIE_JAR" http://127.0.0.1:18080/api/v1/session)
if [[ "$LOGOUT_REPLAY_STATUS" != "401" ]]; then
	echo "FAIL: logged-out session replay status was ${LOGOUT_REPLAY_STATUS}, expected 401"
	exit 1
fi

ANONYMOUS_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	"http://127.0.0.1:18080/api/v1/namespaces/default/runs/${RUN_NAME}/transcript")
if [[ "$ANONYMOUS_STATUS" != "401" ]]; then
	echo "FAIL: anonymous transcript status was ${ANONYMOUS_STATUS}, expected 401"
	exit 1
fi
curl --fail --silent --no-buffer --max-time 10 \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/default/runs/${RUN_NAME}/transcript" > /tmp/swe-platform-transcript.out &
STREAM_PID=$!
sleep 1
APPEND_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${PRODUCER_TOKEN}" \
	-H 'Content-Type: application/json' \
	-d '{"type":"output","data":{"text":"e2e transcript event"}}' \
	"http://127.0.0.1:18080/api/v1/namespaces/default/runs/${RUN_NAME}/transcript")
if [[ "$APPEND_STATUS" != "202" ]]; then
	echo "FAIL: run-scoped producer append status was ${APPEND_STATUS}, expected 202"
	exit 1
fi
DENIED_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${PRODUCER_TOKEN}" \
	-H 'Content-Type: application/json' \
	-d '{"type":"output","data":{"text":"forged"}}' \
	http://127.0.0.1:18080/api/v1/namespaces/default/runs/auth-scope-run-b/transcript)
if [[ "$DENIED_STATUS" != "403" ]]; then
	echo "FAIL: cross-run producer append status was ${DENIED_STATUS}, expected 403"
	exit 1
fi
UNKNOWN_STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	http://127.0.0.1:18080/api/v1/namespaces/default/runs/unknown-run/transcript)
if [[ "$UNKNOWN_STATUS" != "404" ]]; then
	echo "FAIL: unknown Run transcript status was ${UNKNOWN_STATUS}, expected 404"
	exit 1
fi
for _ in $(seq 1 30); do
	if grep -q 'e2e transcript event' /tmp/swe-platform-transcript.out; then
		break
	fi
	sleep 1
done
kill "$STREAM_PID" >/dev/null 2>&1 || true
wait "$STREAM_PID" >/dev/null 2>&1 || true
if ! grep -q 'e2e transcript event' /tmp/swe-platform-transcript.out; then
	echo "FAIL: transcript event was not received from the SSE stream"
	cat /tmp/swe-platform-transcript.out
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
printf '%s' "$E2E_ROTATED_AGENT_API_KEY" | bin/swe credentials rotate e2e-claude --api-key-stdin

kubectl delete run "$RUN_NAME" --wait=true >/dev/null
if ! kubectl get environment "$RUN_ENV_NAME" >/dev/null 2>&1; then
	echo "FAIL: deleting Run removed its claimed Environment"
	exit 1
fi

echo "==> verifying shared terminal through swe attach"
printf 'printf terminal-e2e-ok; if [ -n "${ANTHROPIC_API_KEY+x}" ]; then printf credential-present; else printf credential-absent; fi; exit\n' | \
	SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$E2E_BOOTSTRAP_TOKEN" \
	bin/swe attach "$ENV_NAME" > /tmp/swe-platform-terminal.out
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

echo "==> verifying setup runs only once when the pod is recreated"
kubectl delete pod "$POD_NAME" --wait=true >/dev/null
for _ in $(seq 1 30); do
	if kubectl get pod "$POD_NAME" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
kubectl wait --for=condition=Ready pod/"$POD_NAME" --timeout=2m
if ! kubectl exec "$POD_NAME" -- sh -c 'test "$(wc -l < /workspace/setup-result)" -eq 1'; then
	echo "FAIL: .agents/setup ran again for an initialized workspace"
	exit 1
fi

echo "==> verifying pause retains the workspace and resume runs its hook"
bin/swe environment hold "$ENV_NAME"
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
HOLD_REVISION=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle.hold.revision}')
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle.hold.enabled}')" != "true" || -z "$HOLD_REVISION" ]]; then
	echo "FAIL: swe environment hold did not publish enabled revisioned policy"
	exit 1
fi
bin/swe environment release "$ENV_NAME"
RELEASE_REVISION=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle.hold.revision}')
if [[ "$(kubectl get environment "$ENV_NAME" -o jsonpath='{.spec.lifecycle.hold.enabled}')" != "false" || "$RELEASE_REVISION" -le "$HOLD_REVISION" ]]; then
	echo "FAIL: swe environment release did not publish a newer disabled policy"
	exit 1
fi
kubectl wait --for=jsonpath='{.status.phase}'=Ready environment/"$ENV_NAME" --timeout=2m
kubectl wait --for=condition=Ready pod/"$POD_NAME" --timeout=2m
RESUMED_IMAGE_ID=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.imageID}')
RESUMED_POD_IMAGE_ID=$(kubectl get pod "$POD_NAME" -o jsonpath='{.status.containerStatuses[?(@.name=="environment")].imageID}')
if [[ -z "$RESUMED_IMAGE_ID" || "$RESUMED_IMAGE_ID" != "$RESUMED_POD_IMAGE_ID" ]]; then
	echo "FAIL: resumed environment image ID '${RESUMED_IMAGE_ID:-<empty>}' does not match pod image ID '${RESUMED_POD_IMAGE_ID:-<empty>}'"
	exit 1
fi
if ! kubectl exec "$POD_NAME" -- sh -c \
	'test "$(cat /workspace/resume-result)" = credential-absent && test "$(wc -l < /workspace/agent-credential-marker)" -eq 1 && test -z "${ANTHROPIC_API_KEY+x}" && ! tr "\000" "\n" < /proc/1/environ | grep -q "^ANTHROPIC_API_KEY="'; then
	echo "FAIL: resume hook or fresh sandboxd received the agent API key before agent launch"
	exit 1
fi
if ! kubectl exec "$POD_NAME" -- sh -c 'test "$(wc -l < /workspace/setup-result)" -eq 1'; then
	echo "FAIL: .agents/setup ran again while resuming"
	exit 1
fi

echo "==> verifying the rotated key is materialized only for a fresh agent launch"
RESUME_RUN_NAME=e2e-resume-credential-run
bin/swe run "resume credential smoke test" --name "$RESUME_RUN_NAME" --environment "$ENV_NAME" \
	--credential-profile e2e-claude --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$RESUME_RUN_NAME" --timeout=3m
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$RESUME_RUN_NAME" --timeout=3m
RESUME_RUN_UID=$(kubectl get run "$RESUME_RUN_NAME" -o jsonpath='{.metadata.uid}')
if ! kubectl exec "$POD_NAME" -- sh -c \
	'test "$(wc -l < /workspace/agent-credential-marker)" -eq 2 && test -z "${ANTHROPIC_API_KEY+x}" && ! tr "\000" "\n" < /proc/1/environ | grep -q "^ANTHROPIC_API_KEY="'; then
	echo "FAIL: fresh agent launch did not receive the rotated profile in isolation"
	exit 1
fi
check_sandboxd_process "$POD_NAME" "$RESUME_RUN_UID" "$E2E_ROTATED_AGENT_API_KEY"
set +e
curl --silent --no-buffer --max-time 2 \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/default/runs/${RESUME_RUN_NAME}/transcript" \
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
kubectl delete run "$RESUME_RUN_NAME" --wait=true >/dev/null

echo "==> verifying fake Amp real Run lifecycle without credentials or network"
AMP_RUN_NAME=e2e-fake-amp-run
bin/swe run "fake Amp lifecycle smoke test" --name "$AMP_RUN_NAME" --environment "$ENV_NAME" \
	--agent amp --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$AMP_RUN_NAME" --timeout=3m
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$AMP_RUN_NAME" --timeout=3m
set +e
curl --silent --no-buffer --max-time 2 \
	-H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/default/runs/${AMP_RUN_NAME}/transcript" \
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
	! grep -Fq 'fake-amp-stderr-marker' /tmp/swe-platform-amp-process-output.out; then
	echo "FAIL: Amp transcript did not retain both output stream markers"
	cat /tmp/swe-platform-amp-process-output.out
	exit 1
fi
kubectl delete run "$AMP_RUN_NAME" --wait=true >/dev/null

echo "==> verifying fake Codex real Run lifecycle without credentials or network"
CODEX_RUN_NAME=e2e-fake-codex-run
bin/swe run "fake Codex lifecycle smoke test" --name "$CODEX_RUN_NAME" --environment "$ENV_NAME" --agent codex --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Running run/"$CODEX_RUN_NAME" --timeout=3m
kubectl wait --for=jsonpath='{.status.state}'=Succeeded run/"$CODEX_RUN_NAME" --timeout=3m
set +e
curl --silent --no-buffer --max-time 2 -H "Authorization: Bearer ${E2E_BOOTSTRAP_TOKEN}" \
	"http://127.0.0.1:18080/api/v1/namespaces/default/runs/${CODEX_RUN_NAME}/transcript" > /tmp/swe-platform-codex-transcript.out
CODEX_TRANSCRIPT_STATUS=$?
set -e
if [[ "$CODEX_TRANSCRIPT_STATUS" != "0" && "$CODEX_TRANSCRIPT_STATUS" != "28" ]]; then echo "FAIL: Codex transcript read failed"; exit 1; fi
grep -F '"source":"codex"' /tmp/swe-platform-codex-transcript.out | grep -F '"type":"codex.process-output"' | \
	grep -oE '"data":"[A-Za-z0-9+/=]+"' | sed 's/^"data":"//; s/"$//' | while IFS= read -r encoded; do printf '%s' "$encoded" | base64 --decode || exit 1; done > /tmp/swe-platform-codex-process-output.out
for marker in fake-codex-thread fake-codex-stderr-marker turn.completed; do grep -Fq "$marker" /tmp/swe-platform-codex-process-output.out || { echo "FAIL: missing Codex marker $marker"; exit 1; }; done
kubectl delete run "$CODEX_RUN_NAME" --wait=true >/dev/null

echo "==> verifying fake Codex terminal failure through the real controller"
CODEX_FAILED_RUN_NAME=e2e-fake-codex-failed-run
bin/swe run "fake Codex failure smoke test" --name "$CODEX_FAILED_RUN_NAME" --environment "$ENV_NAME" --agent codex --wait=false
kubectl wait --for=jsonpath='{.status.state}'=Failed run/"$CODEX_FAILED_RUN_NAME" --timeout=3m
kubectl delete run "$CODEX_FAILED_RUN_NAME" --wait=true >/dev/null

echo "==> verifying idle pause and terminal wake through the control plane"
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
printf 'printf web-terminal-e2e-ok; exit\n' | \
	SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$E2E_BOOTSTRAP_TOKEN" \
	bin/swe attach "$ENV_NAME" > /tmp/swe-platform-web-terminal.out
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

POD_PHASE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.status.phase}')
if [[ "$POD_PHASE" != "Running" ]]; then
	echo "FAIL: pod ${POD_NAME} is ${POD_PHASE}, expected Running"
	echo "--- operator log ---"
	kubectl logs deployment/swe-platform-swe-platform --tail=50
	exit 1
fi

echo "==> sandboxd logs from the environment pod"
kubectl logs "$POD_NAME" -c environment | head -3

echo "E2E OK"
