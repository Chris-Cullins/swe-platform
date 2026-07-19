#!/usr/bin/env bash
# End-to-end acceptance: build everything, spin up kind, install CRDs, run the
# operator through Helm, and drive a real environment through the swe CLI.
#
# No registries or cloud credentials required: env-base is built locally and
# loaded into the kind cluster directly.
#
# Prerequisites: go, docker, kind, kubectl, helm.
# Env: KIND_CLUSTER (default swe-e2e), KEEP_CLUSTER=true to skip teardown.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-swe-e2e}"
ENV_IMAGE="ghcr.io/chris-cullins/swe-platform/env-base:dev"
OPERATOR_IMAGE="ghcr.io/chris-cullins/swe-platform/operator:dev"
CONTROL_PLANE_IMAGE="ghcr.io/chris-cullins/swe-platform/control-plane:dev"
PORT_FORWARD_PID=""
WEB_TERMINAL_CLIENT=""
PROJECT_REPO=""
PROJECT_WORKTREE=""

cleanup() {
	if [[ -n "$PORT_FORWARD_PID" ]]; then
		kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
		wait "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
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
	if [[ "${KEEP_CLUSTER:-false}" != "true" ]]; then
		kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

cd "$(dirname "$0")/.."

echo "==> building binaries"
make build

echo "==> creating kind cluster '$CLUSTER'"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER"

echo "==> building platform images"
make docker-build >/dev/null

echo "==> loading images into kind"
kind load docker-image "$ENV_IMAGE" "$OPERATOR_IMAGE" "$CONTROL_PLANE_IMAGE" --name "$CLUSTER"

echo "==> installing operator, CRDs, and kind template through Helm"
E2E_BOOTSTRAP_TOKEN="$(openssl rand -hex 32)"
kubectl create secret generic swe-platform-bootstrap --from-literal=token="$E2E_BOOTSTRAP_TOKEN"
helm upgrade --install swe-platform charts/swe-platform \
	--namespace default --values charts/swe-platform/values-kind.yaml \
	--set controlPlane.auth.bootstrapTokenSecret.name=swe-platform-bootstrap \
	--wait --timeout 2m

echo "==> waiting for warm environment"
kubectl wait --for=jsonpath='{.status.warmPoolReady}'=1 environmenttemplate/small --timeout=2m
WARM_ENV_NAME=$(kubectl get environments -l swe.dev/warm-pool=small -o jsonpath='{.items[0].metadata.name}')
if [[ -z "$WARM_ENV_NAME" ]]; then
	echo "FAIL: warm pool did not create an environment"
	exit 1
fi
WARM_POD_UID=$(kubectl get pod "env-${WARM_ENV_NAME}" -o jsonpath='{.metadata.uid}')

echo "==> creating project configuration"
kubectl create secret generic e2e-project-config --from-literal=SWE_E2E_PROJECT_CONFIG=project-config-ok
PROJECT_REPO="$(mktemp -d /tmp/swe-e2e-project-XXXXXX)"
PROJECT_WORKTREE="$(mktemp -d /tmp/swe-e2e-worktree-XXXXXX)"
git -C "$PROJECT_WORKTREE" init -b main >/dev/null
git -C "$PROJECT_WORKTREE" config user.name "swe e2e"
git -C "$PROJECT_WORKTREE" config user.email "swe-e2e@example.invalid"
mkdir -p "$PROJECT_WORKTREE/.agents"
cat > "$PROJECT_WORKTREE/.agents/setup" <<'EOF'
printf '%s\n' "$SWE_E2E_PROJECT_CONFIG" >> setup-result
EOF
cat > "$PROJECT_WORKTREE/.agents/resume" <<'EOF'
printf '%s\n' "$SWE_E2E_PROJECT_CONFIG" >> resume-result
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
  secretRef:
    name: e2e-project-config
EOF

echo "==> creating project environment + run intent via swe"
bin/swe run "end-to-end smoke test" --project e2e --wait=false
RUN_NAME=$(kubectl get runs -o jsonpath='{.items[0].metadata.name}')
kubectl wait --for=jsonpath='{.status.state}'=Failed run/"$RUN_NAME" --timeout=3m
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

echo "==> verifying live transcript stream through the control plane"
kubectl port-forward service/swe-platform-swe-platform-control-plane 18080:80 >/tmp/swe-platform-port-forward.log 2>&1 &
PORT_FORWARD_PID=$!
for _ in $(seq 1 30); do
	if curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null; then
		break
	fi
	sleep 1
done
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

kubectl delete run "$RUN_NAME" --wait=true >/dev/null
if ! kubectl get environment "$RUN_ENV_NAME" >/dev/null 2>&1; then
	echo "FAIL: deleting Run removed its claimed Environment"
	exit 1
fi

echo "==> verifying shared terminal through swe attach"
printf 'printf terminal-e2e-ok; printf "\\n%%s\\n" "$SWE_E2E_PROJECT_CONFIG"; exit\n' | \
	SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080 SWE_CONTROL_PLANE_TOKEN="$E2E_BOOTSTRAP_TOKEN" \
	bin/swe attach "$ENV_NAME" > /tmp/swe-platform-terminal.out
if ! grep -q 'terminal-e2e-ok' /tmp/swe-platform-terminal.out; then
	echo "FAIL: terminal output was not received through swe attach"
	cat /tmp/swe-platform-terminal.out
	exit 1
fi
if ! grep -q 'project-config-ok' /tmp/swe-platform-terminal.out; then
	echo "FAIL: project Secret was not injected into the environment"
	cat /tmp/swe-platform-terminal.out
	exit 1
fi
POD_NAME=$(kubectl get environment "$ENV_NAME" -o jsonpath='{.status.podName}')
PVC_NAME=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.volumes[?(@.name=="workspace")].persistentVolumeClaim.claimName}')
if ! kubectl exec "$POD_NAME" -- sh -c 'test "$(cat /workspace/setup-result)" = project-config-ok'; then
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
kubectl patch environment "$ENV_NAME" --type=merge -p '{"spec":{"paused":true}}' >/dev/null
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
if ! kubectl get pvc "$PVC_NAME" >/dev/null 2>&1; then
	echo "FAIL: pause removed the workspace PVC"
	exit 1
fi
kubectl patch environment "$ENV_NAME" --type=merge -p '{"spec":{"paused":false}}' >/dev/null
kubectl wait --for=jsonpath='{.status.phase}'=Ready environment/"$ENV_NAME" --timeout=2m
kubectl wait --for=condition=Ready pod/"$POD_NAME" --timeout=2m
if ! kubectl exec "$POD_NAME" -- sh -c 'test "$(cat /workspace/resume-result)" = project-config-ok'; then
	echo "FAIL: .agents/resume did not run with the project Secret"
	exit 1
fi
if ! kubectl exec "$POD_NAME" -- sh -c 'test "$(wc -l < /workspace/setup-result)" -eq 1'; then
	echo "FAIL: .agents/setup ran again while resuming"
	exit 1
fi

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
