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

cleanup() {
	if [[ -n "$PORT_FORWARD_PID" ]]; then
		kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
		wait "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
	fi
	if [[ -n "$WEB_TERMINAL_CLIENT" ]]; then
		rm -f "$WEB_TERMINAL_CLIENT"
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
helm upgrade --install swe-platform charts/swe-platform \
	--namespace default --values charts/swe-platform/values-kind.yaml --wait --timeout 2m

echo "==> creating project configuration"
kubectl create secret generic e2e-project-config --from-literal=SWE_E2E_PROJECT_CONFIG=project-config-ok
kubectl apply -f - <<'EOF'
apiVersion: swe.dev/v1alpha1
kind: Project
metadata:
  name: e2e
spec:
  repositories:
    - https://github.com/Chris-Cullins/swe-platform
  templateRef: small
  secretRef:
    name: e2e-project-config
EOF

echo "==> creating project environment + run via swe"
bin/swe run "end-to-end smoke test" --project e2e --timeout 3m

echo "==> verifying state"
kubectl get environments
kubectl get pods -l app.kubernetes.io/managed-by=swe-platform

echo "==> verifying live transcript stream through the control plane"
kubectl port-forward service/swe-platform-swe-platform-control-plane 18080:80 >/tmp/swe-platform-port-forward.log 2>&1 &
PORT_FORWARD_PID=$!
for _ in $(seq 1 30); do
	if curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null; then
		break
	fi
	sleep 1
done
curl --fail --silent --no-buffer --max-time 10 \
	http://127.0.0.1:18080/api/v1/runs/e2e-run/transcript > /tmp/swe-platform-transcript.out &
STREAM_PID=$!
sleep 1
curl --fail --silent \
	-H 'Content-Type: application/json' \
	-d '{"type":"output","data":{"text":"e2e transcript event"}}' \
	http://127.0.0.1:18080/api/v1/runs/e2e-run/transcript >/dev/null
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

ENV_NAME=$(kubectl get environments -o jsonpath='{.items[0].metadata.name}')
echo "==> verifying shared terminal through swe attach"
printf 'printf terminal-e2e-ok; printf "\\n%%s\\n" "$SWE_E2E_PROJECT_CONFIG"; exit\n' | bin/swe attach "$ENV_NAME" > /tmp/swe-platform-terminal.out
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

echo "==> verifying web terminal through the control plane"
WEB_TERMINAL_CLIENT=$(mktemp /tmp/swe-web-terminal-XXXXXX.go)
cat > "$WEB_TERMINAL_CLIENT" <<'EOF'
package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	connection, _, err := websocket.DefaultDialer.Dial(os.Args[1], nil)
	if err != nil { panic(err) }
	defer connection.Close()
	_ = connection.SetReadDeadline(time.Now().Add(15 * time.Second))
	if err := connection.WriteJSON(map[string]any{"type": "open", "cols": 80, "rows": 24}); err != nil { panic(err) }
	if err := connection.WriteMessage(websocket.BinaryMessage, []byte("printf web-terminal-e2e-ok\n")); err != nil { panic(err) }
	var output strings.Builder
	for !strings.Contains(output.String(), "web-terminal-e2e-ok") {
		messageType, data, err := connection.ReadMessage()
		if err != nil { panic(err) }
		if messageType == websocket.BinaryMessage { _, _ = output.Write(data) }
	}
	fmt.Print(output.String())
}
EOF
go run "$WEB_TERMINAL_CLIENT" "ws://127.0.0.1:18080/api/v1/environments/${ENV_NAME}/terminal" > /tmp/swe-platform-web-terminal.out
if ! grep -q 'web-terminal-e2e-ok' /tmp/swe-platform-web-terminal.out; then
	echo "FAIL: terminal output was not received through the control-plane websocket"
	cat /tmp/swe-platform-web-terminal.out
	exit 1
fi

POD_PHASE=$(kubectl get pod "env-${ENV_NAME}" -o jsonpath='{.status.phase}')
if [[ "$POD_PHASE" != "Running" ]]; then
	echo "FAIL: pod env-${ENV_NAME} is ${POD_PHASE}, expected Running"
	echo "--- operator log ---"
	kubectl logs deployment/swe-platform-swe-platform --tail=50
	exit 1
fi

echo "==> sandboxd logs from the environment pod"
kubectl logs "env-${ENV_NAME}" -c environment | head -3

echo "E2E OK"
