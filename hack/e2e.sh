#!/usr/bin/env bash
# End-to-end acceptance: build everything, spin up kind, install CRDs, run the
# operator, and drive a real environment through the swe CLI.
#
# No registries or cloud credentials required: env-base is built locally and
# loaded into the kind cluster directly.
#
# Prerequisites: go, docker, kind, kubectl.
# Env: KIND_CLUSTER (default swe-e2e), KEEP_CLUSTER=true to skip teardown.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-swe-e2e}"
IMAGE="ghcr.io/chris-cullins/swe-platform/env-base:dev"
OPERATOR_PID=""

cleanup() {
	if [[ -n "$OPERATOR_PID" ]]; then
		kill "$OPERATOR_PID" 2>/dev/null || true
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

echo "==> building env-base image"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -C sandboxd -o ../bin/env-base/sandboxd ./cmd/sandboxd
docker build -t "$IMAGE" -f images/env-base/Dockerfile bin/env-base >/dev/null

echo "==> loading image into kind"
kind load docker-image "$IMAGE" --name "$CLUSTER"

echo "==> installing CRDs and samples"
make install-crds >/dev/null
kubectl apply -f config/samples/

echo "==> starting operator (logs: /tmp/swe-e2e-operator.log)"
go run ./cmd/operator >/tmp/swe-e2e-operator.log 2>&1 &
OPERATOR_PID=$!
sleep 8

echo "==> creating environment + run via swe"
bin/swe run "end-to-end smoke test" -t small --timeout 3m

echo "==> verifying state"
kubectl get environments
kubectl get pods -l app.kubernetes.io/managed-by=swe-platform

ENV_NAME=$(kubectl get environments -o jsonpath='{.items[0].metadata.name}')
POD_PHASE=$(kubectl get pod "env-${ENV_NAME}" -o jsonpath='{.status.phase}')
if [[ "$POD_PHASE" != "Running" ]]; then
	echo "FAIL: pod env-${ENV_NAME} is ${POD_PHASE}, expected Running"
	echo "--- operator log ---"
	tail -50 /tmp/swe-e2e-operator.log
	exit 1
fi

echo "==> sandboxd logs from the environment pod"
kubectl logs "env-${ENV_NAME}" -c environment | head -3

echo "E2E OK"
