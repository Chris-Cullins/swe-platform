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

cleanup() {
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
kind load docker-image "$ENV_IMAGE" "$OPERATOR_IMAGE" --name "$CLUSTER"

echo "==> installing operator, CRDs, and kind template through Helm"
helm upgrade --install swe-platform charts/swe-platform \
	--namespace default --values charts/swe-platform/values-kind.yaml --wait --timeout 2m

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
	kubectl logs deployment/swe-platform-swe-platform --tail=50
	exit 1
fi

echo "==> sandboxd logs from the environment pod"
kubectl logs "env-${ENV_NAME}" -c environment | head -3

echo "E2E OK"
