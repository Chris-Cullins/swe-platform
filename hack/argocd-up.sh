#!/usr/bin/env bash
# Create (or reuse) the swe-argo kind cluster, install Argo CD and the Image
# Updater, and point an Application at this repository's main branch so the
# cluster always runs the latest platform published from main.
#
# This cluster is separate from the `make kind-up` dev cluster on purpose:
# two operators must never reconcile the same custom resources, and Argo CD
# selfHeal would fight manual dev installs.
#
# Prerequisites: kind, kubectl. Env overrides: KIND_ARGO_CLUSTER (default
# swe-argo), ARGOCD_VERSION, IMAGE_UPDATER_VERSION.
set -euo pipefail

CLUSTER="${KIND_ARGO_CLUSTER:-swe-argo}"
ARGOCD_VERSION="${ARGOCD_VERSION:-v3.4.5}"
IMAGE_UPDATER_VERSION="${IMAGE_UPDATER_VERSION:-v1.2.2}"
ARGOCD_NAMESPACE="argocd"
APP_NAMESPACE="swe-platform-system"
BOOTSTRAP_SECRET="swe-platform-bootstrap"

cd "$(dirname "$0")/.."
KUBECTL=(kubectl --context "kind-$CLUSTER")

if kind get clusters | grep -qx "$CLUSTER"; then
	echo "kind cluster '$CLUSTER' already exists"
else
	kind create cluster --name "$CLUSTER"
fi

"${KUBECTL[@]}" cluster-info >/dev/null

echo "==> installing Argo CD $ARGOCD_VERSION"
"${KUBECTL[@]}" create namespace "$ARGOCD_NAMESPACE" --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
# Server-side apply: the upstream CRDs exceed the client-side last-applied
# annotation size limit.
"${KUBECTL[@]}" apply --server-side --force-conflicts -n "$ARGOCD_NAMESPACE" \
	-f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml"

echo "==> installing Argo CD Image Updater $IMAGE_UPDATER_VERSION"
"${KUBECTL[@]}" apply --server-side --force-conflicts -n "$ARGOCD_NAMESPACE" \
	-f "https://raw.githubusercontent.com/argoproj-labs/argocd-image-updater/${IMAGE_UPDATER_VERSION}/config/install.yaml"

echo "==> waiting for Argo CD"
"${KUBECTL[@]}" -n "$ARGOCD_NAMESPACE" rollout status deployment/argocd-server --timeout=5m
"${KUBECTL[@]}" -n "$ARGOCD_NAMESPACE" rollout status deployment/argocd-repo-server --timeout=5m
"${KUBECTL[@]}" -n "$ARGOCD_NAMESPACE" rollout status deployment/argocd-image-updater-controller --timeout=5m

echo "==> creating control-plane bootstrap credential (out of band)"
"${KUBECTL[@]}" create namespace "$APP_NAMESPACE" --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
if ! "${KUBECTL[@]}" -n "$APP_NAMESPACE" get secret "$BOOTSTRAP_SECRET" >/dev/null 2>&1; then
	"${KUBECTL[@]}" -n "$APP_NAMESPACE" create secret generic "$BOOTSTRAP_SECRET" \
		--from-literal=token="$(openssl rand -hex 32)"
fi

echo "==> applying swe-platform Application and ImageUpdater"
"${KUBECTL[@]}" apply -n "$ARGOCD_NAMESPACE" -f hack/argocd/

if ! curl -sf "https://raw.githubusercontent.com/Chris-Cullins/swe-platform/main/charts/swe-platform/values-argocd.yaml" >/dev/null; then
	cat <<'EOF'

WARNING: charts/swe-platform/values-argocd.yaml is not on origin/main yet.
The Application will stay OutOfSync until this change is pushed; Argo CD
syncs what GitHub serves, not the local working tree.
EOF
fi

cat <<EOF

Cluster '$CLUSTER' is tracking origin/main through Argo CD.

Status:
  ${KUBECTL[*]} -n $ARGOCD_NAMESPACE get application swe-platform
  ${KUBECTL[*]} -n $ARGOCD_NAMESPACE get imageupdater swe-platform

Argo CD UI:
  ${KUBECTL[*]} -n $ARGOCD_NAMESPACE port-forward svc/argocd-server 8443:443
  user: admin
  password: \$(${KUBECTL[*]} -n $ARGOCD_NAMESPACE get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d)

Drive it with the swe CLI:
  ${KUBECTL[*]} -n $APP_NAMESPACE port-forward svc/swe-platform-swe-platform-control-plane 18080:80
  export SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080
  export SWE_CONTROL_PLANE_TOKEN=\$(${KUBECTL[*]} -n $APP_NAMESPACE get secret $BOOTSTRAP_SECRET -o jsonpath='{.data.token}' | base64 -d)
  bin/swe run "fix the flaky tests" -t small

Teardown: kind delete cluster --name $CLUSTER
EOF
