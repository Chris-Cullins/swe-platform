#!/usr/bin/env bash
# Create (or reuse) the swe-argo kind cluster, install Argo CD and the Image
# Updater, and point an Application at this repository's main branch so the
# cluster always runs the latest platform published from main.
#
# This cluster is separate from the `make kind-up` dev cluster on purpose:
# two operators must never reconcile the same custom resources, and Argo CD
# selfHeal would fight manual dev installs.
#
# Prerequisites: kind, kubectl, and a container runtime that gives one kind node
# at least 5 CPUs and 6 GiB allocatable. Env overrides: KIND_ARGO_CLUSTER
# (default swe-argo), ARGOCD_VERSION, IMAGE_UPDATER_VERSION.
set -euo pipefail

CLUSTER="${KIND_ARGO_CLUSTER:-swe-argo}"
ARGOCD_VERSION="${ARGOCD_VERSION:-v3.4.5}"
IMAGE_UPDATER_VERSION="${IMAGE_UPDATER_VERSION:-v1.2.2}"
# Two tiny Environments reserve 2 CPUs/4 GiB during warm-pool replacement; the
# remainder covers the observed Argo, Kubernetes, and swe-platform workloads.
MIN_NODE_CPU_MILLICORES=5000
MIN_NODE_MEMORY_MIB=6144
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

capacity_ok=false
capacity_summary=()
while IFS='|' read -r node cpu memory; do
	[[ -n "$node" ]] || continue
	cpu_millicores="$(awk -v value="$cpu" 'BEGIN {
		if (value ~ /m$/) { sub(/m$/, "", value); print int(value) }
		else { print int(value * 1000) }
	}')"
	memory_mib="$(awk -v value="$memory" 'BEGIN {
		if (value ~ /Ki$/) { sub(/Ki$/, "", value); print int(value / 1024) }
		else if (value ~ /Mi$/) { sub(/Mi$/, "", value); print int(value) }
		else if (value ~ /Gi$/) { sub(/Gi$/, "", value); print int(value * 1024) }
		else { print int(value / 1048576) }
	}')"
	capacity_summary+=("$node: ${cpu_millicores}m CPU, ${memory_mib}Mi memory allocatable")
	if (( cpu_millicores >= MIN_NODE_CPU_MILLICORES && memory_mib >= MIN_NODE_MEMORY_MIB )); then
		capacity_ok=true
	fi
done < <("${KUBECTL[@]}" get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.status.allocatable.cpu}{"|"}{.status.allocatable.memory}{"\n"}{end}')

if [[ "$capacity_ok" != true ]]; then
	printf 'kind cluster %q needs one node with at least 5 CPUs and 6 GiB allocatable before installing the Argo demo.\n' "$CLUSTER" >&2
	printf 'The warm pool intentionally keeps one 1-CPU tiny Environment ready and creates a second during a claim; Argo and system workloads need the remaining capacity.\n' >&2
	printf 'Increase the CPU/memory available to your container runtime, delete this undersized cluster, and run make argocd-up again.\n' >&2
	printf 'Observed: %s\n' "${capacity_summary[@]:-no nodes}" >&2
	exit 1
fi
printf '==> capacity check passed: %s\n' "${capacity_summary[@]}"

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
  KIND_ARGO_CLUSTER=$CLUSTER make argocd-ui
  export SWE_CONTROL_PLANE_URL=http://127.0.0.1:18080
  export SWE_CONTROL_PLANE_TOKEN=\$(${KUBECTL[*]} -n $APP_NAMESPACE get secret $BOOTSTRAP_SECRET -o jsonpath='{.data.token}' | base64 -d)
  bin/swe run "fix the flaky tests" -t small

Teardown: kind delete cluster --name $CLUSTER
EOF
