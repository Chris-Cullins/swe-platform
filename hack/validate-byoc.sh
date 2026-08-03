#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHART="${SWE_CHART:-$ROOT/charts/swe-platform}"
SYSTEM_NAMESPACE="${SWE_SYSTEM_NAMESPACE:-swe-platform-system}"
RELEASE="${SWE_RELEASE:-swe-platform}"

usage() {
	cat <<'EOF'
Usage: hack/validate-byoc.sh <render|preflight|installed> <k3s|gke|eks>

  render      lint and render the selected production preset without a cluster
  preflight   render, then validate the current cluster's supported prerequisites
  installed   preflight, then validate the installed Helm release and workloads

Environment:
  SWE_CHART             chart directory (default: charts/swe-platform)
  SWE_SYSTEM_NAMESPACE system namespace (default: swe-platform-system)
  SWE_RELEASE           Helm release name (default: swe-platform)
  SWE_PRIVATE_VALUES    persisted site values file
  SWE_RELEASE_MANIFEST  downloaded release-manifest.json for digest pinning
  SWE_IMAGE_TAG         coordinated traceable tag fallback, exactly sha-<7 hex>
EOF
}

fail() {
	echo "ERROR: $*" >&2
	exit 1
}

need() {
	command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

warn_security_boundary() {
	cat >&2 <<'EOF'
WARNING: this validates deployability, not production isolation. Default-deny
egress, the authenticated egress proxy, and the complete credential/isolation
contracts remain open in issues #68, #9, and #10.
EOF
}

mode="${1:-}"
provider="${2:-}"
[[ $# -eq 2 ]] || { usage >&2; exit 2; }
[[ "$mode" =~ ^(render|preflight|installed)$ ]] || fail "unknown mode: $mode"
[[ "$provider" =~ ^(k3s|gke|eks)$ ]] || fail "unsupported provider preset: $provider"

need helm
values="$CHART/values-$provider.yaml"
[[ -r "$values" ]] || fail "preset not found: $values"

helm_args=(--namespace "$SYSTEM_NAMESPACE" --values "$values")
if [[ -n "${SWE_PRIVATE_VALUES:-}" ]]; then
	[[ -r "$SWE_PRIVATE_VALUES" ]] || fail "private values not found: $SWE_PRIVATE_VALUES"
	helm_args+=(--values "$SWE_PRIVATE_VALUES")
fi
if [[ -n "${SWE_RELEASE_MANIFEST:-}" ]]; then
	need jq
	[[ -r "$SWE_RELEASE_MANIFEST" ]] || fail "release manifest not found: $SWE_RELEASE_MANIFEST"
	for image in operator control-plane env-base; do
		repository="$(jq -er --arg image "$image" '.images[$image].repository' "$SWE_RELEASE_MANIFEST")" || fail "release manifest has no $image repository"
		digest="$(jq -er --arg image "$image" '.images[$image].digest | select(test("^sha256:[0-9a-f]{64}$"))' "$SWE_RELEASE_MANIFEST")" || fail "release manifest has no valid $image digest"
		[[ "$repository" == "ghcr.io/chris-cullins/swe-platform/$image" ]] || fail "unexpected $image repository in release manifest: $repository"
		case "$image" in
		operator) helm_args+=(--set-string "image.digest=$digest") ;;
		control-plane) helm_args+=(--set-string "controlPlane.image.digest=$digest") ;;
		env-base) helm_args+=(--set-string "environmentImage.digest=$digest") ;;
		esac
	done
elif [[ -n "${SWE_IMAGE_TAG:-}" ]]; then
	[[ "$SWE_IMAGE_TAG" =~ ^sha-[0-9a-f]{7}$ ]] || fail "SWE_IMAGE_TAG must match sha-<7 lowercase hex>; use SWE_RELEASE_MANIFEST for immutable digest pinning"
	helm_args+=(
		--set-string "image.tag=$SWE_IMAGE_TAG"
		--set-string "controlPlane.image.tag=$SWE_IMAGE_TAG"
		--set-string "environmentImage.tag=$SWE_IMAGE_TAG"
	)
fi

helm lint "$CHART" --values "$values" >/dev/null
rendered="$(helm template "$RELEASE" "$CHART" "${helm_args[@]}")"
grep -q -- '--tenancy-mode=scoped' <<<"$rendered" || fail "effective values must use scoped tenancy"
grep -A1 'name: SWE_SESSION_BACKEND' <<<"$rendered" | grep -q 'value: "postgres"' || fail "preset does not select PostgreSQL sessions"
grep -A5 'name: SWE_POSTGRES_URL' <<<"$rendered" | grep -q 'secretKeyRef:' || fail "preset does not source PostgreSQL from a Secret"
postgres_secret="$(awk '/name: SWE_POSTGRES_URL/{found=1; next} found && /name:/{gsub(/[" ]/, "", $2); print $2; exit}' <<<"$rendered")"
postgres_key="$(awk '/name: SWE_POSTGRES_URL/{found=1; next} found && /key:/{gsub(/[" ]/, "", $2); print $2; exit}' <<<"$rendered")"
keyring_secret="$(awk '/secretName:/{gsub(/[" ]/, "", $2); print $2; exit}' <<<"$rendered")"
keyring_key="$(awk '/secretName:/{found=1; next} found && /key:/{value=($1 == "-" ? $3 : $2); gsub(/[" ]/, "", value); print value; exit}' <<<"$rendered")"
[[ -n "$postgres_secret" && -n "$postgres_key" && -n "$keyring_secret" && -n "$keyring_key" ]] || fail "cannot resolve effective PostgreSQL/keyring Secret references"

mapfile -t images < <(awk '$1 == "image:" {gsub(/"/, "", $2); print $2}' <<<"$rendered")
[[ ${#images[@]} -eq 3 ]] || fail "expected exactly three coordinated images, found ${#images[@]}"
for image in "${images[@]}"; do
	[[ "$image" != *:latest && "$image" != *:dev ]] || fail "mutable image rendered: $image"
done
if [[ -n "${SWE_RELEASE_MANIFEST:-}" ]]; then
	for image in operator control-plane env-base; do
		digest="$(jq -r --arg image "$image" '.images[$image].digest' "$SWE_RELEASE_MANIFEST")"
		grep -q "ghcr.io/chris-cullins/swe-platform/$image@$digest" <<<"$rendered" || fail "$image is not pinned to its release-manifest digest"
	done
fi

echo "render validation passed for values-$provider.yaml"
warn_security_boundary
[[ "$mode" == render ]] && exit 0

need kubectl
need jq
context="$(kubectl config current-context)"
[[ -n "$context" ]] || fail "kubectl has no current context"
echo "validating current context: $context"

server_version="$(kubectl version -o json | jq -er '.serverVersion | [(.major | tonumber), (.minor | sub("[^0-9].*$"; "") | tonumber)]')" || fail "cannot read Kubernetes server version"
server_major="$(jq -r '.[0]' <<<"$server_version")"
server_minor="$(jq -r '.[1]' <<<"$server_version")"
(( server_major > 1 || (server_major == 1 && server_minor >= 33) )) || fail "Kubernetes 1.33 or newer is required"

kubectl get --raw /apis/networking.k8s.io/v1 | jq -e '.resources[] | select(.name == "networkpolicies")' >/dev/null || fail "NetworkPolicy API is unavailable"
default_storage_classes="$(kubectl get storageclass -o json | jq '[.items[] | select(.metadata.annotations["storageclass.kubernetes.io/is-default-class"] == "true" or .metadata.annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true")] | length')"
(( default_storage_classes > 0 )) || fail "no default StorageClass is configured"

if [[ "$provider" == gke ]]; then
	kubectl get runtimeclass gvisor >/dev/null || fail "the GKE preset requires RuntimeClass gvisor"
else
	echo "NOTICE: values-$provider.yaml uses the cluster default runtime; it does not provide a sandbox RuntimeClass" >&2
fi

kubectl get namespace "$SYSTEM_NAMESPACE" >/dev/null || fail "system namespace $SYSTEM_NAMESPACE does not exist"
postgres_present="$(kubectl -n "$SYSTEM_NAMESPACE" get secret "$postgres_secret" -o go-template="{{if index .data \"$postgres_key\"}}present{{end}}")" || fail "Secret $postgres_secret with key $postgres_key is required"
[[ "$postgres_present" == present ]] || fail "Secret $postgres_secret has an empty $postgres_key key"
keyring_present="$(kubectl -n "$SYSTEM_NAMESPACE" get secret "$keyring_secret" -o go-template="{{if index .data \"$keyring_key\"}}present{{end}}")" || fail "Secret $keyring_secret with key $keyring_key is required"
[[ "$keyring_present" == present ]] || fail "Secret $keyring_secret has an empty $keyring_key key"
unset postgres_present keyring_present

echo "cluster preflight passed for values-$provider.yaml"
[[ "$mode" == preflight ]] && exit 0

helm status "$RELEASE" --namespace "$SYSTEM_NAMESPACE" >/dev/null || fail "Helm release $RELEASE is not installed"
installed_manifest="$(helm get manifest "$RELEASE" --namespace "$SYSTEM_NAMESPACE")"
grep -q -- '--tenancy-mode=scoped' <<<"$installed_manifest" || fail "installed release does not use scoped tenancy"
grep -A1 'name: SWE_SESSION_BACKEND' <<<"$installed_manifest" | grep -q 'value: "postgres"' || fail "installed release does not use PostgreSQL sessions"
expected_namespaces="$(awk '$2 ~ /^--tenancy-namespace=/{print $2}' <<<"$rendered" | sort)"
installed_namespaces="$(awk '$2 ~ /^--tenancy-namespace=/{print $2}' <<<"$installed_manifest" | sort)"
[[ "$installed_namespaces" == "$expected_namespaces" ]] || fail "installed scoped namespace allowlist differs from effective values"
expected_control_namespaces="$(awk '/name: SWE_TENANCY_NAMESPACES/{found=1; next} found && /value:/{gsub(/[" ]/, "", $2); print $2; exit}' <<<"$rendered")"
installed_control_namespaces="$(awk '/name: SWE_TENANCY_NAMESPACES/{found=1; next} found && /value:/{gsub(/[" ]/, "", $2); print $2; exit}' <<<"$installed_manifest")"
[[ "$installed_control_namespaces" == "$expected_control_namespaces" ]] || fail "installed control-plane namespace allowlist differs from effective values"
installed_postgres_secret="$(awk '/name: SWE_POSTGRES_URL/{found=1; next} found && /name:/{gsub(/[" ]/, "", $2); print $2; exit}' <<<"$installed_manifest")"
installed_postgres_key="$(awk '/name: SWE_POSTGRES_URL/{found=1; next} found && /key:/{gsub(/[" ]/, "", $2); print $2; exit}' <<<"$installed_manifest")"
installed_keyring_secret="$(awk '/secretName:/{gsub(/[" ]/, "", $2); print $2; exit}' <<<"$installed_manifest")"
installed_keyring_key="$(awk '/secretName:/{found=1; next} found && /key:/{value=($1 == "-" ? $3 : $2); gsub(/[" ]/, "", value); print value; exit}' <<<"$installed_manifest")"
[[ "$installed_postgres_secret:$installed_postgres_key" == "$postgres_secret:$postgres_key" ]] || fail "installed PostgreSQL Secret reference differs from effective values"
[[ "$installed_keyring_secret:$installed_keyring_key" == "$keyring_secret:$keyring_key" ]] || fail "installed keyring Secret reference differs from effective values"
for image in "${images[@]}"; do
	grep -Fq "$image" <<<"$installed_manifest" || fail "installed manifest does not use expected image $image"
done
live_deployments="$(kubectl -n "$SYSTEM_NAMESPACE" get deployments -l "app.kubernetes.io/instance=$RELEASE" -o json)"
expected_workload_images="$(printf '%s\n' "${images[@]}" | grep -E '/(operator|control-plane)(@|:)' | sort)"
live_workload_images="$(jq -r '.items[].spec.template.spec.containers[].image' <<<"$live_deployments" | sort)"
[[ "$live_workload_images" == "$expected_workload_images" ]] || fail "live workload images differ from effective values"
live_namespaces="$(jq -r '.items[].spec.template.spec.containers[].args[]? | select(startswith("--tenancy-namespace="))' <<<"$live_deployments" | sort)"
[[ "$live_namespaces" == "$expected_namespaces" ]] || fail "live operator namespace allowlist differs from effective values"
live_control_namespaces="$(jq -r '.items[].spec.template.spec.containers[].env[]? | select(.name == "SWE_TENANCY_NAMESPACES") | .value' <<<"$live_deployments")"
[[ "$live_control_namespaces" == "$expected_control_namespaces" ]] || fail "live control-plane namespace allowlist differs from effective values"
live_postgres_ref="$(jq -r '.items[].spec.template.spec.containers[].env[]? | select(.name == "SWE_POSTGRES_URL") | "\(.valueFrom.secretKeyRef.name):\(.valueFrom.secretKeyRef.key)"' <<<"$live_deployments")"
live_keyring_ref="$(jq -r '.items[].spec.template.spec.volumes[]? | select(.name == "session-keyring") | "\(.secret.secretName):\(.secret.items[0].key)"' <<<"$live_deployments")"
[[ "$live_postgres_ref" == "$postgres_secret:$postgres_key" ]] || fail "live PostgreSQL Secret reference differs from effective values"
[[ "$live_keyring_ref" == "$keyring_secret:$keyring_key" ]] || fail "live keyring Secret reference $live_keyring_ref differs from effective values $keyring_secret:$keyring_key"
kubectl -n "$SYSTEM_NAMESPACE" rollout status deployment \
	-l "app.kubernetes.io/instance=$RELEASE" --timeout=5m
kubectl -n "$SYSTEM_NAMESPACE" wait pods \
	-l "app.kubernetes.io/instance=$RELEASE" --for=condition=Ready --timeout=5m
installation="$(awk -v RS='---' '/kind: Installation/{for (i=1;i<=NF;i++) if ($i == "name:") {print $(i+1); exit}}' <<<"$installed_manifest")"
[[ -n "$installation" ]] || fail "installed manifest has no Installation identity"
kubectl -n "$SYSTEM_NAMESPACE" get installation "$installation" >/dev/null || fail "Installation identity is missing"

echo "installed release validation passed for $RELEASE in $SYSTEM_NAMESPACE"
