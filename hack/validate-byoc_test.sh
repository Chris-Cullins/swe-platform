#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VALIDATOR="$ROOT/hack/validate-byoc.sh"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "$TEST_ROOT"' EXIT

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

for provider in k3s gke eks; do
	"$VALIDATOR" render "$provider" >"${TMPDIR:-/tmp}/validate-byoc-$provider.out" 2>&1 || {
		cat "${TMPDIR:-/tmp}/validate-byoc-$provider.out" >&2
		fail "render validation failed for $provider"
	}
done

if "$VALIDATOR" render kind >/dev/null 2>&1; then
	fail "development preset was accepted as BYOC production input"
fi
if SWE_IMAGE_TAG=latest "$VALIDATOR" render k3s >/dev/null 2>&1; then
	fail "mutable latest image override was accepted"
fi
SWE_IMAGE_TAG=sha-0123456 "$VALIDATOR" render k3s >/dev/null 2>&1 || fail "traceable main SHA override was rejected"
mapfile -t fallback_args < <(SWE_IMAGE_TAG=sha-0123456 "$VALIDATOR" image-args k3s)
[[ ${#fallback_args[@]} -eq 6 ]] || fail "fallback did not emit three Helm image overrides"
fallback_render="$(helm template swe-platform "$ROOT/charts/swe-platform" --namespace swe-platform-system \
	--values "$ROOT/charts/swe-platform/values-k3s.yaml" "${fallback_args[@]}")"
for image in operator control-plane env-base; do
	grep -q "ghcr.io/chris-cullins/swe-platform/$image:sha-0123456" <<<"$fallback_render" || fail "fallback install args did not select $image SHA tag"
done

digest="sha256:$(printf 'a%.0s' {1..64})"
jq -n --arg digest "$digest" '{images:{operator:{repository:"ghcr.io/chris-cullins/swe-platform/operator",digest:$digest},"control-plane":{repository:"ghcr.io/chris-cullins/swe-platform/control-plane",digest:$digest},"env-base":{repository:"ghcr.io/chris-cullins/swe-platform/env-base",digest:$digest}}}' >"$TEST_ROOT/release-manifest.json"
SWE_RELEASE_MANIFEST="$TEST_ROOT/release-manifest.json" "$VALIDATOR" render gke >/dev/null 2>&1 || fail "release-manifest digest pins were rejected"

cat >"$TEST_ROOT/unsafe-values.yaml" <<'EOF'
tenancy:
  mode: trusted-admin
EOF
if SWE_PRIVATE_VALUES="$TEST_ROOT/unsafe-values.yaml" "$VALIDATOR" render eks >/dev/null 2>&1; then
	fail "trusted-admin private values were accepted by BYOC validation"
fi

cat >"$TEST_ROOT/zero-templates.yaml" <<'EOF'
environmentTemplates: []
EOF
SWE_PRIVATE_VALUES="$TEST_ROOT/zero-templates.yaml" "$VALIDATOR" render k3s >/dev/null 2>&1 || fail "zero catalog templates were rejected"
SWE_PRIVATE_VALUES="$TEST_ROOT/zero-templates.yaml" SWE_RELEASE_MANIFEST="$TEST_ROOT/release-manifest.json" \
	"$VALIDATOR" render k3s >/dev/null 2>&1 || fail "zero templates were rejected with digest pinning"

cat >"$TEST_ROOT/multiple-gvisor-templates.yaml" <<'EOF'
environmentTemplates:
  - name: medium
    spec:
      size: medium
      runtimeClass: gvisor
      idleTimeout: 15m
      diskSize: 40Gi
  - name: large
    spec:
      size: large
      runtimeClass: gvisor
      idleTimeout: 15m
      diskSize: 80Gi
EOF
SWE_PRIVATE_VALUES="$TEST_ROOT/multiple-gvisor-templates.yaml" "$VALIDATOR" render gke >/dev/null 2>&1 || fail "multiple valid GKE templates were rejected"
SWE_PRIVATE_VALUES="$TEST_ROOT/multiple-gvisor-templates.yaml" SWE_RELEASE_MANIFEST="$TEST_ROOT/release-manifest.json" \
	"$VALIDATOR" render gke >/dev/null 2>&1 || fail "repeated digest-pinned environment images were rejected"

cat >"$TEST_ROOT/missing-gvisor-template.yaml" <<'EOF'
environmentTemplates:
  - name: medium
    spec:
      size: medium
      idleTimeout: 15m
      diskSize: 40Gi
EOF
if SWE_PRIVATE_VALUES="$TEST_ROOT/missing-gvisor-template.yaml" "$VALIDATOR" render gke >/dev/null 2>&1; then
	fail "GKE template without runtimeClass was accepted"
fi

sed 's/runtimeClass: gvisor/runtimeClass: runc/' "$TEST_ROOT/multiple-gvisor-templates.yaml" >"$TEST_ROOT/changed-gvisor-templates.yaml"
if SWE_PRIVATE_VALUES="$TEST_ROOT/changed-gvisor-templates.yaml" "$VALIDATOR" render gke >/dev/null 2>&1; then
	fail "GKE template with a changed RuntimeClass was accepted"
fi

mkdir -p "$TEST_ROOT/bin"
cat >"$TEST_ROOT/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  "config current-context") echo byoc-test ;;
  "version -o json") echo '{"serverVersion":{"major":"1","minor":"33+"}}' ;;
  "get --raw /apis/networking.k8s.io/v1") echo '{"resources":[{"name":"networkpolicies"}]}' ;;
  "get storageclass -o json") echo '{"items":[{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}]}' ;;
  "get runtimeclass gvisor") : ;;
  "get namespace swe-platform-system") : ;;
  *"get secret swe-platform-postgres"*) echo -n present ;;
  *"get secret swe-platform-session-keyring"*) echo -n present ;;
  *"get deployments -l app.kubernetes.io/instance=swe-platform -o json")
    cat <<'JSON'
{"items":[{"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/chris-cullins/swe-platform/operator:0.2.0","args":[]} ]}}}},{"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/chris-cullins/swe-platform/control-plane:0.2.0","env":[{"name":"SWE_TENANCY_NAMESPACES","value":""},{"name":"SWE_POSTGRES_URL","valueFrom":{"secretKeyRef":{"name":"swe-platform-postgres","key":"url"}}}]}],"volumes":[{"name":"session-keyring","secret":{"secretName":"swe-platform-session-keyring","items":[{"key":"keyring.json"}]}}]}}}}]}
JSON
    ;;
  *"rollout status deployment -l app.kubernetes.io/instance=swe-platform --timeout=5m") : ;;
  *"wait pods -l app.kubernetes.io/instance=swe-platform --for=condition=Ready --timeout=5m") : ;;
  *"get installation swe-platform-swe-platform") : ;;
  *) echo "unexpected fake kubectl call: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$TEST_ROOT/bin/kubectl"
PATH="$TEST_ROOT/bin:$PATH" "$VALIDATOR" preflight gke >/dev/null 2>&1 || fail "valid mocked GKE preflight failed"

real_helm="$(command -v helm)"
"$real_helm" template swe-platform "$ROOT/charts/swe-platform" --namespace swe-platform-system \
  --values "$ROOT/charts/swe-platform/values-gke.yaml" >"$TEST_ROOT/installed-manifest.yaml"
cat >"$TEST_ROOT/bin/helm" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == status ]]; then
  exit 0
fi
if [[ "$1 $2" == "get manifest" ]]; then
  cat "$FAKE_INSTALLED_MANIFEST"
  exit 0
fi
exec "$REAL_HELM" "$@"
EOF
chmod +x "$TEST_ROOT/bin/helm"
PATH="$TEST_ROOT/bin:$PATH" REAL_HELM="$real_helm" FAKE_INSTALLED_MANIFEST="$TEST_ROOT/installed-manifest.yaml" \
  "$VALIDATOR" installed gke >/dev/null 2>&1 || fail "valid mocked installed release failed"

cat >"$TEST_ROOT/scoped-project-values.yaml" <<'EOF'
tenancy:
  mode: scoped
  namespaces: [test-project]
EOF
if PATH="$TEST_ROOT/bin:$PATH" REAL_HELM="$real_helm" FAKE_INSTALLED_MANIFEST="$TEST_ROOT/installed-manifest.yaml" \
  SWE_PRIVATE_VALUES="$TEST_ROOT/scoped-project-values.yaml" "$VALIDATOR" installed gke >/dev/null 2>&1; then
	fail "installed validation accepted a stale scoped namespace allowlist"
fi

echo "BYOC validator covers production renders, digest pins, unsafe values, preflight, and installed state"
