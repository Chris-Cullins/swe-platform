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
{"items":[{"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/chris-cullins/swe-platform/operator:0.1.0","args":[]} ]}}}},{"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/chris-cullins/swe-platform/control-plane:0.1.0","env":[{"name":"SWE_TENANCY_NAMESPACES","value":""},{"name":"SWE_POSTGRES_URL","valueFrom":{"secretKeyRef":{"name":"swe-platform-postgres","key":"url"}}}]}],"volumes":[{"name":"session-keyring","secret":{"secretName":"swe-platform-session-keyring","items":[{"key":"keyring.json"}]}}]}}}}]}
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
