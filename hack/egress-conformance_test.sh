#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
h=hack/egress-conformance.sh
felix=hack/egress-conformance-felix-check.sh
tmp=$(mktemp -d)
trap '[[ -z "${server_pid:-}" ]] || kill "$server_pid" >/dev/null 2>&1 || true; rm -rf "$tmp"' EXIT

bash -n "$h" hack/egress-conformance_test.sh
sh -n "$felix"
if EGRESS_CONFORMANCE_EXPERIMENTAL=1 EGRESS_CONFORMANCE_EXPECTED_KIND_CLUSTER=must-not-be-used EGRESS_CONFORMANCE_EXPECTED_CONTEXT=must-not-be-used EGRESS_CONFORMANCE_EXPECTED_CLUSTER_UID=must-not-be-used \
	env -u EGRESS_CONFORMANCE_EXPERIMENTAL -u EGRESS_CONFORMANCE_EXPECTED_KIND_CLUSTER -u EGRESS_CONFORMANCE_EXPECTED_CONTEXT -u EGRESS_CONFORMANCE_EXPECTED_CLUSTER_UID \
	"$h" run >"$tmp/off" 2>&1; then echo "runner was not default-off" >&2; exit 1; fi
grep -q 'experimental runner disabled' "$tmp/off"

# The runner is isolated, one-shot, diagnostic-only, and materially smaller than
# the rejected persisted-proof implementation.
(( $(wc -l <"$h") < 300 ))
grep -q "SELECTOR='swe.dev/egress-conformance=eligible'" "$h"
grep -q "CALICO_MANIFEST_SHA256='[0-9a-f]\{64\}'" "$h"
grep -q 'kind-calico-conformance.yaml' "$h"
grep -q 'EGRESS_CONFORMANCE_EXPECTED_KIND_CLUSTER' "$h"
grep -q 'kind get clusters' "$h"
grep -q 'kind get kubeconfig --name "\$KIND_CLUSTER"' "$h"
grep -q 'active context server or CA differs from exact kind kubeconfig' "$h"
grep -q 'kubectl() { command kubectl --context "\$CONTEXT"' "$h"
grep -q 'timeout 8 kubectl --context "\$CONTEXT"' "$h"
grep -q 'kubectl delete namespace "\$NS"' "$h"
[[ "$(grep -c 'command kubectl' "$h")" == 1 ]]
grep -q 'kind fixture must have one control-plane and two exact eligible v1.35.0 workers' "$h"
grep -q 'validating admission webhooks are unsupported' "$h"
grep -q 'mutating admission webhooks are unsupported' "$h"
grep -q 'aggregated APIServices are unsupported' "$h"
grep -q 'unexpected Calico daemonset/container shape' "$h"
grep -q 'unexpected Calico pod/image shape' "$h"
grep -q 'unexpected Calico kube-controller shape' "$h"
grep -q 'effective Felix override' "$h"
grep -q 'Namespace-derived Profile inputs changed' "$h"
grep -q 'ServiceAccount-derived Profile inputs changed' "$h"
grep -q 'HostEndpoints are unsupported' "$h"
grep -q 'Tier inventory is not canonical' "$h"
grep -q 'fixture Calico policy inventory changed' "$h"
grep -q 'swe.dev/egress-role ==.*probe' "$h"
grep -q 'selector:.*allowed.*ports: \[8443\]' "$h"
grep -q 'server_pod.*allowed.*tcp:8443,8444 udp:8443' "$h"
grep -q 'server_pod.*udp.*udp:53,443' "$h"
grep -q 'expect_denied.*proxy-wrong-port' "$h"
grep -q 'expect_denied.*non-proxy-8443' "$h"
grep -q 'expect_denied.*resident-node-live-port' "$h"
grep -q 'resident-node-live-port.*"\$other_control"' "$h"
grep -q 'expect_denied.*metadata' "$h"
grep -q 'expect_denied.*direct-public-443' "$h"
grep -q 'expect_denied.*direct-public-8443' "$h"
grep -q 'expect_denied.*UDP-53' "$h"
grep -q 'expect_denied.*UDP-443' "$h"
grep -q 'expect_denied.*IPv6-direct' "$h"
grep -q 'expect_denied.*Kubernetes-Service' "$h"
grep -q 'expect_denied.*direct-API' "$h"
grep -q 'direct-API.*"\$control".*"\$api"' "$h"
! grep -q 'direct-API.*resident' "$h"
grep -q 'selected-policy post-check' "$h"
grep -q 'target post-check' "$h"
! grep -Eq 'schemaVersion|expiresAt|OUTPUT_DIR|expected-policy-inventory|trusted.digest' "$h"
! test -e cmd/egress-proof-validator
! test -e internal/egressproof
! grep -q 'egress-conformance.sh' hack/e2e.sh
! grep -Rqs 'egress-conformance.sh' charts/swe-platform/templates charts/swe-platform/values*.yaml
! grep -Rqs 'egress-conformance.sh run' .github/workflows

# Fixture protocol exercises TCP and UDP over both families without Kubernetes.
python3 -m py_compile hack/egress-conformance-client.py hack/egress-conformance-server.py
python3 hack/egress-conformance-server.py tcp:18443 udp:18444 >"$tmp/server.log" 2>&1 &
server_pid=$!
sleep .2
for endpoint in 127.0.0.1 ::1; do
	[[ "$(python3 hack/egress-conformance-client.py once tcp "$endpoint" 18443)" == REACHED ]]
	[[ "$(python3 hack/egress-conformance-client.py once udp "$endpoint" 18444)" == REACHED ]]
done
[[ "$(python3 hack/egress-conformance-client.py once tcp 127.0.0.1 18445)" == UNREACHED ]]
kill "$server_pid"; wait "$server_pid" || true; server_pid=

echo "experimental egress conformance runner guards passed"
