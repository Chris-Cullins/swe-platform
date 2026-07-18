#!/usr/bin/env bash
# Create (or reuse) the local kind dev cluster.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-swe-dev}"

if kind get clusters | grep -qx "$CLUSTER"; then
	echo "kind cluster '$CLUSTER' already exists"
else
	kind create cluster --name "$CLUSTER"
fi

kubectl cluster-info --context "kind-$CLUSTER" >/dev/null

cat <<EOF

Cluster '$CLUSTER' is ready.

Next steps:
  make install-crds   # install the swe.dev CRDs
  make run            # run the operator locally against the cluster
  bin/swe run "fix the flaky tests" -t <template-name>

Note: gVisor (runsc) is not installed on kind nodes by default, so the local
dev flow runs environments without a RuntimeClass. That matches the `kind`
values preset; gVisor is expected on anything real.
EOF
