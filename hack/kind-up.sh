#!/usr/bin/env bash
# Create (or reuse) the local kind dev cluster with gVisor and snapshot-capable CSI.
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-swe-dev}"
CONTEXT="kind-$CLUSTER"
GVISOR_RELEASE="20260714"
SNAPSHOTTER_VERSION="v8.6.0"
HOSTPATH_VERSION="v1.18.0"
HOSTPATH_DEPLOYMENT="kubernetes-1.34"
TMP_DIR=""
SMOKE_NAMESPACE=""

cleanup() {
	if [[ -n "$TMP_DIR" ]]; then
		rm -rf "$TMP_DIR"
	fi
	if [[ -n "$SMOKE_NAMESPACE" ]]; then
		kubectl --context "$CONTEXT" delete namespace "$SMOKE_NAMESPACE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

for command in curl docker git kind kubectl; do
	if ! command -v "$command" >/dev/null 2>&1; then
		echo "required command not found: $command" >&2
		exit 1
	fi
done

if command -v sha512sum >/dev/null 2>&1; then
	CHECKSUM=(sha512sum --check)
elif command -v shasum >/dev/null 2>&1; then
	CHECKSUM=(shasum -a 512 --check)
else
	echo "required command not found: sha512sum or shasum" >&2
	exit 1
fi

if kind get clusters | grep -qx "$CLUSTER"; then
	echo "==> reusing kind cluster '$CLUSTER'"
else
	echo "==> creating kind cluster '$CLUSTER'"
	kind create cluster --name "$CLUSTER"
fi

kubectl cluster-info --context "$CONTEXT" >/dev/null
TMP_DIR="$(mktemp -d)"
SMOKE_NAMESPACE="swe-bootstrap-check-$$-$RANDOM"

NODES=()
while IFS= read -r node; do
	NODES+=("$node")
done < <(kind get nodes --name "$CLUSTER")
if [[ "${#NODES[@]}" -eq 0 ]]; then
	echo "kind cluster '$CLUSTER' has no nodes" >&2
	exit 1
fi
ARCH="$(docker exec "${NODES[0]}" uname -m)"
case "$ARCH" in
	x86_64|aarch64) ;;
	arm64) ARCH=aarch64 ;;
	*) echo "unsupported gVisor node architecture: $ARCH" >&2; exit 1 ;;
esac
for node in "${NODES[@]}"; do
	node_arch="$(docker exec "$node" uname -m)"
	if [[ "$node_arch" == arm64 ]]; then
		node_arch=aarch64
	fi
	if [[ "$node_arch" != "$ARCH" ]]; then
		echo "mixed kind node architectures are unsupported: ${NODES[0]}=$ARCH, $node=$node_arch" >&2
		exit 1
	fi
done

echo "==> installing gVisor release $GVISOR_RELEASE"
GVISOR_URL="https://storage.googleapis.com/gvisor/releases/release/$GVISOR_RELEASE/$ARCH"
for binary in runsc containerd-shim-runsc-v1; do
	curl --fail --location --silent --show-error "$GVISOR_URL/$binary" --output "$TMP_DIR/$binary"
	curl --fail --location --silent --show-error "$GVISOR_URL/$binary.sha512" --output "$TMP_DIR/$binary.sha512"
	(cd "$TMP_DIR" && "${CHECKSUM[@]}" "$binary.sha512")
done

for node in "${NODES[@]}"; do
	for binary in runsc containerd-shim-runsc-v1; do
		docker cp "$TMP_DIR/$binary" "$node:/tmp/$binary"
		docker exec "$node" install -m 0755 "/tmp/$binary" "/usr/local/bin/$binary"
	done
	if ! docker exec "$node" grep -Fq 'containerd.runtimes.runsc]' /etc/containerd/config.toml; then
		if ! docker exec "$node" grep -Eq '^[[:space:]]*version[[:space:]]*=[[:space:]]*2[[:space:]]*$' /etc/containerd/config.toml; then
			echo "unsupported containerd config in $node: expected version = 2" >&2
			exit 1
		fi
		docker exec -i "$node" sh -c 'cat >> /etc/containerd/config.toml' <<'EOF'

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
EOF
	fi
	docker exec "$node" systemctl restart containerd
done

kubectl --context "$CONTEXT" wait --for=condition=Ready nodes --all --timeout=2m
kubectl --context "$CONTEXT" apply -f - <<'EOF'
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
EOF

echo "==> verifying the gVisor RuntimeClass"
kubectl --context "$CONTEXT" create namespace "$SMOKE_NAMESPACE"
kubectl --context "$CONTEXT" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: runsc-smoke
  namespace: $SMOKE_NAMESPACE
spec:
  runtimeClassName: gvisor
  restartPolicy: Never
  containers:
    - name: smoke
      image: busybox:1.36.1
      command: [sh, -c, "sleep 300"]
EOF
kubectl --context "$CONTEXT" wait --for=condition=Ready pod/runsc-smoke -n "$SMOKE_NAMESPACE" --timeout=2m
kubectl --context "$CONTEXT" exec -n "$SMOKE_NAMESPACE" runsc-smoke -- dmesg | grep -qi gvisor
kubectl --context "$CONTEXT" delete pod/runsc-smoke -n "$SMOKE_NAMESPACE" --wait=true >/dev/null

echo "==> installing VolumeSnapshot APIs and controller $SNAPSHOTTER_VERSION"
SNAPSHOTTER_BASE="https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/$SNAPSHOTTER_VERSION"
for manifest in \
	client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml \
	client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml \
	client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml \
	deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml \
	deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml; do
	kubectl --context "$CONTEXT" apply -f "$SNAPSHOTTER_BASE/$manifest"
done
kubectl --context "$CONTEXT" rollout status deployment/snapshot-controller -n kube-system --timeout=3m

echo "==> installing CSI hostpath driver $HOSTPATH_VERSION"
git clone --quiet --depth=1 --branch "$HOSTPATH_VERSION" \
	https://github.com/kubernetes-csi/csi-driver-host-path.git "$TMP_DIR/csi-driver-host-path"
REAL_KUBECTL="$(command -v kubectl)"
mkdir "$TMP_DIR/bin"
cat > "$TMP_DIR/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
exec "$REAL_KUBECTL" --context "$KUBE_CONTEXT" "$@"
EOF
chmod +x "$TMP_DIR/bin/kubectl"
export REAL_KUBECTL KUBE_CONTEXT="$CONTEXT"
PATH="$TMP_DIR/bin:$PATH" "$TMP_DIR/csi-driver-host-path/deploy/$HOSTPATH_DEPLOYMENT/deploy.sh"
kubectl --context "$CONTEXT" apply -f "$TMP_DIR/csi-driver-host-path/examples/csi-storageclass.yaml"
kubectl --context "$CONTEXT" rollout status statefulset/csi-hostpathplugin --timeout=3m
kubectl --context "$CONTEXT" annotate storageclass standard \
	storageclass.kubernetes.io/is-default-class- --overwrite >/dev/null
kubectl --context "$CONTEXT" annotate storageclass csi-hostpath-sc \
	storageclass.kubernetes.io/is-default-class=true --overwrite >/dev/null

echo "==> verifying CSI volume snapshots"
kubectl --context "$CONTEXT" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: snapshot-source
  namespace: $SMOKE_NAMESPACE
spec:
  storageClassName: csi-hostpath-sc
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 16Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: snapshot-writer
  namespace: $SMOKE_NAMESPACE
spec:
  restartPolicy: Never
  containers:
    - name: writer
      image: busybox:1.36.1
      command: [sh, -c, "echo snapshot-ready > /data/marker && sleep 300"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: snapshot-source
EOF
kubectl --context "$CONTEXT" wait --for=condition=Ready pod/snapshot-writer -n "$SMOKE_NAMESPACE" --timeout=2m
kubectl --context "$CONTEXT" delete pod/snapshot-writer -n "$SMOKE_NAMESPACE" --wait=true >/dev/null
kubectl --context "$CONTEXT" apply -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: bootstrap-smoke
  namespace: $SMOKE_NAMESPACE
spec:
  volumeSnapshotClassName: csi-hostpath-snapclass
  source:
    persistentVolumeClaimName: snapshot-source
EOF
kubectl --context "$CONTEXT" wait --for=jsonpath='{.status.readyToUse}'=true \
	volumesnapshot/bootstrap-smoke -n "$SMOKE_NAMESPACE" --timeout=2m
kubectl --context "$CONTEXT" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: snapshot-restore
  namespace: $SMOKE_NAMESPACE
spec:
  storageClassName: csi-hostpath-sc
  dataSource:
    name: bootstrap-smoke
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 16Mi
---
apiVersion: v1
kind: Pod
metadata:
  name: snapshot-reader
  namespace: $SMOKE_NAMESPACE
spec:
  restartPolicy: Never
  containers:
    - name: reader
      image: busybox:1.36.1
      command: [sh, -c, "grep -qx snapshot-ready /data/marker"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: snapshot-restore
EOF
kubectl --context "$CONTEXT" wait --for=jsonpath='{.status.phase}'=Succeeded \
	pod/snapshot-reader -n "$SMOKE_NAMESPACE" --timeout=2m

cat <<EOF

Cluster '$CLUSTER' is ready with RuntimeClass/gvisor and snapshot-capable
StorageClass/csi-hostpath-sc.

Next steps:
  make docker-build
  kind load docker-image ghcr.io/chris-cullins/swe-platform/operator:dev ghcr.io/chris-cullins/swe-platform/control-plane:dev ghcr.io/chris-cullins/swe-platform/env-base:dev --name $CLUSTER
  helm upgrade --install swe-platform charts/swe-platform -n default -f charts/swe-platform/values-kind.yaml --set-string 'environmentTemplates[0].spec.runtimeClass=gvisor'
  bin/swe run "fix the flaky tests" -t small
EOF
