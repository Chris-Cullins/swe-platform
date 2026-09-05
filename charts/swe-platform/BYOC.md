# BYOC operator runbooks

These runbooks operate the current k3s, GKE, and EKS Helm presets in a customer-owned
Kubernetes cluster. They cover installation, scoped Project onboarding/offboarding, upgrades,
PostgreSQL backup/restore, and first-response incidents. Commands require a dedicated PostgreSQL
database (with its own role-owned schema/search path), Bash, and Kubernetes 1.33 or newer.

> **Security status:** these presets are deployable baselines, not a production-security
> certification or a hosted service. Default-deny egress and its authenticated proxy are not
> implemented ([#68](https://github.com/Chris-Cullins/swe-platform/issues/68)); the complete
> credential and fail-closed isolation contracts remain open
> ([#9](https://github.com/Chris-Cullins/swe-platform/issues/9),
> [#10](https://github.com/Chris-Cullins/swe-platform/issues/10)). Environment egress is
> unrestricted unless the customer supplies an independent cluster control, and a non-empty
> `Project.spec.egressAllowlist` is rejected rather than silently ignored.

The current chart provisions only the Pod backend with the Linux `env-base` image. Native
Windows Environments, PowerShell `.ps1` setup/resume hooks, and ConPTY terminals are unsupported;
sandboxd's Windows terminal selection fails closed until a real ConPTY backend exists. These
runbooks must not be adapted into a Windows-hosted offering by changing node selectors or images.

## Choose and pin an input

The foundation [v0.1.0](https://github.com/Chris-Cullins/swe-platform/releases/tag/v0.1.0)
is published; `0.2.0` is release preparation and needs separate publication before installation.
Use the chart from the exact release commit and its release manifest; there is no separately
published Helm package. The image workflow also publishes coordinated `sha-<short SHA>` image
tags for each successful main build and stores a workflow artifact containing all three image
digests. A latest-main installation must use an exact commit checkout and its successful
`publish-images` run, never mutable `latest`. A tagged release may instead use the chart's
`appVersion` defaults. All upgrades across the cleanup boundary must follow the
[two-stage procedure below](#cleanup-release-order-and-rollback-boundary), even when using digests.

```sh
set -euo pipefail
git clone https://github.com/Chris-Cullins/swe-platform.git
cd swe-platform
export TARGET_COMMIT=0123456789abcdef0123456789abcdef01234567 # replace with approved full SHA
[[ "$TARGET_COMMIT" =~ ^[0-9a-f]{40}$ ]]
git checkout "$TARGET_COMMIT"
COMMIT="$(git rev-parse HEAD)"
RUN_ID="$(gh run list --workflow publish-images.yaml --commit "$COMMIT" \
  --json databaseId,status,conclusion \
  --jq '[.[] | select(.status == "completed" and .conclusion == "success")][0].databaseId')"
test -n "$RUN_ID" -a "$RUN_ID" != null
CI_RUN_ID="$(gh run list --workflow ci.yaml --commit "$COMMIT" \
  --json databaseId,status,conclusion \
  --jq '[.[] | select(.status == "completed" and .conclusion == "success")][0].databaseId')"
test -n "$CI_RUN_ID" -a "$CI_RUN_ID" != null
mkdir -p .release
RELEASE_DIR="$(mktemp -d ".release/${COMMIT}.XXXXXX")"
gh run download "$RUN_ID" --pattern "swe-platform-release-*-$COMMIT" --dir "$RELEASE_DIR"
mapfile -t manifests < <(find "$RELEASE_DIR" -name release-manifest.json -type f)
test "${#manifests[@]}" -eq 1
export SWE_RELEASE_MANIFEST="${manifests[0]}"
test -s "$SWE_RELEASE_MANIFEST"
```

Retain that release manifest with the exact chart checkout and private values; Actions artifacts
expire. The validator reads all three digests and makes the chart render
`repository@sha256:...`. The artifact name contains the source ref and full SHA; verify it is the
exact commit selected above. Also require the exact commit's normal `ci` workflow to be green.

Set common values for every procedure:

```sh
export PROVIDER=k3s                 # exactly one of: k3s, gke, eks
export SWE_SYSTEM_NAMESPACE=swe-platform-system
export SWE_RELEASE=swe-platform
export PRIVATE_VALUES=/secure/path/swe-platform-values.yaml
export SWE_PRIVATE_VALUES="$PRIVATE_VALUES"
export POSTGRES_SECRET=swe-platform-postgres
export POSTGRES_SECRET_KEY=url
export KEYRING_SECRET=swe-platform-session-keyring
export KEYRING_SECRET_KEY=keyring.json
test -s "$PRIVATE_VALUES"          # persist tenancy.namespaces and local overrides here
grep -q 'mode: scoped' "$PRIVATE_VALUES"
make build-cli
export PATH="$PWD/bin:$PATH"
./hack/validate-byoc.sh render "$PROVIDER"
```

Do not store database URLs, session keys, bootstrap tokens, or agent keys in Helm values.
A runbook installation must not set `nameOverride` or `fullnameOverride`; the recovery commands
use the chart's default resource names.
A minimal initial private values file is:

```yaml
tenancy:
  mode: scoped
  namespaces: []
```

GitHub App repository credentials are optional and disabled by default. When enabled, keep only
the App client ID and administrator-owned private-key Secret reference in this file; create and
back up the Secret out of band as described in
[GitHub App repository credentials](README.md#github-app-repository-credentials). The BYOC
renderer validates the required references, and installed validation catches a missing mount via
workload readiness; it never reads or prints the App private key.

## Provider runbooks

All providers require a default `ReadWriteOnce` StorageClass whose volume implementation grants
supplementary group 10001 write access to the mounted workspace. This can use kubelet ownership
changes (`fsGroupPolicy: File`, or `ReadWriteOnceWithFSType` when its conditions hold), correct CSI
`VOLUME_MOUNT_GROUP` delegation, or equivalent non-CSI behavior. They also require an enforcing
CNI for the chart's ingress NetworkPolicies, outbound access to GHCR for image pulls, and workload
outbound access needed by Git, setup/package managers, and the selected agent provider. That
outbound access is not constrained by the platform today.

### k3s

Use `values-k3s.yaml`. Confirm that the installed k3s release supplies Kubernetes 1.33 or newer
and a default local-path or customer-selected durable StorageClass. k3s does not ship gVisor;
this preset deliberately runs Environment pods with the cluster's default OCI runtime. Decide
whether that isolation is acceptable before admitting untrusted repositories.

```sh
export PROVIDER=k3s
kubectl get nodes -o wide
kubectl get storageclass
```

### GKE

Use `values-gke.yaml` only on a cluster whose Environment nodes have GKE Sandbox enabled. The
preset names `RuntimeClass/gvisor`; the operator verifies that exact object exists, but cannot
prove handler installation or runtime isolation. Test GKE Sandbox on eligible nodes separately.

```sh
export PROVIDER=gke
kubectl get runtimeclass gvisor
kubectl get nodes -L cloud.google.com/gke-nodepool
```

Ensure scheduling configuration places Environment pods on Sandbox-capable nodes if the cluster
also has ordinary node pools. The checked-in preset does not select a node pool.

### EKS

Use `values-eks.yaml`. Install and operate the EBS CSI driver and mark the intended EBS
StorageClass as default before onboarding Projects. EKS has no standard gVisor RuntimeClass;
the preset deliberately uses the cluster default runtime unless the administrator adds a tested
RuntimeClass to the catalog Template override.

```sh
export PROVIDER=eks
kubectl -n kube-system get deployment ebs-csi-controller
kubectl get storageclass
```

## Install

### 1. Prepare external state

Create a dedicated PostgreSQL database. Its role needs normal table DML and
`CREATE`/`ALTER` on its own schema because the control plane applies ordered migrations at
startup. Configure TLS verification appropriate to the database provider; do not use
`sslmode=disable` outside disposable tests.

Create the namespace and Secrets without committing their values:

```sh
kubectl create namespace "$SWE_SYSTEM_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

read -rsp 'PostgreSQL URL: ' SWE_POSTGRES_URL; echo
printf %s "$SWE_POSTGRES_URL" | kubectl -n "$SWE_SYSTEM_NAMESPACE" create secret generic \
  "$POSTGRES_SECRET" --from-file="$POSTGRES_SECRET_KEY=/dev/stdin" \
  --dry-run=client -o yaml | kubectl apply -f -
unset SWE_POSTGRES_URL

umask 077
KEYRING_DIR="$(mktemp -d)"
trap 'rm -rf "$KEYRING_DIR"' EXIT
KEYRING_FILE="$KEYRING_DIR/keyring.json"
KEY_ID="initial-$(date -u +%Y%m%dT%H%M%SZ)"
openssl rand 32 | base64 | tr '+/' '-_' | tr -d '=\n' | \
  jq -Rs --arg id "$KEY_ID" \
  '{version:1,activeKeyId:$id,keys:[{id:$id,masterKey:.}]}' >"$KEYRING_FILE"
kubectl -n "$SWE_SYSTEM_NAMESPACE" create secret generic "$KEYRING_SECRET" \
  --from-file="$KEYRING_SECRET_KEY=$KEYRING_FILE" --dry-run=client -o yaml | kubectl apply -f -
# Transfer the keyring into the customer secret backup system before deleting this file.
rm -rf "$KEYRING_DIR"
trap - EXIT
unset KEYRING_DIR KEYRING_FILE KEY_ID
```

The database and keyring must be backed up under separate access controls. A database-plus-
keyring disclosure can recover encrypted browser bearer credentials.

### 2. Validate and install the empty scoped release

`preflight` reads Secret keys only to confirm that non-empty data exists; it never prints or
decodes their values. It checks Kubernetes version, the NetworkPolicy API, a default
StorageClass, and GKE's named RuntimeClass. It cannot prove that a CNI or runtime enforces the
advertised isolation, or that the storage implementation applies the required filesystem-group
access. Validate all three behaviors independently before admitting untrusted workloads.

```sh
./hack/validate-byoc.sh preflight "$PROVIDER"

IMAGE_ARGS=()
mapfile -t IMAGE_ARGS < <(./hack/validate-byoc.sh image-args "$PROVIDER")
helm upgrade --install "$SWE_RELEASE" ./charts/swe-platform \
  --namespace "$SWE_SYSTEM_NAMESPACE" \
  --values "./charts/swe-platform/values-$PROVIDER.yaml" \
  --values "$PRIVATE_VALUES" \
  "${IMAGE_ARGS[@]}" \
  --wait --timeout 10m
./hack/validate-byoc.sh installed "$PROVIDER"
```

The first scoped install must keep `tenancy.namespaces: []`. Do not use `trusted-admin` as a
shortcut. `image-args` is the single source for both release-manifest digest pins and the
traceable `SWE_IMAGE_TAG` fallback; do not reconstruct these overrides separately.

### 3. Onboard and activate a Project

Choose every quota explicitly, then create or deliberately adopt one dedicated namespace:

```sh
export PROJECT_NAMESPACE=my-project
export PROJECT_NAME=my-project
export INSTALLATION="$SWE_RELEASE-swe-platform"

swe --namespace "$PROJECT_NAMESPACE" project onboard "$PROJECT_NAME" \
  --system-namespace "$SWE_SYSTEM_NAMESPACE" --installation "$INSTALLATION" \
  --repository https://github.com/example/project.git \
  --default-template medium --template medium \
  --quota-hard requests.cpu=8 \
  --quota-hard requests.memory=32Gi \
  --quota-hard requests.storage=200Gi \
  --quota-hard persistentvolumeclaims=20 \
  --quota-hard pods=20 \
  --quota-hard secrets=50 \
  --quota-hard count/runs.swe.dev=100 \
  --quota-hard count/environments.swe.dev=20 \
  --quota-hard count/agentcredentialprofiles.swe.dev=20
```

Replace the example quotas with approved customer limits. Add the namespace to the complete
`tenancy.namespaces` list in `$PRIVATE_VALUES`, run the Helm command from step 2 again, and run
`installed` validation. Never replace the complete list with an ad-hoc one-item `--set` on an
existing multi-Project installation.

## Upgrade

### Cleanup release order and rollback boundary

`v0.1.0` at
[`77ae942`](https://github.com/Chris-Cullins/swe-platform/commit/77ae94206ca00adb4d2a487d01abdb8a9d63d6ad)
contains the cleanup endpoint without the new operator deletion dependency. `0.2.0` adds that
dependency. Publication of the foundation is complete, but each installation must deploy and
verify it **before** a separate `0.2.0` rollout. Do not substitute a main/SHA image for either
release or deploy this unreleased chart merely because its version is already `0.2.0`.

1. Preserve the existing release name, namespace/Installation identity, complete scoped namespace
   list, database, keyring, and private values. Take the backups below. Confirm the Kubernetes
   context names the intended cluster. Do not offboard or recreate namespaces to perform an upgrade.
2. In a separate clean checkout, pin the foundation and download its successful tag workflow's
   manifest (the workflow must be complete and successful):

   ```sh
   git fetch origin tag v0.1.0
   git checkout --detach v0.1.0
   test "$(git rev-parse HEAD)" = 77ae94206ca00adb4d2a487d01abdb8a9d63d6ad
   test "$(gh run view 33987642471 --json conclusion --jq .conclusion)" = success
   mkdir -p .release
   RELEASE_DIR="$(mktemp -d .release/foundation.XXXXXX)"
   gh run download 33987642471 --pattern 'swe-platform-release-*' --dir "$RELEASE_DIR"
   mapfile -t manifests < <(find "$RELEASE_DIR" -name release-manifest.json -type f)
   test "${#manifests[@]}" -eq 1
   export SWE_RELEASE_MANIFEST="$(realpath "${manifests[0]}")"
   jq -e '.chartVersion == "0.1.0" and .appVersion == "0.1.0"' "$SWE_RELEASE_MANIFEST"
   export SWE_CHART="$PWD/charts/swe-platform"
   unset SWE_IMAGE_TAG
   ./hack/validate-byoc.sh render "$PROVIDER"
   mapfile -t IMAGE_ARGS < <(./hack/validate-byoc.sh image-args "$PROVIDER")
   ```

   Retain a copy of the manifest: Actions artifacts expire. If unavailable, obtain the matching
   manifest from the release assets or the administrator's retained copy; never guess digests.
   Run the CRD/Helm commands in **Apply one verified release** below using this checkout and
   `IMAGE_ARGS` (for a new installation, use the Install procedure instead). Require `installed`
   validation to succeed: it waits for workloads and checks the manifest-pinned deployed images.
   Record that successful foundation deployment before proceeding.
3. Only after `0.2.0` is separately published and approved for deployment, select its exact release
   commit, successful tag publish run, and matching manifest using **Choose and pin an input**.
   Resolve `TARGET_COMMIT` from the fetched `v0.2.0` tag and add `--branch v0.2.0` to that
   procedure's publish workflow lookup so a main build cannot substitute for the tag build.
   Require both manifest `chartVersion` and `appVersion` to equal `0.2.0`.
   Change `SWE_CHART` to that checkout and `SWE_RELEASE_MANIFEST` to its manifest; rebuild
   `IMAGE_ARGS`, render, and apply that release separately. Do not reuse the foundation's pins.
   Keep the same database/identity/private values and verify `installed` again. Never use automatic
   rollback on this transition (including Helm 3 `--atomic` or Helm 4 `--rollback-on-failure`).

**Rollback to `0.1.0` or earlier is unsupported; roll forward only.** The older operator can
remove Run finalizers without the cleanup dependency. A control-plane downgrade alone can also
break the required endpoint contract. No validated API-wide Run-deletion freeze/drain procedure
exists in this release. Keep or restore the compatible cleanup-capable operator/control-plane
pair and repair transport/database failures so pending cleanup can complete; do not strip
finalizers, disable transcript transport, remove scoped namespaces, or delete Namespace/Project
resources to escape a blocked deletion. Stopping the operator is not a freeze: Kubernetes still
accepts deletions, and the older operator can process them after restart.

For incident evidence, an administrator can list pending Run deletions without reading transcript
content or credentials:

```sh
kubectl get runs.swe.dev --all-namespaces -o json |
  jq '[.items[] | select(.metadata.deletionTimestamp != null) |
       {namespace: .metadata.namespace, name: .metadata.name, uid: .metadata.uid,
        deletingSince: .metadata.deletionTimestamp}]'
```

An empty list is only a point-in-time observation, **not rollback permission**. A future validated
procedure must enforce a freeze on every deletion entry point (including direct Kubernetes
clients, automation, collection deletion, and namespace teardown), keep it enforced while the
current compatible cleanup path drains all accepted deletions, prove that drain, and maintain
the freeze across downgrade until a compatible cleanup path is restored. Ordinary caller RBAC
changes or a CLI-only pause cannot establish that invariant. Helm rollback also does not restore
deleted primary transcript rows, downgrade CRDs, or reverse database migrations.

### Apply one verified release

1. Record `helm get values "$SWE_RELEASE" -n "$SWE_SYSTEM_NAMESPACE" --all` and the current
   commit/release manifest. Verify the target commit's publish and CI workflows, download its
   release manifest, reconstruct `IMAGE_ARGS` with the `image-args` command above, and run
   `render`.
2. Take and verify the PostgreSQL backup below. Confirm the backed-up keyring contains every key
   required by the one-hour session TTL and rollback window.
3. Review the target preset and all private values. Rerun `project onboard` for each active
   Project when catalog sources changed; that explicitly synchronizes managed Template copies.
4. Apply CRDs first, then upgrade. Helm does not upgrade files under `crds/`.

```sh
kubectl apply --server-side --force-conflicts -f ./charts/swe-platform/crds
helm upgrade "$SWE_RELEASE" ./charts/swe-platform \
  --namespace "$SWE_SYSTEM_NAMESPACE" \
  --values "./charts/swe-platform/values-$PROVIDER.yaml" \
  --values "$PRIVATE_VALUES" \
  "${IMAGE_ARGS[@]}" \
  --wait --timeout 10m
./hack/validate-byoc.sh installed "$PROVIDER"
```

The operator and singleton control plane use `Recreate`; expect open SSE/WebSocket connections
to reconnect. PostgreSQL preserves transcripts and browser sessions, but this is not control-
plane HA. Roll forward on failure. In particular, the cleanup boundary above has no supported
rollback procedure; a coordinated image set and retained session keyring do not make it safe.

## PostgreSQL backup and restore

This database contains transcript events, cursor/idempotency metadata, migration records, and
encrypted browser sessions. Back up the complete dedicated database rather than selecting
name-only Run rows. PostgreSQL backup does not include Kubernetes CRDs, Namespace claims,
workspace PVCs, chart values, or secret material.

### Backup and test

```sh
umask 077
export BACKUP="swe-platform-$(date -u +%Y%m%dT%H%M%SZ).dump"
read -rsp 'PostgreSQL URL: ' SWE_POSTGRES_URL; echo
PGDATABASE="$SWE_POSTGRES_URL" pg_dump --format=custom --no-owner --no-acl \
  --file="$BACKUP"
pg_restore --list "$BACKUP" >/dev/null
sha256sum "$BACKUP" >"$BACKUP.sha256"
unset SWE_POSTGRES_URL
```

Copy the dump, checksum, exact chart/commit, private values, and separately protected keyring to
the customer backup system. A successful `pg_restore --list` checks archive readability, not a
recovery. Regularly restore into an isolated empty database and run a disposable control plane
against it; define customer-owned RPO/RTO from that exercise.

### Restore the complete application database

Restore into a new empty dedicated database; do not overwrite a live database. Keep the
control plane stopped while restoring, then change `swe-platform-postgres` to the recovered URL.
The operator may remain running, but transcript API calls fail while the control plane is down.

```bash
(
set -euo pipefail
: "${BACKUP:?set BACKUP to the recovered dump}"
: "${RECOVERY_KEYRING_FILE:?set RECOVERY_KEYRING_FILE to the matching keyring backup}"
test -s "$BACKUP"
test -s "$BACKUP.sha256"
test -s "$RECOVERY_KEYRING_FILE"
sha256sum --check "$BACKUP.sha256"
CONTROL_PLANE="$SWE_RELEASE-swe-platform-control-plane"
kubectl -n "$SWE_SYSTEM_NAMESPACE" scale deployment "$CONTROL_PLANE" --replicas=0
kubectl -n "$SWE_SYSTEM_NAMESPACE" wait pod \
  -l "app.kubernetes.io/instance=$SWE_RELEASE,app.kubernetes.io/component=control-plane" \
  --for=delete --timeout=5m
read -rsp 'EMPTY recovery PostgreSQL URL: ' RECOVERY_POSTGRES_URL; echo
relation_count="$(PGDATABASE="$RECOVERY_POSTGRES_URL" psql -XAt -v ON_ERROR_STOP=1 \
  -c "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\';")"
test "$relation_count" = 0
PGDATABASE="$RECOVERY_POSTGRES_URL" pg_restore --single-transaction --exit-on-error \
  --no-owner --no-acl "$BACKUP"
PGDATABASE="$RECOVERY_POSTGRES_URL" psql -XAt -v ON_ERROR_STOP=1 \
  -c 'SELECT count(*) FROM transcript_schema_migrations;' >/dev/null

kubectl -n "$SWE_SYSTEM_NAMESPACE" create secret generic "$KEYRING_SECRET" \
  --from-file="$KEYRING_SECRET_KEY=$RECOVERY_KEYRING_FILE" \
  --dry-run=client -o yaml | kubectl apply -f -
printf %s "$RECOVERY_POSTGRES_URL" | kubectl -n "$SWE_SYSTEM_NAMESPACE" create secret generic \
  "$POSTGRES_SECRET" --from-file="$POSTGRES_SECRET_KEY=/dev/stdin" \
  --dry-run=client -o yaml | kubectl apply -f -
unset RECOVERY_POSTGRES_URL
kubectl -n "$SWE_SYSTEM_NAMESPACE" scale deployment "$CONTROL_PLANE" --replicas=1
kubectl -n "$SWE_SYSTEM_NAMESPACE" rollout status deployment "$CONTROL_PLANE" --timeout=5m
./hack/validate-byoc.sh installed "$PROVIDER"
)
```

Startup applies any newer embedded migrations. Restore the matching keyring before startup or
encrypted sessions using absent keys will fail. A database restore does not reconstruct Runs,
Environments, Namespace claims, or workspaces. There is no tested coordinated cluster-loss
restore: back up Kubernetes/etcd and PVCs with provider-supported tools and test that procedure
under the customer's own recovery objectives.

## Offboard without deleting customer data

```sh
swe --namespace "$PROJECT_NAMESPACE" project offboard "$PROJECT_NAME" \
  --system-namespace "$SWE_SYSTEM_NAMESPACE" --installation "$INSTALLATION" \
  --timeout 15m
```

Verify the Namespace is `fenced`, remove it from the complete `tenancy.namespaces` list, and run
the normal Helm upgrade. Offboarding intentionally retains the Namespace, CRs, PVCs, credential
profiles/Secrets, and transcripts. Purge is not implemented; direct Namespace, PVC, Secret, or
name-only SQL deletion is not a supported platform purge.

## Incident first response

Preserve the exact commit, release manifest, `helm get values`, Kubernetes events, component logs,
and object UIDs. Never paste Secret data into tickets or logs.

| Symptom | Contain | Diagnose and recover |
|---|---|---|
| Control plane crash loop after install/upgrade | Leave it stopped; do not switch to memory sessions/transcripts | Check database reachability, migration errors, Secret key presence, and keyring validity; restore/roll back only under the upgrade constraints above |
| Suspected agent API-key disclosure | Cancel affected Runs and hold their Environments | Rotate the upstream key, then `swe --namespace <project> credentials rotate <profile> --api-key-stdin`; transcripts are not guaranteed to redact a key |
| Database-only disclosure | Restrict database access and rotate its credential | Database ciphertext does not alone reveal browser bearers, but treat transcript content as disclosed; update the PostgreSQL Secret and roll the control plane |
| Database plus keyring or control-plane compromise | Stop/exclude the control plane and revoke underlying Kubernetes credentials | Invalidate browser sessions in the dedicated database, rotate database credentials and the session keyring, update both Secrets, then restart. Preserve evidence before destructive session invalidation |
| Environment suspected compromised | Cancel its Run and apply an explicit Environment hold | Pod deletion/pause rotates sandboxd credentials but retained PVC and transcripts may contain attacker changes; preserve and investigate them before any customer-approved deletion |
| Project removal | Run the supported offboard command | Wait for `fenced`, remove it from scoped values, and retain data. Do not improvise purge |

NetworkPolicy API presence is not evidence that the CNI enforces policy. Until #68/#10 ship,
network containment of an Environment requires customer-owned cluster controls and must not be
described as a platform production guarantee.

After evidence capture and explicit incident-authority approval, invalidate every browser
session while the control plane is stopped with:

```sh
read -rsp 'PostgreSQL incident URL: ' INCIDENT_POSTGRES_URL; echo
PGDATABASE="$INCIDENT_POSTGRES_URL" psql -X -v ON_ERROR_STOP=1 \
  -c 'DELETE FROM browser_sessions;'
unset INCIDENT_POSTGRES_URL
```

This does not revoke the underlying Kubernetes bearer credentials; revoke those at their issuer
as a separate containment step. It also does not remove transcript evidence.

## Hosted alpha: decisions required before implementation

Issue #79 cannot safely create a shared hosted environment from the current BYOC chart. A
maintainer must approve all of the following before infrastructure or customer onboarding:

1. **Product and responsibility boundary:** hosted control plane vs fully hosted execution,
   target customer/region, alpha limits, data ownership/residency, terms, support channel, and
   whether untrusted repositories are admitted while #9/#10/#68 remain open.
2. **Tenant and identity model:** organization/user identity provider, invitation and removal,
   Kubernetes identity translation, Project ownership, administrator roles, audit requirements,
   and the supported credential classes. Namespace-per-Project is the approved platform target;
   no alternate shared-namespace boundary should be invented.
3. **Infrastructure ownership:** cloud account/project, regions, cluster and node-pool topology,
   enforcing CNI/runtime, PostgreSQL, DNS/TLS, registry access, secret/KMS ownership, egress
   enforcement, and customer data separation.
4. **Commercial controls:** billing owner, metering unit, quotas, abuse/spend limits, admission,
   suspension, and customer offboarding/retention/deletion policy.
5. **Operations:** named on-call owner, SLOs, monitoring/audit retention, vulnerability and
   incident response, backup RPO/RTO with restore evidence, upgrades/rollback, capacity tests,
   and disaster-recovery ownership.
6. **Launch gates:** disposition of #9, #10, and #68; external threat/security review; supported
   agent/Git credential paths; and a tested, exact-UID purge or an explicit alpha retention
   contract acknowledging that purge is not implemented.

Until those decisions are recorded, the coherent #79 deliverable is these customer-owned
runbooks and their validator—not speculative shared infrastructure or a production-security
claim.
