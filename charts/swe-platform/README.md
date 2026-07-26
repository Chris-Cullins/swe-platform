# swe-platform Helm chart

The bundled environment image provides the default `claude-code` adapter and the explicitly
selected `amp` adapter (`swe run --agent amp ...`). In accordance with the official
[Amp non-interactive authentication contract](https://ampcode.com/manual#non-interactive-environments),
an API-key profile selected for an Amp Run is delivered as `AMP_API_KEY` only to its Run-owned
managed process through sandboxd launch material. It is not injected by chart values or into the
operator, sandboxd, setup/resume hooks, ordinary processes, public process specification, or
workspace by the platform. The image pins the public Amp CLI and disables its update check.

Do not place Amp credentials in chart values, Project configuration, prompts, or custom-image
ambient environment variables. Profiles are shared within their namespace, the selected agent
and descendants can read or explicitly output the key, same-UID peers are not strongly isolated,
and transcript redaction is not guaranteed. A newly created keyed process receives the
then-current key; duplicate acceptance in the same sandbox epoch ignores rotated launch material,
while a fresh epoch recreates the process with the current key. Subscription/OAuth login
persistence, refresh/writeback, leases, and stronger per-user isolation remain unsupported.

The image also bundles the explicitly selected `codex` adapter and pinned Codex CLI. Codex API
key profiles use sandboxd's process-scoped launch-material path as `CODEX_API_KEY`; never put the
key in chart values, Project configuration, or custom-image ambient environment variables.

The image also bundles pinned Pi (`swe run --agent pi ...`). Pi has no credential-profile or
credential-injection support: selecting a profile fails before allocation or credential reads.
The stock image contains no Pi authentication. Ambient auth or configuration introduced by a
custom image, attached user, hooks/repository code, or the process environment is outside the
supported process-scoped credential contract and should not be added to chart values.

This chart installs the swe-platform CRDs, operator, first control-plane API, and one
system-namespaced `Installation` identity. Configured `environmentTemplates` are inert catalog
sources in that system namespace; Helm never creates a Project tenancy there.
The control plane accepts adapter-owned transcript events and streams them over SSE.
Production installs must configure the PostgreSQL transcript store described below. The chart
still requires one control-plane replica and uses a non-overlapping `Recreate` deployment:
durable transcripts and encrypted PostgreSQL browser sessions survive process replacement, but
open SSE/WebSocket connections do not and this release makes no control-plane HA claim. Portal
proxying is not implemented yet.

The operator reconciles each `Run` as the single task intent and allocates or claims its
`Environment`; clients must not create the two resources independently. Its RBAC permits
Run status/finalizer updates and Environment allocation/claim updates. Process execution
remains behind the environment's portable sandboxd contract rather than Kubernetes exec.

## Install

Choose the values preset for the target cluster and install into a dedicated namespace:

```sh
kubectl create namespace swe-platform-system --dry-run=client -o yaml | kubectl apply -f -
kubectl -n swe-platform-system create secret generic swe-platform-postgres \
  --from-literal=url='postgres://swe:REDACTED@postgres.example:5432/swe?sslmode=require'
# Generate a fresh key locally; do not copy a key from this documentation.
KEY_ID="initial-$(date +%Y%m%d)"
MASTER_KEY="$(openssl rand 32 | base64 | tr '+/' '-_' | tr -d '=\n')"
jq -n --arg id "$KEY_ID" --arg key "$MASTER_KEY" \
  '{version:1,activeKeyId:$id,keys:[{id:$id,masterKey:$key}]}' >keyring.json
kubectl -n swe-platform-system create secret generic swe-platform-session-keyring \
  --from-file=keyring.json=keyring.json
rm -f keyring.json
helm upgrade --install swe-platform ./charts/swe-platform \
  --namespace swe-platform-system --create-namespace \
  --values ./charts/swe-platform/values-k3s.yaml
```

The k3s, GKE, and EKS production presets explicitly select PostgreSQL sessions and require both
out-of-band Secrets by default. Helm rejects a partial configuration; a Secret that is named but
absent keeps the pod from starting. There is no runtime fallback to memory. Override the two
Secret names when your installation uses different coordinated names.

Tenancy mode is required. The production presets explicitly use `scoped` with an initially
empty, restart-bound `tenancy.namespaces` list. The isolated kind and Argo development presets
deliberately opt into `trusted-admin`; missing or invalid configuration never falls back to
cluster-wide authority. Trusted-admin discovers exact claimed namespaces dynamically, but does
not bypass Installation/Namespace/Project claims. Cluster-scoped RBAC names include the system
namespace, so same-named releases in different namespaces do not manage each other's roles or
claims.

### Onboard a Project namespace

In scoped mode, install the system identity/catalog first, then use the CLI to create or
explicitly adopt one dedicated namespace. Every quota value is required and must be chosen by
the administrator; the platform has no capacity defaults:

```sh
SYSTEM_NAMESPACE=swe-platform-system
PROJECT_NAMESPACE=my-project
INSTALLATION=swe-platform-swe-platform

swe --namespace "$PROJECT_NAMESPACE" project onboard my-project \
  --system-namespace "$SYSTEM_NAMESPACE" --installation "$INSTALLATION" \
  --repository https://github.com/example/project.git \
  --default-template medium --template medium \
  --quota-hard requests.cpu="$PROJECT_CPU_QUOTA" \
  --quota-hard requests.memory="$PROJECT_MEMORY_QUOTA" \
  --quota-hard requests.storage="$PROJECT_STORAGE_QUOTA" \
  --quota-hard persistentvolumeclaims="$PROJECT_PVC_QUOTA" \
  --quota-hard pods="$PROJECT_POD_QUOTA" \
  --quota-hard secrets="$PROJECT_SECRET_QUOTA" \
  --quota-hard count/runs.swe.dev="$PROJECT_RUN_QUOTA" \
  --quota-hard count/environments.swe.dev="$PROJECT_ENVIRONMENT_QUOTA" \
  --quota-hard count/agentcredentialprofiles.swe.dev="$PROJECT_CREDENTIAL_QUOTA"
```

The command creates/adopts the Namespace under exact Installation and Project UID annotations,
enforces one Project, copies selected catalog sources into same-namespace managed Templates,
and installs the versioned ResourceQuota, environment ServiceAccount, workload RoleBindings,
and ingress-only default-deny policy. Existing unclaimed namespaces require `--adopt`.
Foreign/stale claims, a second Project, unowned Template/baseline collisions, and incomplete
quota inputs fail closed. The baseline does not restrict egress; that remains future work.

Add the active namespace to the complete persisted list and perform a controlled Helm
upgrade/restart. Do not merely change a mutable label or start using the namespace before this
step:

```sh
# Illustrative one-Project list; keep every active Project namespace in the
# production values file rather than relying on ad-hoc --set state.
helm upgrade swe-platform ./charts/swe-platform \
  --namespace "$SYSTEM_NAMESPACE" \
  --values ./charts/swe-platform/values-k3s.yaml \
  --set-string "tenancy.namespaces[0]=$PROJECT_NAMESPACE"

swe --namespace "$PROJECT_NAMESPACE" run --project my-project "Fix the flaky test"
```

Both operator and control plane validate every configured exact claim before startup, including
transition lifecycles so interrupted fencing can safely resume. The operator cache then watches
only those namespaces, and every reconcile/mutation revalidates the live
Installation UID, Namespace UID, claim, lifecycle, and sole Project through uncached reads.
The control plane still performs TokenReview/SAR first, then requires allowlist membership and
the same exact active claim before resource work. Namespaced RoleBindings, not broad scoped-mode
cluster bindings, provide workload authority.

Rerun `project onboard` explicitly to sync managed Template source UID/revision/spec changes in
place; the local Template UID and warm-pool status are preserved. A deleted catalog source never
implicitly deletes the retained local copy. Catalog sources themselves are not runnable.

### Fence and retain a Project namespace

Offboarding is non-destructive in this phase:

```sh
swe --namespace "$PROJECT_NAMESPACE" project offboard my-project \
  --system-namespace "$SYSTEM_NAMESPACE" --installation "$INSTALLATION" \
  --timeout 15m
```

The command first changes the exact claim to `fencing`, publishes Run cancellation and
Environment hold intents, waits for terminal Runs and suspended podless Environments, and only
then marks the Namespace `fenced`. It deletes no Namespace, Project, Template, quota, RBAC,
policy, credential profile or owned Secret, PVC, Run, Environment, or transcript row. Normal
Environment suspension still revokes that pod incarnation's ephemeral sandboxd credential
Secret. Retain/archive is the safe result. Remove the fenced namespace from
`tenancy.namespaces`, perform the controlled Helm upgrade/restart, and preserve the resources
according to local retention policy. Purge is not implemented; it requires a future exact
Namespace-UID-preconditioned durable resumable operation. Do not treat direct namespace, PVC,
credential, or transcript deletion as platform purge.

## Upgrade

Helm installs definitions from a chart's `crds/` directory only on the first install; it does
not upgrade existing CRDs. Before every plain-Helm upgrade, server-side apply the CRDs from the
same checked-out or unpacked chart version, then upgrade the release:

```sh
kubectl apply --server-side --force-conflicts -f ./charts/swe-platform/crds
helm upgrade swe-platform ./charts/swe-platform \
  --namespace swe-platform-system \
  --values ./charts/swe-platform/values-k3s.yaml
```

Skipping the apply leaves the prior API schemas installed: new resource kinds and fields will
be unavailable, and removed fields will continue to be admitted. `--force-conflicts` makes the
checked-in CRD definition authoritative when ownership moves from Helm's initial create to
server-side apply. The Argo CD preset does not need this manual step because Argo synchronizes
the chart's `crds/` files as manifests.

The production presets create a `medium` catalog source using the published `env-base` image;
onboarding creates each runnable Project-local Template copy. Operator, control-plane, and
env-base tags default to the chart
`appVersion`, keeping a released chart on one tested version set and making Helm rollback
restore that set. Override all three tags with immutable release or SHA tags when testing
a different coordinated set; `latest` and `dev` are development-only choices.

Each image publish run emits a `swe-platform-release-*` artifact containing the chart
version, app version, and the registry digest of every image for incident diagnosis and
digest-pinned installation overrides.

### Environment image footprint

The coordinated image remains the default instead of selecting a different image per agent.
The following measurement was recorded on 2026-07-21 from images published by the existing
`linux/amd64,linux/arm64` workflow. Size is the sum of the compressed layer payload sizes in
each platform's OCI manifest; it excludes the image config, index, manifests, attestations,
and transfer overhead. Build time is the observed wall time of the workflow's `Build and push`
step, including both architectures and registry export.

| Bundled CLIs | Commit/SHA tag | amd64 layer payload | arm64 layer payload | Multi-arch build and push |
|---|---|---:|---:|---:|
| Claude Code | `ea9e5af` / `sha-ea9e5af` | 243,209,510 B (231.94 MiB) | 241,177,665 B (230.00 MiB) | 63 s |
| + Amp | `70968da` / `sha-70968da` | 286,284,455 B (273.02 MiB) | 283,060,674 B (269.95 MiB) | 103 s |
| + Codex | `bfeab4f` / `sha-bfeab4f` | 418,687,171 B (399.29 MiB) | 408,239,734 B (389.33 MiB) | 198 s |
| + Pi (historical; reverted by PR #93) | `c9daef7` / `sha-c9daef7` | 494,042,178 B (471.16 MiB) | 483,574,399 B (461.17 MiB) | 257 s |

The four-CLI row is explicitly a historical measurement of the implementation reverted by PR
#93; it is retained only as evidence for the coordinated-image decision. The current clean-room
image bundles and registers Claude Code, Amp, Codex, and Pi, but has not yet been remeasured, so
the historical row must not be presented as its size or build time.

Relative to Claude Code alone, all four CLIs added 250,832,668 compressed bytes on amd64
(103.1%) and 242,396,734 bytes on arm64 (100.5%); the observed publish step was 194 seconds
longer (4.08x total). The sequential image additions were 43,074,945/41,883,009 bytes for Amp,
132,402,716/125,179,060 bytes for Codex, and 75,355,007/75,334,665 bytes for Pi
(amd64/arm64). These are measured image deltas, not standalone CLI package sizes: in
particular, the Pi revision changed the final base from Debian slim to Node slim to satisfy its
runtime requirement.

These observations still favor retaining one coordinated image as the lower-complexity default:
the largest measured compressed layer payload remains below 500 MB, the corresponding release
step completed in under five minutes, nodes can reuse one image across Runs, and a warm
Environment contains the binaries for every currently registered adapter (Claude Code, Amp,
Codex, and Pi). Per-agent images could avoid some unused bytes on a cold pull, but would multiply
release artifacts and require agent-specific template/image and warm-pool selection. Per-agent
images and startup latency were not measured, so the data does not establish a performance
advantage for the coordinated image; it shows no reason to add that operational dimension yet.
Revisit the decision if production pull latency or node storage data shows the overhead is
material.

Reproduce the registry measurement with
[`crane`](https://github.com/google/go-containerregistry/tree/main/cmd/crane) v0.20.6 (repeat
for each tag and platform):

```sh
image=ghcr.io/chris-cullins/swe-platform/env-base
index=sha256:dcc5c8c6f6c867bd48d3e95504432ec21b1503c992daa1392a4dad69aab33a93
arch=amd64
digest=$(crane manifest "$image@$index" |
  jq -r --arg arch "$arch" '.manifests[] |
    select(.platform.os == "linux" and .platform.architecture == $arch) | .digest')
crane manifest "$image@$digest" | jq '[.layers[].size] | add'
```

For all four rows, the index digests as resolved and recorded on 2026-07-21 are, in table
order: `sha256:11c12d7aa9903d71644816f846616c62c21df2543453e403c7ce94dd3e9e47dc`,
`sha256:2491095b62972dbbe64b9bc6b1e08a0d66d6b0d8cd7f6314f9e1da28f6a8e987`,
`sha256:92212fd083af5ee2c79f0ffa17fe3f73417bf704553f67d1265461b15b817899`, and
`sha256:dcc5c8c6f6c867bd48d3e95504432ec21b1503c992daa1392a4dad69aab33a93`.
Use these immutable digests rather than assuming the convenience SHA tags cannot be moved.

The build timings are available from the public Actions runs for
[`ea9e5af`](https://github.com/Chris-Cullins/swe-platform/actions/runs/29715892572),
[`70968da`](https://github.com/Chris-Cullins/swe-platform/actions/runs/29718546229),
[`bfeab4f`](https://github.com/Chris-Cullins/swe-platform/actions/runs/29725763660), and
[`c9daef7`](https://github.com/Chris-Cullins/swe-platform/actions/runs/29731010260). They are
real publish observations, but not controlled cold-build benchmarks: BuildKit's shared GitHub
Actions cache and network/runner variation were not disabled. A local rebuild was unavailable
because this orb has no Docker-compatible builder; registry manifests provide measured evidence
for both production architectures, while architecture-specific local build CPU cost remains
unmeasured.

| Preset | Assumptions |
|---|---|
| `values-kind.yaml` | Isolated local kind development with explicit process-local `memory` sessions, deliberate `trusted-admin`, `:dev` images, and insecure HTTP browser sessions. `make kind-up` installs gVisor and snapshot-capable CSI; pass the printed `environmentTemplates[0].spec.runtimeClass=gvisor` catalog override. Primary acceptance instead overrides this preset to scoped mode and distinct system/Project namespaces. |
| `values-argocd.yaml` | Isolated local Argo CD mirror with explicit process-local `memory` sessions, deliberate `trusted-admin`, mutable `:latest` images, an out-of-band bootstrap Secret, and insecure HTTP browser sessions. `hack/argocd-up.sh` requires one kind node with at least 5 CPUs and 6 GiB allocatable. |
| `values-k3s.yaml` | Production `scoped` mode with PostgreSQL transcripts and sessions and out-of-band `swe-platform-postgres` and `swe-platform-session-keyring` Secrets. Uses one operator replica and the default OCI runtime because k3s does not ship gVisor. |
| `values-gke.yaml` | Production `scoped` mode with PostgreSQL transcripts and sessions and both out-of-band Secrets. GKE Sandbox is enabled on environment nodes. Sets `runtimeClass: gvisor` and runs two operator replicas with leader election. |
| `values-eks.yaml` | Production `scoped` mode with PostgreSQL transcripts and sessions and both out-of-band Secrets. Uses a default EBS CSI StorageClass and two operator replicas. EKS does not provide a standard gVisor RuntimeClass, so managed Templates use the cluster default unless overridden. |

The RuntimeClass applies to environment pods, not the operator. Before creating or retaining
execution, the operator verifies that a RuntimeClass explicitly named by a template exists. A
missing named RuntimeClass fails the Environment with `InvalidConfiguration`; removing one
withdraws readiness and fences its exact-owned pod, while recreating it allows provisioning to
recover. RuntimeClass identity is pinned by UID, so the first operator upgrade containing this
check also replaces existing pods that use a named RuntimeClass; their workspace PVCs are
retained. The chart does not create or manage provider RuntimeClasses.

This existence check does not prove that the runtime handler works or that eligible nodes can
run it. Cluster operators must still install and test the handler on every eligible environment
node; unsupported scheduling or handler failures may leave pods Pending. `make kind-up` performs
that separate smoke test for its pinned gVisor installation. Templates that omit `runtimeClass`
continue to select the cluster default runtime as documented in the preset table above.

The operator creates a default ingress NetworkPolicy for each environment. It permits the
environment's sandboxd port only from this release's control-plane- and operator-labeled pods
in the configured control-plane namespace. The cluster CNI must enforce
Kubernetes NetworkPolicy for this defense in depth; TLS identity and capability authorization
remain mandatory regardless. See [the security model](../../SECURITY.md) for credential
lifecycle and backend requirements.

The chart does not install default-deny egress or an egress allowlist proxy. Project
`egressAllowlist` values are reserved and rejected when non-empty; empty or omitted values leave
egress subject to the cluster's network configuration.

For local development, use `values-kind.yaml`; it references locally loaded `:dev`
images and disables leader election. `values-argocd.yaml` is the preset for the
local Argo CD mirror (`hack/argocd-up.sh`): it tracks the mutable `:latest` images
published from main and references an out-of-band bootstrap token Secret. Its `tiny`
template requests 1 CPU and 2 GiB per Environment. The configured warm minimum of one
therefore needs capacity for two Environments while a claimed member and its replacement
overlap. The bootstrap checks for at least 5 CPUs and 6 GiB allocatable on one node before
installing Argo, leaving the capacity beyond the Environments' 2 CPUs/4 GiB for Kubernetes,
Argo CD, the Image Updater, the operator, and the control plane. If local capacity is more
important than warm starts, explicitly set `environmentTemplates[0].spec.warmPool.min=0`;
that removes replacement overlap but makes every Run wait for environment provisioning and
the `env-base` image pull.

## Durable transcript storage

The production presets set `controlPlane.transcripts.postgresSecret.name` to
`swe-platform-postgres`. The named, out-of-band Secret must contain a PostgreSQL connection URL
under the configured key (default `url`). Do not put the URL or password directly in values:

```sh
kubectl create namespace swe-platform-system --dry-run=client -o yaml | kubectl apply -f -
kubectl -n swe-platform-system create secret generic swe-platform-postgres \
  --from-literal=url='postgres://swe:REDACTED@postgres.example:5432/swe?sslmode=require'
helm upgrade --install swe-platform ./charts/swe-platform \
  --namespace swe-platform-system --create-namespace \
  --values ./charts/swe-platform/values-k3s.yaml
```

The control plane connects and applies ordered embedded migrations under a PostgreSQL advisory
lock before listening. Migration or connection failure fails startup; an append is acknowledged
only after its event, per-Run sequence, retention changes, and retained-window idempotency record
commit in one transaction. Back up the database before upgrades and grant the database role
`CREATE`/`ALTER` rights on its dedicated schema for migrations plus ordinary table DML. There is
currently no separate migration job or schema-name setting. Pre-create a role-owned application
schema, fix the connection's `search_path` to it, and revoke `CREATE` on that schema from
untrusted roles. Applied migration files are immutable; upgrades add a new ordered file.

`maxEventsPerRun` (10,000) and `maxBytesPerRun` (64 MiB) bound retained data independently for
each immutable `(namespace name, Namespace UID, Run UID)`. Eviction removes the event and its
idempotency key in the same append transaction, preserving the existing retained-window
idempotency contract. Replay is
bounded by `maxReplayEvents` (1,000). `subscriberBuffer`, `maxSubscribers`, and `pollInterval`
bound process-local SSE polling resources. Database replay/polling is correctness truth; no
notification facility is required. A subscriber overtaken by retention is disconnected and its
normal SSE reconnect receives the existing explicit cursor-gap response.

With no PostgreSQL Secret (including the checked-in kind and Argo development presets), the
control plane logs a warning and uses the bounded process-local memory store. That mode loses
transcripts and invalidates cursors on restart and is not supported for production. To exercise
the PostgreSQL integration tests locally, provide a disposable database:

```sh
SWE_TEST_POSTGRES_URL='postgres://postgres:postgres@localhost:5432/swe_test?sslmode=disable' \
  go test ./internal/controlplane -run TestPostgresTranscriptStoreContract -count=1
```

PostgreSQL makes replay and idempotency durable across restart, but this release deliberately
keeps `controlPlane.replicaCount=1`; it does not claim multi-replica control-plane HA.

## Durable browser sessions

`controlPlane.sessions.backend` is required to be `memory` or `postgres`. The default, kind,
and Argo presets explicitly use bounded, process-local `memory` storage for development. The
production presets use `postgres`, reuse `controlPlane.transcripts.postgresSecret` for
`SWE_POSTGRES_URL`, and mount the administrator-owned keyring Secret read-only at
`/var/run/secrets/swe-platform/session-keyring/keyring.json`. The chart never creates this
Secret and grants no Secret RBAC. Its configured key is projected to the fixed filename
`keyring.json`; the JSON schema is exactly:

```json
{"version":1,"activeKeyId":"<id>","keys":[{"id":"<id>","masterKey":"<canonical unpadded base64url 32 bytes>"}]}
```

The control plane's ordered PostgreSQL migrations include the `browser_sessions` storage.
Startup fails on a database, migration, keyring, or configuration error: production never
falls back to process memory. Session credentials are encrypted before database storage, but
the database necessarily stores ciphertext and metadata. Encryption protects a database-only
disclosure; compromise of the control-plane process, mounted keyring, Kubernetes administrator,
or a database-plus-keyring backup can recover credentials. Protect and separately control
access to both systems.

To rotate, generate a new 32-byte canonical unpadded base64url key, add it with a unique ID,
set `activeKeyId` to that ID, retain every prior key for at least the one-hour absolute session
TTL **plus operational margin**, update the Secret, and roll the control-plane Deployment. Only
after that interval may the old key be removed and another rollout performed. A rollback must
receive a keyring containing every key needed by sessions it may read; retain old keys through
the rollback window. The administrator-owned Secret survives Helm uninstall and rollback.

PostgreSQL sessions survive a control-plane pod replacement, and every cookie request still
repeats Kubernetes TokenReview and exact SubjectAccessReview, so upstream expiry/revocation and
RBAC remain authoritative. Sessions have a one-hour absolute TTL and a 10,000-session limit;
at capacity new session exchange is rejected rather than evicting an existing session. Logout,
expiry, or failed TokenReview removes the record. The chart intentionally enforces one control-
plane replica and `Recreate`: replacement interrupts open SSE and WebSocket connections, which
clients must reconnect, and neither durable sessions nor transcripts constitute a live-connection
survival or HA claim.

Per-Run event and byte limits do not bound total database size across Run churn. Deleting a Run
or fencing a Project does not reclaim its UID-fenced transcript rows. New and safely associated
rows include the immutable Namespace UID. Legacy rows with no Namespace UID are associated only
through an authorized exact current Run UID; otherwise they are retained indefinitely. The
retain-only Project offboarding phase deliberately has no deletion API. Until a future exact
Namespace-UID-preconditioned durable purge operation ships, operators must monitor and provision
the dedicated transcript database for accumulated history.
Total retained storage across all Runs can be checked directly against the per-Run accounting
columns the append path maintains:

```sql
SELECT count(*) AS runs,
       coalesce(sum(retained_events), 0) AS retained_events,
       coalesce(sum(retained_bytes), 0) AS retained_bytes
FROM transcript_runs;
```

or grouped by namespace:

```sql
SELECT namespace, namespace_uid, count(*) AS runs,
       coalesce(sum(retained_events), 0) AS retained_events,
       coalesce(sum(retained_bytes), 0) AS retained_bytes
FROM transcript_runs
GROUP BY namespace, namespace_uid
ORDER BY coalesce(sum(retained_bytes), 0) DESC;
```

These queries are read-only and report retained history only; they do not advise when or whether
to delete. There is no supported manual purge recipe: name-only SQL can cross a reused Namespace,
and direct row deletion is neither Namespace-UID-preconditioned nor resumable. Retain the rows.

### Backup and restore

The following categories of state require backup planning for a BYOC installation; the list
is not exhaustive:

| State | Location | Helm reconstructs it? |
|---|---|---|
| Transcript events | PostgreSQL database | No |
| Infrastructure state (Installation, Namespace claims, Project, Run, Environment, AgentCredentialProfile, managed Project-local EnvironmentTemplate copies) | Kubernetes API / etcd | No — Helm reapplies the system Installation and catalog sources, not Project namespace resources |
| Workspace contents (cloned repos, agent work, uncommitted changes) | Environment PVCs | No |
| Installation and credential material (out-of-band PostgreSQL URL, session keyring, bootstrap token Secrets, chart values overrides) | Kubernetes Secrets and local configuration | No |

Back up the PostgreSQL transcript database before every upgrade using your provider's backup
mechanism (`pg_dump`, managed-service snapshots, or equivalent). To restore transcripts only,
point the same connection URL at the recovered database and restart the control-plane pod.
The control plane applies ordered embedded migrations under a PostgreSQL advisory lock on
startup, so a restored database at an older migration version is brought forward automatically.
This database-only recovery path restores transcript events; it does not reconstruct
infrastructure state, workspace contents, or installation material.

Separately, export or back up the Namespace claims and custom-resource instances (Project, Run,
Environment, AgentCredentialProfile, and managed Project-local EnvironmentTemplate copies) and snapshot or back
up workspace PVCs that your recovery objectives require. Helm alone cannot reconstruct either:
it reapplies CRD schemas, the Installation object, and catalog sources, not the Project resources that
represent desired and observed infrastructure state, and workspace PVCs are per-environment
volumes whose contents survive pause/resume but are not replicated or backed up by the
platform. AgentCredentialProfile backing Secrets are controller-created and bound to the
profile's exact owner UID; do not copy them into a replacement cluster. Instead, preserve or
re-provision out-of-band secret sources (the PostgreSQL connection Secret, session keyring, bootstrap token,
and any chart values overrides), and recreate agent API-key credentials through the supported
`swe credentials create` / `--api-key-stdin` flow, then rotate if necessary.

A coordinated cluster-loss restore order, RPO, and RTO are not tested or provided by this
release. The monitoring queries above report retained transcript history so you can size
database backups. Per-Run retention limits bound individual Run transcript windows, but total
database size is not bounded across Run churn until the garbage-collection policy in
[#101](https://github.com/Chris-Cullins/swe-platform/issues/101) ships.

## BYOC operations

The k3s, GKE, and EKS presets are the starting point for running swe-platform in your own
cluster. This section consolidates the provider-specific prerequisites, pre-flight validation,
and networking requirements that a BYOC operator needs beyond the [install](#install) and
[upgrade](#upgrade) procedures above. The acceptance criteria track
[#79](https://github.com/Chris-Cullins/swe-platform/issues/79); the hosted offering alpha is
out of scope here and requires separate maintainer product input.

### Provider prerequisites

Each production preset assumes out-of-band `swe-platform-postgres` and
`swe-platform-session-keyring` Secrets (see [Install](#install),
[Durable transcript storage](#durable-transcript-storage), and
[Durable browser sessions](#durable-browser-sessions)) and a default StorageClass for
environment workspace PVCs. The operator creates PVCs with
`ReadWriteOnce` access and leaves `spec.storageClassName` unset, so the cluster's default
StorageClass admission supplies the storage; the chart does not create or manage
StorageClasses. Provider-specific runtime, replica, and isolation assumptions are
documented in the [preset table](#environment-image-footprint) above.

### Pre-flight validation

Validate every production preset before installation. The chart's CI runs the same checks;
reproducing them locally confirms that the preset renders against the current chart version:

```sh
(
  set -e
  for preset in k3s gke eks; do
    helm lint ./charts/swe-platform --values "./charts/swe-platform/values-${preset}.yaml"
    helm template swe-platform ./charts/swe-platform \
      --namespace swe-platform-system \
      --values "./charts/swe-platform/values-${preset}.yaml" >/dev/null
  done
)
```

The production presets reference both out-of-band Secrets by name; the chart does not create
them, so the template renders whether or not they exist. A missing Secret keeps the control-
plane pod from starting after installation, with no memory fallback.

### Networking requirements

| Requirement | Chart behavior | Operator action |
|---|---|---|
| Control-plane exposure | ClusterIP Service on port 80 (HTTP); no Ingress or TLS termination created | Provide a TLS-terminating reverse proxy or load balancer; production browser sessions require HTTPS |
| Environment sandboxd isolation | Ingress NetworkPolicy per environment pod: port 50051 admitted only from release-namespace control-plane and operator pods | CNI must enforce Kubernetes NetworkPolicy for defense in depth; TLS and capability authorization remain mandatory regardless |
| Environment egress | No default-deny egress or egress proxy | Environment egress is subject to cluster network configuration; `Project.spec.egressAllowlist` is reserved and rejected when non-empty |
| Operator → control plane | In-cluster HTTP to the control-plane Service | No external networking required; both components run in the release namespace |

The `controlPlane.auth.trustProxyHeaders` option is available for a trusted reverse proxy
that overwrites `X-Forwarded-Host` and `X-Forwarded-Proto`; see
[Control-plane authentication and authorization](#control-plane-authentication-and-authorization)
for session, CSRF, and WebSocket origin requirements.

## Control-plane authentication and authorization

Terminal and transcript endpoints require a credential. The control plane authenticates
Kubernetes bearer tokens with `TokenReview` for the `swe-platform` audience (configurable
with `controlPlane.auth.tokenAudience`), then asks `SubjectAccessReview` about the
exact namespace, resource name, and subresource on every request:

| Operation | Kubernetes authorization attributes |
|---|---|
| List Runs | `list` on `runs` with an empty `resourceName` |
| Watch Runs | `watch` on `runs` with an empty `resourceName` |
| Create a Run | `create` on `runs` with an empty `resourceName` |
| Read a Run | `get` on `runs` with the requested Run `resourceName` |
| Cancel a Run | `update` on base `runs` with the requested Run `resourceName` |
| Read an Environment | `get` on `environments` with the requested Environment `resourceName` |
| Read a transcript | `get` on `runs/transcript` with the requested Run `resourceName` |
| Append a transcript event | `update` on `runs/transcript` with the requested Run `resourceName` |
| Open a direct Environment terminal | `get` on `environments/terminal` with the requested Environment `resourceName` |
| Open a Run terminal | `get` on the requested base `runs` `resourceName`, then `get` on the resolved `environments/terminal` `resourceName` |

This permits producer credentials to be restricted to one Run using an RBAC Role with
`resourceNames`. The namespace is part of the URL only as a resource selector; it becomes
authoritative only after RBAC authorizes that exact namespaced identity. In scoped mode,
authorization happens first and is then supplemented by the restart-bound
`tenancy.namespaces` allowlist and an uncached proof of the exact active Installation UID,
Namespace UID, Namespace annotations, sole Project name/UID, and lifecycle. TokenReview/SAR is
never replaced by labels or claims. Unknown Runs are
rejected before transcript state is allocated; every transcript read and append additionally
requires the exact `SWE-Run-UID` header so a recreated Run never receives stale events or
readers. An already-open stream is not continuously reauthorized or closed when its Run is
deleted. Transcript event `data` remains opaque, adapter-owned JSON.

Service clients send `Authorization: Bearer <token>`. A browser exchanges an explicit,
non-bootstrap bearer credential with `POST /api/v1/session`. After a successful TokenReview,
the control plane stores that credential in the configured bounded session backend and places only a
random 256-bit opaque identifier in an `HttpOnly`, `Secure`, `SameSite=Strict`, `Path=/`
cookie named `swe-platform-session`; it does not issue a platform token or refresh token.
Every cookie-authenticated request resolves the server-side credential and repeats TokenReview
before SAR, so upstream expiry and revocation still apply. Sessions have a one-hour absolute
lifetime, credentials are limited to 16 KiB, and the backend accepts at most 10,000 active
sessions, rejecting new exchanges at capacity rather than evicting entries. Logout, absolute
expiry, or a failed TokenReview deletes the server-side entry.
Memory-backend restart logs browsers out; PostgreSQL sessions survive replacement as described
above. `GET /api/v1/session` validates
the current session and `DELETE /api/v1/session` revokes it. Production session exchange requires HTTPS. Only the
kind and Argo development presets set `controlPlane.auth.allowInsecureSessions=true`, which
allows HTTP and omits the cookie's `Secure` flag.

Cookie-authenticated Run creation, cancellation, and session deletion require an exact
same-origin `Origin`; explicit bearer service clients remain supported without `Origin`.
Session cookies remain rejected for transcript appends, which require an explicit bearer
service credential. WebSocket requests with an `Origin` must be
same-origin, including scheme, host, and port. Forwarded headers are ignored by default.
Behind a trusted reverse proxy, set `controlPlane.auth.trustProxyHeaders=true`; the control
plane then requires single-valued `X-Forwarded-Host` and `X-Forwarded-Proto` headers, so
the proxy must overwrite (not append or pass through) both. Non-browser WebSocket clients
without `Origin` are allowed only with an explicit bearer credential. Tokens are never
accepted in query parameters.

Direct Environment WebSockets use
`/api/v1/namespaces/{namespace}/environments/{name}/terminal` and require the exact bounded
Environment UID in `SWE-Environment-UID`. This keeps terminal-subresource-only RBAC viable:
`swe attach ENVIRONMENT --environment-uid UID` never performs a base-Environment read.
The native Run terminal route `/runs/{run}/terminal` retains its bounded `SWE-Run-UID` and
`SWE-Environment-UID` headers. Browsers, which cannot set those headers in the WebSocket
constructor, use `/runs/{run}/terminal/{runUID}/{environmentUID}` with each UID encoded as one
path segment. UIDs are non-secret identity preconditions whose URL/log/devtools visibility is
intentional; credentials and tickets are never accepted there.

For the browser route, authentication and exact base-Run authorization precede UID decoding,
bounds, and staleness checks. The server then resolves the exact current Run/Environment
association, authorizes the exact Environment terminal, checks WebSocket origin and upgrade,
and enters the same repeatedly association-, hold-, revision-, execution-, and backend-fenced
dial path used by native Run clients. Direct stale/missing identities and stale, released, or
reassigned Run associations fail before activity, wake, readiness polling, connector/backend
resolution, health checks, or upgrade. The Run DTO exposes `environment.uid` and
`terminalAvailable` only for an exact current association; browser terminal navigation must be
absent otherwise and must retain both identities on remount.

When the control plane is enabled, the chart projects a rotating `swe-platform`-audience
service-account token into the operator and grants that identity `update` on
`runs/transcript`. The operator uses it only to forward opaque adapter events to the
control-plane Service. This platform transport credential is separate from agent provider
credentials, which the chart never adds to ambient component or Environment state; supported
API-key adapters deliver them only as write-only process launch material.

For initial self-hosted setup, an optional static bootstrap token provides all control-plane
API permissions. Create it out of band and reference it during installation:

```sh
kubectl -n swe-platform-system create secret generic swe-platform-bootstrap \
  --from-literal=token="$(openssl rand -hex 32)"
helm upgrade --install swe-platform ./charts/swe-platform \
  --namespace swe-platform-system --create-namespace \
  --set controlPlane.auth.bootstrapTokenSecret.name=swe-platform-bootstrap
```

The bootstrap token bypasses Kubernetes RBAC and is therefore equivalent to a control-plane
administrator credential. It must contain at least 32 characters, is accepted only as an
explicit bearer credential (never as a browser session), and changes require a control-plane
rollout. Use it only over TLS, store it outside values files, configure normal Kubernetes
Roles/RoleBindings, then remove the Helm value and Secret. Without this option, only
Kubernetes tokens with the configured audience and authorization from RBAC can use the APIs.

For example, this namespaced Role allows one adapter ServiceAccount to append only to
`run-123`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: run-123-transcript-producer
  namespace: project-a
rules:
  - apiGroups: ["swe.dev"]
    resources: ["runs/transcript"]
    resourceNames: ["run-123"]
    verbs: ["update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: run-123-transcript-producer
  namespace: project-a
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: run-123-transcript-producer
subjects:
  - kind: ServiceAccount
    name: run-123-adapter
    namespace: project-a
```

Create the ServiceAccount, then mint a short-lived credential with
`kubectl create token run-123-adapter -n project-a --audience=swe-platform`.

## Operations console

The production control-plane binary and image embed the React operations console and serve it
from `/` on the same origin as the resource API, transcript SSE stream, and terminal WebSocket.
There is no separate UI workload or Service. For local access, forward the existing Service and
open `http://127.0.0.1:8080/`:

```sh
kubectl -n swe-platform-system port-forward \
  service/swe-platform-swe-platform-control-plane 8080:80
```

For the Argo kind mirror, use `make argocd-ui` instead and open
`http://127.0.0.1:18080/`. That foreground helper targets the `kind-swe-argo` context and
restarts its loopback-only Service forward when a rollout replaces the selected pod. Set
`KIND_ARGO_CLUSTER` or `ARGO_UI_PORT` to override its cluster or local port. A reconnect
restores the URL and all control-plane routes, but cannot preserve an open SSE/WebSocket TCP
connection across control-plane replacement. This development preset's memory-backed browser
sessions are also lost on replacement; production PostgreSQL sessions remain valid.

The kind preset permits HTTP browser sessions for this local flow. Production browser sessions
still require HTTPS. To build the embedded binary outside the image build, run `make ui-build`
followed by `make build-control-plane-production`; ordinary Go builds intentionally omit the UI
and do not require generated Vite assets.

## Run and Environment resource API

The console-facing resource API exposes explicit DTOs rather than Kubernetes objects:

- `GET/POST /api/v1/namespaces/{namespace}/runs`
- `GET /api/v1/namespaces/{namespace}/runs/{name}`
- `POST /api/v1/namespaces/{namespace}/runs/{name}/cancel`
- `GET /api/v1/namespaces/{namespace}/environments/{name}`

Representative JSON contracts are committed in
[`internal/controlplane/testdata/contracts`](../../internal/controlplane/testdata/contracts).
Run lists default to 50 items and accept `limit=1..200` plus an opaque, bounded `continue`
token. Create bodies are limited to 1 MiB, reject unknown fields, and require a caller-chosen
Kubernetes DNS-subdomain `name` as the retry key. An existing same-name Run is returned only
when the caller is separately authorized to get that exact Run and its immutable intent
matches; otherwise the API returns a conflict without exposing it. Clients select either an
existing Environment or a Project/Template allocation intent. Only the Run is created—the
Run controller exclusively allocates or claims Environments. Cancellation monotonically sets
`spec.cancel` and retries bounded Kubernetes update conflicts. Every cancellation body must
include the expected immutable Run UID, for example `{"runUID":"<uid from the Run response>"}`.
Missing or empty UIDs fail before resource resolution, and a UID that no longer matches a
same-name Run replacement returns `409 Conflict`.

Run and Environment responses omit raw CRDs, managed fields, conditions, transcript storage
references, sandboxd/terminal endpoints, pod names, image IDs, and secrets. Environment
`ready` is derived only from the current generation's Ready condition. New REST errors use
`application/problem+json`; transcript event framing and terminal WebSocket wire contracts are
unchanged. Transcript GET identity now requires the exact Run UID header described below.

## Transcript API

After forwarding the control-plane Service, adapters can append JSON transcript events
and clients can consume replay plus live events as an SSE stream:

```sh
kubectl -n swe-platform-system port-forward \
  service/swe-platform-swe-platform-control-plane 8080:80
TOKEN="$(kubectl create token my-reader -n project-a --audience=swe-platform)"
RUN_UID="$(kubectl get run run-123 -n project-a -o jsonpath='{.metadata.uid}')"
curl -N -H "Authorization: Bearer ${TOKEN}" -H "SWE-Run-UID: ${RUN_UID}" \
  http://127.0.0.1:8080/api/v1/namespaces/project-a/runs/run-123/transcript
curl -H "Authorization: Bearer ${TOKEN}" -H 'Content-Type: application/json' \
  -H "SWE-Run-UID: ${RUN_UID}" \
  -d '{"source":"adapter","sourceSequence":1,"idempotencyKey":"event-1","type":"output","data":{"text":"hello"}}' \
  http://127.0.0.1:8080/api/v1/namespaces/project-a/runs/run-123/transcript
```

The platform envelope owns transport metadata only:

- A transcript belongs to the immutable `(namespace name, Namespace UID, Run UID)` identity,
  so a deleted/recreated Namespace, names reused across namespaces, and Run recreation do not
  inherit prior events. Every GET stream and POST append
  must carry the exact current Run UID in the `SWE-Run-UID` header; a missing header returns
  `428 Precondition Required`, a mismatch with the currently resolved Run UID returns
  `409 Conflict`, and an overlong UID returns `400 Bad Request`. Authentication and exact
  transcript authorization happen before identity validation; identity rejection happens
  before store access. Readers must retain the same expected UID on every reconnect so a
  delete/recreate replacement never receives stale events or readers.
- `source` is a bounded, producer-selected idempotency partition, not authenticated
  provenance. `sourceSequence` is optional producer metadata and does not determine order.
- `(Run identity, source, idempotencyKey)` identifies one append while that event remains
  retained. An exact retry returns the original event with `200 OK` and
  `Idempotent-Replayed: true`; reuse with different `type`, `sourceSequence`, or raw `data`
  bytes returns `409 Conflict`. The first committed append returns `201 Created`. The original
  `{type,data}` envelope remains temporarily accepted with `202 Accepted` for compatibility,
  but is explicitly non-idempotent; reliable producers must send `source` and `idempotencyKey`.
- `sequence` is a stable, contiguous total order per Run. `id` is an opaque, versioned,
  store-issued cursor; clients must not parse or synthesize it. `Last-Event-ID` takes
  precedence over `?after=<cursor>` on reconnect.
- Cursors malformed, unverifiable after a memory-store restart, forged, for another Run,
  or ahead of its high-water mark return `400 invalid_cursor`. Authenticated cursors whose
  events are no longer retained return
  `410 cursor_expired` with an `application/problem+json` recovery boundary. A new stream
  without a cursor receives an ID-less `transcript-gap` control event before retained
  history when earlier events have expired.
- The memory store bounds Run count, per-Run and aggregate retained events/bytes,
  idempotency entries, replay size, subscriber count, and subscriber buffers. Capacity
  failures are explicit; a slow subscriber is disconnected rather than blocking producers.

`TranscriptStore` is the durability boundary. The PostgreSQL implementation makes append
linearizable per Run (idempotency check, sequence allocation, persistence, and publication are
one transaction). A repeatable-read replay snapshot records the live cut, then database polling
continues strictly after it. Polling—not process notification—is correctness truth, so restart
and rollout require no sticky transcript routing; clients reconnect with the last SSE event ID.
The store generation and cursor signing key live in PostgreSQL and survive process replacement.
The memory implementation deliberately changes both on restart, making old cursors explicitly
`400 invalid_cursor` instead of silently skipping events. Both stores use retained-window
idempotency: after an event is evicted, its key may be reused and creates a new event.

## Validate

```sh
helm lint ./charts/swe-platform --values ./charts/swe-platform/values-gke.yaml
helm template swe-platform ./charts/swe-platform \
  --namespace swe-platform-system \
  --values ./charts/swe-platform/values-gke.yaml >/dev/null
```
