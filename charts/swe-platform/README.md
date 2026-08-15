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

## GitHub App repository credentials

Per-Run private repository access is disabled by default. To enable
`swe run --git-credential github-app`, create a GitHub App whose installation can access the
intended repositories and whose repository permissions include **Contents: Read and write**.
Do not grant pull-request or administration permissions for this platform contract. Install the
App on each repository that Runs may select, then create the private-key Secret out of band:

```sh
kubectl -n swe-platform-system create secret generic swe-platform-github-app \
  --from-file=private-key.pem=/secure/path/to/github-app-private-key.pem
```

Configure the App client ID and Secret reference in an administrator-owned values file:

```yaml
operator:
  githubApp:
    enabled: true
    clientID: Iv1.example
    secretName: swe-platform-github-app
    privateKeyKey: private-key.pem
```

The chart references but never creates or copies this Secret, and mounts its sole selected key
only into the operator. The operator accepts only canonical
`https://github.com/<owner>/<repo>[.git]` repositories and requests one short-lived installation
token scoped to the exact frozen repository with only `contents:write`. Tokens are held in
finalizer-managed, Run-UID-named lease Secrets in the Project namespace, delivered only to the
clone init container and selected agent process, refreshed through an execution fence, and
revoked on terminal cleanup. They are not projected into hooks, sandboxd, or other containers.
Removing or disabling the App configuration while live credentialed Runs exist prevents new
issuance and leaves terminal revocation pending until the provider is restored or each token's
fixed expiry proves it inactive. Never place an App key or installation token in values, Project
configuration, prompts, or repository URLs.

This chart installs the swe-platform CRDs, operator, first control-plane API, and one
system-namespaced `Installation` identity. Configured `environmentTemplates` are inert catalog
sources in that system namespace; Helm never creates a Project tenancy there.
The control plane accepts adapter-owned transcript events and streams them over SSE.
Production installs must configure the PostgreSQL transcript store described below. The chart
still requires one control-plane replica and uses a non-overlapping `Recreate` deployment:
durable transcripts and encrypted PostgreSQL browser sessions survive process replacement, but
open SSE/WebSocket connections do not and this release makes no control-plane HA claim. The
optional authenticated portal gateway exposes explicitly declared HTTP services through one
wildcard host namespace.

Set `controlPlane.enabled=false` for an operator-only installation. While disabled, the chart
omits the control-plane workload, Service, identity/RBAC, and Ingress and ignores other
`controlPlane.*` settings. Those settings are validated again before the control plane can be
re-enabled, so stale values retained with `--reuse-values` must be corrected first. The
portal-status admission fence remains pre-staged even in operator-only installs.

## Control-plane metrics

The stable control-plane Service exposes a dedicated internal Prometheus scrape port named
`metrics` (8082 by default). Scrape `/metrics` on that port; the listener serves no console,
session, transcript, terminal, or other application route. The binary's
`--metrics-bind-address` defaults to `127.0.0.1:8082` for fail-closed direct execution, while
the chart explicitly binds `:<controlPlane.metrics.port>` so the cluster-internal Service is a
usable scrape target. The port must be 1-65535 and cannot overlap the API's Service port 80 or
container port 8080. The chart
does not install a ServiceMonitor or make the Service externally reachable; apply cluster-local
scrape discovery and NetworkPolicy appropriate to your Prometheus installation.

All custom labels are fixed outcome, kind, or reason sets. They never contain credentials,
sessions, users, namespaces, repositories, URLs, Runs, or Environments. Healthy traffic should
show committed/replayed appends, admitted subscribers returning to baseline, delivered replay
or live events with low lag, terminal grants, and allowed authentication reviews. Investigate:

| Metric | Labels |
|---|---|
| `swe_control_plane_transcript_appends_total` | `outcome`: `committed`, `replayed`, `rejected`, `error` |
| `swe_control_plane_transcript_append_duration_seconds` | same `outcome` values |
| `swe_control_plane_transcript_sse_subscribers` | none |
| `swe_control_plane_transcript_sse_deliveries_total` | `kind`: `replay`, `live`, `gap`; `outcome`: `delivered`, `error`, plus `dropped` for live delivery |
| `swe_control_plane_transcript_sse_delivery_lag_seconds` | `kind`: `replay`, `live` |
| `swe_control_plane_transcript_cleanup_total` | `outcome`: `committed`, `already_absent`, `rejected`, `error` |
| `swe_control_plane_transcript_cleanup_duration_seconds` | same `outcome` values |
| `swe_control_plane_transcript_cleanup_reclaimed_events_total` | none |
| `swe_control_plane_transcript_cleanup_reclaimed_bytes_total` | none |
| `swe_control_plane_terminal_lease_grants_total` | `outcome`: `granted`, `failed` |
| `swe_control_plane_terminal_lease_revocations_total` | `reason`: `run_association_changed`, `environment_changed`, `execution_changed`, `hold_policy_changed` |
| `swe_control_plane_token_reviews_total` | `outcome`: `authenticated`, `denied`, `error` |
| `swe_control_plane_token_review_duration_seconds` | same `outcome` values |
| `swe_control_plane_subject_access_reviews_total` | `outcome`: `allowed`, `denied`, `error` |
| `swe_control_plane_subject_access_review_duration_seconds` | same `outcome` values |

- sustained `swe_control_plane_transcript_appends_total{outcome="error"}` or
  `{outcome="rejected"}` growth;
- a `swe_control_plane_transcript_sse_subscribers` gauge that does not fall after clients
  disconnect, `swe_control_plane_transcript_sse_deliveries_total{outcome="dropped"}` growth,
  or rising `swe_control_plane_transcript_sse_delivery_lag_seconds`;
- repeated `swe_control_plane_terminal_lease_grants_total{outcome="failed"}` or unexpected
  `swe_control_plane_terminal_lease_revocations_total` growth; and
- `swe_control_plane_token_reviews_total` or
  `swe_control_plane_subject_access_reviews_total` denial/error spikes, together with elevated
  matching review duration histograms. Denials can be expected for unauthorized traffic, so
  correlate them with request volume and audit logs rather than paging on every denial.

Histogram families add the standard `_bucket`, `_sum`, and `_count` series. Prometheus Go and
process collectors are also exposed for runtime and process health.

## Operator metrics

The internal `<release>-swe-platform-operator-metrics` Service exposes controller-runtime's
existing operator metrics endpoint on port `metrics` (`operator.metrics.port`, 8080 by
default). Scrape `/metrics` on that port.
The chart does not install a ServiceMonitor or make this Service externally reachable; apply
cluster-local scrape discovery and NetworkPolicy appropriate to your Prometheus installation.
The same endpoint retains controller-runtime's standard Go, process, workqueue, and controller
metrics alongside these bounded-cardinality platform collectors:

| Metric | Labels |
|---|---|
| `swe_operator_run_allocation_duration_seconds` | `path`: `owned_create`, `warm_claim` |
| `swe_operator_warm_pool_allocations_total` | `outcome`: `hit`, `miss` |
| `swe_operator_execution_fence_rejections_total` | `component`: `environment_uid`, `execution_generation`, `lifecycle_epoch`, `hold_revision`; `call_site`: `ensure_accepted`, `observe`, `run_cancel`, `terminal_cleanup`, `finalizer_cleanup` |
| `swe_operator_adapter_operations_total` | `adapter`: registered adapter (`amp`, `claude-code`, `codex`, `pi`); `operation`: `ensure_accepted`, `observe`, `cancel`; `outcome`: `success`, `pending`, `rejected`, `error` |
| `swe_operator_adapter_operation_duration_seconds` | same `adapter`, `operation`, and `outcome` values |
| `swe_operator_pod_recovery_transitions_total` | `transition`: `attempt`, `exhausted` |
| `swe_operator_environment_lifecycle_transitions_total` | `transition`: `suspend`, `resume`; `reason`: `Hold`, `Idle`, `Requested` |

Allocation duration starts at the durable Run creation timestamp and ends only when the Run
controller successfully publishes `EnvironmentAllocated`. Automatic owned creation is a warm
pool miss; a successful warm claim is a hit. A recovered allocation is not observed again,
which prevents reconcile/restart duplicates but can omit a process-local sample after an
uncertain prior status response. The current durable contract has no stable warm-claim
timestamp that survives the later readiness transition, so this release deliberately does not
publish claim-to-ready latency rather than approximating it or adding status solely for metrics.

Healthy operation normally shows allocation observations paired with warm hit/miss traffic,
successful adapter operations, and occasional balanced suspend/resume transitions. Investigate:

- sustained adapter `error` or acceptance `rejected` growth, or elevated adapter duration;
- execution-fence rejection spikes, especially repeated rejection at one closed call site;
- repeated recovery `attempt` growth or any `exhausted` transition; and
- sustained aggregate suspension growth without corresponding aggregate resumes; use the
  fixed reason labels diagnostically because a suspended Environment can change reasons.

`pending` cancellation is an expected retryable adapter state, not a failure by itself. Fence
components name tuple members only, never their values. No custom label contains credentials,
sessions, cookies, users, namespaces, repositories, URLs, errors, Runs, Environments, Pods, or
other resource identity.

The operator reconciles each `Run` as the single task intent and allocates or claims its
`Environment`; clients must not create the two resources independently. Its RBAC permits
Run status/finalizer updates and Environment allocation/claim updates. Process execution
remains behind the environment's portable sandboxd contract rather than Kubernetes exec.
Environment status is controller-owned: the control-plane role can read and patch the base
Environment resource where required and receives update-only `environments/status` authority
for the gateway's bounded portal route allocation fields. That narrow authority remains when
portals are disabled so discovery can tombstone active routes and publish a durable denial
generation; a release-scoped fail-closed admission policy rejects changes by that ServiceAccount
to every controller-owned status field.
The same exact-ServiceAccount fence restricts ordinary Environment patches to current,
Idle-scoped Terminal/Portal wake intents and the fixed Terminal/Portal activity annotation
slots; it rejects changes to holds, suspend intents, services, all other spec fields, labels,
`generateName`, finalizers, owner references, and other annotations. API-server bookkeeping
(`resourceVersion`, `generation`, and `managedFields`) remains exempt so legitimate patches are
not denied. These policies require the chart's stated Kubernetes 1.33 minimum and do not match
the operator ServiceAccount. Helm rejects an operator `serviceAccount.name` equal to the derived
control-plane ServiceAccount name even when the control plane is disabled, because the admission
policies are always pre-staged for later enablement. A reciprocal policy
rejects operator ServiceAccount changes to gateway-owned portal route status. Both policies are
pre-staged when portals are disabled so later enablement cannot expose an unfenced status writer.
The chart rejects configurations that give the operator and control plane the same ServiceAccount
name because that shared principal cannot meet the disjoint field-ownership contract.

## Install

Kubernetes 1.33 or newer is required.

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

`installation.isolation.mode` is a separately explicit, non-defaulted API selection. The kind
and Argo development presets set `UnrestrictedDevelopment`. Production presets deliberately
leave it empty for staged legacy migration because the only restricted mode,
`RestrictedProductionCalicoV3_32_1`, is schema-only and cannot become active in this release.
The restricted values path requires exact policy ConfigMap name, RuntimeClass name/handler, and
StorageClass name/CSI driver; it does not create or verify those objects. Keep an
administrator-selected value in the Helm release values on every upgrade so Helm remains the
owner of the Installation selection rather than relying on an out-of-band edit. The selection
does not change the Installation UID or Namespace-claim authority.

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

Environment provisioning inputs are captured in controller-owned `status.provisioning` before
child resources are created, then verified against uncached exact source incarnations in a
separate reconcile before provisioning proceeds. Updating a catalog or managed Template's image, size, disk,
RuntimeClass, or backend affects new Environments (and rolls stale warm-pool members), not an
existing Environment's replacement or resume; `idleTimeout` and `warmPool.min` remain live
policy. PVC expansion is unsupported, so disk-size changes require a new Environment. Apply the
CRD upgrade above before relying on this status schema and selector immutability admission.
The upgraded operator migrates a pre-snapshot Environment by first durably recording its exact
PVC UID in controller-owned status, withdrawing readiness, deleting its exact-owned Pod, and
then revoking its exact-owned sandboxd credential. It retains
the exact-owned PVC and NetworkPolicy, freezes the current authoritative managed Template and
Project values plus the retained PVC UID through the normal unverified/verified handshake, and
only then may recreate an active Pod on the retained workspace. Missing, deleting, or same-name
replacement PVCs fail closed. Uncached authority checks immediately before and after Pod creation
delete a raced Pod before readiness publication. This cannot reconstruct historical source values that the
old API never stored. Held or suspended Environments remain podless and paused during capture;
foreign fixed-name children are neither adopted nor deleted.

The production presets create a `medium` catalog source using the published `env-base` image;
onboarding creates each runnable Project-local Template copy. Operator, control-plane, and
env-base tags default to the chart
`appVersion`, keeping a released chart on one tested version set and making Helm rollback
restore that set. Override all three tags with a coordinated release or traceable SHA tag when
testing a different set; `latest` and `dev` are development-only choices. Use digests below when
the registry reference must be immutable.

For an exact published build, set `image.digest`, `controlPlane.image.digest`, and
`environmentImage.digest` to the three `sha256:` values in the publish workflow's release
manifest. A digest takes precedence over its corresponding tag. The
[`BYOC runbook`](BYOC.md#choose-and-pin-an-input) downloads and validates this artifact.

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
| `values-kind.yaml` | Isolated local kind development with explicit inert `UnrestrictedDevelopment` isolation selection, process-local `memory` sessions, deliberate `trusted-admin`, `:dev` images, insecure HTTP browser sessions, and `Recreate` operator upgrades. `make kind-up` installs gVisor and snapshot-capable CSI; pass the printed `environmentTemplates[0].spec.runtimeClass=gvisor` catalog override. Primary acceptance instead overrides this preset to scoped mode and distinct system/Project namespaces. |
| `values-argocd.yaml` | Isolated local Argo CD mirror with explicit inert `UnrestrictedDevelopment` isolation selection, process-local `memory` sessions, deliberate `trusted-admin`, mutable `:latest` images, an out-of-band bootstrap Secret, insecure HTTP browser sessions, and `Recreate` operator upgrades. `hack/argocd-up.sh` requires one kind node with at least 5 CPUs and 6 GiB allocatable. |
| `values-k3s.yaml` | Production `scoped` mode with legacy-unclassified isolation, PostgreSQL transcripts and sessions, and out-of-band `swe-platform-postgres` and `swe-platform-session-keyring` Secrets. Uses one operator replica, `Recreate` operator upgrades, and the default OCI runtime because k3s does not ship gVisor. It does not claim restricted production isolation. |
| `values-gke.yaml` | Production `scoped` mode with legacy-unclassified isolation, PostgreSQL transcripts and sessions, and both out-of-band Secrets. GKE Sandbox is enabled on environment nodes. Sets `runtimeClass: gvisor`, runs two operator replicas with leader election, and uses `Recreate` upgrades for this compatibility transition. It does not claim restricted production isolation. |
| `values-eks.yaml` | Production `scoped` mode with legacy-unclassified isolation, PostgreSQL transcripts and sessions, and both out-of-band Secrets. Uses a default EBS CSI StorageClass, two operator replicas, and `Recreate` upgrades for this compatibility transition. EKS does not provide a standard gVisor RuntimeClass, so managed Templates use the cluster default unless overridden. It does not claim restricted production isolation. |

The RuntimeClass applies to environment pods, not the operator. Before creating or retaining
execution, the operator verifies that a RuntimeClass explicitly named by a template exists. A
missing named RuntimeClass fails the Environment with `InvalidConfiguration`; removing one
withdraws readiness and fences its exact-owned pod, while recreating it allows provisioning to
recover. RuntimeClass identity is pinned by UID, so the first operator upgrade containing this
check also replaces existing pods that use a named RuntimeClass; their workspace PVCs are
retained. The chart does not create or manage provider RuntimeClasses.

Every Linux Environment Pod also receives supplementary workspace access group `10001` with
`fsGroupChangePolicy: OnRootMismatch`. This makes `/workspace` writable without overriding the
image's UID or primary GID and remains in force after pause/resume. The selected StorageClass and
volume implementation must grant that group access using kubelet ownership changes (CSI
`fsGroupPolicy: File`, or `ReadWriteOnceWithFSType` when its conditions hold), correct CSI
`VOLUME_MOUNT_GROUP` delegation, or equivalent non-CSI behavior. `OnRootMismatch` applies only to
kubelet-side ownership changes, not delegated CSI mounts. Drivers declaring `None` without
effective delegated mount-group handling, root-squashed volumes that cannot grant the group, and
non-POSIX volumes without equivalent access semantics are unsupported. The chart cannot attest
this behavior; operators must validate it for their production storage and runtime. Future
Windows or non-Pod backends require equivalent ACL/preparation semantics rather than the numeric
Linux GID.

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
lifecycle and backend requirements. CI exercises the allow and deny paths on its pinned Calico
kind fixture, but the operator does not detect or attest production CNI enforcement and
Environment status does not report it as effective isolation.

The chart does not install default-deny egress or an egress allowlist proxy. Project
`egressAllowlist` values are reserved and rejected when non-empty; empty or omitted values leave
egress subject to the cluster's network configuration. The Go foundation can validate a future
immutable, content-addressed system policy ConfigMap and perform disabled uncached currentness
proofs. The restricted Installation values can name that future administrator-owned ConfigMap,
but the chart deliberately has no egress-policy document values, ConfigMap, Deployment, Service,
ServiceAccount, RBAC, TLS Secret, or NetworkPolicy for that foundation. No production preset
enables it.

The repository also contains a deliberately disabled experimental Calico v3.32.1 diagnostic
runner on a separate dual-stack kind topology. It is not a chart component, production preset
capability, attestation mechanism, or runtime input, and normal E2E does not run it. Its
human-readable one-shot results do not change this table or claim production enforcement.

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
expiry, or a definitive TokenReview unauthenticated or audience-mismatch result removes the
record. TokenReview transport or `status.error` failures return 503 and retain it; SAR denial
returns 403 and also retains it. The chart intentionally enforces one control-plane replica and
`Recreate`: replacement interrupts open SSE and WebSocket connections, which clients must
reconnect, and neither durable sessions nor transcripts constitute a live-connection survival
or HA claim.

Per-Run event and byte limits do not bound total database size across Run churn. This release
contains the control-plane/store half of exact deleting-Run transcript cleanup, including cutoff,
drain, and idempotent deletion, but deliberately has no operator finalizer/client/RBAC or CLI
wiring. Consequently, deleting a Run or fencing a Project still does not automatically reclaim
its UID-fenced transcript rows. New and safely associated
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

The executable, provider-specific procedures are in the
[`BYOC operator runbooks`](BYOC.md). They pin latest-main images to an exact successful publish,
run the checked-in validator, preserve scoped-tenancy ordering, and cover PostgreSQL backup,
restore, and incident response without claiming production isolation.

### Active Run capacity

Use **50 concurrently active, non-terminal Runs per elected operator leader** as the conservative
planning envelope for this release, not as an admission limit or a demonstrated maximum. Validate
larger installations against their Kubernetes API-server latency, sandboxd RPC duration, and
transcript backend before raising that envelope. Standby operator replicas do not add Run
throughput because only the leader reconciles.

Adapter observation still uses a fixed two-second requeue, so 50 continuously active Runs imply a
nominal 25 adapter polls per second before event-driven reconciles and retries. ProcessService
connections are reused by exact Environment UID/execution generation and idle-close after 30
seconds; deterministic one-minute-equivalent tests reduce process-connector reads from 150 to 124,
complete adapter-path reads from 390 to 364, and physical connection creations from 30 to 1 per
continuously polled Run. Each poll intentionally
retains uncached pre-call Run association, complete execution/Template/Pod/credential lease, and
post-call exact association/backend currentness reads. Pooling therefore removes TLS-redial and
some connector resolution amplification but does **not** remove API-server work, guarantee every
Run is observed exactly every two seconds under queueing, or make the in-process polling design
horizontally scalable. Capacity above this planning envelope needs measured validation;
replacing polling or extracting adapters is separate work.

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
| Control-plane exposure | ClusterIP Service on port 80. Optional portal Ingress owns only `*.controlPlane.portal.suffix` | Expose the ordinary API separately; provide wildcard DNS and an administrator-owned wildcard TLS Secret for portals |
| Environment sandboxd isolation | Ingress NetworkPolicy per environment pod: port 50051 admitted only from release-namespace control-plane and operator pods | CNI must enforce Kubernetes NetworkPolicy for defense in depth; TLS and capability authorization remain mandatory regardless |
| Environment egress | No default-deny egress, policy ConfigMap instance, or egress proxy; parser/currentness code is inert and not chart-rendered | Environment egress is subject to cluster network configuration; `Project.spec.egressAllowlist` is reserved and rejected when non-empty |
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
| Internal deleting-Run transcript cleanup foundation (not currently invoked) | `delete` on `runs/transcript` with the requested Run `resourceName` |
| Open a direct Environment terminal | `get` on `environments/terminal` with the requested Environment `resourceName` |
| Open a Run terminal | `get` on the requested base `runs` `resourceName`, then `get` on the resolved `environments/terminal` `resourceName` |
| Discover/use a portal | exact `get` on `environments/portal`, exact `get` on synthetic `environmentservices/portal` name `<environment>.<service>`, then exact current base Run or Project `get` |

This permits producer credentials to be restricted to one Run using an RBAC Role with
`resourceNames`. The namespace is part of the URL only as a resource selector; it becomes
authoritative only after RBAC authorizes that exact namespaced identity. In scoped mode,
authorization happens first and is then supplemented by the restart-bound
`tenancy.namespaces` allowlist and an uncached proof of the exact active Installation UID,
Namespace UID, Namespace annotations, sole Project name/UID, and lifecycle. The sole lifecycle
exception is the explicit-bearer, exact-name deleting-Run transcript cleanup above during exact
offboarding fencing; it does not admit sessions or any other operation. TokenReview/SAR is never
replaced by labels or claims. Unknown Runs are
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
expiry, or a definitive TokenReview unauthenticated or audience-mismatch result deletes the
server-side entry. TokenReview transport or `status.error` failures return 503 and retain it;
SAR denial returns 403 and also retains it.
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

### Declared-service portals

Portals are disabled in every checked-in preset. Production enables a canonical suffix and
HTTPS. The optional Ingress owns only the wildcard host and references, but never creates, TLS:

```yaml
controlPlane:
  auth:
    # Required for the chart's TLS-terminating portal Ingress. The ingress
    # must overwrite both X-Forwarded-Host and X-Forwarded-Proto.
    trustProxyHeaders: true
  portal:
    enabled: true
    suffix: portal.example.com
    scheme: https
    ingress:
      enabled: true
      ingressClassName: nginx
      annotations: {}
      tls:
        secretName: portal-example-wildcard
```

All wildcard-host paths route to the control-plane Service `http` port; configure ordinary API
ingress separately. HTTP is valid only with explicit insecure development sessions and portal
Ingress disabled. For kind, forward the Service, run
`SWE_CONTROL_PLANE_TOKEN=... swe --namespace project-a portal ENV SERVICE`, and send requests to
the forward with the printed URL's exact Host and bearer. This tests the same gateway/sandboxd
transport used by a hairpin, not arbitrary wildcard DNS. Real in-Environment use requires
operator-supplied wildcard DNS, an internal route to the gateway, and explicit authentication;
cookies are host-only, never Domain cookies.

Helm/onboarding deliberately grants no arbitrary user portal access. A least-privilege Role is:

```yaml
rules:
  - apiGroups: ["swe.dev"]
    resources: ["environments/portal"]
    resourceNames: ["env-a"]
    verbs: ["get"]
  - apiGroups: ["swe.dev"]
    resources: ["environmentservices/portal"]
    resourceNames: ["env-a.web"]
    verbs: ["get"]
  - apiGroups: ["swe.dev"]
    resources: ["runs"] # or projects, with its exact name
    resourceNames: ["run-a"]
    verbs: ["get"]
```

When portals are enabled, the control plane can list namespaced Environments, update Environment
status routes, and has Secret `get` (but not list/watch) in onboarded namespaces; sandboxclient
requests only the exact credential named by the current pod. Environment NetworkPolicy is
unchanged: only installation-labeled operator/control-plane pods reach sandboxd port 50051, and
Ingress never targets an Environment pod. The operator additionally receives exact `get` on
`environments/portal` and `environmentservices/portal`. It uses a rotating projected
service-account token to discover each repository service's exact proof-bearing URL—or the
gateway's durable denial generation when portals are disabled; it never constructs either.

Repositories can declare up to 32 processes in strict version-1 `.swe/services.yaml`:

```yaml
version: 1
services:
  web:
    command: ["npm", "run", "dev"]
    port: 3000 # optional; otherwise durably allocated from 49152..65535
```

`command` is a required non-empty direct argv array, not a shell string. Names are DNS-1123
labels up to 32 bytes. The bounded parser rejects unknown/duplicate fields or names, aliases,
anchors, merge keys, multiple documents, oversized input/argv, and invalid ports. Repository
entries converge into `Environment.spec.services` without replacing API entries; same-name and
repository process port collisions fail closed, while API port aliases remain supported. The CLI
refuses to mutate repository-owned entries. Services receive only `PORT` and discovered
`PUBLIC_URL`, not a portal credential or projected token. If portals are disabled, the gateway
tombstones active routes and the operator stops the complete managed service set; if discovery is
unavailable, declarations remain durable but launch fails closed. Portal UI (#95) is not implemented.

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

The internal `DELETE` on the same exact transcript route is release-sequencing foundation, not a
current user workflow. It accepts only an explicit bearer credential, requires both the exact
`SWE-Run-UID` and `SWE-Namespace-UID`, and repeats TokenReview, exact `delete` SAR, Namespace
identity, and exact deleting-Run currentness after draining admitted appends. Tenancy must be
active except that this exact no-session DELETE may finish during offboarding fencing; onboarding
fencing, fenced claims, and all other operations remain denied. Successful cleanup returns `204`
for both committed and already-absent deletion. Cutoff cancels existing streams and causes future
reads/appends to return the fixed `410 transcript-retention-cutoff` problem. Failed cleanup
remains cut off for retry in the current process; after restart, the still-deleting Run
reestablishes the cutoff before an idempotent store retry. The bounded cleanup metrics above have
no namespace, name, or UID labels. No supported caller ships in this release.

## Validate

```sh
helm lint ./charts/swe-platform --values ./charts/swe-platform/values-gke.yaml
helm template swe-platform ./charts/swe-platform \
  --namespace swe-platform-system \
  --values ./charts/swe-platform/values-gke.yaml >/dev/null
```
