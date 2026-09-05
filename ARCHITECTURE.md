# swe-platform architecture

This root document is the canonical, tracked architecture source for `swe-platform`. It
supersedes the private, gitignored `docs/ARCHITECTURE.md` referenced by earlier decisions; do
not recreate that path. This document records the implemented system and the contracts already
approved for its next stages. Code and generated CRDs remain authoritative for runtime
behavior. Whenever CRD fields or contracts change, update the relevant entries in the current
resource table and ownership/reference documentation below in the same commit.

## Implemented today

### System topology

```text
CLI / TUI / local MCP / browser console
                    |
                    | HTTP, SSE, WebSocket
                    v
             control plane -------- shared PostgreSQL database
                    |                 (transcripts + browser sessions,
                    |                  or bounded dev memory stores)
                    | Kubernetes API
                    v
       CRDs <---- operator/controllers
                    |
                    | Pod backend
                    v
        Environment pod + workspace PVC
          agent processes + sandboxd
                    ^
                    | authenticated TLS gRPC
                    +---- control plane and operator-hosted adapters
```

- **CRDs and controllers.** Kubernetes `swe.dev/v1alpha1` resources hold durable
  infrastructure and task intent. Controllers allocate or claim Environments for Runs,
  reconcile Environment pods and volumes, maintain warm pools, reap idle Environments, and
  drive adapter lifecycle. Platform services and the Installation identity live in a system
  namespace; each onboarded Project and its resources live in one dedicated claimed namespace.
  The chart exposes controller-runtime's existing operator `/metrics` endpoint through an
  internal Service. Platform-specific collectors use only fixed allocation, fence-component,
  call-site, registered-adapter, operation, outcome, recovery, transition, and lifecycle-reason
  labels; they never label tenant or resource identity.
- **Control plane.** A singleton HTTP service exposes typed Run and Environment operations,
  Run watches, transcript ingestion/SSE, browser sessions, the embedded operations console,
  terminal WebSockets, and a wildcard-host portal gateway. Portal discovery allocates durable
  opaque route/tombstone state; every request repeats authorization, policy, currentness, and
  wake checks. A separate internal HTTP listener exposes bounded-cardinality
  Prometheus process and operational metrics at `/metrics`; it does not serve application
  routes. The control plane acts through Kubernetes APIs and the sandboxd connector; it does
  not exec into Environment pods.
- **Clients.** `swe run`, `logs`, `attach`, `tui`, and the bounded local stdio MCP server use
  the control-plane or Kubernetes contracts appropriate to each command. The React console
  uses the same control-plane resource, transcript, and terminal APIs; portal UI remains out of
  scope. The CLI `swe portal ENV SERVICE` performs authenticated discovery and prints the exact
  stable URL.
- **`sandboxd`.** A small daemon in each Environment provides authenticated gRPC services for
  connection-bound exec, keyed managed processes, workspace-confined filesystem access, one
  shared terminal, health, bounded stateless TCP-connect observations, and a capability-gated
  byte tunnel to one declared logical-loopback port. sandboxd does not interpret HTTP or choose
  routes. The observation primitive retains no declarations or results; an operator-only
  connector invokes it with an observation-only credential and a dedicated controller owns the
  fenced advisory status envelope.

Inbox delivery and child-run spawning, branch/PR publication, and non-Pod
Environment backends are not implemented.

### Current CRDs and sources of truth

CRDs are the source of truth for desired and observed infrastructure state. PostgreSQL is
used for durable transcript events and encrypted browser sessions, not as a second
infrastructure-state store. The control plane owns one shared `pgxpool` and ordered,
versioned migrations; migration 003 adds browser sessions.

All current CRDs are namespaced:

| Resource | Current contract |
|---|---|
| `Installation` | System-namespace identity whose immutable Kubernetes UID remains the stable identity used by namespace claims, catalog sources, managed Template copies, and baseline resources. An optional, non-defaulted installation-wide `isolation` selection admits only explicit unrestricted development or the future Calico v3.32.1 restricted contract; omission exists only for staged legacy migration. The isolation controller publishes bounded `LegacyUnclassified`, `Fencing`, `Blocked`, or `Active` status, fixed conditions, exact non-secret observed object identities, and a canonical revision. Unrestricted development can be active but is explicitly non-production. Restricted selection fences every Environment and warm pool, then remains blocked because runtime activation is not implemented. |
| `Project` | One repository URL (represented as a one-item list), a same-namespace default Template reference, changes-workflow metadata, and up to 64 exact lowercase-ASCII FQDN egress selections. The selection contract is admitted, but a non-empty selection remains rejected when an Environment uses the Project because runtime egress enforcement is not enabled. |
| `EnvironmentTemplate` | Pod image, size/resources, disk, runtime class, idle timeout, warm-pool minimum, and backend. Admission currently permits only the `pod` backend. Chart-owned system-namespace objects are inert catalog sources; execution accepts only installation-managed same-namespace copies bound to exact Installation, source, and Project identities. |
| `Environment` | Immutable Template selection, an immutable optional backend override, one-way empty-to-nonempty Project binding, explicit hold policy, bounded wake/suspend/activity intents, and up to 32 API- or Repository-owned service declarations. New declarations have an immutable-per-incarnation random `instanceID`; upgrade-retained legacy declarations may omit it, receive no portal route, and can add it only with a higher revision. Omitted legacy service ownership defaults only to `API` and is immutable thereafter. Controller-owned status includes the immutable resolved `provisioning` snapshot, readiness, lifecycle/execution, claim, observations, activity, and nested backend-neutral recovery state; recovery records attempts, exhaustion, the exact failed execution generation being accounted, and the next allowed attempt time, while generation zero means no recovery identity. The gateway owns bounded `portalRoutes` and monotonic `nextPortalRouteGeneration`; inactive routes are denial tombstones preserved by other status writers. |
| `Run` | Immutable agent task and Environment/Project/Template/agent-credential/repository-credential selection plus monotonic cancellation; status records normalized lifecycle, exact Environment name/UID and ownership, exact credential profile identity, repository credential readiness, accepted lifecycle epoch, and accepted execution generation. The cleanup finalizer retains deletion authority until work/credentials/claims are fenced and exact live-primary transcript cleanup commits (next-release sequencing blocker below). `notify` and `parentRef` are schema placeholders without an implemented inbox. |
| `AgentCredentialProfile` | Immutable adapter and `APIKey` type metadata. Key bytes live in an owner-linked Secret whose name is derived from the profile UID. |

The current ownership/reference shape is:

`Installation.spec.isolation` is installation-administrator authority; tenant resources cannot
select or weaken it. The chart renders it only from explicit bounded values and otherwise keeps
the legacy field omitted. One system-scoped controller owns `Installation.status`; it resolves
restricted dependencies through uncached reads and publishes only bounded non-secret observations.
Neither configuration nor status changes the Installation UID or the Namespace-claim authority
derived from it.

Environment status is controller-owned: the operator and its Run controller retain status
subresource authority. The control plane has update-only `environments/status` authority for
the gateway's bounded `portalRoutes` and `nextPortalRouteGeneration` ownership, including while
portals are disabled so authenticated discovery can tombstone active routes and publish a
durable denial generation. A fail-closed admission policy binds that exact gateway ServiceAccount
to those fields and rejects changes to all controller-owned status, including the provisioning
snapshot.

The control plane's ordinary Environment patch authority is similarly bound by a fail-closed
exact-ServiceAccount admission policy. It may replace only `spec.lifecycle.wake` with a current
UID/current hold-policy, Idle-scoped Terminal or Portal wake intent, and may update only the
fixed `lifecycle.swe.dev/activity-terminal` and `lifecycle.swe.dev/activity-portal` annotation
slots. All other Environment spec, labels, finalizers, owner references, and annotations are
frozen for that identity. This leaves hold/suspend/service and controller bookkeeping ownership
with their existing CLI and controller writers without constraining the operator ServiceAccount.
The fence also freezes mutable `generateName`; it deliberately ignores API-server bookkeeping
(`resourceVersion`, `generation`, and `managedFields`) whose admission-time changes accompany
legitimate patches and do not alter business intent.
Chart validation unconditionally prevents the operator and control plane from resolving to the
same ServiceAccount identity, including while the control plane is disabled and its admission
fence remains pre-staged.

Ordinary Environment mutation ownership is currently:

| Base-resource field | Legitimate writer |
|---|---|
| `spec.lifecycle.wake` | Control-plane terminal/portal traffic and the Run controller through the shared UID/hold-revision-fenced lifecycle helper |
| `metadata.annotations[lifecycle.swe.dev/activity-{terminal,portal}]` | Control-plane terminal and portal heartbeats through the execution-fenced lifecycle helper |
| `metadata.annotations[lifecycle.swe.dev/activity-{inbox,agent,run}]` | Reserved shared lifecycle-helper slots for those platform sources; no current control-plane writer |
| `spec.lifecycle.suspend` | Run cleanup/credential refresh through the shared lifecycle helper; the Environment controller clears rejected or replayed intent |
| `spec.lifecycle.hold` | Environment hold/release CLI, Project offboarding CLI, and Environment controller offboarding/legacy-pause migration |
| `spec.paused` | Deprecated input accepted from legacy clients; only the Environment controller clears it while migrating to `hold` |
| `spec.services` | Environment services CLI for API-owned declarations and declared-service controller for Repository-owned declarations |
| `spec.projectRef`, warm-pool label, and Template owner reference | Run controller's atomic warm-Environment promotion |
| Environment finalizer | Environment controller |
| Repository-provisioning and warm-pool-cleanup annotations | Environment controller and warm-pool controller respectively |

`spec.templateRef` and `spec.backend` have no legitimate update writer after creation. Other
labels, owner references, finalizers, and annotations are not control-plane-owned merely because
Kubernetes permits their mutation. Status ownership remains the separate subresource contract
above. A reciprocal policy binds the exact operator ServiceAccount and rejects changes to the
two gateway-owned fields. Both status policies and the base-intent policy are installed even
when the gateway or control plane is disabled, so the fences precede any later RBAC enablement.
One Kubernetes principal cannot satisfy these disjoint writer contracts, hence the unconditional
ServiceAccount collision check.

```text
Installation --UID claim--> Namespace --exact name + UID--> Project
                              |
                              +--> managed EnvironmentTemplate copy
                              +--> quota, service accounts, RBAC, baseline policy

Project -----same namespace----> EnvironmentTemplate
   |                                  |
   +--------> Run --------------------+
                |                     |
                | name + UID          v
                +--------------> Environment
                                      |
                                      +--> Pod
                                      +--> workspace PVC
                                      +--> sandboxd credential Secret
                                      +--> sandboxd ingress NetworkPolicy

AgentCredentialProfile --owner UID--> Secret
          ^
          | exact name + UID in Run status
          +---------------- Run

GitHub App --short-lived exact-repository token--> Run UID lease Secret
                                                       |
                                                       +--> clone init container
                                                       +--> selected agent process launch material
```

The Run controller creates an owned Environment or exclusively claims a reusable one.
Owned allocations have a Run controller owner reference; claimed allocations carry Run name
and UID in `Environment.status.claimedBy`. Exact UIDs prevent same-name replacements from
inheriting allocation, cancellation, transcript, terminal, or credential authority.

Environment selector admission is deliberately narrow: `templateRef` cannot change;
`backend` cannot change value or presence (including omitted to explicit `pod`); and
`projectRef` may move once from absent/empty to non-empty, but can then neither change nor be
cleared. Lifecycle and service intents remain independently mutable under their own validation.
The Environment controller exclusively owns `status.provisioning`, whose exact schema is
`template {name, uid, generation}`, `backend`, `image`, `size`, `resources` (resolved CPU and
memory requests/limits), `runtimeClassName`, `diskSize`, and optional
`project {name, uid, generation, repository}` and migration-only `legacyWorkspacePVCUID`, plus monotonic `templateVerified` and
`projectVerified` capture-handshake bits. Status admission makes the snapshot immutable except
for adding that Project object once, initially unverified, during warm-member promotion.

Before creating any Pod, PVC, credential Secret, or NetworkPolicy, the controller uncached-reads
the same Environment UID/generation and exact Template and optional Project UID/generation,
resolves defaults, then atomically publishes the unverified snapshot. A later reconcile
uncached-revalidates the exact sources and Environment selectors before monotonically marking
the relevant capture verified; no child or warm capacity is created or counted before then. It
never infers a snapshot from existing child specifications. A legacy Environment with no
snapshot first withdraws readiness,
deletes its exact-owned Pod, then revokes its exact-owned sandboxd credential while retaining its
exact-owned PVC and NetworkPolicy. Only after that fence completes does it resolve and freeze the
current authoritative managed Template and optional exact Project sources through the normal
unverified/verified handshake. Before deleting any execution child it durably records the exact
PVC UID in immutable controller-owned migration status, then copies that identity into the
snapshot. This deterministic migration cannot recover unknowable historical
Template or Project values; it deliberately adopts the current authoritative sources, reuses the
retained workspace, freezes that PVC's exact UID in the immutable snapshot, and runs the resume
path for an active Environment. A missing, deleting, or same-name replacement PVC fails closed
and is never recreated. A suspended or held
Environment remains podless while capture completes and returns to `Paused`. Foreign fixed-name
children are neither adopted nor deleted and cannot authorize capture. Every later reconcile
validates the complete snapshot and exact live source UIDs before consuming it. Deletion and
recreation of a selected Template or bound Project under the same
name therefore fences and removes execution rather than granting the replacement authority;
the old snapshot is retained and the Environment does not silently recover.
Immediately before and after every replacement Pod creation, the controller uncached-revalidates
the exact Environment generation and nonsuspended policy plus the captured Template and optional
Project incarnations and managed Template authority. Drift prevents creation or UID-precondition
deletes the just-created Pod before credentials are bound or readiness is published. Source
generation advances remain legal because resolved provisioning inputs are already immutable.

For one Environment incarnation, Template image, size/resolved CPU and memory, disk size,
runtime-class name, and effective backend plus the Project repository are frozen provisioning
inputs. Template generation and Project generation are recorded for diagnosis but live edits do
not rewrite the snapshot. Template `idleTimeout` remains live lifecycle policy;
`warmPool.min` remains live pool policy; and Project egress allowlist remains live security
policy (currently any non-empty list fences execution as unsupported, even though its bounded v1
selection grammar is admitted). Project-less warm members
are current only when Template name/UID and the resolved provisioning projection match; generation
alone is not compared because it can contain policy-only edits. A provisioning edit makes old
members unclaimable and quarantined while bounded replacement members roll out; the existing
five-minute cleanup grace and at-most-`2 * min` surge rules still apply. Policy-only idle/warm-pool
edits do not stale otherwise-current members.

A snapshotted non-empty RuntimeClass name is resolved on every provisioning attempt. The
RuntimeClass UID is execution-scoped rather than stored in the snapshot: same-name UID
replacement fences the old Pod and a subsequent attempt may use the replacement UID, while a
missing named RuntimeClass fails before child creation. PVC expansion is not supported. The
requested storage remains the snapshotted `diskSize`; changing a Template cannot resize an
existing workspace, and a larger disk requires a new Environment.

`/workspace` must be writable by the Environment image's execution identity before sandboxd is
usable and after every pause/resume. The Linux Pod backend supplies fixed supplementary group
10001 with `fsGroupChangePolicy: OnRootMismatch`; it does not override the image's UID or primary
GID. The storage implementation must grant that supplementary group access, whether through
kubelet ownership changes (`fsGroupPolicy: File`, or `ReadWriteOnceWithFSType` when its access-mode
and filesystem conditions hold), correct CSI `VOLUME_MOUNT_GROUP` delegation, or equivalent
non-CSI volume behavior. `OnRootMismatch` controls only kubelet-side ownership changes and is not
applied when CSI performs the delegation. Drivers that declare `None` without effective delegated
mount-group handling, root-squashed storage that cannot grant the group, and non-POSIX storage
without an equivalent access mechanism are unsupported. This is a required storage contract, not
runtime or CSI capability attestation. Other backends, including a future Windows backend, must
provide equivalent ACL or workspace preparation semantics rather than reproducing a numeric GID.
Security revision 5 replaces older Environment Pods to apply this contract while retaining the
exact workspace PVC and frozen provisioning inputs.

### Tenancy and Project namespace lifecycle

The chart installs one `Installation` in its system namespace. Its optional isolation selection
is explicit and non-defaulted; omission is the legacy migration shape. The object's immutable
Kubernetes UID is still the installation identity; cluster-scoped RBAC names also include
a stable hash of the system namespace, and the leader-election lease name is derived from the
Installation UID, so releases do not share roles, bindings, or leadership. Names and labels are
discovery data, not authority.

`swe project onboard` creates or explicitly adopts one namespace and enforces exactly one
Project in it. Authority is the live Namespace object's own immutable `metadata.uid` plus
RBAC-protected exact annotations for Installation namespace/name/UID, Project name/UID, and
`active`, `fencing`, or `fenced` lifecycle. The Namespace must contain exactly the annotated
live Project UID. Missing, stale, conflicting, deleting, or multiple identities fail closed.
The Project namespace contains the Project, managed Template copies, Runs, Environments and
warm pools, credential profiles and their owner-UID Secrets, workspace PVCs, and a versioned
baseline. Onboarding requires the administrator to supply positive values for every fixed
ResourceQuota key; the platform does not choose capacity. The baseline adds the environment
ServiceAccount with token automount disabled, exact operator/control-plane RoleBindings, and
default-deny ingress. Per-Environment policies still admit sandboxd only from this
installation's system components. No default-deny egress or egress restriction is claimed.

Chart `environmentTemplates` render only as inert catalog sources in the system namespace.
The operator binds each source to the live Installation UID. Onboarding copies selected sources
to the Project namespace with exact Installation, Project, source UID, catalog name, and
revision annotations. An explicit onboarding rerun updates managed copies in place, including
source replacement and spec/revision drift, preserving the local Template UID and warm-pool
status. It refuses unowned or foreign collisions. Removing a source never deletes a local
copy, and catalog sources themselves cannot provision Environments.

Tenancy mode is required and has no compatibility default:

- **`scoped`.** `tenancy.namespaces` is an explicit restart-bound allowlist. Startup reads the
  Installation and each listed Namespace/Project directly and accepts only exact claims;
  transition lifecycles remain startable so interrupted fencing can drain after a restart.
  The operator's controller-runtime cache watches only the listed Project namespaces; with no
  entries, workload controllers are disabled while system catalog preparation remains
  available. Reconcile entry and every client mutation re-read the exact Installation,
  Namespace UID/claim/lifecycle, and sole Project through an uncached reader. The control plane
  performs TokenReview/SAR first, then requires both allowlist membership and the same uncached
  active-claim proof before namespaced resource work, subject only to the exact transcript
  deletion exception below. Namespaced RoleBindings supply workload authority only after
  onboarding.
- **`trusted-admin`.** Explicit opt-in cluster-wide cache and workload RBAC, allowing newly
  claimed namespaces to be discovered without a restart-bound list. Exact Installation,
  Namespace, and sole-Project claims remain mandatory, so multiple releases do not reconcile
  each other's namespaces. It does not result from missing or invalid scoped configuration.

The safe scoped ordering is: install system identity/catalog with an empty list; onboard and
populate a Project namespace; add it to `tenancy.namespaces`; then perform a controlled Helm
upgrade/restart of operator and control plane. Offboarding reverses the security boundary:
`swe project offboard` changes the exact claim to `fencing`, publishes Run cancellation and
Environment hold intents, waits for terminal Runs and suspended podless Environments, then
marks the namespace `fenced`. It deletes no Namespace, Project, Template, quota, RBAC, policy,
credential profile or owned Secret, PVC, Run, Environment, or transcript data. Normal
Environment suspension still revokes the pod incarnation's ephemeral sandboxd credential
Secret. Offboarding does not initiate transcript deletion, but exact cleanup for a separately
deleting Run may finish while the claim remains `fencing` + `offboarding`; `fenced` is denied.
After fencing, operators may retain/archive the namespace, remove it from `tenancy.namespaces`,
and restart the scoped components. Purge is not implemented; it remains
blocked on an exact Namespace-UID-preconditioned durable resumable operation.

### Environment boundary and backend portability

`sandboxd` is the only supported execution/filesystem/terminal contract into an Environment.
Platform consumers receive a backend-neutral connector or adapter sandbox and do not inspect
containers, PIDs, tmux, or OS signals. The protobuf uses logical paths, process owner/role keys,
portable process controls, and opaque execution IDs. The sandboxd module is separate and has
Windows coverage for process containment and filesystem behavior; the current shared terminal
backend is tmux on non-Windows systems. Windows selects an explicit fail-closed backend rather
than trying to invoke tmux; native Windows terminal sessions remain blocked on a real ConPTY
implementation with shared-session and per-attachment semantics.

Only the Kubernetes **Pod backend** is implemented. The connector currently resolves an exact
owned Pod, Pod IP, Pod UID, execution-generation annotation, restart policy, and per-Pod
credential Secret behind its consumer interface. It also exposes an opaque, non-dialing
execution handle/currentness check; Pod identity remains private to the connector.
`kubevirt` and `external-runner` names are retained as planned Go API values, but CRD admission
rejects them and the controller fails unsupported existing resources before provisioning.
Backend pluggability is an invariant on interfaces, not a claim that those backends ship.

Each Environment pod receives a per-incarnation TLS identity and capability tokens. The
operator also creates an ingress NetworkPolicy allowing sandboxd traffic only from the
release's control-plane- and operator-labeled pods in the configured control-plane namespace.
NetworkPolicy enforcement is a cluster/CNI prerequisite; authenticated TLS and capabilities
remain mandatory. Default-deny egress and an egress proxy do not exist today.

### Run, Environment, pause, and execution lifecycle

The Run UID is the idempotency key for allocation, adapter acceptance, and sandboxd managed
process ownership. A durable acceptance-attempt condition is written before calling an
adapter, so cancellation remains conservative after an uncertain response. Run status exposes
adapter-neutral milestones from allocation through terminal success, failure, or cancellation.
Normalized adapter observation reasons and messages are fixed, bounded platform vocabulary;
the Run controller derives the persisted message from the observation enum and never persists
an adapter-returned detail string. Process errors, provider fields, result text, thread IDs, and
other agent-controlled bytes remain only in adapter-owned opaque transcript events. Permanent
adapter rejection and cancellation conditions likewise use fixed platform messages. Reconcile
also normalizes legacy adapter observation/rejection messages before terminal or deletion cleanup.

`Run.status.usage` is currently an unwritten compatibility placeholder. Lifecycle wall duration
is derivable as `FinishedAt - StartedAt`; it includes `Paused` and `NeedsInput` intervals and is
unavailable for Runs that were never accepted because `StartedAt` remains absent. Usage-, token-,
or cost-looking provider data in transcript events remains adapter-owned opaque bytes. The
platform does not normalize or project it into Run status, and it is not accounting truth.

One Environment controller owns suspension transitions. Explicit user/admin policy is a
revisioned `spec.lifecycle.hold`; ordinary callers publish UID- and hold-revision-fenced wake,
suspend, or source-keyed activity intents. The controller owns observed suspension state.
Automatic idle suspension is prevented by an exact non-terminal Run owner or claim. Ordinary
wakes cannot clear an explicit hold, and terminal traffic wakes only an Idle suspension.

The Environment lifecycle controller also owns a monotonically increasing, backend-neutral
execution generation distinct from the suspension epoch. It reserves a fresh positive value
before every actual backend creation attempt; failed or uncertain attempts may leave gaps, but
values are never reused. Initial provision, pause/resume, terminal recovery, Project or security
replacement, and equivalent future-backend replacement paths all pass through that reservation.
Pod executions additionally require `restartPolicy: Never` and an exact canonical generation
annotation, so one generation cannot silently contain a container restart. Legacy generation
zero, missing/malformed annotations, and pre-generation activity intents or receipts fail closed
until the lifecycle controller provisions a fresh execution.

Pause is disk plus transcript, not process checkpointing:

1. the lifecycle epoch increases before suspension;
2. readiness and connection identity are withdrawn;
3. the Environment pod and its sandboxd credential are deleted;
4. the workspace PVC and already-ingested transcript remain;
5. resume creates a fresh pod/credential epoch, runs the repository resume hook, and repeats
   idempotent adapter acceptance with the same Run UID.

`lifecycle.ExecutionFence` is the mandatory backend-neutral capture/revalidation mechanism for
execution-scoped work. Its opaque value always carries the Environment UID, execution generation,
lifecycle epoch, and hold-policy revision; callers cannot construct a partial fence. Terminal
attachment captures one before publishing activity, then atomically rejects stale activity. Its
connector-owned opaque handle also revalidates the exact live backend execution throughout the
lease. Run adapter acceptance, observation, and cancellation use the same complete fence. After
each adapter call, the controller uncached-reads the exact allocation and asks the connector to
prove that the same live backend execution remains current before publishing Run state, releasing
a claim, or removing a finalizer. Delayed results from disappeared or same-name replacement Pods
are therefore discarded even if Environment status has not yet advanced.

Only the Pod backend performs execution today. Project-backed initialization clones the
Project's single repository into `/workspace`, runs `.agents/setup` once per workspace when
present, and runs `.agents/resume` after resume when present. Terminal execution failure has
bounded replacement/backoff while retaining the PVC. `Environment.status.recovery` persists
that backend-neutral progress and deduplicates the exact failed execution by execution
generation; raw Pod identity remains connector-private. Warm pools maintain ready unclaimed Environments;
claiming a generic warm member for a Project recreates its pod so Project initialization runs
before the Run becomes ready.

For one compatibility rollout, the deprecated flat `podRecoveryAttempts`,
`podRecoveryExhausted`, `podRecoveryUID`, and `podRecoveryNextAttemptAt` status fields remain
served. In an early migration phase after deletion handling and before lifecycle or dependency
gates, meaningful nested `recovery` state is authoritative and stale flat
state is cleared. Otherwise the controller atomically migrates the legacy budget, exhaustion
latch, and deadline before enforcing them. It maps identity to the current execution generation
only when an uncached read proves the exact legacy Pod UID, Environment ownership, canonical
annotation, and current generation; a missing Pod therefore leaves generation zero. Confirmed
sandboxd readiness clears both representations atomically. Remove these compatibility fields
after one rollout has allowed all persisted legacy recovery sequences to settle. Chart installs
unconditionally use `Recreate` operator upgrades, including leader-elected production presets,
so flat-only and nested-aware controller versions cannot write status concurrently.

### Durable desired service declarations

`Environment.spec.services` is a map-list of at most 32 durable desired declarations keyed by
DNS-1123 service name. Logical identity is `(Environment UID, service name)`, never a target
port. Each declaration has a positive revision, the sole v1 protocol `HTTP`, an explicit
loopback target port from 1 through 65535 other than sandboxd control port 50051, Project-only
visibility intent, and `TCPConnect` readiness intent. There is intentionally no host, IP, DNS,
address, allocated port, route, or URL input. Duplicate target ports are valid aliases.
These defaults follow the [maintainer's implementation decision in #16](https://github.com/Chris-Cullins/swe-platform/issues/16#issuecomment-5084221807).

Admission defaults an omitted `source` to `API`, including declarations persisted before source
ownership existed; that one legacy adoption cannot become `Repository`, and ownership is
immutable thereafter. Admission permits an exact same-name no-op and requires a strictly greater
revision for every same-name configuration change. A legacy declaration persisted before
`instanceID` existed stays schema-valid but cannot receive a portal route; its next CLI `update`
assigns an ID and advances the revision, after which that ID is immutable. New names always
require an ID. The CLI supplies `list`, `declare`, `update`, and `remove`;
mutations pin the Environment UID on their first read, use optimistic conflict retries, and
refuse to mutate a same-name replacement. `declare` starts at revision 1 and recovers an exact
same-intent retry, `update` increments revision only for a real change, and `remove` is an
idempotent durable desired-state removal. A name change is remove plus declare.

Declarations survive process restart, pause/resume, and Project-less warm-pool states because
they are Environment spec. A dedicated controller observes ready executions through sandboxd
and is the sole writer of one atomic `status.serviceObservations` envelope. The bounded,
name-keyed records correlate full declaration revisions, metadata and execution generations,
lifecycle epoch, hold revision, and observation time. Pending and suspended results deliberately
carry no execution generation; probe and transport outcomes are publishable only after an
uncached post-call proof of the full `lifecycle.ExecutionFence`, declaration snapshot, Pod,
template, backend, endpoint, TLS identity, and private observation capability. This advisory
state is reader-freshness-qualified and never route authority. The implemented control-plane
gateway separately owns bounded durable
`status.portalRoutes` records containing opaque locators, declaration identity/revision,
an opaque presentation identity for the gateway's scheme/suffix, monotonic route generations,
and active/tombstone state. A changed gateway presentation rotates the route rather than leaving
a process pinned to a stale URL. These records are routing state, not
service-observation state: observations remain advisory and every connection still proves the
current declaration, execution, and private sandboxd backend. A Project-less Environment may
retain declarations but cannot receive a route until an exact current Project and Installation
claim authorizes it. Removal tombstones the old route; re-adding the same name receives a new
route generation and locator and can never resurrect an old URL. Declarations intentionally
contain no route, locator, or URL input.

Repository-owned declarations come from `.swe/services.yaml`. Its strict version-1 schema is a
`services` mapping keyed by a DNS-1123 name of at most 32 bytes; each value has a required
non-empty direct argv string array `command` and optional canonical integer `port`. The parser
accepts at most 32 services, 64 KiB of UTF-8 input, 64 arguments per command, 4096 bytes per
argument, and 16 KiB aggregate argv. It rejects unknown and duplicate fields or names, aliases,
anchors, merge keys, multiple documents, non-string arguments, and ports outside 1–65535 or
equal to 50051. For example:

```yaml
version: 1
services:
  web:
    command: ["npm", "run", "dev"]
    port: 3000
  docs:
    command: ["python", "-m", "http.server"]
```

The declaration controller converges this into canonical `Environment.spec.services` with
`Source=Repository` and `Launch.argv`, while preserving `Source=API` entries. A repository/API
same-name collision fails closed, and the CLI refuses mutation of repository-owned entries.
Omitted ports are allocated deterministically from 49152–65535 using Environment UID and name,
then retained durably. Distinct repository processes may not share a port; API declarations may
remain port aliases. Malformed input, exhaustion, and collisions preserve the last canonical set
and do not launch ambiguous intent.

The gateway remains sole URL and route owner. For each repository process the operator uses its
rotating projected service-account token to call existing authenticated portal discovery; RBAC
grants only `get` on `environments/portal` and `environmentservices/portal`. It consumes the
exact proof-bearing URL and never constructs one. Processes receive only `PORT` and `PUBLIC_URL`,
never that token or a portal credential. When portals are disabled, the same authenticated
gateway authority tombstones active routes and returns a durable denial generation; the operator
publishes an empty managed set at that generation. Unavailable discovery preserves declarations
but launch fails closed.

The file read uses a distinct filesystem-only sandboxd capability and process reconciliation uses
the separate process capability; neither raw token is mounted or injected. Before and after calls
the connector proves the full Environment UID/execution, backend, Pod, endpoint, TLS identity,
Secret, and exact capability. The desired process set is Environment UID plus the
lexicographically monotonic Environment generation/gateway route generation pair. sandboxd
restarts crashes and successful exits with bounded backoff,
starts a new daemon epoch after resume, and stops removals while suppressing restart. Pause,
removal, and Environment or Pod replacement fence old processes and URLs. Observations remain
advisory; gateway route state remains authority.

### Adapters, processes, and transcripts

The agent layer is adapters. `internal/agent` is the exact adapter contract boundary, including
an agent-owned string type for immutable Environment identity that does not expose a backend API
type. Platform code supplies an immutable Run UID/task, an ephemeral credential, a backend-neutral
sandboxd process dialer, and an opaque event sink. Adapters own agent command/protocol details and
event payload meaning; the platform does not impose a shared transcript schema.

The registered `claude-code` (default), `amp`, `codex`, and `pi` adapters each run a foreground
CLI through sandboxd's keyed managed-process service. Starts are duplicate-safe within one
sandboxd daemon epoch, and bounded output includes absolute offsets and observable gaps.
Adapters map their own completion protocol to normalized Run state. Process exit alone is not
a universal platform completion rule. The operator owns one concurrency-safe ProcessService
connection pool and injects its connector into Run and service-observation controllers. The pool
keys physical gRPC/TLS connections by exact `(Environment UID, execution generation)`, retains
only the process capability, and treats adapter close callbacks as leases. Before every reuse it
uncached-revalidates the complete execution fence, exact Template/backend, owned Pod
UID/endpoint, and credential Secret UID/resourceVersion, TLS identity, and process token;
pause, deletion, hold or lifecycle drift, generation changes, credential drift, and unreachable
or replaced backends evict the entry. A miss reuses that pre-resolved target and post-resolves the
same complete proof after dialing without exposing those details. Idle entries close after 30
seconds, active borrowers delay invalidation close until release, and manager shutdown closes all
entries.
Interactive terminal connections and the separately capability-scoped service-observation RPC
are not pooled.

Uncached reads around an adapter call remain deliberate: the Run controller proves the exact
Run/Environment association before the call, adapter acceptance revalidates the complete active
Installation/Namespace/Project claim at the process-dial boundary, the pool proves the complete
execution fence before every lease, and the controller repeats exact association plus
backend-execution currentness after the call. Cached reads are not accepted for these
authorization/currentness boundaries. In the stable
one-minute-equivalent contract (30 polls at the fixed two-second cadence), reuse reduces physical
connection creations from 30 to 1 and process-connector Kubernetes reads from 150 to 124. Across
the complete adapter call path, including unchanged mandatory association and execution-receipt
proofs, the recorded read contract moves from 390 to 364 reads (6.7% fewer): the
first complete miss costs two reads each of Environment, Template, Pod, and Secret, while every
hit costs one read of each object to preserve complete backend and credential currentness. This
is deterministic contract evidence, not an end-to-end API-server load benchmark.

Transcript transport is fenced by namespace name, immutable Namespace UID, Run name, and
immutable Run UID. It supplies
idempotent append, monotonic per-Run cursors, bounded retention, explicit gaps, replay, and SSE.
Established transcript SSE connections repeat TokenReview, exact transcript SAR, and tenancy
authorization on the existing 15-second keepalive cadence and close on any failure.
Production presets require an external PostgreSQL URL; live PostgreSQL delivery is currently
database-polled. With no URL, the control plane logs a warning and uses a bounded, non-durable,
process-local development store. Durable legacy rows without a Namespace UID are associated
only when an authorized request resolves the exact current Run UID; conflicting identity is
rejected, and otherwise-unprovable rows are retained indefinitely. The control plane now has an
internal, explicit-bearer exact transcript DELETE foundation for a deleting Run. It requires
`delete` authorization on the exact `runs/transcript`, an exact Namespace UID precondition,
fresh tenancy and deleting-Run proof both before and after a bounded process-local cutoff/drain,
and then idempotent exact `(namespace name, Namespace UID, Run UID)` store deletion. Tenancy must
be active except that this exact no-session DELETE alone may continue during
`LifecycleFencing` + `OperationOffboarding`; onboarding fencing, fenced claims, GET/POST/SSE,
and every other namespaced operation remain denied. PostgreSQL uses the existing Namespace-UID
association and one transaction; the memory development store reclaims its accounting and
subscribers. The operator's existing `swe.dev/run-cleanup` finalizer now invokes this endpoint
through its rotating projected bearer credential only after work, repository credentials, and
claims are fenced. It uncached-resolves Namespace UID and supplies that plus Run UID. Only the
endpoint's committed/already-absent 204 permits finalizer removal; errors, old-server responses,
redirects, unavailable configured transport, and uncertain commits leave deletion retrying.
The empty operator `--transcript-url` explicitly disables both ingestion and cleanup; this is
the sole cleanup no-op. Terminal states and retain-only offboarding do not invoke deletion.
`swe delete-run RUN --namespace NS` captures the current Run UID before Kubernetes DELETE and
never retries a conflict against a replacement UID. Callers need Kubernetes get/delete on Runs.
The control plane remains sole store/ingress owner; operator RBAC adds only transcript delete
alongside its existing append authority. Existing cleanup metrics remain bounded and identity-free.

**Pre-live decision:** the maintainer superseded the earlier staged-rollout requirement because
there are no live installations; feature completeness and UX now take priority. The `0.2.0`
cleanup implementation is not gated on a foundation-first deployment. This changes deployment
planning, not the exact-identity/authentication fences or fail-closed cleanup contract above.
Retention lasts for the exact Run's lifetime; there is no TTL, hard global
byte budget, whole-Project purge, legal hold, or backup/restore cleanup guarantee.

### Authentication and exact identity fences

Normal control-plane bearer credentials are authenticated with Kubernetes TokenReview for the
configured audience. SubjectAccessReview then authorizes the reviewed username, UID, groups,
and extras against the exact API group, version, namespace, resource, subresource, verb, and,
where applicable, object name. Namespace is taken from the route, never a query parameter.
In scoped mode this authorization completes before the configured-namespace and exact active
Installation/Namespace/Project claim checks; claim validation supplements and never replaces
TokenReview/SAR.

The optional bootstrap bearer token bypasses SAR for initial self-hosted setup. It cannot be
exchanged for a browser session and should be removed after RBAC is configured. Browser login
stores the Kubernetes token server-side behind an opaque cookie; each cookie use repeats
TokenReview and exact SAR authorization retains the reviewed username, UID, groups, and extras.
Production cookies are Secure, HttpOnly, and SameSite Strict, and cookie-authenticated
mutations require same-origin checks.

Session storage is explicitly `memory` or `postgres`. Memory is bounded and process-local.
PostgreSQL stores encrypted bearer ciphertext and metadata, never a cookie value or raw bearer.
Cookies have the form `s1.<key-id>.<random>`; HKDF derives independent HMAC selector and
AES-256-GCM encryption keys from each administrator-provided master key, and stable
microsecond timestamps form part of the authenticated associated data. The strictly validated,
version-1 JSON keyring is created out of band, mounted read-only, and has one live active key.
Sessions have a one-hour absolute TTL. Under a transaction advisory lock, creation purges
expired rows and enforces a global 10,000-live-session cap by rejecting rather than evicting.
Logout and definitive unauthenticated or audience-mismatch results durably and idempotently
revoke the row. TokenReview transport or `status.error` failures return 503 and retain it;
SAR denial returns 403 and also retains it.

Startup fails on database connection, migration, keyring, or missing-live-key errors and never
falls back to memory. Rotation adds a unique key and makes it active; old keys must remain for
at least the TTL plus operational and rollback margin before retirement. Database-only theft
does not reveal bearers, but the running control plane, keyring, cluster administrator, or a
database-plus-keyring disclosure can. The keyring is not a substitute for database, Kubernetes,
backup, or process security.

Object names are not identities. Current sensitive operations add these fences:

- Run cancellation compares the caller's expected Run UID with the live object.
- Transcript append/read and reconnect target an expected Run UID; replacement Runs cannot
  inherit old events.
- Native Environment terminal access requires an expected Environment UID.
- Run terminal access requires expected Run and Environment UIDs, exact current owned/claimed
  association, separate Run and Environment authorization, and continued association checks.
- The sandboxd connector pins an exact ready Environment, owned Pod UID and endpoint, per-Pod
  TLS identity, and capability token. Terminal leases also revalidate lifecycle epoch and Pod
  identity; per-execution sandboxd credential rotation prevents old connections authenticating
  to a replacement pod.
- Same-name Run-create recovery returns an existing object only after exact-name `get`
  authorization and immutable-intent comparison. Run watches are separately namespace-level
  `list`/`watch` operations and use opaque Kubernetes resource versions.

The CLI no longer has an implicit `default` namespace: every executable command requires an
explicit `--namespace`, and Project onboarding/offboarding additionally require the Project
name plus explicit system Namespace and Installation. The CLI and local MCP server carry the
user's explicit control-plane bearer credential. MCP fixes all calls to one namespace and
currently exposes bounded `create_run` and UID-fenced `read_transcript`; interactive terminal
attachment is deliberately not an MCP tool.

### Credentials

API-key profiles are namespace-shared and adapter-bound. Immediately before acceptance, the
operator uncached-reads and revalidates the exact profile UID, deterministic backing Secret,
sole owner reference, Secret UID/resource version, type, key, and size. The key is then passed
as write-only sandboxd launch material only to the selected child process:

| Adapter | Child variable |
|---|---|
| `claude-code` | `ANTHROPIC_API_KEY` |
| `amp` | `AMP_API_KEY` |
| `codex` | `CODEX_API_KEY` |
| `pi` | Credential profiles are rejected |

The key is absent from public process specifications, setup/resume hooks, sandboxd's ambient
environment, and ordinary exec calls. The selected agent and descendants can still read or
emit it; same-UID peers are not strongly isolated and transcript redaction is not guaranteed.
OAuth/subscription credentials, refresh/writeback for agent profiles, per-user profiles, and
service credentials are not implemented.

Run intent may immutably select `repositoryCredential: GitHubApp`. The provider-neutral
repository-credential boundary exchanges an administrator-owned GitHub App key for a
short-lived installation token scoped to the exact frozen `github.com` repository with only
`contents:write`. The Run finalizer is persisted before issuance. A deterministic Run-UID lease
Secret records the exact frozen source, canonical repository, installation, expiry, token
generation, Environment UID, and execution generation. The clone init container receives the
Secret through one exact key reference and derives only a temporary Git extra header; setup and
resume hooks receive no credential. The selected adapter process receives `GH_TOKEN` and the
same process-scoped Git header through sandboxd write-only launch material. Platform-owned
serialization keeps the token out of status, transcript fields, repository URLs, command
arguments, persistent Git config, workspace files, sandboxd's environment, and public process
specifications. The selected process and descendants can nevertheless read, emit, or persist
the token; transcript redaction is not guaranteed.

Before expiry or after pod loss, a durable rotation record fences the prior execution, revokes
and deletes its exact lease, issues a replacement, and wakes or resumes with the next execution
generation. Terminal cleanup cancels a reachable adapter where applicable, fences the
Environment, revokes/deletes the token, then releases a claim and removes the finalizer.
Provider failures produce stable secret-free conditions and retry without releasing the
finalizer. GitHub Enterprise, SSH repository URLs, broader App permissions, and credentials for
non-GitHub repositories remain unsupported.

Rejected structurally valid token handles and tokens whose active lease cannot be persisted use
a deterministic pending-revocation Secret as a write-ahead log. The immutable record includes
provider, frozen provisioning source, canonical repository, installation, expiry, and
issuance/rotation generation, plus strict `pending`/`complete` state and an optional disposition
(currently `ProviderInvalidToken`). The record is persisted before revoke. Pending records alone
may invoke the provider; successful revoke or authoritative persisted expiry advances the exact
UID/resourceVersion to complete, with uncached exact-content recovery for an ambiguous update.
Complete records never revoke. A complete disposition is written to Run status while retaining
the Secret, and deletion occurs on a later reconciliation only after an uncached read proves the
exact durable failed Run reason. Delete uses UID/resourceVersion preconditions and safely treats
only an uncached absent record as recovery from an ambiguous response. Every pending action and
duplicate/deletion action first validates the expected provider, frozen source, stateless
canonical identity, and issuance generation; an unprovable record retains the Secret and Run
finalizer and reports a secret-free actionable condition. Legacy pending records with neither
state annotation are interpreted as pending; partial or unknown state metadata is rejected.
Terminal active-lease cleanup applies the same frozen provider/source/canonical proof before
revoke or expiry deletion and never falls back to identity asserted by a colliding Secret. This
cleanup authority comes only from the Run's exact Environment UID and its validated,
`ProjectVerified` frozen provisioning snapshot and does not depend on the live Project continuing
to exist. Issuance separately requires the live exact Project. During durable rotation, active
terminal cleanup accepts either the recorded old Secret UID at target generation minus one or a
replacement Secret UID at the target generation; malformed or mismatched records block before
provider or delete side effects.

Every structurally cleanup-capable token in a decoded GitHub response is returned as cleanup
authority even when `expires_at` is absent, malformed, zero, past, too short, or beyond GitHub's
fixed one-hour lifetime. Such responses are never delivered and receive a conservative local
cleanup deadline exactly one hour after a single captured response-validation time. A valid
future provider expiry no more than one hour away may be retained, while delivery additionally
requires strict greater-than minimum validity and exact scope, permissions, and repository.

Residuals remain: if pending persistence and immediate fallback revoke both fail or are
ambiguous, no durable local handle exists and exposure is bounded only by provider expiry.
Issuance response loss likewise may lose the handle.

### Terminal and operations console

`swe attach`, `swe tui`, and the browser console bridge a WebSocket to sandboxd's bidirectional
terminal RPC. Clients open with dimensions, exchange binary terminal bytes, and can resize.
All clients share the Environment's `swe` tmux session. Terminal open and heartbeat record
bounded lifecycle activity; stale hold, Run association, Environment, epoch, or Pod identity
revokes the lease. Established WebSockets also repeat their exact TokenReview, terminal SARs,
and tenancy authorization every five seconds and close on any failure.

The embedded React console and terminal TUI are agent-neutral operations clients. They list,
watch, create, inspect, and cancel Runs; show exact Environment detail and opaque transcripts;
and attach only when the API returns exact terminal identities. The console's per-Run Portal tab
uses an exact Run UID and Environment UID to request a bounded service list. The control plane
filters every declaration through the existing Environment, service, and current Run/Project
portal authorization before returning its target port, freshness-qualified lifecycle state, and
stable locator. Opening a locator uses a 30-second, bounded, one-time, locator-bound browser
handoff: the opaque handoff travels only in a no-store form body, establishes the existing
host-only session cookie on that portal host, and is consumed before redirecting to the clean
stable URL. Neither reusable bearer credentials nor backend addresses enter UI state or URLs.
They do not contact Kubernetes or sandboxd directly. Project/Template/credential administration
is not implemented.

### Chart, presets, tests, and deployment topology

The Helm chart installs CRDs, a system-namespaced Installation identity, operator, singleton
control plane, service accounts/RBAC, and inert system-namespaced EnvironmentTemplate catalog
sources. Optional portal values configure the gateway and a wildcard-only Ingress to the
control-plane Service; the chart never creates TLS material or claims ordinary API ingress.
Project resources and their namespaced workload RoleBindings are installed by the
CLI onboarding path, not by Helm. It does not install PostgreSQL, TLS material,
StorageClasses, RuntimeClasses, or an egress proxy. The control plane is
fixed at one replica with `Recreate`; PostgreSQL makes transcripts and browser sessions
durable across replacement, but open streams disconnect and clients must reconnect. No
multi-replica or control-plane HA claim is made.

- `values-kind.yaml` deliberately opts into trusted-admin for isolated local development,
  explicitly selects active but non-production `UnrestrictedDevelopment`, and uses local
  `:dev` images and explicit memory-backed insecure HTTP sessions. Primary
  acceptance overrides it to scoped mode with distinct system and Project namespaces.
- `values-argocd.yaml` deliberately opts into trusted-admin in the isolated Argo mirror,
  explicitly selects active but non-production `UnrestrictedDevelopment`, and uses mutable
  `:latest` images and explicit memory-backed sessions. Base/default values also
  select memory for development.
- `values-k3s.yaml`, `values-gke.yaml`, and `values-eks.yaml` use coordinated immutable chart
  `appVersion` images, explicitly select PostgreSQL sessions, require out-of-band PostgreSQL
  and session-keyring Secrets, and default to scoped mode with no Project namespaces. They
  remain legacy-unclassified because no restricted runtime is implemented. GKE selects
  `gvisor`; k3s and EKS use the cluster default runtime unless explicitly overridden.
  Each coordinated image also accepts a validated `sha256:` manifest digest that takes
  precedence over its tag, allowing a publish workflow release manifest to pin exact images.

CI builds, vets, and tests both Go modules, runs PostgreSQL transcript/session integration tests,
checks UI lint/type/tests/build, exercises focused sandboxd Windows portability, verifies
generated CRDs/chart copies, lints/renders every preset, and exercises the BYOC production-preset
validator's render, preflight, and installed contracts. The kind acceptance workflow
builds and loads local images and covers CRD upgrade, scoped system/Project topology,
onboarding claims/catalog/baseline, two-release isolation, retained offboarding,
Runs/warm pools/lifecycle, adapters and process-scoped credentials, TokenReview/SAR and browser
sessions, resource APIs/watches, transcript SSE/CLI/MCP, terminal authorization and identity
fencing, and UI embedding. It is path-filtered for pull requests and can be manually
dispatched; a documentation-only change does not add dummy source changes merely to trigger it.

Local development uses `swe-dev`; `make kind-up` adds gVisor plus snapshot-capable CSI. The
Argo mirror uses a separate `swe-argo` cluster tracking pushed `main`; the two operators must
never reconcile the same resources. Published images are operator, control plane, and
environment base for amd64/arm64.

## Implemented portal contract and approved next contracts

This section distinguishes the implemented portal gateway from approved future contracts.
Only capabilities explicitly described as future work remain unimplemented.

### Gateway ownership

The maintainer approved the [three-owner service model in #16](https://github.com/Chris-Cullins/swe-platform/issues/16#issuecomment-5078805652):

- the Environment owns the durable desired Service records described above;
- implemented `.swe/services.yaml` ingestion joins the CLI/API declaration path with explicit
  ownership; listening alone never grants visibility;
- sandboxd and the implemented observation controller report backend-portable listener state
  fenced to the exact current execution; and
- the control-plane/gateway owns Project-only URLs, authentication, authorization, routing,
  route generation, and revocation.

A stable service URL survives listener and pod restarts, but never declaration remove/re-add or
Environment replacement. A locator is not authentication. Discovery and every host request
require TokenReview and exact Environment/service SARs plus current Run or Project authority.
Hold, Requested suspension, stale association/declaration/route/execution, and uncertainty return
the same 404 as an unknown locator. Idle requests wake and await a fresh execution. The proxy
strips credentials, session cookies, and hop-by-hop headers and supports WebSocket upgrade.

### Implemented installation isolation selection lifecycle

The issue #10 contract adds one Installation-owned selection rather than an
`IsolationProfile` CRD. `spec.isolation` has no default and is optional only for staged legacy
migration. Its exact modes are `UnrestrictedDevelopment` and
`RestrictedProductionCalicoV3_32_1`. Development mode has no restricted inputs. Restricted mode
references the existing immutable #68 policy ConfigMap and states exact RuntimeClass
name/handler and StorageClass name/CSI-driver expectations. The ConfigMap remains sole owner of
network mode, Calico profile/CIDRs/resolvers, ceiling/baseline, proxy image, and TLS reference.

The `internal/isolation` helper validates that cross-field contract and derives a
domain-separated SHA-256 revision from the Installation UID, canonical selection, fixed API and
revision domains, and exact policy ConfigMap, RuntimeClass, StorageClass, and CSIDriver UIDs plus
canonical policy content hash and observed handler/driver. There is no administrator-entered
security revision. The Installation isolation controller uses uncached reads to resolve the exact
immutable canonical policy ConfigMap, RuntimeClass handler/UID, StorageClass provisioner/UID, and
CSIDriver UID. It publishes the exact selection and validated identities plus the derived revision;
missing, replaced, deleting, malformed, mismatched, or uncertain authority never permits execution.
For a new or changed restricted selection it durably publishes unresolved `Fencing` before the
first dependency read. From established `Blocked`, any resolved identity/revision change or loss
also publishes `Fencing` before the replacement or empty authority can return to `Blocked`.
In particular,
`RestrictedProductionCalicoV3_32_1` is admitted configuration, not an active or production-ready
profile. Activation still requires the complete #68 currentness, identity, proxy, path-forcing,
and Calico v3.32.1 enforcement path; generic restricted profiles remain unsupported.

The Installation status state vocabulary is `LegacyUnclassified`, `Fencing`, `Blocked`, and
`Active`. Omitted selection remains legacy-unclassified. Explicit unrestricted development can
become active, while `ProductionReady=False` makes its non-production contract explicit.
Restricted selection first publishes `Fencing`; an Environment outer gate ahead of Project,
Template, and provisioning dependencies withdraws readiness and reuses exact-owned Pod-then-
credential invalid-configuration teardown while retaining PVCs, transcripts, and ingress policy.
The warm-pool controller reports no ready capacity and neither replenishes nor cleans up warm
Environment objects during this installation fence; each retained Environment's outer gate fences
its execution. This applies before Project lookup, including empty
allowlists and Project-less Environments. Only after every configured-scope Environment has no
published connection or exact-owned Pod/credential does Installation status settle at `Blocked`
with `IsolationReady=False/RuntimeActivationUnavailable`; restricted mode never becomes `Active`.
Trusted-admin proof lists cluster-wide but uncached-classifies each Environment Namespace by exact
Installation namespace/name/UID: exact foreign claims are ignored, while this Installation's or
uncertain ownership fails closed. Switching a non-legacy lifecycle to unrestricted development (or
back to omission) likewise publishes `Fencing` and proves its exact-owned execution absent before
`Active` (or `LegacyUnclassified`) can reopen execution. Initial and `LegacyUnclassified` adoption
of unrestricted development remains behavior-identical and does not tear down legacy execution.
Dependency identity and revision publication are observations, not runtime authority. Installation
UID remains tenancy identity throughout migration.

### Approved fail-closed proxy-only egress contract

The maintainer approved the [outbound-network contract in #68](https://github.com/Chris-Cullins/swe-platform/issues/68#issuecomment-5231375346):

- production requires a demonstrably enforcing CNI and fails before execution when enforcement
  is unavailable; any unrestricted local mode is explicitly non-production;
- Environment traffic is technically forced through an authenticated shared proxy, with no
  direct fallback; proxy environment variables are compatibility, not enforcement;
- installation administrators define a destination ceiling and baseline; the effective policy
  is the baseline union the Project selection, with both constrained by the ceiling. The
  platform baseline is empty, Templates have no egress-policy role, and no required destination
  is inferred;
- the first version allows canonical HTTPS FQDN destinations on port 443 and HTTPS Git, not
  direct IPs, Git SSH/`git://`, arbitrary TCP/UDP, QUIC, private Project networks, or alternate
  DNS paths;
- the proxy authenticates immutable Project, Environment, and execution identity and never
  trusts repository-controlled headers or source claims;
- the proxy resolves destinations itself and validates every A, AAAA, and CNAME result and
  re-resolution, rejecting metadata, loopback, link-local, pod, service, node, control-plane,
  and private ranges; proxy egress itself is restricted;
- policy contraction terminates tunnels or replaces execution, while pause/resume, recovery,
  and replacement invalidate stale identity and cache; and
- required Git, package, provider, and CDN traffic is explicit and observable, and denials
  provide useful operator evidence.

The [approved v1 implementation package](https://github.com/Chris-Cullins/swe-platform/issues/68#issuecomment-5231375346)
defines `Project.spec.egressAllowlist` as a tenant selection, not a grant. It is a set of 0–64
exact lowercase ASCII FQDNs, each 1–253 bytes with at least two 1–63 byte labels matching
`[a-z0-9]([a-z0-9-]*[a-z0-9])?`. Uppercase, trailing dots, `xn--`, non-ASCII, wildcards,
schemes, ports, paths, queries, fragments, userinfo, percent encoding, whitespace/control bytes,
NUL, and every IP literal form are invalid and are rejected rather than normalized. Each entry
means only TLS-over-CONNECT to that exact FQDN on TCP 443; redirects require independent policy.
The CRD enforces bounded expressible shape, while `internal/egresspolicy` is the sole complete Go
grammar and canonicalization authority.

The future immutable administrator-owned policy has a maximum 256-entry ceiling and maximum
64-entry baseline using the same grammar. Baseline and Project selection must each be subsets of
the ceiling; an invalid or out-of-ceiling value fails closed rather than being silently
intersected. Effective v1 policy is `baseline ∪ Project selection`; the platform baseline is
empty, so two empty sets mean deny-all under the future restricted runtime. Administrators must
explicitly list repository, provider, package, CDN, redirect, and WebSocket destinations. The
platform infers none. EnvironmentTemplates neither grant nor narrow egress, and Project-less warm
Environments receive no egress identity or external egress in the approved runtime design.

The canonical runtime revision is SHA-256 over deterministic canonical bytes containing the
revision domain, exact Installation UID, immutable policy ConfigMap UID and content SHA-256,
sorted ceiling and baseline, exact Project UID, and sorted Project selection. Later operator and
proxy integration must independently recompute this from authoritative uncached reads. Helm
rendering is not runtime authority.

The inert identity and Pod-construction foundation is also implemented, but no reconciler calls
it. The versioned canonical identity binds exact Installation namespace/name/UID, Project
namespace/name/UID, Environment namespace/name/UID, Pod name/UID, execution generation, runtime
policy revision, forwarder security revision, and SHA-256 client-certificate fingerprint.
Certificate subject fields and the fingerprint presented by TLS are untrusted lookup hints: a
disabled currentness-authorizer foundation resolves canonical claims by fingerprint and, within
two seconds, repeats uncached reads for the exact live Installation, active Namespace claim and
sole Project, frozen Project-bound ready Environment execution, controlled non-deleting Pod,
and immutable policy ConfigMap. It independently recomputes policy and revision, checks the Pod's
execution, policy, forwarder, and certificate-fingerprint annotations, and rejects API
uncertainty. Each successful fingerprint receives a currentness signal that closes after any
failed recheck. While any leases exist, the authorizer owns one shared per-fingerprint proof loop:
it starts a proof immediately, then splits the two-second stale-authority window equally between
the next poll cadence and that proof's API deadline. It never waits two seconds and then grants a
proof another two-second timeout.
New targets still receive a complete initial proof. Any shared proof failure, API uncertainty, or
authority mismatch closes every tunnel's signal within the two-second currentness window. The
disabled transport requires the signal and one-shot release, cancels resolution and dialing,
closes an established tunnel on revocation, and releases its lease on every exit. Final release
stops the shared loop, closes the signal, and reclaims the bounded fingerprint state.
No command constructs this authorizer; `egress-proxy serve` still uses the deny-all
`DisabledAuthorizer`. A separate self-signed ECDSA ClientAuth keypair is generated for each
execution and is not sandboxd ServerAuth material.

`internal/egresspolicy` also strictly parses the future administrator policy ConfigMap contract.
The exact live object must have a UID, be non-deleting and immutable, contain only canonical
`policy.json`, and carry its canonical SHA-256 content address. Schema v1 fixes
`unrestricted|restricted` mode, the existing 256-entry ceiling and 64-entry baseline bounds,
an immutable digest-qualified proxy image, and an administrator TLS Secret name. Restricted mode
requires the exact `calico-v3.32.1` profile with 1–4 canonical resolver IPs and bounded canonical
API server, Pod, Service, node, control-plane, and additional-denial CIDR sets. Invalid,
mutable, replaced, or non-canonical authority fails closed; Helm values are not authority. The
chart does not create this ConfigMap, Secret, proxy, ServiceAccount, or RBAC.

The Pod helper resolves the UID ordering problem with a staged Kubernetes scheduling gate. Before
CREATE it accepts a complete Linux Pod-backend preparation input, prepends the native restartable
init container and required non-optional but not-yet-created credential Secret, and leaves the
Pod unscheduled. After the API assigns a UID, the separate binding validator requires the exact
persisted/admission-mutated Pod, Environment owner, execution generation, scheduling gate,
certificate fingerprint, and canonical claims. Future wiring may issue and create that exact
UID-bound credential only after validation, then remove the gate; no unbound credential is ever
placed in a runnable Pod. The foundation's issuer returns one immutable exact-Pod-owned Secret
containing the mutually verified client certificate, matching private key, client trust copy,
and canonical claims plus a private issuance binding. Future wiring must successfully create
(never adopt, including on `AlreadyExists`) that Secret, seal the private binding exactly once
with the UID/resourceVersion from that successful CREATE response, uncached-reread the Secret,
revalidate the exact sealed identity and still-gated Pod, and only then UID/resourceVersion-fence
gate removal. A GET, adoption, empty create identity, mismatched response, or reseal fails closed.
The helper mounts per-execution client cert/key and the separate
administrator-owned proxy server CA only in the forwarder, uses a fixed non-root, read-only,
drop-all security context and bounded resources, and injects loopback proxy variables while
clearing `NO_PROXY`. It rejects Windows and non-Pod backends.

The current non-empty Project allowlist rejection remains unchanged and runs before child or
Pod-spec construction. It stays until identity publication and exact two-second currentness,
proxy readiness, technical path forcing, Calico v3.32.1-only enforcement proof, and acceptance
land together. There is currently no default-deny egress, proxy deployment, runtime policy
ConfigMap instance, active restricted profile, or production egress-enforcement claim; generic
Kubernetes, k3s, GKE, and EKS restricted profiles remain deferred. Issue #68 is not
runtime-complete.

Slice 3 provides only a default-off experimental conformance runner. The separate pinned
dual-stack, two-worker kind topology and `hack/egress-conformance.sh` can perform one disposable,
human-observed Calico v3.32.1 diagnostic run after explicit context and cluster-UID confirmation.
The runner aborts on unavailable prerequisites and exits nonzero on any failed positive or
negative check. It emits human-readable diagnostics only: there is no persisted proof schema,
freshness model, attestation, controller, chart component, production capability, or runtime
input. Normal CI and `hack/e2e.sh` never execute the live runner; CI only checks that it remains
disabled and isolated. This groundwork installs no proxy and changes no Environment policy or
non-empty allowlist rejection behavior.

## Remaining decisions and open work

Repository service ingestion, process URL injection from authenticated discovery, and the
authorization-filtered per-Run console Portal tab are implemented. Other unimplemented areas include inbox and child-run semantics, changes
publication, whole-Project transcript purge, additional credential forms, ConPTY, Windows
setup/resume hook semantics and node-pool/provider requirements, non-Pod
backends, and control-plane HA. Their detailed contracts remain issue work unless and until a
maintainer decision is recorded. In particular, schema placeholders or portable interfaces do
not by themselves make these features implemented.

## Maintainer references

- [#11 — Project namespace onboarding and Template scope](https://github.com/Chris-Cullins/swe-platform/issues/11)
- [#16 — durable Service observations and gateway ownership](https://github.com/Chris-Cullins/swe-platform/issues/16)
- [#68 — fail-closed egress proxy](https://github.com/Chris-Cullins/swe-platform/issues/68)
- [#122 — stale adapter observation fencing](https://github.com/Chris-Cullins/swe-platform/issues/122)
- [#125 — terminal execution fencing](https://github.com/Chris-Cullins/swe-platform/issues/125)
- [#145 — durable PostgreSQL browser sessions](https://github.com/Chris-Cullins/swe-platform/issues/145)
- [`sandboxd` managed-process contract](sandboxd/proto/sandboxd/v1/PROCESS_LIFECYCLE.md)
- [`sandboxd` filesystem contract](sandboxd/proto/sandboxd/v1/FILESYSTEM.md)
- [Helm deployment and preset documentation](charts/swe-platform/README.md)
- [Security model](SECURITY.md)
