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
- **Control plane.** A singleton HTTP service exposes typed Run and Environment operations,
  Run watches, transcript ingestion/SSE, browser sessions, the embedded operations console,
  and terminal WebSockets. It acts through Kubernetes APIs and the sandboxd connector; it
  does not exec into Environment pods.
- **Clients.** `swe run`, `logs`, `attach`, `tui`, and the bounded local stdio MCP server use
  the control-plane or Kubernetes contracts appropriate to each command. The React console
  uses the same control-plane resource, transcript, and terminal APIs.
- **`sandboxd`.** A small daemon in each Environment provides authenticated gRPC services for
  connection-bound exec, keyed managed processes, workspace-confined filesystem access, one
  shared terminal, health, and bounded stateless TCP-connect observations from its logical
  loopback. The observation primitive retains no declarations or results, and no operator token
  or connector currently invokes it.

Portal proxying, inbox delivery and child-run spawning, branch/PR publication, and non-Pod
Environment backends are not implemented.

### Current CRDs and sources of truth

CRDs are the source of truth for desired and observed infrastructure state. PostgreSQL is
used for durable transcript events and encrypted browser sessions, not as a second
infrastructure-state store. The control plane owns one shared `pgxpool` and ordered,
versioned migrations; migration 003 adds browser sessions.

All current CRDs are namespaced:

| Resource | Current contract |
|---|---|
| `Installation` | Empty-spec identity object in the system namespace. Its immutable Kubernetes UID is the stable installation identity used by namespace claims, catalog sources, managed Template copies, and baseline resources. |
| `Project` | One repository URL (represented as a one-item list), a same-namespace default Template reference, changes-workflow metadata, and a reserved egress allowlist. A non-empty allowlist is rejected when an Environment uses the Project. |
| `EnvironmentTemplate` | Pod image, size/resources, disk, runtime class, idle timeout, warm-pool minimum, and backend. Admission currently permits only the `pod` backend. Chart-owned system-namespace objects are inert catalog sources; execution accepts only installation-managed same-namespace copies bound to exact Installation, source, and Project identities. |
| `Environment` | Template/Project selection, explicit hold policy and bounded wake/suspend/activity intents; status owns readiness, lifecycle suspension and epoch, backend-neutral execution generation, exact Run claim, backend observations, activity, and recovery state. |
| `Run` | Immutable agent task and Environment/Project/Template/credential selection plus monotonic cancellation; status records normalized lifecycle, exact Environment name/UID and ownership, exact credential profile identity, accepted lifecycle epoch, and accepted execution generation. `notify` and `parentRef` are schema placeholders without an implemented inbox. |
| `AgentCredentialProfile` | Immutable adapter and `APIKey` type metadata. Key bytes live in an owner-linked Secret whose name is derived from the profile UID. |

The current ownership/reference shape is:

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
```

The Run controller creates an owned Environment or exclusively claims a reusable one.
Owned allocations have a Run controller owner reference; claimed allocations carry Run name
and UID in `Environment.status.claimedBy`. Exact UIDs prevent same-name replacements from
inheriting allocation, cancellation, transcript, terminal, or credential authority.

### Tenancy and Project namespace lifecycle

The chart installs one empty-spec `Installation` in its system namespace. The object's
immutable Kubernetes UID is the installation identity; cluster-scoped RBAC names also include
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
  active-claim proof before any namespaced resource work. Namespaced RoleBindings supply
  workload authority only after onboarding.
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
Secret. After fencing, operators may retain/archive the namespace, remove it from
`tenancy.namespaces`, and restart the scoped components. Purge is not implemented; it remains
blocked on an exact Namespace-UID-preconditioned durable resumable operation.

### Environment boundary and backend portability

`sandboxd` is the only supported execution/filesystem/terminal contract into an Environment.
Platform consumers receive a backend-neutral connector or adapter sandbox and do not inspect
containers, PIDs, tmux, or OS signals. The protobuf uses logical paths, process owner/role keys,
portable process controls, and opaque execution IDs. The sandboxd module is separate and has
Windows coverage for process containment and filesystem behavior; the current shared terminal
backend is tmux, not ConPTY.

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

Terminal attachment captures Environment UID, execution generation, lifecycle epoch, and hold
revision before publishing activity, then atomically rejects stale activity. Its connector-owned
opaque handle also revalidates the exact live backend execution throughout the lease. Run adapter
acceptance, observation, and cancellation similarly capture the allocated Environment UID,
execution generation, epoch, and an opaque non-dialing connector execution handle. After each
adapter call, the controller uncached-reads the exact allocation and asks the connector to prove
that the same live backend execution remains current before publishing Run state, releasing a
claim, or removing a finalizer. Delayed results from disappeared or same-name replacement Pods
are therefore discarded even if Environment status has not yet advanced.

Only the Pod backend performs execution today. Project-backed initialization clones the
Project's single repository into `/workspace`, runs `.agents/setup` once per workspace when
present, and runs `.agents/resume` after resume when present. Terminal Pod failure has bounded
replacement/backoff while retaining the PVC. Warm pools maintain ready unclaimed Environments;
claiming a generic warm member for a Project recreates its pod so Project initialization runs
before the Run becomes ready.

### Adapters, processes, and transcripts

The agent layer is adapters. Platform code supplies an immutable Run UID/task, an ephemeral
credential, a backend-neutral sandboxd process dialer, and an opaque event sink. Adapters own
agent command/protocol details and event payload meaning; the platform does not impose a shared
transcript schema.

The registered `claude-code` (default), `amp`, `codex`, and `pi` adapters each run a foreground
CLI through sandboxd's keyed managed-process service. Starts are duplicate-safe within one
sandboxd daemon epoch, and bounded output includes absolute offsets and observable gaps.
Adapters map their own completion protocol to normalized Run state. Process exit alone is not
a universal platform completion rule.

Transcript transport is fenced by namespace name, immutable Namespace UID, Run name, and
immutable Run UID. It supplies
idempotent append, monotonic per-Run cursors, bounded retention, explicit gaps, replay, and SSE.
Production presets require an external PostgreSQL URL; live PostgreSQL delivery is currently
database-polled. With no URL, the control plane logs a warning and uses a bounded, non-durable,
process-local development store. Durable legacy rows without a Namespace UID are associated
only when an authorized request resolves the exact current Run UID; conflicting identity is
rejected, and otherwise-unprovable rows are retained indefinitely. Transcript rows are not
currently garbage-collected when a Run or Project is deleted.

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
Logout and failed authentication durably and idempotently revoke the row.

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
OAuth/subscription credentials, refresh/writeback, per-user profiles, Git/setup/service
credentials, and GitHub App token minting are not implemented.

### Terminal and operations console

`swe attach`, `swe tui`, and the browser console bridge a WebSocket to sandboxd's bidirectional
terminal RPC. Clients open with dimensions, exchange binary terminal bytes, and can resize.
All clients share the Environment's `swe` tmux session. Terminal open and heartbeat record
bounded lifecycle activity; stale hold, Run association, Environment, epoch, or Pod identity
revokes the lease.

The embedded React console and terminal TUI are agent-neutral operations clients. They list,
watch, create, inspect, and cancel Runs; show exact Environment detail and opaque transcripts;
and attach only when the API returns exact terminal identities. They do not contact Kubernetes
or sandboxd directly. Portal UI/proxying and Project/Template/credential administration are
not implemented.

### Chart, presets, tests, and deployment topology

The Helm chart installs CRDs, a system-namespaced Installation identity, operator, singleton
control plane, service accounts/RBAC, and inert system-namespaced EnvironmentTemplate catalog
sources. Project resources and their namespaced workload RoleBindings are installed by the
CLI onboarding path, not by Helm. It does not install PostgreSQL, ingress/TLS,
StorageClasses, RuntimeClasses, an egress proxy, or a portal gateway. The control plane is
fixed at one replica with `Recreate`; PostgreSQL makes transcripts and browser sessions
durable across replacement, but open streams disconnect and clients must reconnect. No
multi-replica or control-plane HA claim is made.

- `values-kind.yaml` deliberately opts into trusted-admin for isolated local development and
  uses local `:dev` images and explicit memory-backed insecure HTTP sessions. Primary
  acceptance overrides it to scoped mode with distinct system and Project namespaces.
- `values-argocd.yaml` deliberately opts into trusted-admin in the isolated Argo mirror and
  uses mutable `:latest` images and explicit memory-backed sessions. Base/default values also
  select memory for development.
- `values-k3s.yaml`, `values-gke.yaml`, and `values-eks.yaml` use coordinated immutable chart
  `appVersion` images, explicitly select PostgreSQL sessions, require out-of-band PostgreSQL
  and session-keyring Secrets, and default to scoped mode with no Project namespaces. GKE
  selects `gvisor`; k3s and EKS use the cluster default runtime unless explicitly overridden.

CI builds, vets, and tests both Go modules, runs PostgreSQL transcript/session integration tests,
checks UI lint/type/tests/build, exercises focused sandboxd Windows portability, verifies
generated CRDs/chart copies, and lints/renders every preset. The kind acceptance workflow
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

## Approved next contracts (not implemented)

These decisions constrain future implementation. They do not add current API fields or imply
that migrations, controllers, proxies, or gateways already exist.

### Durable services and gateway ownership

The maintainer approved the [three-owner service model in #16](https://github.com/Chris-Cullins/swe-platform/issues/16#issuecomment-5078805652):

- the Environment owns durable desired Service records with immutable service identity;
- `.swe/services.yaml` and the CLI declare exposure; listening alone never grants visibility;
- sandboxd reports backend-portable listener/health observations fenced to the exact current
  execution; and
- the control-plane/gateway owns Project-only URLs, authentication, authorization, routing,
  route generation, and revocation.

A stable service URL may survive a process restart, showing unavailable until healthy, but
never an Environment replacement. Pause disables routing immediately; resume republishes only
after fresh-execution health. Uncertain or stale identity and health fail closed. Old execution
observations and routes cannot target a resumed, recovered, or same-name replacement
Environment. Anonymous/public exposure is deferred.

### Fail-closed proxy-only egress

The maintainer approved the [outbound-network contract in #68](https://github.com/Chris-Cullins/swe-platform/issues/68#issuecomment-5078805740):

- production requires a demonstrably enforcing CNI and fails before execution when enforcement
  is unavailable; any unrestricted local mode is explicitly non-production;
- Environment traffic is technically forced through an authenticated shared proxy, with no
  direct fallback; proxy environment variables are compatibility, not enforcement;
- installation administrators define a destination ceiling and Projects may select only a
  subset;
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

The current non-empty Project allowlist rejection remains until identity, proxy readiness, and
path forcing ship together after Project namespace claims.

## Remaining decisions and open work

The approved contracts above still require reviewable API sketches, migration/rollout plans,
controller ownership, and acceptance tests before implementation. sandboxd now has a stateless
internal loopback observation primitive, but Environment declarations, an observation connector
and controller-owned health status do not exist. The execution-generation contract is
implemented above; future declaration observations, portal routes, and egress identity must
consume the same backend-neutral fence without exposing backend-specific identity.

Other unimplemented areas include portal transport, inbox and child-run semantics, changes
publication, transcript garbage collection, additional credential forms, ConPTY, non-Pod
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
