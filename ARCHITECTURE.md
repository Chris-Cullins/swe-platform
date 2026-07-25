# swe-platform architecture

This is the canonical, tracked architecture source for `swe-platform`. It records the
implemented system and the contracts already approved for its next stages. Code and
generated CRDs remain authoritative for runtime behavior; a CRD field change must update
the current resource sketch below in the same commit.

## Implemented today

### System topology

```text
CLI / TUI / local MCP / browser console
                    |
                    | HTTP, SSE, WebSocket
                    v
             control plane -------- PostgreSQL transcripts
                    |                    (or bounded dev memory)
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
                    +---- control plane and adapters
```

- **CRDs and controllers.** Kubernetes `swe.dev/v1alpha1` resources hold durable
  infrastructure and task intent. Controllers allocate or claim Environments for Runs,
  reconcile Environment pods and volumes, maintain warm pools, reap idle Environments, and
  drive adapter lifecycle.
- **Control plane.** A singleton HTTP service exposes typed Run and Environment operations,
  Run watches, transcript ingestion/SSE, browser sessions, the embedded operations console,
  and terminal WebSockets. It acts through Kubernetes APIs and the sandboxd connector; it
  does not exec into Environment pods.
- **Clients.** `swe run`, `logs`, `attach`, `tui`, and the bounded local stdio MCP server use
  the control-plane or Kubernetes contracts appropriate to each command. The React console
  uses the same control-plane resource, transcript, and terminal APIs.
- **`sandboxd`.** A small daemon in each Environment provides authenticated gRPC services for
  connection-bound exec, keyed managed processes, workspace-confined filesystem access, one
  shared terminal, a process-local port registry, and health. The port registry is not a
  portal gateway.

Portal proxying, inbox delivery and child-run spawning, branch/PR publication, and non-Pod
Environment backends are not implemented.

### Current CRDs and sources of truth

CRDs are the source of truth for desired and observed infrastructure state. PostgreSQL is
used for durable transcript events, not as a second infrastructure-state store. Browser
sessions are bounded process-local state and are lost when the control plane restarts.

All current CRDs are namespaced:

| Resource | Current contract |
|---|---|
| `Project` | One repository URL (represented as a one-item list), a same-namespace default Template reference, changes-workflow metadata, and a reserved egress allowlist. A non-empty allowlist is rejected when an Environment uses the Project. |
| `EnvironmentTemplate` | Pod image, size/resources, disk, runtime class, idle timeout, warm-pool minimum, and backend. Admission currently permits only the `pod` backend. |
| `Environment` | Template/Project selection, explicit hold policy and bounded wake/suspend/activity intents; status owns readiness, lifecycle suspension and epoch, exact Run claim, Pod endpoint observations, activity, and recovery state. |
| `Run` | Immutable agent task and Environment/Project/Template/credential selection plus monotonic cancellation; status records normalized lifecycle, exact Environment name/UID and ownership, exact credential profile identity, and accepted lifecycle epoch. `notify` and `parentRef` are schema placeholders without an implemented inbox. |
| `AgentCredentialProfile` | Immutable adapter and `APIKey` type metadata. Key bytes live in an owner-linked Secret whose name is derived from the profile UID. |

The current ownership/reference shape is:

```text
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

### Tenancy: current state

Namespace-per-Project is **not enforced today**. Projects are ordinary namespaced objects;
clients select a namespace (the CLI defaults to `default`), and Project, Template,
Environment, Run, and credential references resolve within that selected namespace. The Helm
chart creates configured Templates in the release namespace unless an entry explicitly names
another namespace. Its operator and control-plane service accounts currently receive
cluster-wide RBAC.

Consequently, installing the production chart in a system namespace does not itself onboard a
separate Project namespace or copy Templates there. Shared-cluster scoped reconciliation,
installation namespace claims, Project offboarding, and enforced one-Project-per-namespace
cardinality remain future work under the approved contract below.

### Environment boundary and backend portability

`sandboxd` is the only supported execution/filesystem/terminal contract into an Environment.
Platform consumers receive a backend-neutral connector or adapter sandbox and do not inspect
containers, PIDs, tmux, or OS signals. The protobuf uses logical paths, process owner/role keys,
portable process controls, and opaque execution IDs. The sandboxd module is separate and has
Windows coverage for process containment and filesystem behavior; the current shared terminal
backend is tmux, not ConPTY.

Only the Kubernetes **Pod backend** is implemented. The connector currently resolves an exact
owned Pod, Pod IP, Pod UID, and per-Pod credential Secret behind its consumer interface.
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

Pause is disk plus transcript, not process checkpointing:

1. the lifecycle epoch increases before suspension;
2. readiness and connection identity are withdrawn;
3. the Environment pod and its sandboxd credential are deleted;
4. the workspace PVC and already-ingested transcript remain;
5. resume creates a fresh pod/credential epoch, runs the repository resume hook, and repeats
   idempotent adapter acceptance with the same Run UID.

The lifecycle epoch currently identifies suspension cycles; it is **not** a complete backend
execution-generation contract for every Pod recovery. Terminal attachment additionally pins
the exact Pod UID and revalidates Environment UID, lifecycle epoch, hold revision, and Run
association. The approved general execution generation is described below and is not shipped.

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

Transcript transport is fenced by namespace, Run name, and immutable Run UID. It supplies
idempotent append, monotonic per-Run cursors, bounded retention, explicit gaps, replay, and SSE.
Production presets require an external PostgreSQL URL; live PostgreSQL delivery is currently
database-polled. With no URL, the control plane logs a warning and uses a bounded, non-durable,
process-local development store. Transcript rows are not currently garbage-collected when a
Run is deleted.

### Authentication and exact identity fences

Normal control-plane bearer credentials are authenticated with Kubernetes TokenReview for the
configured audience. SubjectAccessReview then authorizes the reviewed username, UID, groups,
and extras against the exact API group, version, namespace, resource, subresource, verb, and,
where applicable, object name. Namespace is taken from the route, never a query parameter.

The optional bootstrap bearer token bypasses SAR for initial self-hosted setup. It cannot be
exchanged for a browser session and should be removed after RBAC is configured. Browser login
stores the Kubernetes token server-side behind an opaque, bounded, process-local cookie ID;
each cookie use repeats TokenReview. Production cookies are Secure, HttpOnly, and SameSite
Strict, and cookie-authenticated mutations require same-origin checks.

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

The CLI and local MCP server carry the user's explicit control-plane bearer credential. MCP
fixes all calls to one namespace and currently exposes bounded `create_run` and UID-fenced
`read_transcript`; interactive terminal attachment is deliberately not an MCP tool.

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

The Helm chart installs CRDs, operator, singleton control plane, service accounts/RBAC, and
optional namespaced EnvironmentTemplates. It does not install PostgreSQL, ingress/TLS,
StorageClasses, RuntimeClasses, an egress proxy, or a portal gateway. The control plane is
fixed at one replica with `Recreate`; PostgreSQL makes transcripts durable, but browser
sessions and full control-plane HA are still process-local limitations.

- `values-kind.yaml` uses local `:dev` images and insecure HTTP sessions for development.
- `values-argocd.yaml` is a separate local mirror of `main` using mutable `:latest` images.
- `values-k3s.yaml`, `values-gke.yaml`, and `values-eks.yaml` use coordinated immutable chart
  `appVersion` images and require an out-of-band PostgreSQL Secret. GKE selects `gvisor`; k3s
  and EKS use the cluster default runtime unless explicitly overridden.

CI builds, vets, and tests both Go modules, runs PostgreSQL transcript integration tests,
checks UI lint/type/tests/build, exercises focused sandboxd Windows portability, verifies
generated CRDs/chart copies, and lints/renders every preset. The kind acceptance workflow
builds and loads local images and covers CRD upgrade, Runs/warm pools/lifecycle, adapters and
process-scoped credentials, TokenReview/SAR and browser sessions, resource APIs/watches,
transcript SSE/CLI/MCP, terminal authorization and identity fencing, and UI embedding. It is
path-filtered for pull requests and can be manually dispatched; a documentation-only change
does not add dummy source changes merely to trigger it.

Local development uses `swe-dev`; `make kind-up` adds gVisor plus snapshot-capable CSI. The
Argo mirror uses a separate `swe-argo` cluster tracking pushed `main`; the two operators must
never reconcile the same resources. Published images are operator, control plane, and
environment base for amd64/arm64.

## Approved next contracts (not implemented)

These decisions constrain future implementation. They do not add current API fields or imply
that migrations, controllers, proxies, or gateways already exist.

### One claimed namespace per Project

The maintainer approved the [Project namespace decision in #11](https://github.com/Chris-Cullins/swe-platform/issues/11#issuecomment-5078431568):

- platform services and PostgreSQL live in a system namespace;
- each Project has one dedicated namespace, explicitly created or adopted under a stable
  installation claim;
- that namespace contains the Project, installation-managed local Template copies, Runs,
  Environments/warm pools, credential profiles and owned Secrets, PVCs, quotas, RBAC, and
  baseline policies;
- normal references remain same-namespace; scoped mode manages only claimed namespaces, while
  cluster-wide trusted-admin mode is an explicit opt-in;
- CLI context is explicit rather than inferred from `default` or the release namespace; and
- offboarding separates fence/disable, retain/archive, and purge, with explicit PVC and
  exact-UID transcript handling.

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

### Backend-neutral execution generation

The maintainer approved the matching [Run observation decision in #122](https://github.com/Chris-Cullins/swe-platform/issues/122#issuecomment-5078805815)
and [terminal activity decision in #125](https://github.com/Chris-Cullins/swe-platform/issues/125#issuecomment-5078805886):

- add a dedicated backend-neutral execution generation, separate from lifecycle epoch;
- increment it for every actual backend execution replacement, including initial provision,
  pause/resume, recovery, security replacement, and equivalent non-Pod transitions;
- delayed operations capture Environment UID plus execution generation and, where relevant,
  hold-policy revision;
- stale adapter observations are discarded before Run status publication, and terminal activity
  updates reject stale Environment/execution/hold identity atomically; and
- service observations, portal routes, and egress identity use the same fence, while Pod names,
  IPs, endpoints, and UIDs remain backend-specific connector observations.

## Remaining decisions and open work

The approved contracts above still require reviewable API sketches, migration/rollout plans,
controller ownership, and acceptance tests before implementation. This document intentionally
does not invent those details.

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
- [`sandboxd` managed-process contract](sandboxd/proto/sandboxd/v1/PROCESS_LIFECYCLE.md)
- [`sandboxd` filesystem contract](sandboxd/proto/sandboxd/v1/FILESYSTEM.md)
- [Helm deployment and preset documentation](charts/swe-platform/README.md)
- [Security model](SECURITY.md)
