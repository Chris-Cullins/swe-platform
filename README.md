# swe-platform

Open-source platform for running coding agents unattended in ephemeral, isolated Kubernetes environments.

Give an agent a task — from the CLI, web UI, or an MCP call — and the platform provisions a
fresh environment (repo clone, toolchain, secrets, setup hooks), runs the agent in it,
streams everything back live, auto-pauses when idle so compute cost drops to ~$0, and ends
with a reviewable diff, branch, or PR.

> **Status: early.** The P0 scaffold is in — CRDs, operator, `sandboxd`, CLI — with a
> passing kind end-to-end (`./hack/e2e.sh`). A first control-plane service accepts and
> streams adapter-owned transcript events through a bounded, tenant-aware transcript-store
> contract over SSE, while `swe attach` and the control
> plane's WebSocket terminal endpoint connect to a shared tmux session through `sandboxd`;
> pause/resume preserves workspace disks and runs repository resume hooks, and idle
> environments pause automatically before terminal requests wake them. Template warm
> pools keep unclaimed environments ready for `swe run` to claim. The `claude-code` (default),
> `amp`, and `codex` adapters run through sandboxd's managed-process API. Portal proxying is not
> built yet.
> The Helm chart installs the
> operator, control plane, and CRDs. Values presets
> cover kind, k3s, GKE with GKE Sandbox, and EKS.

## Why

- **Real isolation** — agents execute untrusted, model-generated code. Environments run
  under gVisor/Kata by default, behind default-deny egress with per-project allowlists.
- **Pause economics** — idle environments are paused (pod deleted, disk retained) and
  woken on demand. An agent waiting on its children costs ~$0.
- **Agent-agnostic** — existing agents plug in via adapters; the platform never depends
  on one agent's internals.
- **Self-hosted** — your cluster, your credentials, your data. Runs on anything from a
  local kind cluster to EKS/GKE.

## Core concepts

| Concept | Meaning |
|---|---|
| **Environment** | One ephemeral machine an agent works in (pod + volume + network policy) |
| **Run** | One agent task executing in an environment |
| **Project** | One or more git repos + config: setup hooks, size, changes workflow |
| **Template** | Environment class: image, size, runtime, egress rules, warm pool |
| **Inbox** | Addressable message queue per run — how agents talk to each other |
| **Portal** | Authenticated URL exposing a dev server inside an environment |

## Architecture (short version)

- **`sandboxd`** — a small daemon inside every environment exposing one gRPC contract:
  exec, filesystem, terminal, port registry, health. The control plane never touches a
  pod except through it.
- **Operator + CRDs** — `Environment`, `EnvironmentTemplate`, `Run`, `Project`, with
  controllers for lifecycle, warm pools (pre-booted environments), and idle reaping.
- **Control plane** — API, auth, transcripts, and wake-on-request: any traffic to a
  paused environment resumes it.
- **Gateway** — live output streaming, a web terminal sharing a tmux session with the
  agent, and reverse proxying for portals.
- **Runs as actors** — every run has an address and an inbox. Agents can spawn child
  runs, message each other, and get resumed when a message arrives.

```
   CLI · Web UI · MCP
          │
   Control plane ──► CRDs ──► Operator ──► Environment pods (gVisor)
          │                                    │  agent + sandboxd
   Gateway (stream/terminal/portals) ◄─────────┘  workspace volume
```

### Run and process lifecycle

`Run` is the durable, server-side task intent. Clients create one Run; the Run
controller allocates an Environment or exclusively claims the existing Environment in
`spec.environmentRef`. The Run UID is the idempotency key for allocation, adapter task
acceptance, and managed processes. Reconciliation therefore converges after a timeout or
partial failure instead of starting a second task.

The ownership boundary is independent of Kubernetes container layout:

| Concern | Lifecycle owner |
|---|---|
| Task intent, allocation/claim, cancellation, and normalized status | Run controller |
| Pod, VM, Windows host, or external-runner infrastructure | Environment controller/backend |
| Agent-specific command/protocol and transcript interpretation | Adapter |
| Agent and declared-service process start, observation, and stop | `sandboxd` managed-process API |

Adapters receive only an immutable Run UID/task and a backend-neutral, securely pinned
sandboxd process dial handle.
Every adapter lifecycle operation is idempotent. A foreground CLI adapter can map one
managed agent process's state to Run status; a long-lived service adapter can keep an
Environment-scoped service process and map its task-acknowledgement/events instead. The
platform does not assume that agent-process exit means task completion. The same contract
maps to pod, KubeVirt, Windows, and external-runner backends because it exposes no Pod,
container, PID, tmux, or OS-signal semantics.

The committed sandboxd process contract is documented beside its protobuf in
[`PROCESS_LIFECYCLE.md`](sandboxd/proto/sandboxd/v1/PROCESS_LIFECYCLE.md). In short,
connection-bound `Exec` supports explicit stdin EOF but is not retry-safe; keyed,
epoch-scoped `ProcessService` provides duplicate-safe launch, portable tree controls,
timeouts, opaque execution identity, and bounded cursor output with observable loss.
It supports both foreground agent processes and reconnectable long-lived services.
The workspace-only filesystem contract in
[`FILESYSTEM.md`](sandboxd/proto/sandboxd/v1/FILESYSTEM.md) uses portable logical paths,
race-safe workspace-confined traversal, ranged reads, atomic streamed writes with SHA-256
preconditions, portable metadata, and paginated listings.

Run states are observable milestones: `Allocating`, `EnvironmentReady`,
`AdapterAccepted`, `Running`, `NeedsInput`, `Paused`, and terminal `Succeeded`, `Failed`,
or `Cancelled`. Conditions additionally report environment readiness, a durable adapter
acceptance-attempt marker written before the acceptance RPC, and confirmed adapter acceptance.
The EnvironmentReady condition tracks the allocation independently from terminal task outcome;
it remains true for an adapter failure while sandboxd is reachable and becomes false after the
allocation is released, lost, paused, or fenced. The attempt marker makes cancellation
conservative after an uncertain response. An unavailable adapter fails explicitly rather than
pretending work started.

Environment ownership and cleanup are explicit:

| Allocation | While running | Completion/cancellation | Run deletion |
|---|---|---|---|
| Controller-created (`Owned`) | Environment has a Run controller owner reference | Stop Run-scoped work and pause the Environment; retain workspace and transcript for review | Finalizer stops work, then Kubernetes garbage-collects the Environment, pod, and PVC |
| Existing (`Claimed`) | `Environment.status.claimedBy` stores Run name + UID; optimistic concurrency permits one claimant | Stop Run-scoped work, clear only the matching UID claim, and leave the Environment active and reusable | Finalizer stops work and releases only the matching claim; it never deletes the Environment |

An explicit `--environment` request fails terminally if another Run already holds the claim or
the Environment has an enabled explicit hold. It does not wait for claim release or hold-policy
changes and unexpectedly start later. Automatic Idle suspension is ordinarily wakeable. A Run
claim can also publish a Requested-scoped wake, but an accepted cleanup fence completes before
that wake releases the Environment.

Pause is not process checkpointing. One Environment lifecycle controller owns every
suspend/resume transition. Explicit user/admin policy lives in
`spec.lifecycle.hold`; automatic idle and requested suspension are controller-owned
observed state in `status.lifecycle`, including a reason and monotonically increasing
epoch. Pausing increments that epoch before fencing the current execution domain and
stops **every** agent and declared-service process by removing the environment pod (or the
backend equivalent), while retaining the workspace disk and adapter-owned transcript.
Ordinary callers never toggle observed suspension. They publish bounded durable
`wake`, `suspend`, or source-keyed `activity` intents. Every intent carries the exact
Environment UID, hold-policy revision, and an idempotency key; stale-incarnation and
stale-policy requests are ignored. A wake consumed while an explicit hold is enabled is
recorded but refused, so terminal, portal, inbox, agent, and Run traffic cannot erase an
administrator's decision. Activity receipts are bounded to one latest request for each
defined source, and an exact non-terminal Run owner or claim remains authoritative active
work for idle decisions. Ordinary wake intents are scoped to the suspension reason they
observed, so an Idle wake racing a Requested cleanup fence is consumed without resuming
the Environment. Terminal access wakes Idle suspension only; Requested suspension,
enabled holds, and legacy pause migration are not terminal-wakeable. A live terminal
heartbeat adopts newer disabled hold-policy revisions, while enabling a hold revokes the
terminal connection instead of continuing stale-revision activity.

The deprecated `spec.paused: true` input is retained for upgrade safety only. On first
reconciliation it becomes an enabled explicit hold at revision 1 and is cleared. New
clients must use `spec.lifecycle`; writing `paused: false` is not a wake operation.
Use the CLI to publish monotonic user/admin hold-policy revisions:

```sh
swe environment hold my-environment
swe environment release my-environment
```

Accepted work is cancelled only while that exact execution incarnation is securely
reachable, or cleanup proceeds without an RPC after pause has removed its pod and
endpoint. For unreachable or unavailable-adapter cleanup, the Run controller requests backend
fencing through a durable lifecycle suspend intent and retains the claim/finalizer until
the Environment reports the pod and endpoint gone.
Resume creates a fresh sandboxd epoch, runs the repository resume hook, and calls the
adapter's idempotent acceptance path again with the same Run UID. Adapters reconstruct or
restart their processes from workspace/transcript state; no old process incarnation is
allowed to overlap the new one.

## Roadmap

- **P0 — skeleton:** `sandboxd`, CRDs, operator, CLI, kind quickstart
- **P1 — secure & streamable:** Helm chart, transcript streaming, web terminal, scoped
  git tokens, egress proxy
- **P2 — economics & portals:** pause/resume, warm pools, portals, repo setup hooks
- **P3 — multiplayer agents:** inboxes/spawning, web UI, metering, MCP server
- **P4 — enterprise:** SSO/RBAC/audit, Windows environments (.NET Framework workloads),
  hibernation tier, hosted offering

## Local development

Development targets a local [kind](https://kind.sigs.k8s.io/) cluster. Run `make kind-up`
to create a cluster with the `gvisor` RuntimeClass and the snapshot-capable CSI hostpath
driver, build and load the platform images, then install
`charts/swe-platform` with `values-kind.yaml` and the printed
`environmentTemplates[0].spec.runtimeClass=gvisor` override. The preset creates the
`small` template in `default`; the explicit override keeps it usable in ordinary CI kind
clusters that do not install runsc. Production installation assumptions and k3s/GKE/EKS
presets are documented in the [chart README](charts/swe-platform/README.md).

Run the acceptance suite against the bootstrapped cluster with gVisor enabled:

```sh
KIND_CLUSTER=swe-dev E2E_USE_EXISTING_CLUSTER=true E2E_RUNTIME_CLASS=gvisor ./hack/e2e.sh
```

For the controller inner loop, this repository uses Skaffold v2.23.0 rather than Tilt: its
native Docker, kind image-loading, and Helm support map directly onto the existing build
and `values-kind.yaml` workflow without adding a cluster-side development service. After
installing Skaffold and Helm and running `make kind-up`, start the watch loop with:

```sh
make dev
```

Skaffold builds and loads the operator and control-plane images, installs or upgrades the
`swe-platform` Helm release with `values-kind.yaml`, and repeats that cycle when relevant
source, chart template, or values files change. `make dev` always targets the
`kind-swe-dev` context (or `kind-$KIND_CLUSTER` when overridden) and refuses the Argo mirror
cluster named by `KIND_ARGO_CLUSTER` (default `swe-argo`) or any target cluster containing
the `argocd` namespace. The environment base image is outside this controller loop; build
and load it separately before starting Runs that need a fresh environment. Helm does not
upgrade CRDs from a chart's `crds/` directory, so apply CRD changes separately with
`kubectl --context "kind-${KIND_CLUSTER:-swe-dev}" apply --server-side --force-conflicts -f
config/crd/bases`.

For the separate Argo mirror created by `make argocd-up`, run `make argocd-ui` in a
foreground terminal and open `http://127.0.0.1:18080/`. The helper explicitly targets
`kind-swe-argo`, binds only to loopback, and reconnects the Service port-forward after an
Argo rollout replaces the selected control-plane pod. Override the cluster with
`KIND_ARGO_CLUSTER` or the local port with `ARGO_UI_PORT`. Stopping the helper also stops
only its own `kubectl` child. Existing SSE or WebSocket connections still disconnect during
a rollout and must reconnect. PostgreSQL-backed transcript streams replay from their last event
ID; a control-plane replacement still invalidates its process-local browser sessions. The
bootstrap requires one kind node with at least 5 CPUs and 6 GiB
allocatable so the Argo/system workload and two 1-CPU/2-GiB `tiny` Environments fit while a
warm member is claimed and replaced. Increase the container runtime's capacity before
running `make argocd-up`; the script checks this before installing Argo.

Create runs with an explicit template, or reference a `Project` to use its default
template:

> **Breaking v1alpha1 credential migration:** `Project.spec.secretRef` has been removed
> and is now rejected by CRD admission. For an existing plain-Helm installation, first
> [server-side apply the chart CRDs](charts/swe-platform/README.md#upgrade), because Helm does
> not upgrade files in a chart's `crds/` directory, and then run `helm upgrade`. The operator
> upgrade replaces existing Environment pods so previously injected ambient Secret values are
> removed. Private repository clones and `.agents/setup` or `.agents/resume` hooks that relied
> on those values will break. There is no fallback; purpose-scoped Git and setup credentials
> remain future work.

```sh
swe run --template small "Fix the flaky test"
swe run --project org-repo "Fix the flaky test"
swe run --name fix-flaky-42 --project org-repo "Fix the flaky test"
swe run --environment warm-env-1 "Fix the flaky test"
swe cancel fix-flaky-42
```

### Claude Code adapter

`claude-code` is the default `swe run` adapter. It starts one non-interactive `claude`
process keyed by the immutable Run UID, observes and cancels that process through sandboxd,
and restarts the same task identity in a fresh sandboxd epoch after an Environment resume.
The coordinated `env-base` image includes a pinned Claude Code CLI. Custom Environment
images must provide a compatible `claude` executable on `PATH`.

The adapter runs Claude Code in print mode with stream JSON output and unattended
permissions inside the isolated Environment. Bounded stdout/stderr chunks are forwarded as
opaque `claude-code.process-output` transcript events when the control plane is enabled.
Those events retain sandboxd's absolute stream offsets and observable gap metadata; consumers
use offsets rather than transcript append order to reconstruct output after a controller retry.
The process output and sandboxd records are epoch-local; the workspace PVC and already
ingested transcript events survive pause. A resumed run therefore restarts the prompt against
the preserved workspace rather than checkpointing the old Claude process or session.

The v1alpha1 credential API models Claude authentication as an `AgentCredentialProfile` with
`credentialType: APIKey`, selected by a Run's `spec.credentialProfileRef`. API keys are the
only supported profile credential type. The CLI creates an owner-linked, same-namespace
backing Secret and never prints the key or Secret representation:

```sh
secret-tool lookup service anthropic | swe credentials create anthropic --agent claude-code --api-key-stdin
swe run --project org-repo --credential-profile anthropic "Fix the flaky test"
secret-tool lookup service anthropic | swe credentials rotate anthropic --api-key-stdin
swe credentials list
```

Immediately before adapter acceptance, the operator revalidates the bound profile UID and its
exact backing Secret through uncached reads. sandboxd supplies the key as `ANTHROPIC_API_KEY`
only to the selected Claude child process; it is absent from public process specifications,
setup/resume hooks, the sandboxd environment, and ordinary sandboxd executions. Rotation does
not restart or compare an existing process; a fresh sandbox epoch reads the newest key.

This boundary prevents automatic platform-wide exposure, not disclosure by the selected agent
or its descendants, repository wrappers left by setup, same-UID peers, or explicit process
output. Transcript redaction is not guaranteed. Anyone authorized to create Runs in a namespace
can initially select any profile there; profile creation and rotation additionally require
Secret and CRD administration. Subscription/OAuth credentials, refresh and writeback, leases,
Amp login persistence, per-user profiles, Git/setup/service credentials, hard same-user
isolation, and stronger redaction remain deferred to issue #9. Never place credentials in a Run
prompt or Project configuration.

Current limitations: Claude print mode has no live input continuation channel, so an exit-zero
successful result remains `Succeeded` even when its history contains permission denials.
Non-success results, non-zero exits, missing executables, malformed/missing final result
events, and permanent transcript rejection map to `Failed`. Production transcript persistence
requires the chart's PostgreSQL configuration; local presets retain the process-local store.

### Amp adapter

Select Amp explicitly with `swe run --agent amp ...`; `claude-code` remains the default.
The coordinated environment image pins `@ampcode/cli@0.0.1784492094-g5d18e2` and disables
its update check. The adapter starts `amp --execute=<prompt> --stream-json --no-ide
--no-notifications` as a Run-UID-keyed sandboxd managed process. It forwards bounded,
gap-aware stdout/stderr as opaque `amp.process-output` events and requires both an exit-zero
process and Amp's final Claude-compatible JSONL `result` event with `subtype: "success"` and
`is_error: false`. Consumers reconstruct each stream by its absolute offsets rather than event
append order; an operator restart can replay an overlapping range because output cursors are
process-local, while retrying an uncertain append within one operator process resends the exact
same event and idempotency key.

For non-interactive environments, the pinned public Amp CLI follows the official
[Amp authentication contract](https://ampcode.com/manual#non-interactive-environments): an
API-key profile selected with `--credential-profile` is delivered as `AMP_API_KEY` only through
sandboxd launch material to the Run-owned Amp process. Credentialless Runs retain the plain
managed-process path. The platform does not persist Amp login files, mount user configuration,
or place the key in the public process specification, setup/resume hooks, sandboxd environment,
or ordinary executions. Rotation does not restart or compare an existing process; acceptance in
a fresh sandbox epoch reads and materializes the current key.

The selected Amp process and its descendants can read or explicitly output the key. Profiles
are namespace-shared: anyone authorized to create Runs there can initially select a profile,
and same-UID peers are not strongly isolated. Transcript redaction is not guaranteed.
Subscription/OAuth login persistence, refresh/writeback, leases, and stronger per-user or
same-user isolation remain unsupported issue #9 work. Never put the key in a Run prompt,
Project configuration, image, or chart values.
Abrupt cancellation stops only the Run-owned local process tree, but Amp's public contract does
not guarantee that remote/server-backed thread work has also stopped.

### Codex adapter

Select Codex with `swe run --agent codex ...`. The coordinated image pins
`@openai/codex@0.144.6`; the adapter invokes `codex exec --json --ephemeral
--ignore-user-config --ignore-rules --sandbox workspace-write --color never
--skip-git-repo-check -- PROMPT`. Exactly `-` is rejected because it is Codex's stdin sentinel,
while other leading-hyphen prompts are protected by `--`. Each Run starts an ephemeral thread:
the adapter never resumes latest or shared state, and requires exit zero, a nonempty
`thread.started` ID, and a final coherent `turn.completed` with usage and no later terminal
failure, error, or malformed line.

Stdout and stderr are forwarded as bounded, gap-visible `codex.process-output` events with
absolute offsets and exact uncertain-append retry behavior. On resume, the same Run identity
starts a fresh process and thread in the new sandbox epoch against the retained workspace.
Codex's nested `workspace-write` sandbox is defense in depth inside the outer Environment;
gVisor availability and its current limitation are tracked in issue #10.

An API-key profile is delivered as `CODEX_API_KEY` only through sandboxd launch material to the
Run-owned process. It is never included in the public process spec or ambient environment, and
other credential types are rejected before dialing. The selected agent and same-UID descendants
can still read or disclose it; stronger credential isolation and additional credential forms
remain issue #9 limitations. Acceptance tests use a fake Codex executable and no provider or
network access.

`--name` is the create idempotency key: retry an uncertain request with the same name and
immutable task arguments. The CLI returns the existing Run only when its intent matches;
the controller creates or claims the Environment server-side.

The repository configured on a Project is cloned into `/workspace` when its
environment is created. If the repository contains `.agents/setup`, the hook runs once
after checkout. `swe environment hold ENVIRONMENT` deletes the pod while retaining its
workspace PVC, and `swe environment release ENVIRONMENT` publishes the next policy revision
to create a fresh pod; `.agents/resume` runs after the volume is reattached.
Setup and resume hooks receive only the controller's
non-secret repository and timeout values. They are limited to 30 minutes each. Failed or
completed environment pods are replaced with bounded exponential
backoff while retaining the workspace PVC; recovery progress and exhaustion are reported by
the `Ready` condition and pod-recovery status fields. Environment readiness is reported by
the current-generation `Ready` condition only after initialization completes and the sandboxd
startup/readiness probes pass; `status.phase` is a display summary rather than the scheduling
contract. GitHub App token minting is not implemented yet.

Transient operational reconciliation errors withdraw readiness with an `OperationalError`
reason and use controller-runtime's rate-limited retry; they do not put the Environment in the
terminal `Failed` phase. Missing or blank references, invalid specifications, and deterministic
Kubernetes `Invalid` or `BadRequest` responses report `Failed` with an `InvalidConfiguration`
reason and wait for the referenced Template or Project, or the Environment spec, to change.

Environments without an active Run are automatically paused after their template's
`idleTimeout` (15 minutes by default). An exact non-terminal Run owner or claim always
prevents an automatic idle pause, while explicit pause requests remain authoritative.
Run reconciliation and attached control-plane terminals refresh activity; terminal
heartbeats retry transient Kubernetes API failures. Opening the web terminal records
activity and wakes a paused environment before connecting.

Set `EnvironmentTemplate.spec.warmPool.min` to keep that many unclaimed environments
ready. `swe run` claims a ready environment before creating a cold one, and the operator
immediately replenishes the pool. Claiming for a Project recreates the generic warm pod
against its existing workspace volume so repository setup completes before the run is
reported ready. Deleting members never count as ready or active. Unclaimed failed, terminated,
or explicitly paused members are replaced immediately, retained for a five-minute recovery
grace, then deleted if they remain unusable; the persisted cleanup timestamp keeps that bound
stable across operator restarts. Replacement surge is bounded to the configured minimum, so at
most twice the minimum exact, unclaimed members remain while an entire replacement set is also
quarantined. Cleanup requires exact Template ownership and UID/resourceVersion preconditions, so
concurrent claims and promotions win without being deleted.

Only the `pod` environment backend is currently supported. An explicit
`Environment.spec.backend` takes precedence over its template's backend; unsupported
values on existing resources fail with an `UnsupportedBackend` Ready-condition reason
before the operator creates a Pod or PVC. CRD admission rejects unsupported values for
new Environments and templates.

The authenticated control plane exposes a browser terminal at
`GET /api/v1/namespaces/{namespace}/environments/{name}/terminal`. The WebSocket client
first sends `{"type":"open","cols":80,"rows":24}`, then uses binary frames for terminal
input and output. Send `{"type":"resize","cols":120,"rows":40}` to resize the shared
terminal. Kubernetes TokenReview authenticates bearer credentials and SubjectAccessReview
authorizes the exact namespaced environment; the namespace is never accepted from a query
parameter. See the [Helm chart documentation](charts/swe-platform/README.md#control-plane-authentication-and-authorization)
for credentials, browser sessions, RBAC, and self-hosted bootstrap setup.
The CLI uses this gateway rather than Kubernetes pod discovery or port forwarding:

```sh
SWE_CONTROL_PLANE_URL=https://swe.example.com \
SWE_CONTROL_PLANE_TOKEN="$TOKEN" swe attach my-environment
```

### Terminal operations console

`swe tui` is a keyboard-first, agent-neutral operations console for one namespace. It uses
the same typed Run and Environment APIs, transcript SSE stream, and WebSocket terminal bridge
as the browser console; it does not access Kubernetes or sandboxd directly and does not run an
agent itself. Supply the control-plane URL and bearer credential used by `swe attach` (the
credential is never persisted or displayed):

```sh
SWE_CONTROL_PLANE_URL=https://swe.example.com \
SWE_CONTROL_PLANE_TOKEN="$TOKEN" swe tui --namespace my-project
```

Use Up/Down (or `j`/`k`) and Enter to browse Run details, `c` to create a Run, `x` to request
confirmed cancellation, `t` to attach to the selected Run's allocated Environment, `r` to
refresh, and `q` to quit. The create form accepts a free-form agent adapter name and uses Tab
to move between fields and Ctrl-S to submit. Esc returns or closes a form; Ctrl-] detaches an
attached terminal and restores the dashboard. Run details show normalized status and usage,
Environment readiness/pause state, and a bounded raw transcript view. Transcript source, type,
payload, and retention gaps are displayed generically; adapter-owned payloads are not parsed as
a common event schema.

For a non-interactive authentication/connectivity check (including CI), use `swe tui --check`.
It validates namespaced Run-list access without starting a terminal UI or printing credentials.

Run transcripts use the same explicit control-plane URL and bearer credential:

```sh
SWE_CONTROL_PLANE_URL=https://swe.example.com \
SWE_CONTROL_PLANE_TOKEN="$TOKEN" swe logs --run fix-flaky-42
```

`swe logs --run RUN` selects that exact Run in `--namespace` and emits one NDJSON
record per SSE event:
`{"event":"transcript","id":"<opaque cursor>","data":<server envelope>}`. The
`data` value remains opaque, adapter-owned JSON; the CLI does not interpret it as a
shared transcript schema. Retention loss is explicit in records whose `event` is
`transcript-gap`. Use `--after <opaque cursor>` to resume explicitly. After a
successfully opened stream drops, the CLI reconnects with the event ID from the last
complete block successfully emitted. Invalid and expired cursors are reported rather
than silently skipped.

For compatibility, `swe logs <environment>` is not deprecated and still follows the
current Environment pod's `environment` container using kubeconfig authentication. It
does not read a Run transcript, and the CLI never infers a Run from a reusable
Environment. Production transcript durability and replay across control-plane restarts
require the chart's PostgreSQL configuration. The default, kind, and Argo development
presets retain bounded process-local storage and cannot promise restart replay.

## Contributing

Too early for code contributions — but design feedback and use-case descriptions are
very welcome in [issues](https://github.com/Chris-Cullins/swe-platform/issues).
