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
> pools keep unclaimed environments ready for `swe run` to claim. Agent adapters
> and portal proxying are not built yet. The Helm chart installs the
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
| **Project** | One or more git repos + config: secrets, setup hooks, size, changes workflow |
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

An explicit `--environment` request fails terminally if another Run already holds the claim; it
does not wait and unexpectedly start later after that claim is released.

Pause is not process checkpointing. Pausing fences the current execution domain and stops
**every** agent and declared-service process by removing the environment pod (or the
backend equivalent), while retaining the workspace disk and adapter-owned transcript.
Accepted work is cancelled only while that exact execution incarnation is securely
reachable, or cleanup proceeds without an RPC after pause has removed its pod and
endpoint. For unreachable or unavailable-adapter cleanup, the Run controller requests backend
pause and retains the claim/finalizer until the Environment reports the pod and endpoint gone.
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

Development targets a local [kind](https://kind.sigs.k8s.io/) cluster. Run `make kind-up`,
build and load the operator and env-base images as printed by that command, then install
`charts/swe-platform` with `values-kind.yaml`. The preset creates the `small` template
in `default`. Production installation assumptions and k3s/GKE/EKS presets are documented
in the [chart README](charts/swe-platform/README.md).

Create runs with an explicit template, or reference a `Project` to use its default
template and inject the environment variables from `spec.secretRef` into the environment:

```sh
swe run --template small "Fix the flaky test"
swe run --project org-repo "Fix the flaky test"
swe run --name fix-flaky-42 --project org-repo "Fix the flaky test"
swe run --environment warm-env-1 "Fix the flaky test"
swe cancel fix-flaky-42
```

`--name` is the create idempotency key: retry an uncertain request with the same name and
immutable task arguments. The CLI returns the existing Run only when its intent matches;
the controller creates or claims the Environment server-side.

The repository configured on a Project is cloned into `/workspace` when its
environment is created. If the repository contains `.agents/setup`, the hook runs once
after checkout. Set `Environment.spec.paused` to `true` to delete the pod while retaining
its workspace PVC, then set it to `false` to create a fresh pod; `.agents/resume` runs
after the volume is reattached. Both hooks can use values from the Project Secret, which
also remains available to the running environment. Setup and resume hooks are limited to
30 minutes each. Failed or completed environment pods are replaced with bounded exponential
backoff while retaining the workspace PVC; recovery progress and exhaustion are reported by
the `Ready` condition and pod-recovery status fields. Environment readiness is reported by
the current-generation `Ready` condition only after initialization completes and the sandboxd
startup/readiness probes pass; `status.phase` is a display summary rather than the scheduling
contract. GitHub App token minting is not implemented yet.

Transient operational reconciliation errors withdraw readiness with an `OperationalError`
reason and use controller-runtime's rate-limited retry; they do not put the Environment in the
terminal `Failed` phase. Invalid references and specifications report `Failed` with an
`InvalidConfiguration` reason and wait for the referenced Template or Project, or the
Environment spec, to change.

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
stable across operator restarts. Cleanup requires exact Template ownership and UID/resourceVersion
preconditions, so concurrent claims and promotions win without being deleted.

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

## Contributing

Too early for code contributions — but design feedback and use-case descriptions are
very welcome in [issues](https://github.com/Chris-Cullins/swe-platform/issues).
