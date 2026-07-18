# swe-platform

Open-source platform for running coding agents unattended in ephemeral, isolated Kubernetes environments.

Give an agent a task — from the CLI, web UI, or an MCP call — and the platform provisions a
fresh environment (repo clone, toolchain, secrets, setup hooks), runs the agent in it,
streams everything back live, auto-pauses when idle so compute cost drops to ~$0, and ends
with a reviewable diff, branch, or PR.

> **Status: early.** The P0 scaffold is in — CRDs, operator, `sandboxd`, CLI — with a
> passing kind end-to-end (`./hack/e2e.sh`). Agent adapters and the control plane are
> not built yet. Watch the repo if this interests you.

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

## Roadmap

- **P0 — skeleton:** `sandboxd`, CRDs, operator, CLI, kind quickstart
- **P1 — secure & streamable:** Helm chart, transcript streaming, web terminal, scoped
  git tokens, egress proxy
- **P2 — economics & portals:** pause/resume, warm pools, portals, repo setup hooks
- **P3 — multiplayer agents:** inboxes/spawning, web UI, metering, MCP server
- **P4 — enterprise:** SSO/RBAC/audit, Windows environments (.NET Framework workloads),
  hibernation tier, hosted offering

## Local development

Development targets a local [kind](https://kind.sigs.k8s.io/) cluster. Quickstart lands
with P0 — watch this space.

## Contributing

Too early for code contributions — but design feedback and use-case descriptions are
very welcome in [issues](https://github.com/Chris-Cullins/swe-platform/issues).
