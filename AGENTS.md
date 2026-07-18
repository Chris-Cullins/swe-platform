# AGENTS.md

Guidance for coding agents working in this repository.

## What this is

`swe-platform` is an open-source platform for running coding agents unattended in
ephemeral, isolated Kubernetes environments. Read `README.md` first for the product
shape and core concepts.

**Detailed design docs live in `docs/` (`ARCHITECTURE.md`, `TODO.md`) — that folder is
gitignored and local-only.** If `docs/` is present, read it before doing anything
architectural. If it's missing, ask the maintainer instead of guessing at design intent.

## Current state

Pre-scaffold. The immediate milestone is **P0** (see `docs/TODO.md` if available):

1. Go module + kubebuilder scaffold, Makefile, CI
2. `sandboxd` — gRPC daemon: exec / filesystem / terminal / ports / health
3. CRDs v1alpha1: `Environment`, `EnvironmentTemplate`, `Run`, `Project`
4. Operator: environment controller (pod + PVC + NetworkPolicy, setup hook, status)
5. CLI `swe`: `run` / `attach` / `logs`
6. kind quickstart: `kind create cluster && helm install && swe run` in <5 min

## Architecture invariants — do not violate these

1. **`sandboxd` is the only contract into an environment.** Control plane components
   never exec into pods or mount their filesystems directly.
2. **The agent layer is adapters.** Platform code must never depend on one agent's
   internals; integrations go through the adapter interface.
3. **Pause = disk + transcript.** Delete the pod, retain the PVC, resume onto a fresh
   pod. No CRIU/process checkpointing.
4. **CRDs are the source of truth for infrastructure state.** Postgres (when it lands)
   is only for transcripts/events, not desired/observed state.
5. **gVisor RuntimeClass by default** wherever it's possible; isolation is a feature.
6. **Namespace-per-project tenancy.** `Project.spec.repositories` is a list from day
   one even though v1 executes single-repo.
7. **Inter-agent messaging is a platform primitive** (inbox + wake + notify). Transcript
   formats stay adapter-owned; don't build a shared transcript schema.
8. **Environment backends are pluggable** (`pod` / `kubevirt` / `external-runner`) and
   `sandboxd` must stay OS-portable: no Linux-only assumptions in its API; abstract
   terminal (tmux vs ConPTY), paths, and exec.

## Conventions

- **Language:** Go for control plane, operator, `sandboxd`, and CLI.
- **Layout:** kubebuilder conventions — `api/v1alpha1/` for types,
  `internal/controllers/`, `cmd/`. Keep `sandboxd` a separate module/binary.
- **APIs:** CRDs are `v1alpha1`; breaking changes are acceptable pre-1.0, but migrate
  the CRD sketch in `docs/ARCHITECTURE.md` when you change fields.
- **CLI-first:** every user-facing feature needs a CLI path before any UI work.
- **Minimal changes:** match existing style; don't refactor beyond the task.

## Build & test

_Not yet scaffolded._ When P0 lands this section must contain: how to build all
binaries, run unit tests, spin up the kind dev cluster, and regenerate CRDs/deepcopy
(`make generate`, `make manifests` or equivalent).

**If you add or change tooling, structure, or workflows, update this file in the same
commit.**

## Safety

- Never commit secrets, tokens, or the `docs/` folder (gitignored — leave it that way).
- Don't create git commits/pushes unless the maintainer explicitly asks.
