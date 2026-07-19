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

P0 scaffold is in place: CRD types, environment controller, `sandboxd` (exec/fs/ports/
health and a shared tmux terminal), CLI (`run`/`logs`/`attach`), kind acceptance, CI,
and a Helm chart for the operator, control plane, and CRDs. The control plane currently
provides in-memory transcript ingestion and SSE streaming.
Remaining gaps are marked `TODO(P0/P1/P2)` in code — most notably setup-hook execution,
agent credential injection, and agent adapters.

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
  `internal/controllers/`, `cmd/{operator,swe}`. `sandboxd/` is a **separate Go
  module** with its own `go.mod`: keep its dependencies minimal (gRPC + protobuf
  only) so it stays portable and the environment base image stays small.
  Generated protobuf code lives in `sandboxd/gen/` and is committed.
- **APIs:** CRDs are `v1alpha1`; breaking changes are acceptable pre-1.0, but migrate
  the CRD sketch in `docs/ARCHITECTURE.md` when you change fields.
- **CLI-first:** every user-facing feature needs a CLI path before any UI work.
- **Minimal changes:** match existing style; don't refactor beyond the task.

## Build & test

Two Go modules — root (operator, CLI, API types) and `sandboxd/`. Everything below
runs both via `make` targets:

- **Orb setup:** `.agents/setup` installs the pinned Go and Helm versions into
  `$HOME/.local` when they are unavailable.

- **Build all binaries:** `make build` (outputs operator, control plane, CLI, and sandboxd to `bin/`, gitignored)
- **Unit tests:** `make test` · **Vet:** `make vet`
- **Regenerate deepcopy:** `make generate` · **CRDs + RBAC:** `make manifests`
  (`manifests` synchronizes chart CRDs; CI fails on a diff). Use `make check-chart-crds`
  to verify the checked-in Helm CRDs independently.
- **Regenerate protobuf:** `make proto` (requires `protoc`; plugins install locally)
- **Dev cluster:** `make kind-up`, build/load the images, then install
  `charts/swe-platform` with `values-kind.yaml` as printed by the script.
- **Production Helm presets:** `charts/swe-platform/values-{k3s,gke,eks}.yaml`; CI lints
  and renders every preset, verifies all rendered production images use the coordinated
  chart `appVersion`, and rejects `latest`/`dev`. Provider assumptions are documented in
  the chart README. Image publish runs attach a release manifest with the chart version
  and all three image digests.
- **E2E acceptance:** `./hack/e2e.sh` — full kind + operator + `swe run` pass with the
  env-base image built and loaded locally (no registry credentials needed). Runs in CI
  as the `e2e` workflow on relevant PRs and via `workflow_dispatch`.
- **Images:** `make docker-build` (operator + env-base)
- **Publish images:** pushes to `main` and `v*` tags publish multi-architecture operator
  and env-base images to GHCR via `.github/workflows/publish-images.yaml`.

**If you add or change tooling, structure, or workflows, update this file in the same
commit.**

## Safety

- Never commit secrets, tokens, or the `docs/` folder (gitignored — leave it that way).
- Don't create git commits/pushes unless the maintainer explicitly asks.
