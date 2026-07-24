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
provides bounded in-memory transcript ingestion and SSE streaming, opaque process-local
browser sessions backed by repeated Kubernetes TokenReview/SAR authorization, and typed
Run/Environment resource APIs for the console.
Remaining gaps are marked `TODO(P0/P1/P2)` in code — most notably secure agent credential
injection, additional agent adapters, GitHub App–scoped git tokens, and egress/portal
networking. The `claude-code` (default), `amp`, and `codex` adapters are registered and use sandboxd
managed processes; Amp's `AMP_API_KEY` delivery remains an unmet prerequisite and adapter
tests use fake process services; Codex supports process-scoped `CODEX_API_KEY` delivery.

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
  `internal/controllers/`, `cmd/{operator,swe}`. Shared fenced Environment intent
  publication and validation belongs in `internal/lifecycle/`; controllers remain
  the sole owners of observed lifecycle transitions. `sandboxd/` is a **separate Go
  module** with its own `go.mod`: keep its dependencies minimal (gRPC + protobuf
  only) so it stays portable and the environment base image stays small.
  Generated protobuf code lives in `sandboxd/gen/` and is committed.
- **APIs:** CRDs are `v1alpha1`; breaking changes are acceptable pre-1.0, but migrate
  the CRD sketch in `docs/ARCHITECTURE.md` when you change fields.
- **CLI-first:** every user-facing feature needs a CLI path before any UI work.
- **Minimal changes:** match existing style; don't refactor beyond the task.

## Sync checklist

Several files must move in lockstep, and CI only enforces some of the pairings.
When a change touches one side of a row, update the other side **in the same
commit**:

- **CRD field changes** → `make generate manifests` (deepcopy + CRDs + RBAC; CI
  diffs `charts/swe-platform/crds`) and migrate the CRD sketch in
  `docs/ARCHITECTURE.md`.
- **Chart values/template changes** → review every `values-*.yaml` preset —
  `kind` uses locally loaded `:dev`, `argocd` tracks `:latest` for the Argo
  mirror, `k3s`/`gke`/`eks` stay immutable on the chart `appVersion` — plus the
  preset table in the chart README.
- **New values preset** → add it to the lint loop in `ci.yaml`; add it to the
  production immutability check only if it pins `appVersion`.
- **New image** → `Makefile` `docker-build-*` target, the `publish-images.yaml`
  matrix, and `hack/argocd/imageupdater.yaml` if the mirror should roll it on
  new `:latest` digests.
- **New user-facing feature** → CLI path first, then extend `hack/e2e.sh`
  acceptance coverage.
- **Tooling, structure, or workflow changes** → update this file.

## Build & test

Two Go modules — root (operator, CLI, API types) and `sandboxd/`. Everything below
runs both via `make` targets:

- **Orb setup:** `.agents/setup` installs the pinned Go and Helm versions into
  `$HOME/.local` when they are unavailable.

- **Build all binaries:** `make build` (outputs operator, control plane, CLI, and sandboxd to `bin/`, gitignored)
- **Unit tests:** `make test` · **Vet:** `make vet`
- **Operations console:** from `ui/`, install with `npm ci`; use `npm run lint`,
  `npm run typecheck`, `npm test -- --run`, and `npm run build`. Start the standalone
  Vite development server with `npm run dev`. Production uses `make ui-build`
  followed by `make build-control-plane-production`; the tagged build embeds `ui/dist`,
  while ordinary Go builds intentionally work without generated assets. The control-plane
  image performs both stages in one multi-stage build.
- **Windows portability:** CI runs focused sandboxd process, launch-material, Exec, and
  filesystem tests on `windows-latest`; keep OS-specific tests behind build tags.
- **Regenerate deepcopy:** `make generate` · **CRDs + RBAC:** `make manifests`
  (`manifests` synchronizes chart CRDs; CI fails on a diff). Use `make check-chart-crds`
  to verify the checked-in Helm CRDs independently.
- **Regenerate protobuf:** `make proto` (requires `protoc`; plugins install locally)
- **Dev cluster:** `make kind-up` creates/reuses `swe-dev`, installs the pinned gVisor
  `gvisor` RuntimeClass plus the CSI hostpath driver and VolumeSnapshot controller, and
  verifies both with smoke resources. Build/load the images, then install
  `charts/swe-platform` with `values-kind.yaml` and the gVisor override printed by the
  script. Run the full acceptance suite against it with
  `KIND_CLUSTER=swe-dev E2E_USE_EXISTING_CLUSTER=true E2E_RUNTIME_CLASS=gvisor ./hack/e2e.sh`.
  For operator/control-plane iteration, `make dev` runs the pinned Skaffold watch loop,
  builds and loads changed images, and upgrades the same Helm release on the explicit
  `kind-swe-dev` context (or `kind-$KIND_CLUSTER`), while refusing the Argo mirror named
  by `KIND_ARGO_CLUSTER` or detected by its `argocd` namespace. Build/load `env-base:dev`
  separately when a test needs a fresh Environment pod; apply CRD upgrades separately
  because Helm does not upgrade `crds/`.
- **Argo CD main mirror:** `make argocd-up` creates a separate `swe-argo` kind
  cluster running Argo CD + the Image Updater (`hack/argocd/`,
  `values-argocd.yaml` preset). It syncs the chart from `origin/main` and rolls
  the operator/control plane on new `:latest` digests — only pushed commits
  take effect. The bootstrap requires one node with at least 5 CPUs and 6 GiB
  allocatable so a warm `tiny` Environment and its replacement remain schedulable
  with Argo/system workloads. `make argocd-ui` keeps a foreground, loopback-only
  control-plane Service forward alive across those rollouts. Keep it isolated from `swe-dev`;
  two operators must never reconcile the same custom resources.
- **Production Helm presets:** `charts/swe-platform/values-{k3s,gke,eks}.yaml`; CI lints
  and renders every preset, verifies all rendered production images use the coordinated
  chart `appVersion`, and rejects `latest`/`dev`. Provider assumptions are documented in
  the chart README. Image publish runs attach a release manifest with the chart version
  and all three image digests.
- **E2E acceptance:** `./hack/e2e.sh` — full kind + operator + `swe run` pass with the
  env-base image built and loaded locally (no registry credentials needed). It also verifies
  the documented server-side CRD upgrade from the pre-scoped-credentials schema,
  control-plane TokenReview/SAR scoping, opaque browser session exchange/logout and CSRF,
  the embedded console entry point/SPA fallback/static assets, typed Run
  list/get/create/retry/cancel, Environment get, transcript SSE, terminal attach, and
  process-scoped fake API-key delivery without ambient setup/resume/sandboxd exposure.
  Runs in CI as the `e2e` workflow on relevant PRs and via `workflow_dispatch`.
- **CRD installation/upgrades:** `make install-crds` uses server-side apply with force-conflicts;
  plain Helm upgrades must apply the chart's `crds/` directory before `helm upgrade`.
- **Images:** `make docker-build` (operator + env-base). The env-base image builds
  its pinned tmux with `images/env-base/tmux-control-output-drain.patch`; keep the
  source checksum and patch synchronized when upgrading tmux. Its `terminal-test`
  target runs the patched-runtime terminal regression during `hack/e2e.sh`. The image
  also includes version-pinned Claude Code (the default adapter) and Amp CLIs. Amp image
  installs must retain `AMP_SKIP_UPDATE_CHECK=1` and the pinned npm integrity check.
- **Publish images:** pushes to `main` and `v*` tags publish multi-architecture operator
  and env-base images to GHCR via `.github/workflows/publish-images.yaml`.

**If you add or change tooling, structure, or workflows, update this file in the same
commit.**

## Safety

- Never commit secrets, tokens, or the `docs/` folder (gitignored — leave it that way).
- Don't create git commits/pushes unless the maintainer explicitly asks.
