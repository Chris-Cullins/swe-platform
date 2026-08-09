# AGENTS.md

Guidance for coding agents working in this repository.

## What this is

`swe-platform` is an open-source platform for running coding agents unattended in
ephemeral, isolated Kubernetes environments. Read `README.md` first for the product
shape and core concepts.

[`ARCHITECTURE.md`](ARCHITECTURE.md) is the canonical tracked architecture source. Read it
before architectural work and keep implemented behavior, approved future contracts, and
open work clearly separated. Do not guess at design intent that is not recorded there or in
an approved maintainer decision.

## Current state

P0 scaffold is in place: CRD types, environment controller, `sandboxd` (exec/fs/ports/
health and a shared tmux terminal), CLI (`run`/`logs`/`attach`), kind acceptance, CI,
and a Helm chart for the operator, control plane, and CRDs. The control plane currently
provides PostgreSQL-backed durable transcript ingestion and database-polled SSE streaming with
a development-only bounded memory fallback, plus explicit memory or encrypted PostgreSQL
browser sessions backed by repeated Kubernetes TokenReview/exact SAR authorization. PostgreSQL
uses one process-owned pgxpool and ordered migrations; production fails closed without its
database and administrator-owned session keyring. The control plane also provides typed
Run/Environment resource APIs for the console.
The CLI also provides a local stdio `swe mcp` server with bounded `create_run` and
UID-fenced `read_transcript` tools that act through the caller's existing explicit
control-plane bearer credential; interactive terminal attach is intentionally not an MCP tool.
The control plane also provides the authenticated declared-service portal gateway, with fenced
wake/currentness checks and a purpose-scoped sandboxd tunnel. Remaining gaps are tracked in
`ARCHITECTURE.md`, code comments, and linked issues — most notably additional credential forms,
additional agent adapters, and egress networking. Repository
`.swe/services.yaml` ingestion and supervised service launch with
gateway-discovered `PUBLIC_URL` are implemented. GitHub App repository credentials use
short-lived, exact-repository `contents:write` installation tokens for authenticated clone and
process-scoped Git/GitHub CLI access, with execution-fenced refresh and terminal cleanup.
The `claude-code` (default), `amp`, `codex`, and `pi` adapters are
registered and use sandboxd managed processes. API-key profiles use process-scoped launch
material as `ANTHROPIC_API_KEY`,
`AMP_API_KEY`, or `CODEX_API_KEY`; tests use fake process services and no provider credentials.
Pi deliberately supports no credential profiles or credential injection.

## Architecture invariants — do not violate these

1. **`sandboxd` is the only contract into an environment.** Control plane components
   never exec into pods or mount their filesystems directly.
2. **The agent layer is adapters.** Platform code must never depend on one agent's
   internals; integrations go through the adapter interface.
3. **Pause = disk + transcript.** Delete the pod, retain the PVC, resume onto a fresh
   pod. No CRIU/process checkpointing.
4. **CRDs are the source of truth for infrastructure state.** PostgreSQL is only for
   transcripts/events, not desired/observed infrastructure state.
5. **gVisor RuntimeClass by default** wherever it's possible; isolation is a feature.
6. **Namespace-per-project tenancy is the approved target.** Do not build an alternate
   tenancy boundary; current gaps and the approved claim model are in `ARCHITECTURE.md`.
   `Project.spec.repositories` is a list from day one even though v1 executes single-repo.
7. **Inter-agent messaging is a future platform primitive** (inbox + wake + notify).
   Transcript formats stay adapter-owned; don't build a shared transcript schema.
8. **Environment backends are pluggable** (`pod` / `kubevirt` / `external-runner`) and
   `sandboxd` must stay OS-portable: no Linux-only assumptions in its API; abstract
   terminal (tmux vs ConPTY), paths, and exec.

## Conventions

- **Language:** Go for control plane, operator, `sandboxd`, and CLI.
- **Layout:** kubebuilder conventions — `api/v1alpha1/` for types,
  `internal/controllers/`, and `cmd/{operator,control-plane,swe,egress-proxy}` for root-module
  binaries. The egress proxy command is a disabled protocol foundation and intentionally remains
  outside `make build` and image publishing until the fail-closed runtime is enabled.
  `internal/egresspolicy/` owns the strict immutable ConfigMap parser as well as destination and
  revision authority; `internal/egressproxy/` contains an uncached two-second currentness
  authorizer foundation, but the command must remain wired to `DisabledAuthorizer`.
  `internal/egressidentity/` and `internal/egresspod/` are likewise inert contract foundations;
  do not wire them into reconciliation or relax the non-empty allowlist fence before the complete
  restricted runtime is atomically enabled. The exact adapter contract boundary is
  `internal/agent/`. Shared fenced Environment intent
  publication and validation belongs in `internal/lifecycle/`; controllers remain
  the sole owners of observed lifecycle transitions. Environment reconciliation is split
  within `internal/controllers/` into ordered gate orchestration, lifecycle intent,
  provisioning/resources, pod recovery, status, and manager setup files; keep gate names
  bounded and preserve the ordering contract in its direct test. `sandboxd/` is a
  **separate Go module** with its own `go.mod`: keep its dependencies minimal so it stays
  portable and the environment base image stays small.
  Generated protobuf code lives in `sandboxd/gen/` and is committed.
- **APIs:** CRDs are `v1alpha1`; breaking changes are acceptable pre-1.0. Whenever CRD
  fields or contracts change, update the relevant current resource table and
  ownership/reference documentation in root [`ARCHITECTURE.md`](ARCHITECTURE.md) in the
  same commit.
- **CLI-first:** every user-facing feature needs a CLI path before any UI work.
- **Minimal changes:** match existing style; don't refactor beyond the task.

## Sync checklist

Several files must move in lockstep, and CI only enforces some of the pairings.
When a change touches one side of a row, update the other side **in the same
commit**:

- **CRD field or contract changes** → `make generate manifests` (deepcopy + CRDs + RBAC;
  CI diffs `charts/swe-platform/crds`) and update the relevant current resource table and
  ownership/reference documentation in root [`ARCHITECTURE.md`](ARCHITECTURE.md).
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
- **Local operator:** `make run` requires explicit `RUN_TENANCY_MODE`,
  `RUN_SYSTEM_NAMESPACE`, and `RUN_INSTALLATION_NAME`; scoped mode also takes a
  space-separated `RUN_TENANCY_NAMESPACES` list.
- **Unit tests:** `make test` (including rendered Helm RBAC, Argo port-forward, BYOC
  production-preset checks, and the disabled egress-conformance guard) · **Vet:** `make vet`.
  PostgreSQL transcript/session integration tests
  run when `SWE_TEST_POSTGRES_URL` points to a disposable database; CI supplies PostgreSQL 17.
  The required `build-test` CI job runs the root and sandboxd Go suites plus
  the four shell checks above, mirroring `make test`. The egress-conformance live runner remains
  default-off on its separate `hack/kind-calico-conformance.yaml` topology and must not be invoked
  by CI/e2e; only its guard test runs. Keep its temporary kind admin kubeconfig and Python bytecode
  inside cleanup-managed temporary paths so both early exits and `make test` leave no artifacts.
- **Operations console:** from `ui/`, install with `npm ci`; use `npm run lint`,
  `npm run typecheck`, `npm test -- --run`, and `npm run build`. Start the standalone
  Vite development server with `npm run dev`. Production uses `make ui-build`
  followed by `make build-control-plane-production`; the tagged build embeds `ui/dist`,
  while ordinary Go builds intentionally work without generated assets. The control-plane
  image performs both stages in one multi-stage build. The ESLint flat config deliberately
  enables only `rules-of-hooks` and `exhaustive-deps` from the React Hooks plugin; treat
  enabling that plugin's expanding recommended preset as a lint-policy change.
- **Windows portability:** CI runs focused sandboxd process, launch-material, Exec,
  filesystem, and terminal-backend-selection tests on `windows-latest`; keep OS-specific
  tests behind build tags. Native terminal selection fails closed until ConPTY is implemented.
- **Regenerate deepcopy:** `make generate` · **CRDs + RBAC:** `make manifests`
  (`manifests` synchronizes chart CRDs; CI fails on a diff). Use `make check-chart-crds`
  to verify the checked-in Helm CRDs independently.
- **Regenerate protobuf:** `make proto` (requires `protoc`; plugins install locally)
- **Dev cluster:** `make kind-up` creates/reuses `swe-dev`, installs the pinned gVisor
  `gvisor` RuntimeClass plus the CSI hostpath driver and VolumeSnapshot controller, and
  configures the fixture driver's `fsGroupPolicy: File` and verifies non-root group-writable
  snapshot/restore behavior with smoke resources. Build/load the images, then install
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
  the chart README. Every ad hoc chart render must also set an explicit tenancy mode; the
  base values intentionally have no default. Image publish runs attach a release manifest
  with the chart version and all three image digests.
- **E2E acceptance:** `./hack/e2e.sh` — full kind + operator + `swe run` pass with the
  env-base image built and loaded locally (no registry credentials needed). It also verifies
  the documented server-side CRD upgrade from the pre-scoped-credentials schema,
  system-namespace installation plus CLI onboarding of a distinct scoped Project namespace,
  exact claims, managed catalog copies, explicit baseline quota/RBAC/policy, scoped denial,
  same-named two-release isolation, and retained offboarding,
  sandboxd ingress NetworkPolicy allow/deny behavior on CI's exact-UID-pinned, fully bootstrapped
  Calico kind fixture, including group-writable fresh and retained CSI workspaces under gVisor
  (without claiming production CNI, CSI, or runtime attestation),
  control-plane TokenReview/SAR scoping, memory and durable encrypted PostgreSQL browser session
  exchange/logout/revocation, capacity and CSRF,
  the embedded console entry point/SPA fallback/static assets, typed Run
  list/get/create/retry/cancel, Environment get, transcript SSE, terminal attach, and
  the local stdio MCP tool list plus UID-fenced bounded transcript read,
  actual-listener service observation through healthy/unhealthy/restart/pause/resume/removal
  transitions with declaration and fresh-execution correlation and no URL,
  declared-service portal allocation/proxy authorization and lifecycle fencing, process-scoped fake Claude and Amp API-key delivery without ambient
  setup/resume/sandboxd exposure, and Secret-only sandboxd process/service-observation/portal
  capability tokens without Environment pod projection.
  Runs in CI as the `e2e` workflow on relevant PRs and via `workflow_dispatch`.
- **CRD installation/upgrades:** `make install-crds` uses server-side apply with force-conflicts;
  plain Helm upgrades must apply the chart's `crds/` directory before `helm upgrade`.
- **Images:** `make docker-build` (operator + env-base). The env-base image builds
  its pinned tmux with `images/env-base/tmux-control-output-drain.patch`; keep the
  source checksum and patch synchronized when upgrading tmux. Its `terminal-test`
  target runs the patched-runtime terminal regression during `hack/e2e.sh`. The image
  also includes version-pinned Claude Code (the default adapter), Amp, Codex, and Pi CLIs. Amp
  image installs must retain `AMP_SKIP_UPDATE_CHECK=1` and the pinned npm integrity check.
- **Publish images:** pushes to `main` and `v*` tags publish multi-architecture operator
  and env-base images to GHCR via `.github/workflows/publish-images.yaml`.

**If you add or change tooling, structure, or workflows, update this file in the same
commit.**

## Safety

- Never commit secrets or tokens.
