# swe-platform Helm chart

This chart installs the swe-platform CRDs, operator, and the first control-plane API.
The control plane accepts adapter-owned transcript events and streams them over SSE.
Its bounded transcript store is currently process-local, so the chart requires one control-plane
replica and uses a non-overlapping `Recreate` deployment. A durable shared store and portal
proxying are not implemented yet.

The operator reconciles each `Run` as the single task intent and allocates or claims its
`Environment`; clients must not create the two resources independently. Its RBAC permits
Run status/finalizer updates and Environment allocation/claim updates. Process execution
remains behind the environment's portable sandboxd contract rather than Kubernetes exec.

## Install

Choose the values preset for the target cluster and install into a dedicated namespace:

```sh
helm upgrade --install swe-platform ./charts/swe-platform \
  --namespace swe-platform-system --create-namespace \
  --values ./charts/swe-platform/values-k3s.yaml
```

The production presets create a `medium` `EnvironmentTemplate` using the published
`env-base` image. Operator, control-plane, and env-base tags default to the chart
`appVersion`, keeping a released chart on one tested version set and making Helm rollback
restore that set. Override all three tags with immutable release or SHA tags when testing
a different coordinated set; `latest` and `dev` are development-only choices.

Each image publish run emits a `swe-platform-release-*` artifact containing the chart
version, app version, and the registry digest of every image for incident diagnosis and
digest-pinned installation overrides.

| Preset | Assumptions |
|---|---|
| `values-k3s.yaml` | A default CSI-backed StorageClass is available. Uses one operator replica and the default OCI runtime because k3s does not ship gVisor. |
| `values-gke.yaml` | GKE Sandbox is enabled on every node that can host environments. Sets `runtimeClass: gvisor` and runs two operator replicas with leader election. |
| `values-eks.yaml` | A default EBS CSI StorageClass is available. Runs two operator replicas with leader election. EKS does not provide a standard gVisor RuntimeClass, so environments use the cluster default unless you override `environmentTemplates[].spec.runtimeClass`. |

The RuntimeClass applies to environment pods, not the operator. A preset that names a
RuntimeClass will leave environments Pending unless that RuntimeClass is installed and
supported by eligible nodes.

The operator creates a default ingress NetworkPolicy for each environment. It permits the
environment's sandboxd port only from this release's control-plane-labeled pods in the release
namespace. The cluster CNI must enforce
Kubernetes NetworkPolicy for this defense in depth; TLS identity and capability authorization
remain mandatory regardless. See [the security model](../../SECURITY.md) for credential
lifecycle and backend requirements.

For local development, use `values-kind.yaml`; it references locally loaded `:dev`
images and disables leader election. `values-argocd.yaml` is the preset for the
local Argo CD mirror (`hack/argocd-up.sh`): it tracks the mutable `:latest` images
published from main and references an out-of-band bootstrap token Secret.

## Control-plane authentication and authorization

Terminal and transcript endpoints require a credential. The control plane authenticates
Kubernetes bearer tokens with `TokenReview` for the `swe-platform` audience (configurable
with `controlPlane.auth.tokenAudience`), then asks `SubjectAccessReview` about the
exact namespace, resource name, and subresource on every request:

| Operation | Kubernetes authorization attributes |
|---|---|
| Read a transcript | `get` on `runs/transcript` with the requested Run `resourceName` |
| Append a transcript event | `update` on `runs/transcript` with the requested Run `resourceName` |
| Open a terminal | `get` on `environments/terminal` with the requested Environment `resourceName` |

This permits producer credentials to be restricted to one Run using an RBAC Role with
`resourceNames`. The namespace is part of the URL only as a resource selector; it becomes
authoritative only after RBAC authorizes that exact namespaced identity. Unknown Runs are
rejected before transcript state is allocated; an already-open stream is not continuously
reauthorized or closed when its Run is deleted. Transcript event `data` remains opaque,
adapter-owned JSON.

Service clients send `Authorization: Bearer <token>`. Browser transcript readers and
terminal clients may instead use an `HttpOnly`, `Secure`, `SameSite=Strict` cookie named
`swe-platform-session`; a trusted authentication proxy is responsible for creating that
session. Session cookies are deliberately rejected for transcript appends, which require
an explicit bearer service credential. WebSocket requests with an `Origin` must be
same-origin, including scheme, host, and port. Forwarded headers are ignored by default.
Behind a trusted reverse proxy, set `controlPlane.auth.trustProxyHeaders=true`; the control
plane then requires single-valued `X-Forwarded-Host` and `X-Forwarded-Proto` headers, so
the proxy must overwrite (not append or pass through) both. Non-browser WebSocket clients
without `Origin` are allowed only with an explicit bearer credential. Tokens are never
accepted in query parameters.

For initial self-hosted setup, an optional static bootstrap token provides all control-plane
API permissions. Create it out of band and reference it during installation:

```sh
kubectl -n swe-platform-system create secret generic swe-platform-bootstrap \
  --from-literal=token="$(openssl rand -hex 32)"
helm upgrade --install swe-platform ./charts/swe-platform \
  --namespace swe-platform-system --create-namespace \
  --set controlPlane.auth.bootstrapTokenSecret.name=swe-platform-bootstrap
```

The bootstrap token bypasses Kubernetes RBAC and is therefore equivalent to a control-plane
administrator credential. It must contain at least 32 characters, is accepted only as an
explicit bearer credential (never as a browser session), and changes require a control-plane
rollout. Use it only over TLS, store it outside values files, configure normal Kubernetes
Roles/RoleBindings, then remove the Helm value and Secret. Without this option, only
Kubernetes tokens with the configured audience and authorization from RBAC can use the APIs.

For example, this namespaced Role allows one adapter ServiceAccount to append only to
`run-123`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: run-123-transcript-producer
  namespace: project-a
rules:
  - apiGroups: ["swe.dev"]
    resources: ["runs/transcript"]
    resourceNames: ["run-123"]
    verbs: ["update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: run-123-transcript-producer
  namespace: project-a
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: run-123-transcript-producer
subjects:
  - kind: ServiceAccount
    name: run-123-adapter
    namespace: project-a
```

Create the ServiceAccount, then mint a short-lived credential with
`kubectl create token run-123-adapter -n project-a --audience=swe-platform`.

## Transcript API

After forwarding the control-plane Service, adapters can append JSON transcript events
and clients can consume replay plus live events as an SSE stream:

```sh
kubectl port-forward service/swe-platform-swe-platform-control-plane 8080:80
TOKEN="$(kubectl create token my-reader -n project-a --audience=swe-platform)"
curl -N -H "Authorization: Bearer ${TOKEN}" \
  http://127.0.0.1:8080/api/v1/namespaces/project-a/runs/run-123/transcript
curl -H "Authorization: Bearer ${TOKEN}" -H 'Content-Type: application/json' \
  -d '{"source":"adapter","sourceSequence":1,"idempotencyKey":"event-1","type":"output","data":{"text":"hello"}}' \
  http://127.0.0.1:8080/api/v1/namespaces/project-a/runs/run-123/transcript
```

The platform envelope owns transport metadata only:

- A transcript belongs to the immutable `(namespace, Run UID)` identity, so names reused
  across namespaces or after Run recreation never collide.
- `source` is a bounded, producer-selected idempotency partition, not authenticated
  provenance. `sourceSequence` is optional producer metadata and does not determine order.
- `(Run identity, source, idempotencyKey)` identifies one append while that event remains
  retained. An exact retry returns the original event with `200 OK` and
  `Idempotent-Replayed: true`; reuse with different `type`, `sourceSequence`, or raw `data`
  bytes returns `409 Conflict`. The first committed append returns `201 Created`. The original
  `{type,data}` envelope remains temporarily accepted with `202 Accepted` for compatibility,
  but is explicitly non-idempotent; reliable producers must send `source` and `idempotencyKey`.
- `sequence` is a stable, contiguous total order per Run. `id` is an opaque, versioned,
  store-issued cursor; clients must not parse or synthesize it. `Last-Event-ID` takes
  precedence over `?after=<cursor>` on reconnect.
- Cursors malformed, unverifiable after a memory-store restart, forged, for another Run,
  or ahead of its high-water mark return `400 invalid_cursor`. Authenticated cursors whose
  events are no longer retained return
  `410 cursor_expired` with an `application/problem+json` recovery boundary. A new stream
  without a cursor receives an ID-less `transcript-gap` control event before retained
  history when earlier events have expired.
- The memory store bounds Run count, per-Run and aggregate retained events/bytes,
  idempotency entries, replay size, subscriber count, and subscriber buffers. Capacity
  failures are explicit; a slow subscriber is disconnected rather than blocking producers.

`TranscriptStore` is the durability boundary. Every durable implementation must make append
linearizable per Run (idempotency check, sequence allocation, persistence, and publication are
one operation) and make subscription an atomic replay/live cut. All replicas must use that
same store and fan-out mechanism, so replay and live delivery require no sticky sessions.
Store generations and cursors must survive restart and rolling deployment. The current memory
implementation deliberately changes generation and signing key on restart, making old cursors
explicitly `400 invalid_cursor` instead of silently skipping events; it does not provide durable replay.
Idempotency is correspondingly retained-window idempotency: after an event is evicted, its key
may be reused and creates a new event.

## Validate

```sh
helm lint ./charts/swe-platform --values ./charts/swe-platform/values-gke.yaml
helm template swe-platform ./charts/swe-platform \
  --namespace swe-platform-system \
  --values ./charts/swe-platform/values-gke.yaml >/dev/null
```
