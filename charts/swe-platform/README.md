# swe-platform Helm chart

This chart installs the swe-platform CRDs, operator, and the first control-plane API.
The control plane accepts adapter-owned transcript events and streams them over SSE.
Its current transcript store is in-memory and single-replica; durable storage, auth,
and the gateway are not implemented yet.

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
`env-base` image. Override image tags with immutable release or SHA tags for controlled
rollouts.

| Preset | Assumptions |
|---|---|
| `values-k3s.yaml` | A default CSI-backed StorageClass is available. Uses one operator replica and the default OCI runtime because k3s does not ship gVisor. |
| `values-gke.yaml` | GKE Sandbox is enabled on every node that can host environments. Sets `runtimeClass: gvisor` and runs two operator replicas with leader election. |
| `values-eks.yaml` | A default EBS CSI StorageClass is available. Runs two operator replicas with leader election. EKS does not provide a standard gVisor RuntimeClass, so environments use the cluster default unless you override `environmentTemplates[].spec.runtimeClass`. |

The RuntimeClass applies to environment pods, not the operator. A preset that names a
RuntimeClass will leave environments Pending unless that RuntimeClass is installed and
supported by eligible nodes.

For local development, use `values-kind.yaml`; it references locally loaded `:dev`
images and disables leader election.

## Transcript API

After forwarding the control-plane Service, adapters can append JSON transcript events
and clients can consume replay plus live events as an SSE stream:

```sh
kubectl port-forward service/swe-platform-swe-platform-control-plane 8080:80
curl -N http://127.0.0.1:8080/api/v1/runs/run-123/transcript
curl -H 'Content-Type: application/json' \
  -d '{"type":"output","data":{"text":"hello"}}' \
  http://127.0.0.1:8080/api/v1/runs/run-123/transcript
```

SSE reconnects honor `Last-Event-ID`. Callers may also request events after a known ID
with `?after=<id>`.

## Validate

```sh
helm lint ./charts/swe-platform --values ./charts/swe-platform/values-gke.yaml
helm template swe-platform ./charts/swe-platform \
  --namespace swe-platform-system \
  --values ./charts/swe-platform/values-gke.yaml >/dev/null
```
