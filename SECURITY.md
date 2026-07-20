# Security model

## sandboxd threat model

`sandboxd` is a privileged capability endpoint: successful calls can execute arbitrary
commands, read and write the environment filesystem, and attach to its shared terminal.
Environment workloads are untrusted. The design therefore assumes that an environment may
scan cluster addresses, observe or tamper with traffic available to it, and recover every
credential mounted in its own pod. Network location and pod IP addresses are not identities.

The security boundary protects one environment from another and protects control-plane calls
from redirection to a stale or substituted sandboxd process. Code already executing inside an
environment can control that same environment; sandboxd authentication does not attempt to
sandbox a workload from itself. HTTP user authentication and authorization are a separate
control-plane boundary.

### Transport and caller authorization

Every sandboxd server requires TLS 1.3 and bearer capability authorization. The operator
creates a self-signed ECDSA server certificate whose random DNS identity names one pod
incarnation. It records that identity on both the pod and credential Secret. A client reads the
current Environment and its UID-owned pod, then pins the identity and public trust certificate
published atomically on that pod. It verifies the identity during the TLS handshake. Connecting
to a different environment, a stale pod, or a process without the current private key therefore
fails.

Bearer tokens are random per incarnation and map to explicit service capabilities (`health`,
`terminal`, `exec`, `process`, `filesystem`, and `ports`). sandboxd interceptors authorize both
unary and streaming RPCs before handlers run. The terminal credential grants `health` and
`terminal`; a separate operator-held adapter credential grants only `process`. The mounted
authorization file contains SHA-256 verifiers, not raw tokens, and the raw process token is not
projected into the environment pod or published in pod annotations. Possession of one
environment's token grants nothing in another environment.

The Environment-owned Secret contains the private key, authorization configuration, and raw
process token. The public trust certificate and terminal token are published atomically as pod
annotations, which makes pod `get` plus `pods/portforward` the Kubernetes authorization boundary
for CLI attachment without granting callers access to arbitrary namespace Secrets. The control
plane has pod `get` but not Secret access. The operator validates the exact Run, Environment,
pod, and Secret incarnations before constructing a pinned process-only connection for an
adapter. HTTP authorization for the control-plane terminal endpoint remains a separate
requirement.

The operator also creates an ingress NetworkPolicy for every environment pod. Port 50051 is
admitted only from pods matching this installation's name and instance and either its
control-plane or operator component label in the configured namespace. Thus an
environment pod is denied direct ingress to another environment's sandboxd by NetworkPolicy,
while protocol authentication remains the durable boundary on clusters without NetworkPolicy
enforcement. Environment pods do not receive a Kubernetes service-account token by default.
NetworkPolicies are additive, destination-node traffic may be exempt, and Kubernetes API
port-forwarding is governed by `pods/portforward` RBAC rather than this policy.

### Credential lifecycle

1. **Bootstrap:** before creating a pod, the operator generates a new certificate, private key,
   random identity, and random capability tokens. It writes them to an Environment-owned
   Kubernetes Secret. The pod projects only the TLS keypair and hashed capability configuration
   read-only at `/var/run/swe-platform/sandboxd`; the raw process token remains available only
   through an exact-name operator Secret read. sandboxd fails closed if any TLS or capability
   file is absent or invalid.
2. **Rotation:** whenever the backing pod has disappeared and is recreated (including resume),
   the operator replaces every credential and annotates the new pod with the new identity.
   Container restarts within the same pod retain that pod incarnation's credentials.
3. **Revocation and deletion:** pause first deletes the pod, terminating active connections,
   and then deletes its credential Secret. An Environment finalizer applies the same ordering on
   deletion and retains the NetworkPolicy until the pod and Secret are gone. Dependents are
   checked against the Environment UID, so recreating the same name cannot adopt a stale pod or
   credential. Recreating an environment or pod cannot authenticate with copied credentials
   from the prior incarnation.
4. **Storage:** credentials are held in a Kubernetes Secret volume, never the retained workspace
   PVC. Pausing retains `/workspace` but removes the credential source and pod.

Certificates are valid for one year to avoid expiring a continuously running pod; normal pod
recreation rotates them much earlier. Operators should recreate any pod approaching that limit.

## Run-scoped agent API keys

An `AgentCredentialProfile` binds an immutable adapter and `APIKey` credential type in one
namespace. Its API key is stored in a same-namespace Secret whose name is derived from the
profile UID and whose exact controller owner, type, sole `apiKey` entry, encoding, and bounded
size are validated before use. A Run records the selected profile's name and UID in status
before environment allocation; deleting and recreating a same-name profile cannot satisfy that
historical binding. Missing profiles and backing Secrets receive bounded retries, while replaced
profiles and malformed or foreign Secret collisions fail closed.

The operator performs uncached, exact-name profile and Secret reads immediately before adapter
acceptance, copies the key into a short-lived adapter credential, and best-effort clears the
buffers after the call. The Claude Code adapter sends it only through sandboxd's distinct
`StartWithLaunchMaterial` RPC as `ANTHROPIC_API_KEY`. It never falls back to the ordinary Start
RPC if an old sandboxd server reports `Unimplemented`. sandboxd validates launch material before
publishing a process, applies it only to the child environment, and stores and returns only the
public process specification plus a private launch-mode bit used for idempotency fencing.

This prevents automatic ambient delivery to setup/resume hooks, sandboxd, and unrelated child
processes. It is not hard same-user process isolation: the selected agent and its descendants,
repository wrappers left by setup, same-UID peers, or explicit output can disclose the key, and
transcript redaction is not guaranteed. Anyone allowed to create a Run in a namespace can
initially select any profile in that namespace; profile management requires separate Secret and
CRD administration. OAuth/subscription files, refresh and writeback, leases, per-user ownership,
Git/setup/service credentials, Amp login persistence, and stronger confinement remain deferred.

### Other environment backends

TLS identity plus bearer service capabilities are backend-portable and do not use Kubernetes
identity as the protocol contract. A KubeVirt backend should inject the same per-incarnation
bundle into ephemeral guest storage, not the workspace disk. An external runner must obtain a
fresh bundle over its authenticated registration/bootstrap channel and normally establish an
outbound connection or tunnel; the control plane must still pin the advertised incarnation and
apply the same capabilities. Disconnect, replacement, pause, or deletion must revoke that
registration and discard the bundle. Those backend bootstrap channels are not implemented yet;
they must not fall back to plaintext, IP allowlisting, or credentials retained with workspace
storage.
