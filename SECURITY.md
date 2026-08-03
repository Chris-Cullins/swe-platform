# Security model

## GitHub App repository credentials

When explicitly selected by immutable Run intent, the operator exchanges its
administrator-owned GitHub App private key for a short-lived installation token.
The recipient is only the canonical `github.com` repository in frozen provisioning
data; scope is exactly that repository with `contents:write`. App keys are mounted
only in the operator. Tokens are intended only for clone and Run-owned agent
processes, never status, transcripts, sandboxd, hooks, command arguments,
repository URLs, or persistent git config. Cleanup fences the Environment before
revocation. GitHub's fixed one-hour maximum expiry bounds a crash between issuance
and persistence, where no token exists locally to revoke.

Repository initialization is split into ordered clone and project-hook init
containers. After uncached exact Run, Environment, frozen repository, and lease
validation, kubelet injects the token from the deterministic Run Secret only into
the clone container. That shell derives a temporary Git extra header for the
`git clone` process and clears the token and header variables; the hook and main
containers receive no reference to the repository credential Secret.

Only `https://github.com/<owner>/<repo>[.git]` is accepted. SSH, userinfo,
ports, query/fragment components, redirects, enterprise hosts, and alternate API
hosts are rejected.

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
`terminal`, `exec`, `process`, `filesystem`, `service-observation`, and `portal`). sandboxd interceptors
authorize both unary and streaming RPCs before handlers run. The terminal credential grants
`health` and `terminal`; separate operator/control-plane-held credentials grant only
`process`, `service-observation`, or `portal`. When portals are enabled, RBAC grants the control
plane Secret `get` (never list/watch) in onboarded namespaces; connector code reads only the exact
Environment-owned Secret named by the current pod. These raw private tokens are never projected
into the workload. Portal transport accepts bounded byte frames only to one validated logical
loopback port, refuses sandboxd's own control port, bounds concurrent tunnels, and applies a
dial timeout. Portal URLs and authorization are gateway authority; advisory service status
is never route or authorization proof.
`filesystem` authorizes the declaration controller's bounded workspace-only read of
`.swe/services.yaml`; `process` separately authorizes supervised-process reconciliation. Their
raw tokens are never mounted or injected into services. Connector calls prove the current
Environment UID/execution, backend, Pod, endpoint, TLS identity, Secret, and exact capability
before and after each operation.
`service-observation` authorizes only bounded stateless TCP-connect probes against sandboxd's
logical loopback. The operator issues a distinct observation-only token and its connector uses
it; the control plane's Secret read authority is rendered only when portals are enabled and is
used only inside sandboxclient for portal transport. The mounted authorization file contains SHA-256
verifiers, not raw tokens, and the raw process, observation, and portal tokens exist only in the
Environment-owned Secret: none is mounted, injected, or published in pod annotations.
Possession of
one environment's token grants nothing in another environment.

The Environment-owned Secret contains the private key, authorization configuration, and raw
process, observation, and portal tokens. The public trust certificate and terminal token are published atomically as pod
annotations, which makes pod `get` plus `pods/portforward` the Kubernetes authorization boundary
for CLI attachment without granting callers access to arbitrary namespace Secrets. The control
plane has pod `get` and, when portals are enabled, namespaced Secret `get`; sandboxclient resolves
only the current pod's exact credential. The operator validates the exact Run, Environment,
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

Default-deny egress and the per-project egress proxy are not implemented. The
`Project.spec.egressAllowlist` field is reserved for that future contract; the operator rejects
non-empty values and fences any existing Environment execution rather than running with an
unenforced allowlist. Empty or omitted allowlists do not restrict environment egress, which is
subject to the cluster's network configuration.

When an EnvironmentTemplate explicitly names a RuntimeClass, the operator verifies that the
cluster-scoped RuntimeClass object exists before execution and fences exact-owned execution if
it is removed or replaced under the same name. Environment pods pin the RuntimeClass UID; the
first operator upgrade containing this check therefore replaces legacy pods using named
RuntimeClasses while retaining their workspace PVCs. This is a fail-closed prerequisite check,
not runtime capability discovery: it does not verify the configured handler, eligible-node
installation, or the isolation properties of that runtime. The chart does not own provider
RuntimeClasses; cluster operators must install and test them separately. An omitted RuntimeClass
intentionally uses the cluster default runtime.

### Credential lifecycle

1. **Bootstrap:** before creating a pod, the operator generates a new certificate, private key,
   random identity, and random capability tokens. It writes them to an Environment-owned
   Kubernetes Secret. The pod projects only the TLS keypair and hashed capability configuration
   read-only at `/var/run/swe-platform/sandboxd`; the distinct raw process and service-observation
   tokens remain available only through exact-name operator Secret reads and are never mounted,
   annotated, or injected into a process. sandboxd fails closed if any TLS or capability
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

## Project namespace authority

Platform services and the immutable-UID `Installation` identity live in a system namespace;
each Project has a dedicated namespace. In scoped mode, names and labels are discovery data,
not authority. The boundary is the live Namespace object's immutable UID plus exact annotations
for the Installation namespace/name/UID, sole Project name/UID, and lifecycle. Onboarding is an
administrator operation: scoped workload RoleBindings do not grant the operator or control
plane permission to mutate Namespace claims. Missing, partial, stale, conflicting, deleting, or
multiple Project identities fail closed.

The scoped operator cache is restart-bound to the explicit `tenancy.namespaces` list. Startup,
reconcile entry, and every mutation revalidate the Installation and exact Namespace/Project
claim through uncached reads. The control plane first performs its existing TokenReview and
SubjectAccessReview checks, then independently requires allowlist membership and an uncached
active-claim proof. A second release has different Installation and cluster-role identities and
an Installation-UID-derived leader lease; it cannot satisfy or receive another release's
namespaced workload authority. `trusted-admin` is a separate explicit cluster-wide mode; absent
or invalid mode configuration never enables it. Its cache and RBAC are cluster-wide, but exact
Installation/Namespace/Project claims remain mandatory at mutation boundaries.

Onboarding installs an ingress-only default-deny NetworkPolicy and a tokenless Environment
ServiceAccount. The existing Environment-specific policy adds only the sandboxd ingress needed
from the installation's system components. These policies do not restrict egress. Offboarding
fences before drain and retains the Namespace, workspace PVCs, credential profiles and their
owned Secrets, and transcript data. Suspending an Environment still revokes its ephemeral
per-pod sandboxd credential Secret as described above. Destructive Project purge is not
implemented.

## Portal gateway

Portal locators are random host-routing keys, not credentials or disclosure authority. There is
no anonymous, public, bootstrap-token, magic-link, URL credential, or shared Domain-cookie path.
Every request performs TokenReview and exact SARs for `environments/portal` on the Environment
and `environmentservices/portal` on `<environment>.<service>`, then requires exact current Run
`get` or Project `get` authority. Released claimed Runs immediately lose Run-derived authority.

Bearer exchange occurs only at the exact portal host's `/api/v1/session`. The purpose-scoped
server-side session cookie is random, HttpOnly, SameSite=Strict, host-only, and Secure outside
explicit HTTP development. Exchange requires an explicit bearer over the configured secure
origin; every cookie use repeats TokenReview/SAR, and cookie-authenticated mutations and
WebSocket upgrades enforce exact Origin/CSRF policy. Unknown, unauthorized, held, stale, and
unavailable routes share one 404.

Requests revalidate Environment UID, declaration instance/revision, route generation,
association, lifecycle, execution fence, pod, Secret, TLS identity, and exact portal capability.
Idle may queue a bounded wake; hold and Requested suspension do not. Remove/re-add tombstones the
old locator. Authorization, platform session cookies, and hop-by-hop headers are stripped before
forwarding; response hop-by-hop headers are filtered. WebSocket upgrades use the same checks.
Environment NetworkPolicy still admits only sandboxd port 50051 from installation-labeled
operator/control-plane pods; Ingress reaches the control-plane Service, never pods directly.

Production requires wildcard DNS and HTTPS. The chart's optional Ingress owns only `*.suffix`
and references an administrator-owned wildcard TLS Secret; it creates no certificate/Secret and
claims no ordinary API ingress. Operators own DNS, termination, and hairpin routing. A forwarded
Service plus exact Host exercises gateway hairpin but does not provide in-Environment wildcard
DNS. Repository services receive `PORT` and the exact gateway-discovered `PUBLIC_URL`, but no
portal credential or projected service-account token. The operator uses a rotating projected
token and only exact `get` authority on the two virtual portal resources for authenticated
discovery; it never constructs a URL. Disabled discovery returns a gateway-owned durable denial
generation that tombstones routes and stops the complete managed set; unavailable discovery
preserves declarations but prevents launch. Process reconciliation is fenced by Environment UID and the monotonic
Environment-intent/gateway-route revision pair;
pause/resume creates a new daemon epoch, and pause, removal, or replacement revokes the old
process and URL. Portal UI work (#95) remains out of scope.

## Browser sessions

Browser session storage is an explicit `memory` or `postgres` choice. Production k3s, GKE, and
EKS presets use PostgreSQL; default, kind, and Argo development configurations use bounded,
process-local memory. PostgreSQL uses the control plane's shared `pgxpool` and ordered migrations
(version 003 creates session storage). Startup fails closed on database connection, migration,
configuration, keyring, or missing-live-key errors, with no memory fallback.

The administrator owns a strictly validated version-1 JSON keyring out of band and mounts it
read-only; the chart neither creates it nor grants Secret-read RBAC. Each 32-byte master key is
expanded with HKDF into independent HMAC selector and AES-256-GCM keys. The cookie is
`s1.<key-id>.<random>`; the database stores only a keyed selector, encrypted bearer, authenticated
metadata, and timestamps—never the cookie or raw bearer. Stable microsecond timestamp encoding
is included in AES-GCM associated data. This limits database-only disclosure, but compromise of
the process or keyring, a Kubernetes administrator, or combined database and keyring backups can
recover credentials.

Sessions expire absolutely after one hour. Creation takes a global PostgreSQL advisory
transaction lock, purges expired rows, and rejects a new exchange once 10,000 live rows remain;
it never evicts a live session. Logout, expiry, and definitive TokenReview unauthenticated or
audience-mismatch results revoke durably and idempotently. TokenReview transport or
`status.error` failures return 503 and retain the session for retry; SAR denial returns 403 and
also retains it. Every use repeats TokenReview, then exact SAR with the reviewed username, UID,
groups, and extras, so Kubernetes credential expiry/revocation and RBAC remain authoritative.
Cookie security and same-origin mutation checks remain mandatory.

For rotation, add a uniquely identified key, select it as active, and roll the control plane.
Retain old keys for at least the one-hour TTL plus operational and rollback margin; retire them
only after no live or rollback-visible session needs them, then roll again. Backups and rollback
keyrings must preserve required keys. The deployment remains one replica with `Recreate`:
PostgreSQL sessions survive replacement, but open SSE/WebSocket connections must reconnect and
this is not an HA claim.

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

The Pi adapter never accepts a credential profile or injects provider credentials. The
controller rejects such Runs before Environment allocation and before reading the selected
profile or Secret, and the stock image contains no Pi authentication. Pi still loads ambient
authentication, configuration, and extensions from its controlled Environment. State introduced
outside the profile path by a custom image, attached user, setup/resume or repository code, or
the process environment is unsupported and is not confined to the managed Pi process.

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
