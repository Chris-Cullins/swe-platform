package sandboxclient

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"reflect"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const sandboxdPort = "50051"
const executionGenerationAnnotation = "swe.dev/execution-generation"

// Connector is the single Kubernetes resolution boundary for authenticated
// sandboxd connections. Consumers identify an Environment and capability; they
// do not inspect pods, addresses, ports, or backend-specific credentials.
// A future reverse-connected backend can satisfy the same consumer contract
// without exposing its transport identity to terminal, exec, filesystem, or
// process features.
type Connector struct {
	Reader      client.Reader
	ProcessPool *ProcessConnectionPool
}

// Execution is a connector-owned opaque identity for one exact live backend
// execution. Backend-specific observations never leave this package.
type Execution struct {
	environmentUID      types.UID
	executionGeneration int64
	lifecycleEpoch      int64
	podName             string
	podUID              types.UID
	endpoint            string
}

// TerminalExecution is retained as the terminal feature's opaque handle.
type TerminalExecution = Execution

// DialTerminal resolves the current ready Environment incarnation and returns
// terminal and health clients sharing one authenticated, pod-pinned connection.
func (c Connector) DialTerminal(ctx context.Context, fence lifecycle.ExecutionFence) (sandboxdv1.TerminalServiceClient, sandboxdv1.HealthServiceClient, TerminalExecution, func() error, error) {
	env, pod, err := c.resolvePod(ctx, fence)
	if err != nil {
		return nil, nil, TerminalExecution{}, nil, err
	}
	dialOptions, err := DialOptions(pod)
	if err != nil {
		return nil, nil, TerminalExecution{}, nil, err
	}
	conn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd, dialOptions...)
	if err != nil {
		return nil, nil, TerminalExecution{}, nil, err
	}
	execution := executionForPod(env, pod)
	// Re-read after pinning the endpoint and credentials. A lifecycle transition
	// or same-name Pod replacement racing the dial must not produce an attachment
	// whose independent activity heartbeat can outlive that execution.
	currentEnvironment, currentPod, err := c.resolvePod(ctx, fence)
	if err != nil || executionForPod(currentEnvironment, currentPod) != execution {
		_ = conn.Close()
		if err != nil {
			return nil, nil, TerminalExecution{}, nil, fmt.Errorf("environment execution changed while resolving terminal endpoint: %w", err)
		}
		return nil, nil, TerminalExecution{}, nil, fmt.Errorf("environment execution changed while resolving terminal endpoint")
	}
	return sandboxdv1.NewTerminalServiceClient(conn), sandboxdv1.NewHealthServiceClient(conn), execution, conn.Close, nil
}

// ResolveExecution non-dialingly proves that an Environment generation maps to
// one exact live backend execution and returns its opaque connector identity.
func (c Connector) ResolveExecution(ctx context.Context, fence lifecycle.ExecutionFence) (Execution, error) {
	env, pod, err := c.resolvePod(ctx, fence)
	if err != nil {
		return Execution{}, err
	}
	return executionForPod(env, pod), nil
}

// ExecutionCurrent reports whether execution is still the exact active live
// backend execution. NotFound is a stale result; a changed fence is returned
// as lifecycle.ErrExecutionFenceChanged so each consumer can either discard a
// delayed result or distinguish a retryable hold-policy transition. Other API
// read failures remain retryable by callers.
func (c Connector) ExecutionCurrent(ctx context.Context, fence lifecycle.ExecutionFence, execution Execution) (bool, error) {
	env, err := fence.Revalidate(ctx, c.Reader)
	if apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if env.UID != execution.environmentUID || env.Status.ExecutionGeneration != execution.executionGeneration ||
		env.Status.Lifecycle.Epoch != execution.lifecycleEpoch || env.Spec.Paused || env.Status.Lifecycle.Suspended ||
		!platformv1alpha1.IsEnvironmentReady(env) || env.Status.PodName != execution.podName || env.Status.Endpoints.Sandboxd == "" {
		return false, nil
	}
	var pod corev1.Pod
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: execution.podName}, &pod); apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	podGeneration, generationValid := parseExecutionGeneration(&pod)
	return exactEnvironmentOwner(&pod, env) && pod.UID == execution.podUID && pod.DeletionTimestamp.IsZero() &&
		pod.Spec.RestartPolicy == corev1.RestartPolicyNever && processPodReady(&pod) && pod.Status.PodIP != "" &&
		env.Status.Endpoints.Sandboxd == net.JoinHostPort(pod.Status.PodIP, sandboxdPort) && generationValid &&
		podGeneration == execution.executionGeneration, nil
}

func executionForPod(env *platformv1alpha1.Environment, pod *corev1.Pod) Execution {
	return Execution{
		environmentUID: env.UID, executionGeneration: env.Status.ExecutionGeneration,
		lifecycleEpoch: env.Status.Lifecycle.Epoch, podName: pod.Name, podUID: pod.UID,
		endpoint: env.Status.Endpoints.Sandboxd,
	}
}

// DialProcess resolves only the complete captured execution fence and returns
// a process-only sandboxd client.
func (c Connector) DialProcess(ctx context.Context, fence lifecycle.ExecutionFence) (sandboxdv1.ProcessServiceClient, func() error, error) {
	if c.ProcessPool != nil {
		return c.ProcessPool.acquire(ctx, fence)
	}
	env, secret, proof, err := c.resolveProcessTarget(ctx, fence)
	if err != nil {
		return nil, nil, err
	}
	dialOptions, err := processDialOptions(secret, proof.identity)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd, dialOptions...)
	if err != nil {
		return nil, nil, err
	}
	// Re-resolve the complete backend and credential proof after pinning the
	// endpoint, TLS identity, and process token. A racing Pod, Template, Secret,
	// or execution-fence replacement cannot enter either an unpooled client or
	// the reusable pool.
	_, _, currentProof, err := c.resolveProcessTarget(ctx, fence)
	if err != nil || !proof.matches(currentProof) {
		_ = conn.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("environment execution changed while resolving process endpoint: %w", err)
		}
		return nil, nil, fmt.Errorf("environment execution changed while resolving process endpoint")
	}
	return sandboxdv1.NewProcessServiceClient(conn), conn.Close, nil
}

type processConnectionProof struct {
	execution             Execution
	holdPolicyRevision    int64
	templateUID           types.UID
	templateGeneration    int64
	templateSpec          platformv1alpha1.EnvironmentTemplateSpec
	secretName            string
	secretUID             types.UID
	secretResourceVersion string
	identity              string
	secretIdentity        string
	secretPodUID          string
	certificateHash       [sha256.Size]byte
	capabilitiesHash      [sha256.Size]byte
	tokenHash             [sha256.Size]byte
}

func (c Connector) resolveProcessTarget(ctx context.Context, fence lifecycle.ExecutionFence) (*platformv1alpha1.Environment, *corev1.Secret, processConnectionProof, error) {
	env, err := fence.Revalidate(ctx, c.Reader)
	if err != nil {
		return nil, nil, processConnectionProof{}, err
	}
	return c.resolveProcessTargetForEnvironment(ctx, env)
}

func (c Connector) resolveProcessTargetForEnvironment(ctx context.Context, env *platformv1alpha1.Environment) (*platformv1alpha1.Environment, *corev1.Secret, processConnectionProof, error) {
	pod, template, err := c.resolvePodForEnvironment(ctx, env)
	if err != nil {
		return nil, nil, processConnectionProof{}, err
	}
	secretName := pod.Annotations[sandboxdauth.SecretNameAnnotation]
	if secretName == "" {
		return nil, nil, processConnectionProof{}, fmt.Errorf("sandboxd endpoint does not identify its credential")
	}
	var secret corev1.Secret
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: secretName}, &secret); err != nil {
		return nil, nil, processConnectionProof{}, err
	}
	identity := pod.Annotations[sandboxdauth.IdentityAnnotation]
	token := string(secret.Data[sandboxdauth.ProcessTokenKey])
	if identity == "" || secret.UID == "" || pod.Annotations[sandboxdauth.SecretUIDAnnotation] != string(secret.UID) || !exactEnvironmentOwner(&secret, env) || secret.Annotations[sandboxdauth.IdentityAnnotation] != identity || secret.Annotations[sandboxdauth.PodUIDAnnotation] != string(pod.UID) {
		return nil, nil, processConnectionProof{}, fmt.Errorf("sandboxd credential does not identify the current environment pod")
	}
	if token == "" {
		return nil, nil, processConnectionProof{}, fmt.Errorf("sandboxd credential has no process capability")
	}
	if !exactCapability(secret.Data[sandboxdauth.CapabilitiesKey], token, sandboxdauth.CapabilityProcess) {
		return nil, nil, processConnectionProof{}, fmt.Errorf("sandboxd credential has no exact process capability")
	}
	proof := processConnectionProof{
		execution: executionForPod(env, pod), holdPolicyRevision: lifecycle.HoldPolicyRevision(env),
		templateUID: template.UID, templateGeneration: template.Generation, templateSpec: template.Spec,
		secretName: secret.Name, secretUID: secret.UID, secretResourceVersion: secret.ResourceVersion,
		identity: identity, secretIdentity: secret.Annotations[sandboxdauth.IdentityAnnotation], secretPodUID: secret.Annotations[sandboxdauth.PodUIDAnnotation],
		certificateHash: sha256.Sum256(secret.Data[sandboxdauth.TLSCertKey]), capabilitiesHash: sha256.Sum256(secret.Data[sandboxdauth.CapabilitiesKey]), tokenHash: sha256.Sum256([]byte(token)),
	}
	return env, &secret, proof, nil
}

func processDialOptions(secret *corev1.Secret, identity string) ([]grpc.DialOption, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data[sandboxdauth.TLSCertKey]) {
		return nil, fmt.Errorf("sandboxd credential has no valid trust certificate")
	}
	token := string(secret.Data[sandboxdauth.ProcessTokenKey])
	if token == "" {
		return nil, fmt.Errorf("sandboxd credential has no process capability")
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: identity, MinVersion: tls.VersionTLS13})),
		grpc.WithPerRPCCredentials(sandboxdauth.BearerCredentials{Token: token}),
	}, nil
}

func (p processConnectionProof) matches(other processConnectionProof) bool {
	return p.execution == other.execution && p.holdPolicyRevision == other.holdPolicyRevision &&
		p.templateUID == other.templateUID && p.templateGeneration == other.templateGeneration &&
		reflect.DeepEqual(p.templateSpec, other.templateSpec) && p.secretName == other.secretName && p.secretUID == other.secretUID &&
		p.secretResourceVersion == other.secretResourceVersion && p.identity == other.identity && p.secretIdentity == other.secretIdentity &&
		p.secretPodUID == other.secretPodUID && p.certificateHash == other.certificateHash &&
		p.capabilitiesHash == other.capabilitiesHash && p.tokenHash == other.tokenHash
}

func processEnvironmentReachable(env *platformv1alpha1.Environment) bool {
	return !env.Spec.Paused && !env.Status.Lifecycle.Suspended &&
		(env.Spec.Lifecycle.Hold == nil || !env.Spec.Lifecycle.Hold.Enabled) && platformv1alpha1.IsEnvironmentReady(env) &&
		env.Status.PodName != "" && env.Status.Endpoints.Sandboxd != ""
}

func (c Connector) resolvePod(ctx context.Context, fence lifecycle.ExecutionFence) (*platformv1alpha1.Environment, *corev1.Pod, error) {
	env, err := fence.Revalidate(ctx, c.Reader)
	if err != nil {
		return nil, nil, err
	}
	pod, _, err := c.resolvePodForEnvironment(ctx, env)
	return env, pod, err
}

func (c Connector) resolvePodForEnvironment(ctx context.Context, env *platformv1alpha1.Environment) (*corev1.Pod, *platformv1alpha1.EnvironmentTemplate, error) {
	if !processEnvironmentReachable(env) {
		return nil, nil, fmt.Errorf("environment is not the current reachable incarnation")
	}
	var template platformv1alpha1.EnvironmentTemplate
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.TemplateRef}, &template); err != nil {
		return nil, nil, fmt.Errorf("get environment template: %w", err)
	}
	if backend := platformv1alpha1.EffectiveEnvironmentBackend(env, &template); backend != platformv1alpha1.EnvironmentBackendPod {
		return nil, nil, fmt.Errorf("environment backend %q is not supported", backend)
	}
	var pod corev1.Pod
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Status.PodName}, &pod); err != nil {
		return nil, nil, err
	}
	wantEndpoint := net.JoinHostPort(pod.Status.PodIP, sandboxdPort)
	if !exactEnvironmentOwner(&pod, env) {
		return nil, nil, fmt.Errorf("environment pod is not owned by the current environment")
	}
	if pod.UID == "" || !pod.DeletionTimestamp.IsZero() || !processPodReady(&pod) || pod.Status.PodIP == "" || env.Status.Endpoints.Sandboxd != wantEndpoint {
		return nil, nil, fmt.Errorf("sandboxd endpoint does not identify the current environment pod")
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return nil, nil, fmt.Errorf("environment pod restart policy does not identify a single execution")
	}
	podGeneration, generationValid := parseExecutionGeneration(&pod)
	if !generationValid || podGeneration != env.Status.ExecutionGeneration {
		return nil, nil, fmt.Errorf("environment pod does not identify the current execution generation")
	}
	return &pod, &template, nil
}

func parseExecutionGeneration(pod *corev1.Pod) (int64, bool) {
	value := pod.Annotations[executionGenerationAnnotation]
	generation, err := strconv.ParseInt(value, 10, 64)
	return generation, err == nil && generation > 0 && strconv.FormatInt(generation, 10) == value
}

func processPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func exactEnvironmentOwner(object metav1.Object, env *platformv1alpha1.Environment) bool {
	owner := metav1.GetControllerOf(object)
	return owner != nil && owner.APIVersion == platformv1alpha1.GroupVersion.String() && owner.Kind == "Environment" && owner.Name == env.Name && owner.UID == env.UID
}
