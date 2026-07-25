package sandboxclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
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
	Reader client.Reader
}

// Execution is a connector-owned opaque identity for one exact live backend
// execution. Backend-specific observations never leave this package.
type Execution struct {
	environmentUID      types.UID
	executionGeneration int64
	lifecycleEpoch      int64
	podName             string
	podUID              types.UID
}

// TerminalExecution is retained as the terminal feature's opaque handle.
type TerminalExecution = Execution

// DialTerminal resolves the current ready Environment incarnation and returns
// terminal and health clients sharing one authenticated, pod-pinned connection.
func (c Connector) DialTerminal(ctx context.Context, namespace, environment string, environmentUID types.UID) (sandboxdv1.TerminalServiceClient, sandboxdv1.HealthServiceClient, TerminalExecution, func() error, error) {
	env, pod, err := c.resolvePod(ctx, namespace, environment, environmentUID, nil, nil)
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
	currentEnvironment, currentPod, err := c.resolvePod(ctx, namespace, environment, environmentUID, &execution.executionGeneration, &execution.lifecycleEpoch)
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
func (c Connector) ResolveExecution(ctx context.Context, namespace, environment string, environmentUID types.UID, executionGeneration, lifecycleEpoch int64) (Execution, error) {
	env, pod, err := c.resolvePod(ctx, namespace, environment, environmentUID, &executionGeneration, &lifecycleEpoch)
	if err != nil {
		return Execution{}, err
	}
	return executionForPod(env, pod), nil
}

// ExecutionCurrent reports whether execution is still the exact active live
// backend execution. NotFound is a stale result rather than an operational
// error; all other API read failures remain retryable by callers.
func (c Connector) ExecutionCurrent(ctx context.Context, namespace, environment string, execution Execution) (bool, error) {
	var env platformv1alpha1.Environment
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: environment}, &env); apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if env.UID != execution.environmentUID || env.Status.ExecutionGeneration != execution.executionGeneration ||
		env.Status.Lifecycle.Epoch != execution.lifecycleEpoch || env.Spec.Paused || env.Status.Lifecycle.Suspended ||
		!platformv1alpha1.IsEnvironmentReady(&env) || env.Status.PodName != execution.podName || env.Status.Endpoints.Sandboxd == "" {
		return false, nil
	}
	var pod corev1.Pod
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: execution.podName}, &pod); apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	podGeneration, generationValid := parseExecutionGeneration(&pod)
	return exactEnvironmentOwner(&pod, &env) && pod.UID == execution.podUID && pod.DeletionTimestamp.IsZero() &&
		pod.Spec.RestartPolicy == corev1.RestartPolicyNever && processPodReady(&pod) && pod.Status.PodIP != "" &&
		env.Status.Endpoints.Sandboxd == net.JoinHostPort(pod.Status.PodIP, sandboxdPort) && generationValid &&
		podGeneration == execution.executionGeneration, nil
}

func executionForPod(env *platformv1alpha1.Environment, pod *corev1.Pod) Execution {
	return Execution{
		environmentUID: env.UID, executionGeneration: env.Status.ExecutionGeneration,
		lifecycleEpoch: env.Status.Lifecycle.Epoch, podName: pod.Name, podUID: pod.UID,
	}
}

// DialProcess resolves the exact Environment UID immediately before dialing
// and returns a process-only sandboxd client.
func (c Connector) DialProcess(ctx context.Context, namespace, environment string, environmentUID types.UID) (sandboxdv1.ProcessServiceClient, func() error, error) {
	return c.dialProcess(ctx, namespace, environment, environmentUID, nil, nil)
}

// DialProcessForEpoch resolves a process client only when the current
// Environment lifecycle epoch matches the caller's accepted execution epoch.
func (c Connector) DialProcessForEpoch(ctx context.Context, namespace, environment string, environmentUID types.UID, expectedEpoch int64) (sandboxdv1.ProcessServiceClient, func() error, error) {
	return c.dialProcess(ctx, namespace, environment, environmentUID, nil, &expectedEpoch)
}

// DialProcessForExecution resolves a process client only for the exact accepted
// backend execution and lifecycle epoch.
func (c Connector) DialProcessForExecution(ctx context.Context, namespace, environment string, environmentUID types.UID, expectedExecutionGeneration, expectedEpoch int64) (sandboxdv1.ProcessServiceClient, func() error, error) {
	return c.dialProcess(ctx, namespace, environment, environmentUID, &expectedExecutionGeneration, &expectedEpoch)
}

func (c Connector) dialProcess(ctx context.Context, namespace, environment string, environmentUID types.UID, expectedExecutionGeneration, expectedEpoch *int64) (sandboxdv1.ProcessServiceClient, func() error, error) {
	env, pod, err := c.resolvePod(ctx, namespace, environment, environmentUID, expectedExecutionGeneration, expectedEpoch)
	if err != nil {
		return nil, nil, err
	}

	secretName := pod.Annotations[sandboxdauth.SecretNameAnnotation]
	if secretName == "" {
		return nil, nil, fmt.Errorf("sandboxd endpoint does not identify its credential")
	}
	var secret corev1.Secret
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, &secret); err != nil {
		return nil, nil, err
	}
	identity := pod.Annotations[sandboxdauth.IdentityAnnotation]
	if identity == "" || secret.UID == "" || pod.Annotations[sandboxdauth.SecretUIDAnnotation] != string(secret.UID) || !exactEnvironmentOwner(&secret, env) || secret.Annotations[sandboxdauth.IdentityAnnotation] != identity || secret.Annotations[sandboxdauth.PodUIDAnnotation] != string(pod.UID) {
		return nil, nil, fmt.Errorf("sandboxd credential does not identify the current environment pod")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data[sandboxdauth.TLSCertKey]) {
		return nil, nil, fmt.Errorf("sandboxd credential has no valid trust certificate")
	}
	token := string(secret.Data[sandboxdauth.ProcessTokenKey])
	if token == "" {
		return nil, nil, fmt.Errorf("sandboxd credential has no process capability")
	}
	conn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: identity, MinVersion: tls.VersionTLS13})),
		grpc.WithPerRPCCredentials(sandboxdauth.BearerCredentials{Token: token}))
	if err != nil {
		return nil, nil, err
	}
	if expectedExecutionGeneration != nil || expectedEpoch != nil {
		// Re-read after pinning the endpoint, TLS identity, and token. If an
		// epoch transition raced resolution, reject this client; if transition
		// starts after this check, the old credentials cannot authenticate to
		// the replacement pod.
		var current platformv1alpha1.Environment
		if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: environment}, &current); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		if current.UID != environmentUID || expectedExecutionGeneration != nil && current.Status.ExecutionGeneration != *expectedExecutionGeneration ||
			expectedEpoch != nil && current.Status.Lifecycle.Epoch != *expectedEpoch ||
			current.Spec.Paused || current.Status.Lifecycle.Suspended || !platformv1alpha1.IsEnvironmentReady(&current) ||
			current.Status.PodName != env.Status.PodName || current.Status.Endpoints.Sandboxd != env.Status.Endpoints.Sandboxd {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("environment execution changed while resolving process endpoint")
		}
	}
	return sandboxdv1.NewProcessServiceClient(conn), conn.Close, nil
}

func (c Connector) resolvePod(ctx context.Context, namespace, environment string, environmentUID types.UID, expectedExecutionGeneration, expectedEpoch *int64) (*platformv1alpha1.Environment, *corev1.Pod, error) {
	var env platformv1alpha1.Environment
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: environment}, &env); err != nil {
		return nil, nil, err
	}
	if environmentUID != "" && env.UID != environmentUID || env.Status.ExecutionGeneration < 1 || env.Spec.Paused || env.Status.Lifecycle.Suspended || !platformv1alpha1.IsEnvironmentReady(&env) || env.Status.PodName == "" || env.Status.Endpoints.Sandboxd == "" {
		return nil, nil, fmt.Errorf("environment is not the current reachable incarnation")
	}
	if expectedExecutionGeneration != nil && env.Status.ExecutionGeneration != *expectedExecutionGeneration {
		return nil, nil, fmt.Errorf("environment execution generation changed: expected %d, got %d", *expectedExecutionGeneration, env.Status.ExecutionGeneration)
	}
	if expectedEpoch != nil && env.Status.Lifecycle.Epoch != *expectedEpoch {
		return nil, nil, fmt.Errorf("environment lifecycle epoch changed: expected %d, got %d", *expectedEpoch, env.Status.Lifecycle.Epoch)
	}
	var template platformv1alpha1.EnvironmentTemplate
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: env.Spec.TemplateRef}, &template); err != nil {
		return nil, nil, fmt.Errorf("get environment template: %w", err)
	}
	if backend := platformv1alpha1.EffectiveEnvironmentBackend(&env, &template); backend != platformv1alpha1.EnvironmentBackendPod {
		return nil, nil, fmt.Errorf("environment backend %q is not supported", backend)
	}
	var pod corev1.Pod
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: env.Status.PodName}, &pod); err != nil {
		return nil, nil, err
	}
	wantEndpoint := net.JoinHostPort(pod.Status.PodIP, sandboxdPort)
	if !exactEnvironmentOwner(&pod, &env) {
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
	return &env, &pod, nil
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
