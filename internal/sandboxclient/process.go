package sandboxclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const sandboxdPort = "50051"

// Connector is the single Kubernetes resolution boundary for authenticated
// sandboxd connections. Consumers identify an Environment and capability; they
// do not inspect pods, addresses, ports, or backend-specific credentials.
// A future reverse-connected backend can satisfy the same consumer contract
// without exposing its transport identity to terminal, exec, filesystem, or
// process features.
type Connector struct {
	Reader client.Reader
}

// DialTerminal resolves the current ready Environment incarnation and returns
// terminal and health clients sharing one authenticated, pod-pinned connection.
func (c Connector) DialTerminal(ctx context.Context, namespace, environment string, environmentUID types.UID) (sandboxdv1.TerminalServiceClient, sandboxdv1.HealthServiceClient, func() error, error) {
	env, pod, err := c.resolvePod(ctx, namespace, environment, environmentUID)
	if err != nil {
		return nil, nil, nil, err
	}
	dialOptions, err := DialOptions(pod)
	if err != nil {
		return nil, nil, nil, err
	}
	conn, err := grpc.NewClient(env.Status.Endpoints.Sandboxd, dialOptions...)
	if err != nil {
		return nil, nil, nil, err
	}
	return sandboxdv1.NewTerminalServiceClient(conn), sandboxdv1.NewHealthServiceClient(conn), conn.Close, nil
}

// DialProcess resolves the exact Environment UID immediately before dialing
// and returns a process-only sandboxd client.
func (c Connector) DialProcess(ctx context.Context, namespace, environment string, environmentUID types.UID) (sandboxdv1.ProcessServiceClient, func() error, error) {
	env, pod, err := c.resolvePod(ctx, namespace, environment, environmentUID)
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
	return sandboxdv1.NewProcessServiceClient(conn), conn.Close, nil
}

func (c Connector) resolvePod(ctx context.Context, namespace, environment string, environmentUID types.UID) (*platformv1alpha1.Environment, *corev1.Pod, error) {
	var env platformv1alpha1.Environment
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: environment}, &env); err != nil {
		return nil, nil, err
	}
	if environmentUID != "" && env.UID != environmentUID || !platformv1alpha1.IsEnvironmentReady(&env) || env.Status.PodName == "" || env.Status.Endpoints.Sandboxd == "" {
		return nil, nil, fmt.Errorf("environment is not the current reachable incarnation")
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
	return &env, &pod, nil
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
