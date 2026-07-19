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

// DialProcess validates all current Kubernetes identity fences immediately
// before dialing and returns a process-only sandboxd client.
func DialProcess(ctx context.Context, reader client.Reader, namespace, environment string, environmentUID types.UID) (sandboxdv1.ProcessServiceClient, func() error, error) {
	var env platformv1alpha1.Environment
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: environment}, &env); err != nil {
		return nil, nil, err
	}
	if env.UID != environmentUID || !platformv1alpha1.IsEnvironmentReady(&env) || env.Status.PodName == "" || env.Status.Endpoints.Sandboxd == "" {
		return nil, nil, fmt.Errorf("environment is not the current reachable incarnation")
	}
	var pod corev1.Pod
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: env.Status.PodName}, &pod); err != nil {
		return nil, nil, err
	}
	wantEndpoint := net.JoinHostPort(pod.Status.PodIP, "50051")
	if pod.UID == "" || !pod.DeletionTimestamp.IsZero() || !processPodReady(&pod) || !exactEnvironmentOwner(&pod, &env) || pod.Status.PodIP == "" || env.Status.Endpoints.Sandboxd != wantEndpoint {
		return nil, nil, fmt.Errorf("sandboxd endpoint does not identify the current environment pod")
	}
	secretName := pod.Annotations[sandboxdauth.SecretNameAnnotation]
	if secretName == "" {
		return nil, nil, fmt.Errorf("sandboxd endpoint does not identify its credential")
	}
	var secret corev1.Secret
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, &secret); err != nil {
		return nil, nil, err
	}
	identity := pod.Annotations[sandboxdauth.IdentityAnnotation]
	if identity == "" || secret.UID == "" || pod.Annotations[sandboxdauth.SecretUIDAnnotation] != string(secret.UID) || !exactEnvironmentOwner(&secret, &env) || secret.Annotations[sandboxdauth.IdentityAnnotation] != identity || secret.Annotations[sandboxdauth.PodUIDAnnotation] != string(pod.UID) {
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
