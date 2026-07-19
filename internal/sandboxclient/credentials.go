// Package sandboxclient builds authenticated sandboxd client connections.
package sandboxclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"

	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

// DialOptions pins the current pod incarnation's TLS identity and attaches its
// terminal token, which grants only terminal and health capabilities. The
// client bundle is published atomically on the Pod; callers never receive the
// server's private credential Secret.
func DialOptions(pod *corev1.Pod) ([]grpc.DialOption, error) {
	identity := pod.Annotations[sandboxdauth.IdentityAnnotation]
	if identity == "" {
		return nil, fmt.Errorf("environment pod has no sandboxd identity")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(pod.Annotations[sandboxdauth.TrustAnnotation])) {
		return nil, fmt.Errorf("environment pod has no valid sandboxd trust bundle")
	}
	token := pod.Annotations[sandboxdauth.TokenAnnotation]
	if token == "" {
		return nil, fmt.Errorf("environment pod has no sandboxd terminal capability")
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:    roots,
			ServerName: identity,
			MinVersion: tls.VersionTLS13,
		})),
		grpc.WithPerRPCCredentials(sandboxdauth.BearerCredentials{Token: token}),
	}, nil
}
