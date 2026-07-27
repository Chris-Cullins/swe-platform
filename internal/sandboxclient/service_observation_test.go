package sandboxclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

type orderedObservationServer struct {
	sandboxdv1.UnimplementedServiceObservationServiceServer
	response func(*sandboxdv1.ObserveServicesRequest) (*sandboxdv1.ObserveServicesResponse, error)
}

func (s *orderedObservationServer) Observe(_ context.Context, request *sandboxdv1.ObserveServicesRequest) (*sandboxdv1.ObserveServicesResponse, error) {
	if s.response != nil {
		return s.response(request)
	}
	response := &sandboxdv1.ObserveServicesResponse{Observations: make([]*sandboxdv1.ServiceProbeObservation, len(request.Probes))}
	for i, probe := range request.Probes {
		response.Observations[i] = &sandboxdv1.ServiceProbeObservation{Id: probe.Id, Outcome: sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_CONNECTED, Reason: sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_ACCEPTED}
	}
	return response, nil
}

func TestObserveServicesDuplicatePortsRemainNameCorrelatedAndRequestOrdered(t *testing.T) {
	const identity = "observation.sandboxd.swe.dev"
	certificate, key := observationCertificate(t, identity)
	listener, err := net.Listen("tcp", "127.0.0.1:50051")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{certificate.Raw}, PrivateKey: key}}, MinVersion: tls.VersionTLS13})))
	observer := &orderedObservationServer{}
	sandboxdv1.RegisterServiceObservationServiceServer(server, observer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid", Generation: 4}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "default", Services: []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "web", Revision: 1, TargetPort: 8080}, {Name: "admin", Revision: 2, TargetPort: 8080}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, ObservedGeneration: 4, ExecutionGeneration: 3, PodName: "pod", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: listener.Addr().String()}, Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 4}}}}
	owner := []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: processTestPtr(true)}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", UID: "pod-uid", OwnerReferences: owner, Annotations: map[string]string{executionGenerationAnnotation: "3", sandboxdauth.IdentityAnnotation: identity, sandboxdauth.SecretNameAnnotation: "credential", sandboxdauth.SecretUIDAnnotation: "secret-uid"}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever}, Status: corev1.PodStatus{PodIP: "127.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	capabilities, err := json.Marshal(sandboxdauth.Config{Grants: []sandboxdauth.Grant{{TokenHash: sandboxdauth.TokenVerifier("observation-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityServiceObservation}}}})
	if err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: "ns", UID: "secret-uid", OwnerReferences: owner, Annotations: map[string]string{sandboxdauth.IdentityAnnotation: identity, sandboxdauth.PodUIDAnnotation: "pod-uid"}}, Data: map[string][]byte{sandboxdauth.TLSCertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), sandboxdauth.CapabilitiesKey: capabilities, sandboxdauth.ServiceObservationTokenKey: []byte("observation-token")}}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "ns", UID: "template-uid", Generation: 1}}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = platformv1alpha1.AddToScheme(scheme)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(env, pod, secret, template).Build()
	result, err := (Connector{Reader: reader}).ObserveServices(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env))
	if err != nil || result.Failed || len(result.Probes) != 2 || result.Probes[0].Name != "web" || result.Probes[1].Name != "admin" {
		t.Fatalf("ordered duplicate-port result = %#v, error = %v", result, err)
	}
	if current, err := (Connector{Reader: reader}).ServiceObservationCurrent(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env), result); err != nil || !current {
		t.Fatalf("fresh observation proof = %t, %v", current, err)
	}

	newReader := func() client.Client {
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(env.DeepCopy(), pod.DeepCopy(), secret.DeepCopy(), template.DeepCopy()).Build()
	}
	for name, mutate := range map[string]func(*platformv1alpha1.Environment){
		"metadata generation":  func(current *platformv1alpha1.Environment) { current.Generation++ },
		"declaration revision": func(current *platformv1alpha1.Environment) { current.Spec.Services[0].Revision++ },
		"declaration protocol": func(current *platformv1alpha1.Environment) {
			current.Spec.Services[0].Protocol = platformv1alpha1.EnvironmentServiceProtocolHTTP
		},
		"declaration target": func(current *platformv1alpha1.Environment) { current.Spec.Services[0].TargetPort++ },
		"declaration visibility": func(current *platformv1alpha1.Environment) {
			current.Spec.Services[0].Visibility = platformv1alpha1.EnvironmentServiceVisibilityProject
		},
		"declaration readiness": func(current *platformv1alpha1.Environment) {
			current.Spec.Services[0].Readiness = platformv1alpha1.EnvironmentServiceReadinessTCPConnect
		},
		"declaration order": func(current *platformv1alpha1.Environment) {
			current.Spec.Services[0], current.Spec.Services[1] = current.Spec.Services[1], current.Spec.Services[0]
		},
		"execution generation": func(current *platformv1alpha1.Environment) { current.Status.ExecutionGeneration++ },
		"lifecycle epoch":      func(current *platformv1alpha1.Environment) { current.Status.Lifecycle.Epoch++ },
		"hold revision": func(current *platformv1alpha1.Environment) {
			current.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 1}
		},
		"endpoint": func(current *platformv1alpha1.Environment) { current.Status.Endpoints.Sandboxd = "127.0.0.2:50051" },
	} {
		t.Run("post RPC rejects "+name, func(t *testing.T) {
			racing := &environmentChangingReader{Reader: newReader(), mutate: mutate}
			got, err := (Connector{Reader: racing}).ObserveServices(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env))
			if err == nil || got.Failed || len(got.Probes) != 0 || racing.environmentGets < 2 {
				t.Fatalf("result = %#v, error = %v, environment reads = %d", got, err, racing.environmentGets)
			}
		})
	}

	for name, mutate := range map[string]func(*corev1.Pod){
		"pod UID":        func(current *corev1.Pod) { current.UID = "replacement-pod" },
		"restart policy": func(current *corev1.Pod) { current.Spec.RestartPolicy = corev1.RestartPolicyAlways },
		"pod generation": func(current *corev1.Pod) { current.Annotations[executionGenerationAnnotation] = "4" },
		"TLS identity": func(current *corev1.Pod) {
			current.Annotations[sandboxdauth.IdentityAnnotation] = "replacement.sandboxd.swe.dev"
		},
		"credential UID": func(current *corev1.Pod) {
			current.Annotations[sandboxdauth.SecretUIDAnnotation] = "replacement-secret"
		},
		"credential lookup": func(current *corev1.Pod) {
			current.Annotations[sandboxdauth.SecretNameAnnotation] = "replacement-secret"
		},
	} {
		t.Run("post RPC rejects "+name, func(t *testing.T) {
			racing := &podChangingReader{Reader: newReader(), mutate: mutate}
			got, err := (Connector{Reader: racing}).ObserveServices(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env))
			if err == nil || got.Failed || len(got.Probes) != 0 || racing.podGets < 2 {
				t.Fatalf("result = %#v, error = %v, pod reads = %d", got, err, racing.podGets)
			}
		})
	}

	t.Run("final proof rejects credential rotation", func(t *testing.T) {
		currentReader := newReader()
		observed, err := (Connector{Reader: currentReader}).ObserveServices(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env))
		if err != nil {
			t.Fatal(err)
		}
		var currentSecret corev1.Secret
		if err := currentReader.Get(context.Background(), client.ObjectKeyFromObject(secret), &currentSecret); err != nil {
			t.Fatal(err)
		}
		currentSecret.Data[sandboxdauth.TLSCertKey] = []byte("replacement certificate")
		if err := currentReader.Update(context.Background(), &currentSecret); err != nil {
			t.Fatal(err)
		}
		current, err := (Connector{Reader: currentReader}).ServiceObservationCurrent(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env), observed)
		if err != nil || current {
			t.Fatalf("rotated credential proof = %t, %v", current, err)
		}
	})

	t.Run("final proof rejects template mutation", func(t *testing.T) {
		currentReader := newReader()
		observed, err := (Connector{Reader: currentReader}).ObserveServices(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env))
		if err != nil {
			t.Fatal(err)
		}
		var currentTemplate platformv1alpha1.EnvironmentTemplate
		if err := currentReader.Get(context.Background(), client.ObjectKeyFromObject(template), &currentTemplate); err != nil {
			t.Fatal(err)
		}
		currentTemplate.Spec.Image = "replacement"
		currentTemplate.Generation++
		if err := currentReader.Update(context.Background(), &currentTemplate); err != nil {
			t.Fatal(err)
		}
		current, err := (Connector{Reader: currentReader}).ServiceObservationCurrent(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env), observed)
		if err != nil || current {
			t.Fatalf("changed template proof = %t, %v", current, err)
		}
	})

	for name, response := range map[string]func(*sandboxdv1.ObserveServicesRequest) (*sandboxdv1.ObserveServicesResponse, error){
		"transport error": func(*sandboxdv1.ObserveServicesRequest) (*sandboxdv1.ObserveServicesResponse, error) {
			return nil, status.Error(codes.Unavailable, "unavailable")
		},
		"partial response": func(request *sandboxdv1.ObserveServicesRequest) (*sandboxdv1.ObserveServicesResponse, error) {
			return &sandboxdv1.ObserveServicesResponse{Observations: []*sandboxdv1.ServiceProbeObservation{{Id: request.Probes[0].Id, Outcome: sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_CONNECTED, Reason: sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_ACCEPTED}}}, nil
		},
		"wrong correlation": func(request *sandboxdv1.ObserveServicesRequest) (*sandboxdv1.ObserveServicesResponse, error) {
			return &sandboxdv1.ObserveServicesResponse{Observations: []*sandboxdv1.ServiceProbeObservation{{Id: "wrong"}, {Id: request.Probes[1].Id}}}, nil
		},
		"invalid pair": func(request *sandboxdv1.ObserveServicesRequest) (*sandboxdv1.ObserveServicesResponse, error) {
			return &sandboxdv1.ObserveServicesResponse{Observations: []*sandboxdv1.ServiceProbeObservation{{Id: request.Probes[0].Id, Outcome: sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_CONNECTED, Reason: sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_FAILED}, {Id: request.Probes[1].Id}}}, nil
		},
	} {
		t.Run(name+" is proven failed", func(t *testing.T) {
			observer.response = response
			t.Cleanup(func() { observer.response = nil })
			got, err := (Connector{Reader: newReader()}).ObserveServices(context.Background(), lifecycle.CaptureExecutionFence(env), CaptureServiceDeclarationSnapshot(env))
			if err != nil || !got.Failed || len(got.Probes) != 0 {
				t.Fatalf("result = %#v, error = %v", got, err)
			}
			observer.response = nil
		})
	}

	replaced := env.DeepCopy()
	replaced.UID = "replacement-environment"
	if got, err := (Connector{Reader: newReader()}).ObserveServices(context.Background(), lifecycle.CaptureExecutionFence(replaced), CaptureServiceDeclarationSnapshot(replaced)); err == nil || got.Failed || len(got.Probes) != 0 || !strings.Contains(err.Error(), "incarnation changed") {
		t.Fatalf("same-name replacement result = %#v, error = %v", got, err)
	}
}

func observationCertificate(t *testing.T, identity string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: identity}, DNSNames: []string{identity}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, key
}
