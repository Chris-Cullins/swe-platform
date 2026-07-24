package sandboxclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

func TestDialProcessValidatesCurrentEnvironmentPodAndCredentialIncarnation(t *testing.T) {
	const identity = "pod-a.sandboxd.swe.dev"
	certificate := processTestCertificate(t, identity)
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-environment", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, Reason: "SandboxdReady", Message: "sandboxd is ready"}},
	}}
	env.Spec.TemplateRef = "default"
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: env.Spec.TemplateRef, Namespace: env.Namespace}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: env.Status.PodName, Namespace: env.Namespace, UID: "pod-uid", Annotations: map[string]string{
		sandboxdauth.IdentityAnnotation: identity, sandboxdauth.SecretUIDAnnotation: "secret-uid", sandboxdauth.SecretNameAnnotation: "env-environment-sandboxd",
	}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: processTestPtr(true)}}}, Status: corev1.PodStatus{
		PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "env-environment-sandboxd", Namespace: env.Namespace, UID: "secret-uid", Annotations: map[string]string{
		sandboxdauth.IdentityAnnotation: identity, sandboxdauth.PodUIDAnnotation: string(pod.UID),
	}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: processTestPtr(true)}}}, Data: map[string][]byte{
		sandboxdauth.TLSCertKey: certificate, sandboxdauth.ProcessTokenKey: []byte("process-token"),
	}}

	newClient := func(objects ...client.Object) client.Client {
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		if err := platformv1alpha1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objects, template.DeepCopy())...).Build()
	}

	process, closeConnection, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), secret.DeepCopy())}).DialProcess(context.Background(), env.Namespace, env.Name, env.UID)
	if err != nil || process == nil || closeConnection == nil {
		t.Fatalf("valid process dial handle: process nil=%t, close nil=%t, error=%v", process == nil, closeConnection == nil, err)
	}
	if err := closeConnection(); err != nil {
		t.Fatal(err)
	}
	freshEpoch := env.DeepCopy()
	freshEpoch.Status.Lifecycle.Epoch = 1
	if _, _, err := (Connector{Reader: newClient(freshEpoch, pod.DeepCopy(), secret.DeepCopy())}).DialProcessForEpoch(context.Background(), env.Namespace, env.Name, env.UID, 0); err == nil || !strings.Contains(err.Error(), "lifecycle epoch changed") {
		t.Fatalf("stale lifecycle epoch error = %v", err)
	}
	activityReader := &environmentChangingReader{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), secret.DeepCopy()), mutate: func(environment *platformv1alpha1.Environment) {
		environment.Annotations = map[string]string{"lifecycle.swe.dev/activity-terminal": `{"id":"activity"}`}
	}}
	_, closeActivity, err := (Connector{Reader: activityReader}).DialProcessForEpoch(context.Background(), env.Namespace, env.Name, env.UID, 0)
	if err != nil || closeActivity == nil || activityReader.environmentGets != 2 {
		t.Fatalf("metadata-only activity dial: close nil=%t, error=%v, Environment reads=%d", closeActivity == nil, err, activityReader.environmentGets)
	}
	if err := closeActivity(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*platformv1alpha1.Environment)
	}{
		{name: "epoch changes", mutate: func(environment *platformv1alpha1.Environment) {
			environment.Status.Lifecycle.Epoch++
		}},
		{name: "readiness is withdrawn", mutate: func(environment *platformv1alpha1.Environment) {
			environment.Status.Phase = platformv1alpha1.EnvironmentPhaseSetup
			environment.Status.PodName = ""
			environment.Status.Endpoints.Sandboxd = ""
		}},
		{name: "pod and endpoint are replaced", mutate: func(environment *platformv1alpha1.Environment) {
			environment.Status.PodName = "env-environment-replacement"
			environment.Status.Endpoints.Sandboxd = "10.0.0.2:50051"
		}},
	} {
		t.Run("final read "+test.name, func(t *testing.T) {
			reader := &environmentChangingReader{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), secret.DeepCopy()), mutate: test.mutate}
			if _, _, err := (Connector{Reader: reader}).DialProcessForEpoch(context.Background(), env.Namespace, env.Name, env.UID, 0); err == nil || !strings.Contains(err.Error(), "execution changed while resolving") || reader.environmentGets != 2 {
				t.Fatalf("racing execution error = %v, Environment reads = %d", err, reader.environmentGets)
			}
		})
	}
	suspended := env.DeepCopy()
	suspended.Status.Lifecycle.Suspended = true
	suspended.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
	if _, _, err := (Connector{Reader: newClient(suspended, pod.DeepCopy(), secret.DeepCopy())}).DialProcess(context.Background(), env.Namespace, env.Name, env.UID); err == nil || !strings.Contains(err.Error(), "current reachable incarnation") {
		t.Fatalf("suspended environment error = %v", err)
	}
	longNameEnv := env.DeepCopy()
	longNameEnv.Name = strings.Repeat("long-environment-", 5)
	longNamePod := pod.DeepCopy()
	longNamePod.OwnerReferences[0].Name = longNameEnv.Name
	longNamePod.Annotations[sandboxdauth.SecretNameAnnotation] = "bounded-credential-name"
	longNameSecret := secret.DeepCopy()
	longNameSecret.Name = "bounded-credential-name"
	longNameSecret.OwnerReferences[0].Name = longNameEnv.Name
	_, closeLongName, err := (Connector{Reader: newClient(longNameEnv, longNamePod, longNameSecret)}).DialProcess(context.Background(), longNameEnv.Namespace, longNameEnv.Name, longNameEnv.UID)
	if err != nil {
		t.Fatalf("long-name Environment credential lookup: %v", err)
	}
	if err := closeLongName(); err != nil {
		t.Fatal(err)
	}

	wrongOwner := pod.DeepCopy()
	wrongOwner.OwnerReferences[0].Name = "other-environment"
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), wrongOwner, secret.DeepCopy())}).DialProcess(context.Background(), env.Namespace, env.Name, env.UID); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("wrong pod owner error = %v", err)
	}

	staleCredential := secret.DeepCopy()
	staleCredential.Annotations[sandboxdauth.PodUIDAnnotation] = "replaced-pod"
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), staleCredential)}).DialProcess(context.Background(), env.Namespace, env.Name, env.UID); err == nil || !strings.Contains(err.Error(), "current environment pod") {
		t.Fatalf("stale credential error = %v", err)
	}
	replacementSecret := secret.DeepCopy()
	replacementSecret.UID = "replacement-secret-uid"
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), replacementSecret)}).DialProcess(context.Background(), env.Namespace, env.Name, env.UID); err == nil || !strings.Contains(err.Error(), "current environment pod") {
		t.Fatalf("replacement Secret error = %v", err)
	}
	wrongSecretOwner := secret.DeepCopy()
	wrongSecretOwner.OwnerReferences[0].Kind = "Run"
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), wrongSecretOwner)}).DialProcess(context.Background(), env.Namespace, env.Name, env.UID); err == nil || !strings.Contains(err.Error(), "current environment pod") {
		t.Fatalf("wrong Secret owner kind error = %v", err)
	}

	if _, _, err := (Connector{Reader: newClient(env.DeepCopy())}).DialProcess(context.Background(), env.Namespace, env.Name, types.UID("replaced-environment")); err == nil || !strings.Contains(err.Error(), "current reachable incarnation") {
		t.Fatalf("stale environment UID error = %v", err)
	}
}

type environmentChangingReader struct {
	client.Reader
	environmentGets int
	mutate          func(*platformv1alpha1.Environment)
}

func (r *environmentChangingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, object, options...); err != nil {
		return err
	}
	if environment, ok := object.(*platformv1alpha1.Environment); ok {
		r.environmentGets++
		if r.environmentGets > 1 {
			r.mutate(environment)
		}
	}
	return nil
}

func processTestCertificate(t *testing.T, serverName string) []byte {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: serverName}, DNSNames: []string{serverName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func processTestPtr[T any](value T) *T { return &value }
