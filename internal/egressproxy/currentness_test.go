package egressproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/egressidentity"
	"github.com/Chris-Cullins/swe-platform/internal/egresspod"
	"github.com/Chris-Cullins/swe-platform/internal/egresspolicy"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

type staticIdentityLookup struct {
	claims   []byte
	err      error
	wait     bool
	deadline atomic.Int64
}

type secondCycleFailureLookup struct {
	claims         []byte
	calls          atomic.Int32
	firstProofDone chan struct{}
	started        chan time.Duration
}

type failingReader struct{ client.Reader }

type switchableFailingReader struct {
	client.Reader
	fail atomic.Bool
}

type mutatingReader struct {
	client.Client
	projectGets int
}

func (failingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("Kubernetes API unavailable")
}

func (failingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("Kubernetes API unavailable")
}

func (r *switchableFailingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if r.fail.Load() {
		return errors.New("Kubernetes API unavailable")
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func (r *switchableFailingReader) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	if r.fail.Load() {
		return errors.New("Kubernetes API unavailable")
	}
	return r.Reader.List(ctx, list, options...)
}

func (m *mutatingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*platformv1alpha1.Project); ok {
		m.projectGets++
		if m.projectGets == 2 {
			var project platformv1alpha1.Project
			if err := m.Client.Get(ctx, key, &project); err != nil {
				return err
			}
			project.Spec.EgressAllowlist = nil
			if err := m.Client.Update(ctx, &project); err != nil {
				return err
			}
		}
	}
	return m.Client.Get(ctx, key, object, options...)
}

func (s *staticIdentityLookup) Lookup(ctx context.Context, _ [sha256.Size]byte) ([]byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		s.deadline.Store(int64(time.Until(deadline)))
	}
	if s.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return append([]byte(nil), s.claims...), s.err
}

func (b *secondCycleFailureLookup) Lookup(ctx context.Context, _ [sha256.Size]byte) ([]byte, error) {
	call := b.calls.Add(1)
	if call <= 4 {
		if call == 4 {
			close(b.firstProofDone)
		}
		return append([]byte(nil), b.claims...), nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		select {
		case b.started <- time.Until(deadline):
		default:
		}
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type currentnessFixture struct {
	installation *platformv1alpha1.Installation
	namespace    *corev1.Namespace
	project      *platformv1alpha1.Project
	configMap    *corev1.ConfigMap
	environment  *platformv1alpha1.Environment
	pod          *corev1.Pod
	claims       egressidentity.Claims
	hint         Identity
}

func newCurrentnessFixture(t *testing.T) *currentnessFixture {
	t.Helper()
	fingerprint := sha256.Sum256([]byte("presented certificate"))
	config := egresspolicy.Config{
		APIVersion: egresspolicy.ConfigAPIVersion, Mode: egresspolicy.ModeRestricted,
		Ceiling: []egresspolicy.Hostname{"api.example.com", "git.example.com"}, Baseline: []egresspolicy.Hostname{"git.example.com"},
		RestrictedProfile: &egresspolicy.RestrictedProfile{
			Name: egresspolicy.RestrictedProfileCalicoV1, ResolverIPs: []string{"10.96.0.10"},
			APIServerCIDRs: []string{"10.0.0.1/32"}, PodCIDRs: []string{"10.244.0.0/16"},
			ServiceCIDRs: []string{"10.96.0.0/12"}, NodeCIDRs: []string{"192.168.0.0/16"},
			ControlPlaneCIDRs: []string{"172.16.0.0/16"}, AdditionalDeniedCIDRs: []string{},
		}, TLSSecretName: "egress-proxy-tls", ProxyImage: "registry.example.com/egress-proxy@sha256:" + strings.Repeat("a", 64),
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	contentHash := sha256.Sum256(raw)
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "egress-policy", Namespace: "system", UID: "config-uid", Annotations: map[string]string{
		egresspolicy.ConfigContentSHA256Annotation: hex.EncodeToString(contentHash[:]),
	}}, Immutable: ptr.To(true), Data: map[string]string{egresspolicy.ConfigDataKey: string(raw)}}
	revision, err := (egresspolicy.RuntimeRevisionInputs{
		InstallationUID: "installation-uid", ConfigMapUID: configMap.UID, ConfigMapContentSHA256: contentHash,
		ProjectUID: "project-uid", Ceiling: []string{"api.example.com", "git.example.com"}, Baseline: []string{"git.example.com"},
		ProjectSelection: []string{"api.example.com"},
	}).Revision()
	if err != nil {
		t.Fatal(err)
	}
	claims := egressidentity.Claims{
		InstallationNamespace: "system", InstallationName: "main", InstallationUID: "installation-uid",
		ProjectNamespace: "project", ProjectName: "app", ProjectUID: "project-uid",
		EnvironmentNamespace: "project", EnvironmentName: "env", EnvironmentUID: "environment-uid",
		PodName: "env-env", PodUID: "pod-uid", ExecutionGeneration: 7,
		RuntimePolicyRevision: revision.String(), ForwarderRevision: egresspod.ForwarderRevision,
		CertificateFingerprint: hex.EncodeToString(fingerprint[:]),
	}
	controller := true
	return &currentnessFixture{
		installation: &platformv1alpha1.Installation{ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "system", UID: claims.InstallationUID}},
		namespace: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: claims.ProjectNamespace, UID: "namespace-uid", Annotations: map[string]string{
			tenancy.InstallationNamespaceAnnotation: claims.InstallationNamespace, tenancy.InstallationNameAnnotation: claims.InstallationName,
			tenancy.InstallationUIDAnnotation: string(claims.InstallationUID), tenancy.ProjectNameAnnotation: claims.ProjectName,
			tenancy.ProjectUIDAnnotation: string(claims.ProjectUID), tenancy.LifecycleAnnotation: string(tenancy.LifecycleActive),
		}}},
		project:   &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: claims.ProjectName, Namespace: claims.ProjectNamespace, UID: claims.ProjectUID}, Spec: platformv1alpha1.ProjectSpec{EgressAllowlist: []string{"api.example.com"}}},
		configMap: configMap,
		environment: &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: claims.EnvironmentName, Namespace: claims.EnvironmentNamespace, UID: claims.EnvironmentUID, Generation: 1},
			Spec: platformv1alpha1.EnvironmentSpec{ProjectRef: claims.ProjectName}, Status: platformv1alpha1.EnvironmentStatus{
				ObservedGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseReady, ExecutionGeneration: claims.ExecutionGeneration, PodName: claims.PodName,
				Conditions:   []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
				Provisioning: &platformv1alpha1.EnvironmentProvisioningSnapshot{ProjectVerified: true, Project: &platformv1alpha1.EnvironmentProvisioningProject{Name: claims.ProjectName, UID: claims.ProjectUID}},
			}},
		pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: claims.PodName, Namespace: claims.EnvironmentNamespace, UID: claims.PodUID,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: claims.EnvironmentName, UID: claims.EnvironmentUID, Controller: &controller}},
			Annotations: map[string]string{
				egresspod.ExecutionGenerationAnnotation: "7", egresspod.PolicyRevisionAnnotation: claims.RuntimePolicyRevision,
				egresspod.ForwarderRevisionAnnotation: claims.ForwarderRevision, egresspod.CertificateFingerprintAnnotation: claims.CertificateFingerprint,
			}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever}},
		claims: claims, hint: Identity{Fingerprint: fingerprint, Subject: "forged and ignored"},
	}
}

func currentnessScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func (f *currentnessFixture) objects(extra ...client.Object) []client.Object {
	objects := []client.Object{f.installation, f.namespace}
	if f.project != nil {
		objects = append(objects, f.project)
	}
	objects = append(objects, f.configMap, f.environment, f.pod)
	return append(objects, extra...)
}

func (f *currentnessFixture) authorizer(t *testing.T, reader client.Reader) *CurrentnessAuthorizer {
	t.Helper()
	canonical, err := f.claims.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	return &CurrentnessAuthorizer{Reader: reader, Identities: &staticIdentityLookup{claims: canonical},
		Installation:    tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: f.installation.Namespace, Name: f.installation.Name}, UID: f.claims.InstallationUID},
		PolicyConfigMap: types.NamespacedName{Namespace: f.configMap.Namespace, Name: f.configMap.Name}}
}

func TestCurrentnessAuthorizerUnchangedIdentity(t *testing.T) {
	f := newCurrentnessFixture(t)
	reader := fake.NewClientBuilder().WithScheme(currentnessScheme(t)).WithObjects(f.objects()...).Build()
	authorizer := f.authorizer(t, reader)
	authorization, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if authorization.ExecutionKey != "environment-uid/7/pod-uid" || authorization.ProjectKey != "project-uid" || len(authorization.DeniedPrefixes) != 6 || authorization.Currentness == nil || authorization.ReleaseCurrentness == nil || isClosed(authorization.Currentness) {
		t.Fatalf("unexpected authorization: %#v", authorization)
	}
	defer authorization.ReleaseCurrentness()
	second, err := authorizer.Authorize(context.Background(), Identity{Fingerprint: f.hint.Fingerprint, Subject: "another forged subject"}, "api.example.com")
	if err != nil || second.Currentness != authorization.Currentness || isClosed(second.Currentness) {
		t.Fatalf("unchanged recheck = %#v, %v", second, err)
	}
	defer second.ReleaseCurrentness()
	if _, err := authorizer.Authorize(context.Background(), f.hint, "denied.example.com"); err == nil {
		t.Fatal("destination outside effective policy accepted")
	}
	if !isClosed(authorization.Currentness) {
		t.Fatal("failed recheck did not revoke existing currentness")
	}
}

func TestCurrentnessAuthorizerAdversarialCurrentness(t *testing.T) {
	tests := map[string]func(*currentnessFixture) []client.Object{
		"Installation replacement": func(f *currentnessFixture) []client.Object { f.installation.UID = "replacement"; return nil },
		"inactive Namespace": func(f *currentnessFixture) []client.Object {
			f.namespace.Annotations[tenancy.LifecycleAnnotation] = string(tenancy.LifecycleFenced)
			return nil
		},
		"Namespace Installation claim": func(f *currentnessFixture) []client.Object {
			f.namespace.Annotations[tenancy.InstallationUIDAnnotation] = "forged"
			return nil
		},
		"Namespace Project claim": func(f *currentnessFixture) []client.Object {
			f.namespace.Annotations[tenancy.ProjectUIDAnnotation] = "forged"
			return nil
		},
		"Project replacement": func(f *currentnessFixture) []client.Object { f.project.UID = "replacement"; return nil },
		"no Project":          func(f *currentnessFixture) []client.Object { f.project = nil; return nil },
		"multiple Projects": func(f *currentnessFixture) []client.Object {
			return []client.Object{&platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "project", UID: "other-uid"}}}
		},
		"invalid selection": func(f *currentnessFixture) []client.Object {
			f.project.Spec.EgressAllowlist = []string{"UPPER.example.com"}
			return nil
		},
		"outside ceiling": func(f *currentnessFixture) []client.Object {
			f.project.Spec.EgressAllowlist = []string{"outside.example.com"}
			return nil
		},
		"ConfigMap replacement": func(f *currentnessFixture) []client.Object { f.configMap.UID = "replacement"; return nil },
		"mutable ConfigMap":     func(f *currentnessFixture) []client.Object { f.configMap.Immutable = ptr.To(false); return nil },
		"ConfigMap content": func(f *currentnessFixture) []client.Object {
			f.configMap.Data[egresspolicy.ConfigDataKey] = strings.Replace(f.configMap.Data[egresspolicy.ConfigDataKey], "api.example.com", "new.example.com", 1)
			return nil
		},
		"Environment replacement": func(f *currentnessFixture) []client.Object { f.environment.UID = "replacement"; return nil },
		"frozen Project": func(f *currentnessFixture) []client.Object {
			f.environment.Status.Provisioning.Project.UID = "replacement"
			return nil
		},
		"execution": func(f *currentnessFixture) []client.Object { f.environment.Status.ExecutionGeneration++; return nil },
		"not ready": func(f *currentnessFixture) []client.Object { f.environment.Status.Conditions = nil; return nil },
		"suspended": func(f *currentnessFixture) []client.Object {
			f.environment.Status.Lifecycle.Suspended = true
			return nil
		},
		"Pod replacement": func(f *currentnessFixture) []client.Object { f.pod.UID = "replacement"; return nil },
		"deleting Pod": func(f *currentnessFixture) []client.Object {
			now := metav1.Now()
			f.pod.DeletionTimestamp = &now
			f.pod.Finalizers = []string{"test"}
			return nil
		},
		"Pod owner": func(f *currentnessFixture) []client.Object { f.pod.OwnerReferences[0].UID = "replacement"; return nil },
		"Pod restart Always": func(f *currentnessFixture) []client.Object {
			f.pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
			return nil
		},
		"Pod restart OnFailure": func(f *currentnessFixture) []client.Object {
			f.pod.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
			return nil
		},
		"Pod restart default": func(f *currentnessFixture) []client.Object { f.pod.Spec.RestartPolicy = ""; return nil },
		"Pod execution": func(f *currentnessFixture) []client.Object {
			f.pod.Annotations[egresspod.ExecutionGenerationAnnotation] = "8"
			return nil
		},
		"policy revision": func(f *currentnessFixture) []client.Object {
			f.pod.Annotations[egresspod.PolicyRevisionAnnotation] = "stale"
			return nil
		},
		"security revision": func(f *currentnessFixture) []client.Object {
			f.pod.Annotations[egresspod.ForwarderRevisionAnnotation] = "stale"
			return nil
		},
		"fingerprint": func(f *currentnessFixture) []client.Object {
			f.pod.Annotations[egresspod.CertificateFingerprintAnnotation] = strings.Repeat("0", 64)
			return nil
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			f := newCurrentnessFixture(t)
			extra := mutate(f)
			objects := f.objects(extra...)
			filtered := objects[:0]
			for _, object := range objects {
				if object != nil {
					filtered = append(filtered, object)
				}
			}
			reader := fake.NewClientBuilder().WithScheme(currentnessScheme(t)).WithObjects(filtered...).Build()
			if _, err := f.authorizer(t, reader).Authorize(context.Background(), f.hint, "api.example.com"); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestCurrentnessAuthorizerForgedClaimsAndAPIUncertainty(t *testing.T) {
	f := newCurrentnessFixture(t)
	reader := fake.NewClientBuilder().WithScheme(currentnessScheme(t)).WithObjects(f.objects()...).Build()
	authorizer := f.authorizer(t, reader)

	for name, mutate := range map[string]func(*egressidentity.Claims){
		"Installation": func(c *egressidentity.Claims) { c.InstallationUID = "forged" },
		"Project":      func(c *egressidentity.Claims) { c.ProjectUID = "forged" },
		"Environment":  func(c *egressidentity.Claims) { c.EnvironmentUID = "forged" },
		"Pod":          func(c *egressidentity.Claims) { c.PodUID = "forged" },
		"execution":    func(c *egressidentity.Claims) { c.ExecutionGeneration++ },
		"policy":       func(c *egressidentity.Claims) { c.RuntimePolicyRevision = strings.Repeat("0", 64) },
		"fingerprint":  func(c *egressidentity.Claims) { c.CertificateFingerprint = strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			claims := f.claims
			mutate(&claims)
			canonical, err := claims.CanonicalBytes()
			if err != nil {
				t.Fatal(err)
			}
			forged := &CurrentnessAuthorizer{Reader: reader, Identities: &staticIdentityLookup{claims: canonical}, Installation: authorizer.Installation, PolicyConfigMap: authorizer.PolicyConfigMap}
			if _, err := forged.Authorize(context.Background(), f.hint, "api.example.com"); err == nil {
				t.Fatal("forged claims accepted")
			}
		})
	}
	wrongHint := f.hint
	wrongHint.Fingerprint = sha256.Sum256([]byte("other certificate"))
	if _, err := authorizer.Authorize(context.Background(), wrongHint, "api.example.com"); err == nil {
		t.Fatal("forged certificate hint accepted")
	}

	canonical, _ := f.claims.CanonicalBytes()
	authorizer.Identities = &staticIdentityLookup{claims: canonical, err: errors.New("API unavailable")}
	if _, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com"); err == nil {
		t.Fatal("lookup error accepted")
	}
	waiting := &staticIdentityLookup{claims: canonical, wait: true}
	authorizer.Identities = waiting
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := authorizer.Authorize(ctx, f.hint, "api.example.com"); err == nil {
		t.Fatal("timeout accepted")
	}
	waitingDeadline := time.Duration(waiting.deadline.Load())
	if waitingDeadline <= 0 || waitingDeadline > CurrentnessTimeout {
		t.Fatalf("evaluation deadline = %v", waitingDeadline)
	}
	bounded := &staticIdentityLookup{claims: canonical, err: errors.New("stop")}
	authorizer.Identities = bounded
	_, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com")
	boundedDeadline := time.Duration(bounded.deadline.Load())
	if err == nil || boundedDeadline <= CurrentnessTimeout-time.Second || boundedDeadline > CurrentnessTimeout {
		t.Fatalf("authorizer-owned deadline = %v, err = %v", boundedDeadline, err)
	}
}

func TestCurrentnessAuthorizerRejectsSystemProjectNamespace(t *testing.T) {
	f := newCurrentnessFixture(t)
	reader := fake.NewClientBuilder().WithScheme(currentnessScheme(t)).WithObjects(f.objects()...).Build()
	authorizer := f.authorizer(t, reader)
	claims := f.claims
	claims.ProjectNamespace = claims.InstallationNamespace
	claims.EnvironmentNamespace = claims.InstallationNamespace
	canonical, err := claims.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	authorizer.Identities = &staticIdentityLookup{claims: canonical}
	if _, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com"); err == nil || !strings.Contains(err.Error(), "must be distinct") {
		t.Fatalf("system namespace was not rejected at the tenancy boundary: %v", err)
	}
}

func TestCurrentnessAuthorizerRejectsForeignInstallationAndMidProofChange(t *testing.T) {
	f := newCurrentnessFixture(t)
	base := fake.NewClientBuilder().WithScheme(currentnessScheme(t)).WithObjects(f.objects()...).Build()
	authorizer := f.authorizer(t, base)
	authorizer.Installation = tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "foreign"}, UID: "foreign-uid"}
	if _, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com"); err == nil {
		t.Fatal("claims selected an Installation other than the configured authority")
	}

	authorizer = f.authorizer(t, &mutatingReader{Client: base})
	if _, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com"); err == nil {
		t.Fatal("Project contraction during the proof was accepted")
	}
}

func TestCurrentnessAuthorizerStateCapacity(t *testing.T) {
	f := newCurrentnessFixture(t)
	authorizer := &CurrentnessAuthorizer{Reader: failingReader{}, Identities: &staticIdentityLookup{},
		Installation:    tenancy.InstallationIdentity{Key: types.NamespacedName{Namespace: "system", Name: "main"}, UID: "installation-uid"},
		PolicyConfigMap: types.NamespacedName{Namespace: "system", Name: "policy"}, records: make(map[[sha256.Size]byte]*executionRecord, MaxCurrentnessStates)}
	for i := 0; i < MaxCurrentnessStates; i++ {
		fingerprint := sha256.Sum256([]byte(string(rune(i))))
		authorizer.records[fingerprint] = &executionRecord{state: newExecutionState()}
	}
	if _, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com"); err == nil {
		t.Fatal("new fingerprint accepted above bounded state capacity")
	}
}

func TestCurrentnessAuthorizerFailedRecheckRevokes(t *testing.T) {
	f := newCurrentnessFixture(t)
	reader := fake.NewClientBuilder().WithScheme(currentnessScheme(t)).WithObjects(f.objects()...).Build()
	switchingReader := &switchableFailingReader{Reader: reader}
	authorizer := f.authorizer(t, switchingReader)
	first, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	var pod corev1.Pod
	if err := reader.Get(context.Background(), types.NamespacedName{Namespace: f.pod.Namespace, Name: f.pod.Name}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Annotations[egresspod.CertificateFingerprintAnnotation] = strings.Repeat("0", 64)
	if err := reader.Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
	if _, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com"); err == nil {
		t.Fatal("failed recheck accepted")
	}
	if !isClosed(first.Currentness) {
		t.Fatal("existing tunnel state remained current after failed recheck")
	}
	first.ReleaseCurrentness()

	pod.Annotations[egresspod.CertificateFingerprintAnnotation] = f.claims.CertificateFingerprint
	if err := reader.Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
	second, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	defer second.ReleaseCurrentness()
	switchingReader.fail.Store(true)
	if _, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com"); err == nil || !isClosed(second.Currentness) {
		t.Fatal("API uncertainty retained a previous authorization")
	}
}

func TestCurrentnessAuthorizerReleaseReclaimsSuccessfulState(t *testing.T) {
	f := newCurrentnessFixture(t)
	reader := fake.NewClientBuilder().WithScheme(currentnessScheme(t)).WithObjects(f.objects()...).Build()
	authorizer := f.authorizer(t, reader)
	authorizer.currentnessWindow = time.Hour
	first, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	second, err := authorizer.Authorize(context.Background(), f.hint, "git.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if first.Currentness != second.Currentness {
		t.Fatal("same execution tunnels did not share currentness")
	}
	record := authorizer.records[f.hint.Fingerprint]
	record.mu.Lock()
	loop := record.loopDone
	if record.leases != 2 || loop == nil {
		t.Fatalf("shared currentness state = leases %d, loop %p", record.leases, loop)
	}
	record.mu.Unlock()
	if len(authorizer.records) != 1 {
		t.Fatalf("currentness records = %d, want 1", len(authorizer.records))
	}
	first.ReleaseCurrentness()
	record.mu.Lock()
	if record.leases != 1 || record.loopDone != loop || isClosed(second.Currentness) {
		t.Fatalf("first release stopped shared state: leases=%d sameLoop=%t closed=%t", record.leases, record.loopDone == loop, isClosed(second.Currentness))
	}
	record.mu.Unlock()
	second.ReleaseCurrentness()
	second.ReleaseCurrentness()
	if !isClosed(second.Currentness) || len(authorizer.records) != 0 {
		t.Fatalf("final release retained currentness: closed=%t records=%d", isClosed(second.Currentness), len(authorizer.records))
	}
}

func TestCurrentnessAuthorizerSharedLoopUsesOneBoundedCycleDeadline(t *testing.T) {
	f := newCurrentnessFixture(t)
	reader := fake.NewClientBuilder().WithScheme(currentnessScheme(t)).WithObjects(f.objects()...).Build()
	canonical, err := f.claims.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	lookup := &secondCycleFailureLookup{claims: canonical, firstProofDone: make(chan struct{}), started: make(chan time.Duration, 1)}
	authorizer := f.authorizer(t, reader)
	authorizer.Identities = lookup
	authorizer.currentnessWindow = 250 * time.Millisecond
	authorization, err := authorizer.Authorize(context.Background(), f.hint, "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	defer authorization.ReleaseCurrentness()
	select {
	case <-lookup.firstProofDone:
	case <-time.After(time.Second):
		t.Fatal("first shared proof did not complete")
	}
	uncertainAt := time.Now()
	select {
	case deadline := <-lookup.started:
		if deadline <= 0 || deadline > authorizer.currentnessWindow/2 {
			t.Fatalf("shared proof deadline = %v, want half of the complete currentness window", deadline)
		}
	case <-time.After(time.Second):
		t.Fatal("second shared proof did not start")
	}
	select {
	case <-authorization.Currentness:
		if elapsed := time.Since(uncertainAt); elapsed > 5*authorizer.currentnessWindow/4 {
			t.Fatalf("API uncertainty exceeded the complete currentness window: %v", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("API uncertainty did not revoke currentness")
	}
}
