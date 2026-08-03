package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/agent"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/repositorycredential"
)

type fakeRepositoryCredentialProvider struct {
	canonical string
	issued    []string
	revoked   [][]byte
	issueErr  error
	revokeErr error
	now       time.Time
	token     []byte
}

type lostRepositorySecretCreateClient struct {
	client.Client
	lost bool
}

type failRepositoryLeaseCreateClient struct {
	client.Client
	failed bool
}

func (c *lostRepositorySecretCreateClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if err := c.Client.Create(ctx, object, options...); err != nil {
		return err
	}
	if secret, ok := object.(*corev1.Secret); ok && secret.Type == repositorycredential.SecretType && !c.lost {
		c.lost = true
		return errors.New("repository Secret create response lost")
	}
	return nil
}

func (c *failRepositoryLeaseCreateClient) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	if secret, ok := object.(*corev1.Secret); ok && secret.Name == repositorycredential.SecretName("run-uid") && !c.failed {
		c.failed = true
		return errors.New("repository Secret persistence unavailable")
	}
	return c.Client.Create(ctx, object, options...)
}

func (f *fakeRepositoryCredentialProvider) CanonicalRepository(repository string) (string, error) {
	if f.canonical == "" {
		return "", &repositorycredential.Error{Operation: "repository", Reason: "RepositoryUnsupported"}
	}
	return f.canonical, nil
}
func (f *fakeRepositoryCredentialProvider) Issue(_ context.Context, repository string) (*repositorycredential.Credential, error) {
	f.issued = append(f.issued, repository)
	if f.issueErr != nil {
		return nil, f.issueErr
	}
	if f.token == nil {
		f.token = []byte("fake-token")
	}
	return &repositorycredential.Credential{Token: f.token, Repository: f.canonical, InstallationID: 7, ExpiresAt: f.now.Add(time.Hour)}, nil
}
func (f *fakeRepositoryCredentialProvider) Revoke(_ context.Context, credential *repositorycredential.Credential) error {
	f.revoked = append(f.revoked, append([]byte(nil), credential.Token...))
	return f.revokeErr
}

func TestRepositoryCredentialIssueCanonicalizesAndClearsProviderToken(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/acme/repo.git"}}}
	env, tmpl := frozenRepositoryEnvironment(project)
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{ProjectRef: "p", RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}, Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	r := reconciler(t, &scriptedAdapter{}, run, project, env, tmpl)
	provider := &fakeRepositoryCredentialProvider{canonical: "https://github.com/acme/repo", now: now, token: []byte("fake-token")}
	r.RepositoryCredentials, r.Now = provider, func() time.Time { return now }
	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	if _, done, err := r.ensureRepositoryCredential(context.Background(), &current); err != nil || !done {
		t.Fatalf("mark issuing = done %v, err %v", done, err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	if _, done, err := r.ensureRepositoryCredential(context.Background(), &current); err != nil || !done {
		t.Fatalf("issue = done %v, err %v", done, err)
	}
	if len(provider.issued) != 1 || provider.issued[0] != provider.canonical {
		t.Fatalf("issued repositories = %v", provider.issued)
	}
	if string(provider.token) != strings.Repeat("\x00", len(provider.token)) {
		t.Fatalf("provider token was not defensively cleared: %v", provider.token)
	}
	var secret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: repositorycredential.SecretName(run.UID)}, &secret); err != nil {
		t.Fatal(err)
	}
	if got := secret.Annotations[repositorycredential.AnnotationRepository]; got != provider.canonical {
		t.Fatalf("lease repository = %q", got)
	}
}

func TestRepositoryCredentialRecoversLostSecretCreateResponse(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/acme/repo"}}}
	env, tmpl := frozenRepositoryEnvironment(project)
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{ProjectRef: "p", RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}, Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	r := reconciler(t, &scriptedAdapter{}, run, project, env, tmpl)
	provider := &fakeRepositoryCredentialProvider{canonical: "https://github.com/acme/repo", now: now, token: []byte("fake-token")}
	r.RepositoryCredentials, r.Now = provider, func() time.Time { return now }
	lost := &lostRepositorySecretCreateClient{Client: r.Client}
	r.Client = lost

	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	if _, done, err := r.ensureRepositoryCredential(context.Background(), &current); err != nil || !done {
		t.Fatalf("mark issuing = done %v, err %v", done, err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	if _, done, err := r.ensureRepositoryCredential(context.Background(), &current); err != nil || !done {
		t.Fatalf("lost response recovery = done %v, err %v", done, err)
	}
	if !lost.lost || len(provider.revoked) != 0 {
		t.Fatalf("lost=%t revocations=%d", lost.lost, len(provider.revoked))
	}
	var secret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: repositorycredential.SecretName(run.UID)}, &secret); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryCredentialPersistsAndDrainsFailedImmediateRevocation(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/acme/repo"}}}
	env, tmpl := frozenRepositoryEnvironment(project)
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{ProjectRef: "p", RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}, Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	r := reconciler(t, &scriptedAdapter{}, run, project, env, tmpl)
	provider := &fakeRepositoryCredentialProvider{canonical: "https://github.com/acme/repo", now: now, token: []byte("token-pending-revocation"), revokeErr: &repositorycredential.Error{Operation: "revoke", Reason: "ProviderUnavailable", RetryAfter: time.Minute}}
	r.RepositoryCredentials, r.Now = provider, func() time.Time { return now }
	failingClient := &failRepositoryLeaseCreateClient{Client: r.Client}
	r.Client = failingClient

	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	if _, done, err := r.ensureRepositoryCredential(context.Background(), &current); err != nil || !done {
		t.Fatalf("mark issuing = done %v, err %v", done, err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	if result, done, err := r.ensureRepositoryCredential(context.Background(), &current); err != nil || !done || result.RequeueAfter != time.Minute {
		t.Fatalf("persist pending revocation = (%#v, done %v, err %v)", result, done, err)
	}
	if !failingClient.failed || len(provider.issued) != 1 || len(provider.revoked) != 1 {
		t.Fatalf("failed=%t issued=%d revoked=%d", failingClient.failed, len(provider.issued), len(provider.revoked))
	}
	condition := apiMeta.FindStatusCondition(current.Status.Conditions, runConditionRepositoryCredentialReady)
	if condition == nil || condition.Reason != "RevocationPending" {
		t.Fatalf("condition = %#v", condition)
	}
	var pendingSecret corev1.Secret
	pendingKey := types.NamespacedName{Namespace: run.Namespace, Name: repositorycredential.PendingRevocationSecretName(run.UID)}
	if err := r.Get(context.Background(), pendingKey, &pendingSecret); err != nil {
		t.Fatal(err)
	}
	pending, err := repositorycredential.ParsePendingRevocation(&pendingSecret, run.Name, run.UID)
	if err != nil || string(pending.Credential.Token) != "token-pending-revocation" {
		t.Fatalf("pending revocation = %#v, %v", pending, err)
	}

	provider.revokeErr = nil
	if result, done, err := r.ensureRepositoryCredential(context.Background(), &current); err != nil || !done || !result.Requeue {
		t.Fatalf("drain pending revocation = (%#v, done %v, err %v)", result, done, err)
	}
	if len(provider.issued) != 1 || len(provider.revoked) != 2 {
		t.Fatalf("pending drain issued=%d revoked=%d", len(provider.issued), len(provider.revoked))
	}
	if err := r.Get(context.Background(), pendingKey, &pendingSecret); !apierrors.IsNotFound(err) {
		t.Fatalf("pending Secret after drain = %v", err)
	}
}

func TestRepositoryCredentialUsesFrozenRepositoryAfterProjectChanges(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/acme/old"}}}
	env, tmpl := frozenRepositoryEnvironment(project)
	project.Generation, project.Spec.Repositories[0] = 2, "https://github.com/acme/new"
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{ProjectRef: "p", RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}, Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	credential := &repositorycredential.Credential{Token: []byte("old"), Repository: "https://github.com/acme/old", InstallationID: 7, ExpiresAt: now.Add(time.Hour)}
	secret, err := repositorycredential.NewSecret("ns", run.Name, run.UID, string(run.Spec.RepositoryCredential), "https://github.com/acme/old", credential, 1, "", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	r := reconciler(t, &scriptedAdapter{}, run, project, env, tmpl, secret)
	r.RepositoryCredentials, r.Now = &fakeRepositoryCredentialProvider{canonical: "https://github.com/acme/old", now: now}, func() time.Time { return now }
	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	if _, done, err := r.ensureRepositoryCredential(context.Background(), &current); err != nil || done {
		t.Fatalf("frozen repository = done %v, err %v", done, err)
	}
	condition := apiMeta.FindStatusCondition(current.Status.Conditions, runConditionRepositoryCredentialReady)
	if condition == nil || condition.Reason != "Ready" {
		t.Fatalf("condition = %#v", condition)
	}
}

func TestRepositoryRotationRecordStrictJSON(t *testing.T) {
	valid := `{"oldSecretUID":"old-uid","targetGeneration":2,"wake":true}`
	if record, err := parseRepositoryRotationRecord(valid); err != nil || record.OldSecretUID != "old-uid" || record.TargetGeneration != 2 || !record.Wake {
		t.Fatalf("valid record = %#v, %v", record, err)
	}
	for _, value := range []string{"", `{}`, `{"oldSecretUID":"","targetGeneration":2,"wake":false}`, `{"oldSecretUID":"x","targetGeneration":1,"wake":false}`, valid + `{}`, `{"oldSecretUID":"x","targetGeneration":2,"wake":false,"extra":1}`} {
		if _, err := parseRepositoryRotationRecord(value); err == nil {
			t.Errorf("parseRepositoryRotationRecord(%q) succeeded", value)
		}
	}
}

func TestRepositoryCredentialRefreshFencesAfterOldSecretDisappears(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/acme/repo"}}}
	env, tmpl := frozenRepositoryEnvironment(project)
	env.Status.ExecutionGeneration = 1
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{
			Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer},
			Annotations: map[string]string{repositoryRefreshAnnotation: `{"oldSecretUID":"old-secret-uid","targetGeneration":2,"wake":true}`},
		},
		Spec: platformv1alpha1.RunSpec{ProjectRef: "p", RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp},
		Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
			Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned,
		}},
	}
	credential := &repositorycredential.Credential{Token: []byte("fresh-token"), Repository: "https://github.com/acme/repo", InstallationID: 7, ExpiresAt: now.Add(time.Hour)}
	secret, err := repositorycredential.NewSecret("ns", run.Name, run.UID, string(run.Spec.RepositoryCredential), project.Spec.Repositories[0], credential, 2, "", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	secret.UID = "fresh-secret-uid"
	r := reconciler(t, &scriptedAdapter{}, run, project, env, tmpl, secret)
	r.RepositoryCredentials, r.Now = &fakeRepositoryCredentialProvider{canonical: credential.Repository, now: now}, func() time.Time { return now }

	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	result, done, err := r.ensureRepositoryCredential(context.Background(), &current)
	if err != nil || !done || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("refresh recovery = (%#v, %t, %v)", result, done, err)
	}
	var fenced platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fenced); err != nil {
		t.Fatal(err)
	}
	if fenced.Spec.Lifecycle.Suspend == nil || fenced.Spec.Lifecycle.Wake != nil {
		t.Fatalf("refresh lifecycle intent = %#v, want suspend without wake", fenced.Spec.Lifecycle)
	}
	var retained platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Annotations[repositoryRefreshAnnotation] == "" {
		t.Fatal("refresh record was cleared before the old execution was fenced")
	}
}

func TestRepositoryCredentialDoesNotRotateDuringDurablePodProvisioning(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/acme/repo"}}}
	env, tmpl := frozenRepositoryEnvironment(project)
	env.Status.ExecutionGeneration = 1
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{ProjectRef: "p", RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}, Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	credential := &repositorycredential.Credential{Token: []byte("provisioning-token"), Repository: "https://github.com/acme/repo", InstallationID: 7, ExpiresAt: now.Add(time.Hour)}
	secret, err := repositorycredential.NewSecret("ns", run.Name, run.UID, string(run.Spec.RepositoryCredential), project.Spec.Repositories[0], credential, 1, env.UID, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	secret.UID = "provisioning-secret-uid"
	record, _ := json.Marshal(repositoryProvisioningRecord{SecretUID: secret.UID, ExecutionGeneration: 1})
	env.Annotations = map[string]string{repositoryProvisioningAnnotation: string(record)}
	provider := &fakeRepositoryCredentialProvider{canonical: credential.Repository, now: now}
	r := reconciler(t, &scriptedAdapter{}, run, project, env, tmpl, secret)
	r.RepositoryCredentials, r.Now = provider, func() time.Time { return now }

	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	result, done, err := r.ensureRepositoryCredential(context.Background(), &current)
	if err != nil || done || result.RequeueAfter != 50*time.Minute {
		t.Fatalf("provisioning lease = (%#v, %t, %v)", result, done, err)
	}
	if len(provider.revoked) != 0 || current.Annotations[repositoryRefreshAnnotation] != "" {
		t.Fatalf("active provisioning was rotated: revoked=%d annotations=%v", len(provider.revoked), current.Annotations)
	}
}

func TestRepositoryCredentialPendingFrozenSnapshotIsBounded(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/acme/repo"}}}
	env, tmpl := frozenRepositoryEnvironment(project)
	env.Status.Provisioning.ProjectVerified = false
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{ProjectRef: "p", RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}, Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	r := reconciler(t, &scriptedAdapter{}, run, project, env, tmpl)
	r.RepositoryCredentials, r.Now = &fakeRepositoryCredentialProvider{canonical: "https://github.com/acme/repo", now: now}, func() time.Time { return now }
	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	result, done, err := r.ensureRepositoryCredential(context.Background(), &current)
	if err != nil || !done || result.RequeueAfter != repositoryCredentialRequeueDelay {
		t.Fatalf("pending ensure = (%#v, %t, %v)", result, done, err)
	}
	condition := apiMeta.FindStatusCondition(current.Status.Conditions, runConditionRepositoryCredentialReady)
	if condition == nil || condition.Reason != "Issuing" || condition.Status != metav1.ConditionFalse {
		t.Fatalf("pending condition = %#v", condition)
	}
}

func TestExpiredMalformedRepositoryCredentialCleanupRequiresExactIdentity(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	mutations := map[string]func(*corev1.Secret){
		"foreign owner":   func(s *corev1.Secret) { s.OwnerReferences = []metav1.OwnerReference{{Name: "foreign"}} },
		"wrong type":      func(s *corev1.Secret) { s.Type = corev1.SecretTypeOpaque },
		"wrong run name":  func(s *corev1.Secret) { s.Annotations[repositorycredential.AnnotationRunName] = "other" },
		"wrong provider":  func(s *corev1.Secret) { s.Annotations[repositorycredential.AnnotationProvider] = "other" },
		"wrong source":    func(s *corev1.Secret) { s.Annotations[repositorycredential.AnnotationSourceRepository] = "other" },
		"wrong canonical": func(s *corev1.Secret) { s.Annotations[repositorycredential.AnnotationRepository] = "other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) { testExpiredMalformedCleanup(t, now, mutate, false) })
	}
	t.Run("exact expired malformed token", func(t *testing.T) { testExpiredMalformedCleanup(t, now, func(*corev1.Secret) {}, true) })
}

func TestExpiredRepositoryCredentialsCleanUpWithoutProvider(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}}
	credential := &repositorycredential.Credential{Token: []byte("expired-token"), Repository: "https://github.com/acme/repo", InstallationID: 7, ExpiresAt: now.Add(time.Hour)}
	active, err := repositorycredential.NewSecret(run.Namespace, run.Name, run.UID, string(run.Spec.RepositoryCredential), credential.Repository, credential, 1, "", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	active.UID = "active-uid"
	active.Annotations[repositorycredential.AnnotationExpiry] = now.Add(-time.Minute).Format(time.RFC3339Nano)
	r := reconciler(t, &scriptedAdapter{}, run, active)
	r.RepositoryCredentials, r.Now = nil, func() time.Time { return now }
	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	if done, _, err := r.cleanupRepositoryCredential(context.Background(), &current, nil); err != nil || !done {
		t.Fatalf("expired cleanup = done %t, err %v", done, err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(active), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired active Secret retained: %v", err)
	}

	pending, err := repositorycredential.NewPendingRevocationSecret(run.Namespace, run.Name, run.UID, string(run.Spec.RepositoryCredential), credential.Repository, credential, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	pending.UID = "pending-uid"
	pending.Annotations[repositorycredential.AnnotationExpiry] = now.Add(-time.Minute).Format(time.RFC3339Nano)
	if err := r.Create(context.Background(), pending); err != nil {
		t.Fatal(err)
	}
	if done, result, err := r.cleanupPendingRepositoryRevocation(context.Background(), &current); err != nil || done || !result.Requeue {
		t.Fatalf("expired pending cleanup = (%#v, done %t, err %v)", result, done, err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(pending), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired pending Secret retained: %v", err)
	}
}

func testExpiredMalformedCleanup(t *testing.T, now time.Time, mutate func(*corev1.Secret), wantDeleted bool) {
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://github.com/acme/repo"}}}
	env, tmpl := frozenRepositoryEnvironment(project)
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}, Status: platformv1alpha1.RunStatus{EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: env.Name, UID: env.UID, Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	credential := &repositorycredential.Credential{Token: []byte("token"), Repository: "https://github.com/acme/repo", InstallationID: 7, ExpiresAt: now.Add(time.Hour)}
	secret, err := repositorycredential.NewSecret("ns", run.Name, run.UID, string(run.Spec.RepositoryCredential), project.Spec.Repositories[0], credential, 1, "", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	secret.UID = "secret-uid"
	secret.Annotations[repositorycredential.AnnotationExpiry] = now.Add(-time.Minute).Format(time.RFC3339Nano)
	secret.Data[repositorycredential.TokenKey] = nil
	mutate(secret)
	r := reconciler(t, &scriptedAdapter{}, run, project, env, tmpl, secret)
	r.RepositoryCredentials, r.Now = &fakeRepositoryCredentialProvider{canonical: credential.Repository, now: now}, func() time.Time { return now }
	var current platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &current); err != nil {
		t.Fatal(err)
	}
	done, _, cleanupErr := r.cleanupRepositoryCredential(context.Background(), &current, nil)
	if wantDeleted {
		if cleanupErr != nil || !done {
			t.Fatalf("cleanup = done %t, err %v", done, cleanupErr)
		}
	} else if cleanupErr == nil || done {
		t.Fatalf("foreign cleanup = done %t, err %v", done, cleanupErr)
	}
	var retained corev1.Secret
	err = r.Get(context.Background(), client.ObjectKeyFromObject(secret), &retained)
	if wantDeleted != apierrors.IsNotFound(err) {
		t.Fatalf("Secret get error = %v, wantDeleted %t", err, wantDeleted)
	}
}

func frozenRepositoryEnvironment(project *platformv1alpha1.Project) (*platformv1alpha1.Environment, *platformv1alpha1.EnvironmentTemplate) {
	tmpl := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "tmpl", Namespace: "ns", UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "tiny"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: tmpl.Name, ProjectRef: project.Name}}
	env.Status.Provisioning = platformv1alpha1.ResolveEnvironmentProvisioning(env, tmpl, project)
	env.Status.Provisioning.TemplateVerified = true
	env.Status.Provisioning.ProjectVerified = true
	return env, tmpl
}

type scriptedAdapter struct {
	observations          []agent.AdapterObservation
	accepted, observed    int
	cancelled             int
	acceptErr             error
	observeErr            error
	onAccept              func()
	onCancel              func()
	cancelErr             error
	acceptedCredentials   [][]byte
	retainedCredentialKey []byte
}

type blockingObserveAdapter struct {
	observation agent.AdapterObservation
	started     chan struct{}
	release     chan struct{}
}

func (*blockingObserveAdapter) EnsureAccepted(context.Context, agent.AdapterTask, agent.AdapterSandbox, *agent.AdapterLaunchMaterial) error {
	return nil
}

func (a *blockingObserveAdapter) Observe(context.Context, agent.AdapterTask, agent.AdapterSandbox) (agent.AdapterObservation, string, error) {
	close(a.started)
	<-a.release
	return a.observation, string(a.observation), nil
}

func (*blockingObserveAdapter) Cancel(context.Context, agent.AdapterTask, agent.AdapterSandbox) error {
	return nil
}

type failAcceptedStatusClient struct {
	client.Client
	fail bool
}

func (c *failAcceptedStatusClient) Status() client.SubResourceWriter {
	return &failAcceptedStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type failAcceptedStatusWriter struct {
	client.SubResourceWriter
	client *failAcceptedStatusClient
}

func (w *failAcceptedStatusWriter) Update(ctx context.Context, object client.Object, opts ...client.SubResourceUpdateOption) error {
	if run, ok := object.(*platformv1alpha1.Run); ok && run.Status.State == platformv1alpha1.RunStateAdapterAccepted && w.client.fail {
		w.client.fail = false
		return errors.New("simulated lost acceptance status update")
	}
	return w.SubResourceWriter.Update(ctx, object, opts...)
}

// foregroundAdapter models a CLI agent whose managed process exit is the task
// outcome.
type foregroundAdapter struct{ process agent.AdapterObservation }

func (*foregroundAdapter) EnsureAccepted(context.Context, agent.AdapterTask, agent.AdapterSandbox, *agent.AdapterLaunchMaterial) error {
	return nil
}
func (a *foregroundAdapter) Observe(context.Context, agent.AdapterTask, agent.AdapterSandbox) (agent.AdapterObservation, string, error) {
	return a.process, "managed process state", nil
}
func (*foregroundAdapter) Cancel(context.Context, agent.AdapterTask, agent.AdapterSandbox) error {
	return nil
}

// serviceAdapter models a long-lived agent service: task events change while
// the service process remains running, so service exit is not task completion.
type serviceAdapter struct {
	serviceRunning bool
	event          agent.AdapterObservation
}

func (a *serviceAdapter) EnsureAccepted(context.Context, agent.AdapterTask, agent.AdapterSandbox, *agent.AdapterLaunchMaterial) error {
	a.serviceRunning = true
	return nil
}
func (a *serviceAdapter) Observe(context.Context, agent.AdapterTask, agent.AdapterSandbox) (agent.AdapterObservation, string, error) {
	return a.event, "service task event", nil
}
func (a *serviceAdapter) Cancel(context.Context, agent.AdapterTask, agent.AdapterSandbox) error {
	a.serviceRunning = false
	return nil
}

func (a *scriptedAdapter) EnsureAccepted(_ context.Context, _ agent.AdapterTask, _ agent.AdapterSandbox, material *agent.AdapterLaunchMaterial) error {
	var credential *agent.AdapterCredential
	if material != nil {
		credential = material.AgentCredential
	}
	a.accepted++
	if a.onAccept != nil {
		a.onAccept()
	}
	if credential == nil {
		a.acceptedCredentials = append(a.acceptedCredentials, nil)
	} else {
		a.acceptedCredentials = append(a.acceptedCredentials, append([]byte(nil), credential.APIKey...))
		a.retainedCredentialKey = credential.APIKey
	}
	return a.acceptErr
}
func (a *scriptedAdapter) Cancel(context.Context, agent.AdapterTask, agent.AdapterSandbox) error {
	a.cancelled++
	if a.onCancel != nil {
		a.onCancel()
	}
	return a.cancelErr
}
func (a *scriptedAdapter) Observe(context.Context, agent.AdapterTask, agent.AdapterSandbox) (agent.AdapterObservation, string, error) {
	a.observed++
	if a.observeErr != nil {
		return "", "", a.observeErr
	}
	o := a.observations[0]
	if len(a.observations) > 1 {
		a.observations = a.observations[1:]
	}
	return o, string(o), nil
}

func runScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func reconciler(t *testing.T, adapter agent.AdapterLifecycle, objects ...client.Object) *RunReconciler {
	t.Helper()
	s := runScheme(t)
	templates := make(map[types.NamespacedName]bool)
	for _, object := range objects {
		if template, ok := object.(*platformv1alpha1.EnvironmentTemplate); ok {
			templates[types.NamespacedName{Namespace: template.Namespace, Name: template.Name}] = true
		}
	}
	for _, object := range objects {
		env, ok := object.(*platformv1alpha1.Environment)
		if ok && (env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady || env.Status.Phase == platformv1alpha1.EnvironmentPhaseRunning) {
			if env.Spec.TemplateRef == "" {
				env.Spec.TemplateRef = "default"
			}
			env.Status.ExecutionGeneration = 1
			applyEnvironmentStatus(env, env.Status.Phase, env.Status.PodName, env.Status.Endpoints.Sandboxd, "SandboxdReady", "sandboxd is ready", env.Status.LastActiveAt)
			for _, candidate := range objects {
				if tmpl, ok := candidate.(*platformv1alpha1.EnvironmentTemplate); ok && tmpl.Name == env.Spec.TemplateRef && tmpl.Namespace == env.Namespace && env.Labels[warmPoolLabel] != "" {
					setTestProvisioningSnapshot(env, tmpl, nil)
				}
			}
		}
	}
	// Most lifecycle tests predate the ownership/endpoint security fences. Give
	// their intentionally valid fixtures the exact current Run owner and a
	// reachable endpoint; mismatch tests construct their reconciler directly.
	for _, object := range objects {
		run, ok := object.(*platformv1alpha1.Run)
		if !ok || run.Status.EnvironmentRef == nil {
			continue
		}
		for _, candidate := range objects {
			env, ok := candidate.(*platformv1alpha1.Environment)
			if !ok || env.Name != run.Status.EnvironmentRef.Name || env.UID != run.Status.EnvironmentRef.UID {
				continue
			}
			if run.Status.EnvironmentRef.Ownership == platformv1alpha1.EnvironmentOwnershipOwned {
				env.OwnerReferences = []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true)}}
			}
			if env.Status.Phase == platformv1alpha1.EnvironmentPhaseReady || env.Status.Phase == platformv1alpha1.EnvironmentPhaseRunning {
				env.Status.PodName = "env-" + env.Name
				env.Status.Endpoints.Sandboxd = "10.0.0.1:50051"
				if runAccepted(run) && run.Status.AcceptedEnvironmentExecutionGeneration == nil {
					acceptedExecutionGeneration := env.Status.ExecutionGeneration
					run.Status.AcceptedEnvironmentExecutionGeneration = &acceptedExecutionGeneration
				}
			}
		}
	}
	for _, object := range append([]client.Object(nil), objects...) {
		env, ok := object.(*platformv1alpha1.Environment)
		if !ok || !environmentReachable(env) {
			continue
		}
		key := types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.TemplateRef}
		var template *platformv1alpha1.EnvironmentTemplate
		if !templates[key] {
			template = &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, UID: types.UID(key.Name + "-uid"), Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "test/image", Size: "small"}}
			objects = append(objects, template)
			templates[key] = true
		} else {
			for _, candidate := range objects {
				if current, ok := candidate.(*platformv1alpha1.EnvironmentTemplate); ok && current.Name == key.Name && current.Namespace == key.Namespace {
					template = current
					break
				}
			}
		}
		if template != nil {
			setTestProvisioningSnapshot(env, template, nil)
		}
		objects = append(objects, runExecutionPod(env, types.UID("pod-"+env.Name), "10.0.0.1"))
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&platformv1alpha1.Run{}, &platformv1alpha1.Environment{}).WithObjects(objects...).Build()
	return &RunReconciler{Client: c, Scheme: s, Adapters: map[string]agent.AdapterLifecycle{"test": adapter}}
}

func runExecutionPod(env *platformv1alpha1.Environment, uid types.UID, podIP string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: env.Status.PodName, Namespace: env.Namespace, UID: uid,
			Annotations: map[string]string{executionGenerationAnnotation: fmt.Sprintf("%d", env.Status.ExecutionGeneration)},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name,
				UID: env.UID, Controller: ptr(true),
			}},
		},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever},
		Status: corev1.PodStatus{
			PodIP: podIP, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func runExecutionBackendObjects(t *testing.T, r *RunReconciler, env *platformv1alpha1.Environment) []client.Object {
	t.Helper()
	var current platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil {
		t.Fatal(err)
	}
	var template platformv1alpha1.EnvironmentTemplate
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: current.Namespace, Name: current.Spec.TemplateRef}, &template); err != nil {
		t.Fatal(err)
	}
	var pod corev1.Pod
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: current.Namespace, Name: current.Status.PodName}, &pod); err != nil {
		t.Fatal(err)
	}
	return []client.Object{current.DeepCopy(), template.DeepCopy(), pod.DeepCopy()}
}

func credentialProfileAndSecret(run *platformv1alpha1.Run, value []byte) (*platformv1alpha1.AgentCredentialProfile, *corev1.Secret) {
	profile := &platformv1alpha1.AgentCredentialProfile{
		ObjectMeta: metav1.ObjectMeta{Name: run.Spec.CredentialProfileRef, Namespace: run.Namespace, UID: "profile-uid"},
		Spec: platformv1alpha1.AgentCredentialProfileSpec{
			Adapter: run.Spec.Agent, CredentialType: platformv1alpha1.AgentCredentialTypeAPIKey,
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      platformv1alpha1.AgentCredentialSecretName(profile.UID),
			Namespace: profile.Namespace,
			UID:       "secret-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "AgentCredentialProfile",
				Name: profile.Name, UID: profile.UID, Controller: ptr(true), BlockOwnerDeletion: ptr(true),
			}},
		},
		Type: platformv1alpha1.AgentCredentialAPIKeySecretType,
		Data: map[string][]byte{platformv1alpha1.AgentCredentialAPIKeySecretKey: append([]byte(nil), value...)},
	}
	return profile, secret
}

func reconcileRun(t *testing.T, r *RunReconciler, name string) platformv1alpha1.Run {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: name}, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestAllocationIsDeterministicAndRecoversBeforeStatus(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	// This models a successful create followed by a lost Run status update.
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-run-uid", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "r", UID: run.UID, Controller: ptr(true)}}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	for range 3 {
		reconcileRun(t, r, run.Name)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments, client.InNamespace("ns")); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 1 {
		t.Fatalf("environments = %d, want 1", len(environments.Items))
	}
	got := reconcileRun(t, r, run.Name)
	if got.Status.EnvironmentRef == nil || got.Status.EnvironmentRef.Name != "run-run-uid" || got.Status.EnvironmentRef.UID != "env-uid" {
		t.Fatalf("reference = %#v", got.Status.EnvironmentRef)
	}
	if !metav1.IsControlledBy(&environments.Items[0], run) {
		t.Fatal("environment lacks Run controller owner")
	}
}

func TestRepeatedAllocationCreatesOneOwnedEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	r := reconciler(t, &scriptedAdapter{}, run)
	for range 2 {
		ref, err := r.allocateEnvironment(context.Background(), run)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Name != "run-run-uid" {
			t.Fatalf("name = %q", ref.Name)
		}
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments); err != nil {
		t.Fatal(err)
	}
	if len(environments.Items) != 1 || !metav1.IsControlledBy(&environments.Items[0], run) {
		t.Fatalf("environments = %#v", environments.Items)
	}
}

func TestRunClaimsAndRecoversWarmEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", ProjectRef: "project", Agent: "test"}}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "template-uid"}}
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "warm-small-1",
			Namespace: "ns",
			UID:       "warm-uid",
			Labels:    map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true),
			}},
		},
		Spec: platformv1alpha1.EnvironmentSpec{
			TemplateRef: "small",
			Services: []platformv1alpha1.EnvironmentServiceDeclaration{{
				Name:       "web",
				Revision:   1,
				Protocol:   platformv1alpha1.EnvironmentServiceProtocolHTTP,
				TargetPort: 3000,
				Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject,
				Readiness:  platformv1alpha1.EnvironmentServiceReadinessTCPConnect,
			}},
		},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	r := reconciler(t, &scriptedAdapter{}, run, template, warm)
	ref, err := r.allocateEnvironment(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Name != warm.Name || ref.UID != warm.UID || ref.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
		t.Fatalf("reference = %#v", ref)
	}
	var claimed platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &claimed); err != nil {
		t.Fatal(err)
	}
	if claimed.Status.ClaimedBy == nil || claimed.Status.ClaimedBy.UID != run.UID || claimed.Status.LastActiveAt == nil || claimed.Status.Phase != platformv1alpha1.EnvironmentPhaseSetup || claimed.Status.PodName != "" || claimed.Status.Endpoints.Sandboxd != "" {
		t.Fatalf("claim status = %#v", claimed.Status)
	}
	if claimed.Spec.ProjectRef != run.Spec.ProjectRef || len(claimed.Spec.Services) != 1 || claimed.Spec.Services[0] != warm.Spec.Services[0] || claimed.Labels[warmPoolLabel] != "" || metav1.GetControllerOf(&claimed) != nil {
		t.Fatalf("promoted environment = %#v", claimed)
	}
	recovered, err := r.recoverEnvironmentReference(context.Background(), run)
	if err != nil || recovered == nil || recovered.UID != warm.UID || recovered.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
		t.Fatalf("recovered = %#v, error = %v", recovered, err)
	}
}

func TestWarmPromotionWithdrawsReadinessBeforeAdapterAcceptance(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", ProjectRef: "project", Agent: "test"}}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "template-uid"}}
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "warm-small-1", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true)}},
		},
		Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-warm-small-1", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		},
	}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, template, warm)
	got := reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating || got.Status.EnvironmentRef == nil {
		t.Fatalf("Run status = %#v, want allocated but not ready", got.Status)
	}
	got = reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating || adapter.accepted != 0 {
		t.Fatalf("state = %s, adapter accepts = %d", got.Status.State, adapter.accepted)
	}
}

func TestWarmClaimRequiresExactCurrentTemplateOwner(t *testing.T) {
	for _, test := range []struct {
		name       string
		apiVersion string
		kind       string
		ownerName  string
		ownerUID   types.UID
		wantClaim  bool
	}{
		{name: "exact owner", apiVersion: platformv1alpha1.GroupVersion.String(), kind: "EnvironmentTemplate", ownerName: "small", ownerUID: "current-template", wantClaim: true},
		{name: "wrong API version", apiVersion: "swe.dev/v1", kind: "EnvironmentTemplate", ownerName: "small", ownerUID: "current-template"},
		{name: "wrong kind", apiVersion: platformv1alpha1.GroupVersion.String(), kind: "Project", ownerName: "small", ownerUID: "current-template"},
		{name: "wrong name", apiVersion: platformv1alpha1.GroupVersion.String(), kind: "EnvironmentTemplate", ownerName: "other", ownerUID: "current-template"},
		{name: "recreated same-name template", apiVersion: platformv1alpha1.GroupVersion.String(), kind: "EnvironmentTemplate", ownerName: "small", ownerUID: "old-template"},
	} {
		t.Run(test.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", ProjectRef: "project", Agent: "test"}}
			template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "current-template"}}
			warm := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{
					Name: "warm-small", Namespace: "ns", UID: "warm-uid", ResourceVersion: "1", Labels: map[string]string{warmPoolLabel: "small"},
					OwnerReferences: []metav1.OwnerReference{{APIVersion: test.apiVersion, Kind: test.kind, Name: test.ownerName, UID: test.ownerUID, Controller: ptr(true)}},
				},
				Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
				Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
			}
			r := reconciler(t, &scriptedAdapter{}, run, template, warm)
			ref, err := r.claimWarmEnvironment(context.Background(), run, template.Name)
			if err != nil {
				t.Fatal(err)
			}
			if (ref != nil) != test.wantClaim {
				t.Fatalf("claim reference = %#v, wantClaim = %t", ref, test.wantClaim)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &retained); err != nil {
				t.Fatal(err)
			}
			if test.wantClaim {
				if retained.Status.ClaimedBy == nil || retained.Status.ClaimedBy.UID != run.UID || retained.Labels[warmPoolLabel] != "" {
					t.Fatalf("exact-owned member was not claimed and promoted: %#v", retained)
				}
			} else if retained.Status.ClaimedBy != nil || retained.Labels[warmPoolLabel] != template.Name || retained.ResourceVersion == "" {
				t.Fatalf("ineligible member was mutated: %#v", retained)
			}
		})
	}
}

func TestWarmClaimRecoveryRejectsRecreatedTemplateOwner(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "new-template"}}
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "warm-small", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "old-template", Controller: ptr(true)}},
		},
		Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase: platformv1alpha1.EnvironmentPhaseReady, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
		},
	}
	r := reconciler(t, &scriptedAdapter{}, run, template, warm)
	recovered, err := r.recoverEnvironmentReference(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != nil {
		t.Fatalf("recovered old template incarnation: %#v", recovered)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Status.ClaimedBy == nil || retained.Status.ClaimedBy.UID != run.UID || retained.Labels[warmPoolLabel] != template.Name {
		t.Fatalf("recovery mutated rejected member: %#v", retained)
	}
}

func TestWarmClaimRechecksTemplateIncarnationBeforePromotion(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "old-template"}}
	replacement := template.DeepCopy()
	replacement.UID = "new-template"
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "warm-small", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: template.Name, UID: template.UID, Controller: ptr(true)}},
		},
		Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: template.Name},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	r := reconciler(t, &scriptedAdapter{}, run, template, warm)
	reads := 0
	r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey, object client.Object, _ ...client.GetOption) error {
			if key != client.ObjectKeyFromObject(template) {
				return fmt.Errorf("unexpected live read for %s", key)
			}
			reads++
			if reads == 1 {
				template.DeepCopyInto(object.(*platformv1alpha1.EnvironmentTemplate))
			} else {
				replacement.DeepCopyInto(object.(*platformv1alpha1.EnvironmentTemplate))
			}
			return nil
		},
	})

	ref, err := r.claimWarmEnvironment(context.Background(), run, template.Name)
	if err != nil {
		t.Fatal(err)
	}
	if ref != nil || reads != 2 {
		t.Fatalf("claim = %#v, live Template reads = %d", ref, reads)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &retained); err != nil {
		t.Fatal(err)
	}
	owner := metav1.GetControllerOf(&retained)
	if retained.Status.ClaimedBy != nil || retained.Labels[warmPoolLabel] != template.Name || owner == nil || owner.UID != template.UID {
		t.Fatalf("Template replacement donated or mutated old member: %#v", retained)
	}
}

func TestConcurrentWarmClaimPreservesResourceVersionExclusivity(t *testing.T) {
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "template-uid"}}
	warm := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "warm-small", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: template.Name, UID: template.UID, Controller: ptr(true)}},
		},
		Spec:   platformv1alpha1.EnvironmentSpec{TemplateRef: template.Name},
		Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	runs := []*platformv1alpha1.Run{
		{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", UID: "a-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: template.Name, Agent: "test"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns", UID: "b-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: template.Name, Agent: "test"}},
	}
	r := reconciler(t, &scriptedAdapter{}, template, warm, runs[0], runs[1])

	type result struct {
		ref *platformv1alpha1.RunEnvironmentReference
		err error
	}
	results := make(chan result, len(runs))
	var wg sync.WaitGroup
	for _, run := range runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref, err := r.claimWarmEnvironment(context.Background(), run, template.Name)
			results <- result{ref: ref, err: err}
		}()
	}
	wg.Wait()
	close(results)

	winners := 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.ref != nil {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("warm claim winners = %d, want 1", winners)
	}
	var claimed platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &claimed); err != nil {
		t.Fatal(err)
	}
	if claimed.Status.ClaimedBy == nil || (claimed.Status.ClaimedBy.UID != runs[0].UID && claimed.Status.ClaimedBy.UID != runs[1].UID) || claimed.Labels[warmPoolLabel] != "" {
		t.Fatalf("claimed warm Environment = %#v", claimed)
	}
}

func TestClaimsAreExclusiveUIDFencedAndReleasedSafely(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "new"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env"}, Status: platformv1alpha1.EnvironmentStatus{ClaimedBy: &platformv1alpha1.RunReference{Name: "r", UID: "old"}}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	if _, err := r.allocateEnvironment(context.Background(), run); err == nil {
		t.Fatal("stale same-name claim was accepted")
	}
	var stored platformv1alpha1.Environment
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if err := r.releaseClaim(context.Background(), run, &stored); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if stored.Status.ClaimedBy == nil || stored.Status.ClaimedBy.UID != "old" {
		t.Fatal("non-matching claim was released")
	}
	stored.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "r", UID: "new"}
	if err := r.Status().Update(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseClaim(context.Background(), run, &stored); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored)
	if stored.Status.ClaimedBy != nil {
		t.Fatal("matching claim was not released")
	}
}

func TestConcurrentClaimHasOneWinner(t *testing.T) {
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env"}}
	runA := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", UID: "a-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: env.Name, Agent: "test"}}
	runB := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns", UID: "b-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: env.Name, Agent: "test"}}
	r := reconciler(t, &scriptedAdapter{}, env, runA, runB)
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, run := range []*platformv1alpha1.Run{runA, runB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.allocateEnvironment(context.Background(), run)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful claims = %d, want 1", successes)
	}
	var stored platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.ClaimedBy == nil || (stored.Status.ClaimedBy.UID != runA.UID && stored.Status.ClaimedBy.UID != runB.UID) {
		t.Fatalf("claim = %#v", stored.Status.ClaimedBy)
	}
}

func TestRunEnvironmentWatchIgnoresOnlyActivityUpdates(t *testing.T) {
	oldEnvironment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	activity := oldEnvironment.DeepCopy()
	now := metav1.Now()
	activity.Status.LastActiveAt = &now
	if runRelevantEnvironmentUpdate(oldEnvironment, activity) {
		t.Fatal("lastActiveAt-only update would feed back into Run reconciliation")
	}
	activityIntent := activity.DeepCopy()
	activityIntent.Generation++
	activityIntent.Spec.Lifecycle.Activity = []platformv1alpha1.EnvironmentActivityRequest{{
		Source:                      platformv1alpha1.EnvironmentActivitySourceTerminal,
		EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "terminal-1", EnvironmentUID: activityIntent.UID},
	}}
	if runRelevantEnvironmentUpdate(activity, activityIntent) {
		t.Fatal("activity-intent generation would feed back into Run reconciliation")
	}
	activityReceipt := activityIntent.DeepCopy()
	activityReceipt.Status.Lifecycle.ActivityReceipts = []platformv1alpha1.EnvironmentActivityReceipt{{Source: platformv1alpha1.EnvironmentActivitySourceTerminal, RequestID: "terminal-1"}}
	activityReceipt.Status.LastActiveAt = &now
	if runRelevantEnvironmentUpdate(activityIntent, activityReceipt) {
		t.Fatal("activity receipt would feed back into Run reconciliation")
	}
	activityMetadata := activity.DeepCopy()
	activityMetadata.Annotations = map[string]string{"lifecycle.swe.dev/activity-terminal": `{"source":"Terminal"}`}
	if runRelevantEnvironmentUpdate(activity, activityMetadata) {
		t.Fatal("metadata activity intent would feed back into Run reconciliation")
	}
	claim := activity.DeepCopy()
	claim.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: "r", UID: "run-uid"}
	if !runRelevantEnvironmentUpdate(activity, claim) {
		t.Fatal("claim update was filtered from Run reconciliation")
	}
	recovery := activity.DeepCopy()
	recovery.Status.Recovery.Attempts = 1
	if !runRelevantEnvironmentUpdate(activity, recovery) {
		t.Fatal("pod recovery update was filtered from Run reconciliation")
	}
	spec := activity.DeepCopy()
	spec.Generation++
	spec.Spec.Paused = true
	if !runRelevantEnvironmentUpdate(activity, spec) {
		t.Fatal("spec update was filtered from Run reconciliation")
	}
}

func TestAcceptedRunIgnoresActivityWithoutRereadingCredentials(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       platformv1alpha1.RunState
		observation agent.AdapterObservation
	}{
		{name: "running", state: platformv1alpha1.RunStateRunning, observation: agent.AdapterObservationRunning},
		{name: "needs input", state: platformv1alpha1.RunStateNeedsInput, observation: agent.AdapterObservationNeedsInput},
	} {
		t.Run(test.name, func(t *testing.T) {
			acceptedEpoch := int64(0)
			acceptedExecutionGeneration := int64(1)
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}},
				Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "removed-profile"},
				Status: platformv1alpha1.RunStatus{
					State:                                  test.state,
					AcceptedEnvironmentEpoch:               &acceptedEpoch,
					AcceptedEnvironmentExecutionGeneration: &acceptedExecutionGeneration,
					EnvironmentRef:                         &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
					CredentialProfileRef:                   &platformv1alpha1.RunCredentialProfileReference{Name: "removed-profile", UID: "removed-profile-uid"},
					Conditions:                             []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted"}},
				},
			}
			environment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid", Generation: 1}}
			environment.Status.ExecutionGeneration = 1
			applyEnvironmentStatus(environment, platformv1alpha1.EnvironmentPhaseReady, "env-e", "10.0.0.1:50051", "SandboxdReady", "ready", nil)
			adapter := &scriptedAdapter{observations: []agent.AdapterObservation{test.observation}}
			r := reconciler(t, adapter, run, environment)
			credentialReads := 0
			r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, delegate client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
				if _, ok := object.(*platformv1alpha1.Environment); ok {
					return delegate.Get(ctx, key, object, options...)
				}
				credentialReads++
				return apierrors.NewNotFound(platformv1alpha1.GroupVersion.WithResource("agentcredentialprofiles").GroupResource(), "removed-profile")
			}})

			if err := lifecycle.RecordActivity(context.Background(), r.Client, lifecycle.CaptureExecutionFence(environment), platformv1alpha1.EnvironmentActivitySourceTerminal, "terminal-1"); err != nil {
				t.Fatal(err)
			}
			var activity platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &activity); err != nil {
				t.Fatal(err)
			}
			if activity.Generation != environment.Generation || activity.Status.ObservedGeneration != activity.Generation || !platformv1alpha1.IsEnvironmentReady(&activity) {
				t.Fatalf("activity exposed stale environment status: generation=%d status=%#v", activity.Generation, activity.Status)
			}
			// A pre-migration publisher can still advance generation through the
			// legacy spec slot. Accepted work must remain stable while the
			// Environment controller consumes and converges that generation.
			activity.Generation++
			activity.Spec.Lifecycle.Activity = []platformv1alpha1.EnvironmentActivityRequest{{
				Source:                      platformv1alpha1.EnvironmentActivitySourceTerminal,
				EnvironmentLifecycleRequest: platformv1alpha1.EnvironmentLifecycleRequest{ID: "legacy-terminal-1", EnvironmentUID: activity.UID},
			}}
			if err := r.Update(context.Background(), &activity); err != nil {
				t.Fatal(err)
			}

			got := reconcileRun(t, r, run.Name)
			if got.Status.State != test.state || got.Status.AcceptedEnvironmentEpoch == nil || *got.Status.AcceptedEnvironmentEpoch != 0 || adapter.accepted != 0 || credentialReads != 0 {
				t.Fatalf("activity reconcile = state %s, acceptances %d, credential reads %d", got.Status.State, adapter.accepted, credentialReads)
			}
		})
	}
}

func TestExplicitClaimContentionFailsPermanently(t *testing.T) {
	loser := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "loser", Namespace: "ns", UID: "loser-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		ClaimedBy: &platformv1alpha1.RunReference{Name: "winner", UID: "winner-uid"},
	}}
	r := reconciler(t, &scriptedAdapter{}, loser, env)
	reconcileRun(t, r, loser.Name) // finalizer
	failed := reconcileRun(t, r, loser.Name)
	if failed.Status.State != platformv1alpha1.RunStateFailed || failed.Status.EnvironmentRef != nil {
		t.Fatalf("contending Run status = %#v", failed.Status)
	}
	var released platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil {
		t.Fatal(err)
	}
	released.Status.ClaimedBy = nil
	if err := r.Status().Update(context.Background(), &released); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, loser.Name)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil {
		t.Fatal(err)
	}
	if released.Status.ClaimedBy != nil {
		t.Fatalf("failed loser later claimed released Environment: %#v", released.Status.ClaimedBy)
	}
}

func TestCancelBeforeAllocationCreatesNothing(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", Cancel: true}}
	r := reconciler(t, &scriptedAdapter{}, run)
	got := reconcileRun(t, r, "r")
	var list platformv1alpha1.EnvironmentList
	_ = r.List(context.Background(), &list)
	if got.Status.State != platformv1alpha1.RunStateCancelled || len(list.Items) != 0 {
		t.Fatalf("state=%s environments=%d", got.Status.State, len(list.Items))
	}
}

func TestCancelRecoversAllocationCreatedBeforeStatus(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", Cancel: true}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-uid", Namespace: "ns", UID: "euid", OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true)}}}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	got := reconcileRun(t, r, run.Name)
	if got.Status.EnvironmentRef == nil || got.Status.EnvironmentRef.UID != env.UID || got.Status.State != platformv1alpha1.RunStateAllocating {
		t.Fatalf("status = %#v, want recovered allocation", got.Status)
	}
	got = reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
}

func TestTerminalCleanupPausesOwnedEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateSucceeded, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}}
	a := &scriptedAdapter{}
	r := reconciler(t, a, run, env)
	reconcileRun(t, r, "r")
	var got platformv1alpha1.Environment
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), &got)
	if got.Spec.Lifecycle.Suspend == nil || a.cancelled != 0 {
		t.Fatalf("suspend=%#v cancels=%d", got.Spec.Lifecycle.Suspend, a.cancelled)
	}
}

func TestTerminalClaimReleaseRemainsReleasedAcrossReconcileAndRestart(t *testing.T) {
	for _, state := range []platformv1alpha1.RunState{
		platformv1alpha1.RunStateSucceeded,
		platformv1alpha1.RunStateFailed,
		platformv1alpha1.RunStateCancelled,
	} {
		t.Run(string(state), func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Generation: 1}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				State:          state,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue, Reason: "EnvironmentReady", Message: "sandboxd is ready", ObservedGeneration: 1}},
			}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: platformv1alpha1.EnvironmentPhaseReady, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, env)

			for range 2 {
				got := reconcileRun(t, r, run.Name)
				assertTerminalEnvironmentCondition(t, got, state, "EnvironmentReleased", "claimed environment was released")
				if got.Status.EnvironmentRef == nil || got.Status.EnvironmentRef.Name != env.Name || got.Status.EnvironmentRef.UID != env.UID || got.Status.EnvironmentRef.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
					t.Fatalf("historical Environment reference = %#v", got.Status.EnvironmentRef)
				}
			}
			var released platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil || released.Status.ClaimedBy != nil || adapter.cancelled != 0 {
				t.Fatalf("released Environment = %#v, cancellations = %d, error = %v", released, adapter.cancelled, err)
			}

			if err := r.Delete(context.Background(), &released); err != nil {
				t.Fatal(err)
			}
			replacement := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: env.Name, Namespace: env.Namespace, UID: "replacement-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				ClaimedBy: &platformv1alpha1.RunReference{Name: "other", UID: "other-run-uid"},
			}}
			if err := r.Create(context.Background(), replacement); err != nil {
				t.Fatal(err)
			}
			if err := r.Status().Update(context.Background(), replacement); err != nil {
				t.Fatal(err)
			}

			restarted := &RunReconciler{Client: r.Client, Scheme: r.Scheme, Adapters: map[string]agent.AdapterLifecycle{"test": adapter}}
			got := reconcileRun(t, restarted, run.Name)
			assertTerminalEnvironmentCondition(t, got, state, "EnvironmentReleased", "claimed environment was released")
			var retainedReplacement platformv1alpha1.Environment
			if err := restarted.Get(context.Background(), client.ObjectKeyFromObject(replacement), &retainedReplacement); err != nil || retainedReplacement.UID != replacement.UID || retainedReplacement.Status.ClaimedBy == nil || retainedReplacement.Status.ClaimedBy.UID != "other-run-uid" {
				t.Fatalf("replacement Environment = %#v, error = %v", retainedReplacement, err)
			}
		})
	}
}

func TestTerminalClaimLossBeforeReleaseRemainsStrict(t *testing.T) {
	for _, tc := range []struct {
		name        string
		environment *platformv1alpha1.Environment
		wantMessage string
	}{
		{
			name:        "deleted",
			wantMessage: `environments.swe.dev "shared" not found`,
		},
		{
			name: "same-name UID replacement",
			environment: &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "replacement-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				ClaimedBy: &platformv1alpha1.RunReference{Name: "other", UID: "other-run-uid"},
			}},
			wantMessage: `allocated environment is gone or no longer claimed by this run: environment "shared" was replaced (wanted UID env-uid, got replacement-uid)`,
		},
		{
			name: "unexpected claim mismatch",
			environment: &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				ClaimedBy: &platformv1alpha1.RunReference{Name: "other", UID: "other-run-uid"},
			}},
			wantMessage: `allocated environment is gone or no longer claimed by this run: environment "shared" claim does not match run UID run-uid`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				State:          platformv1alpha1.RunStateSucceeded,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue, Reason: "EnvironmentReady", Message: "sandboxd is ready"}},
			}}
			objects := []client.Object{run}
			if tc.environment != nil {
				objects = append(objects, tc.environment)
			}
			r := reconciler(t, &scriptedAdapter{}, objects...)
			got := reconcileRun(t, r, run.Name)
			assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateSucceeded, "EnvironmentLost", tc.wantMessage)
			if tc.environment != nil {
				var retained platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(tc.environment), &retained); err != nil || retained.Spec.Paused || retained.Status.ClaimedBy == nil || retained.Status.ClaimedBy.UID != "other-run-uid" {
					t.Fatalf("foreign Environment = %#v, error = %v", retained, err)
				}
			}
		})
	}
}

func TestOwnedTerminalCleanupAndActiveEnvironmentLossRemainStrict(t *testing.T) {
	t.Run("terminal owned Environment deletion", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
			State:          platformv1alpha1.RunStateSucceeded,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		}}
		r := reconciler(t, &scriptedAdapter{}, run)
		got := reconcileRun(t, r, run.Name)
		assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateSucceeded, "EnvironmentLost", `environments.swe.dev "owned" not found`)
	})

	t.Run("terminal owned Environment fencing", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
			State:          platformv1alpha1.RunStateSucceeded,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		}}
		env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused}}
		r := reconciler(t, &scriptedAdapter{}, run, env)
		got := reconcileRun(t, r, run.Name)
		assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateSucceeded, "EnvironmentFenced", "owned environment is paused and fenced")
	})

	t.Run("active claimed Environment deletion", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
			State:          platformv1alpha1.RunStateRunning,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
			Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue}, {Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
		}}
		r := reconciler(t, &scriptedAdapter{}, run)
		got := reconcileRun(t, r, run.Name)
		assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateFailed, "EnvironmentLost", `environments.swe.dev "shared" not found`)
		got = reconcileRun(t, r, run.Name)
		assertTerminalEnvironmentCondition(t, got, platformv1alpha1.RunStateFailed, "EnvironmentLost", `environments.swe.dev "shared" not found`)
	})
}

func assertTerminalEnvironmentCondition(t *testing.T, run platformv1alpha1.Run, wantState platformv1alpha1.RunState, wantReason, wantMessage string) {
	t.Helper()
	condition := apiMeta.FindStatusCondition(run.Status.Conditions, runConditionEnvironmentReady)
	if run.Status.State != wantState || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != wantReason || condition.Message != wantMessage {
		t.Fatalf("Run status = %#v, EnvironmentReady = %#v; want state %s, reason %q, message %q", run.Status, condition, wantState, wantReason, wantMessage)
	}
}

func TestAdapterShapesPauseResumeAndStatus(t *testing.T) {
	for _, tc := range []struct {
		name         string
		observations []agent.AdapterObservation
		want         []platformv1alpha1.RunState
	}{
		{"foreground-process", []agent.AdapterObservation{agent.AdapterObservationRunning, agent.AdapterObservationSucceeded}, []platformv1alpha1.RunState{platformv1alpha1.RunStateRunning, platformv1alpha1.RunStateSucceeded}},
		{"foreground-process-failure", []agent.AdapterObservation{agent.AdapterObservationFailed}, []platformv1alpha1.RunState{platformv1alpha1.RunStateFailed}},
		{"service-events", []agent.AdapterObservation{agent.AdapterObservationRunning, agent.AdapterObservationNeedsInput}, []platformv1alpha1.RunState{platformv1alpha1.RunStateRunning, platformv1alpha1.RunStateNeedsInput}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAllocating, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
			a := &scriptedAdapter{observations: tc.observations}
			r := reconciler(t, a, run, env)
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady {
				t.Fatal(got.Status.State)
			}
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady || !acceptanceAttempted(&got) {
				t.Fatalf("acceptance marker state=%s attempted=%t", got.Status.State, acceptanceAttempted(&got))
			}
			if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateAdapterAccepted {
				t.Fatal(got.Status.State)
			}
			for _, want := range tc.want {
				if got := reconcileRun(t, r, "r"); got.Status.State != want {
					t.Fatalf("state=%s want=%s", got.Status.State, want)
				}
			}
			if tc.name == "service-events" {
				_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), env)
				env.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
				_ = r.Status().Update(context.Background(), env)
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStatePaused {
					t.Fatal(got.Status.State)
				}
				env.Status.Phase = platformv1alpha1.EnvironmentPhaseReady
				_ = r.Status().Update(context.Background(), env)
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateEnvironmentReady {
					t.Fatal(got.Status.State)
				}
				if got := reconcileRun(t, r, "r"); got.Status.State != platformv1alpha1.RunStateAdapterAccepted || a.accepted != 2 {
					t.Fatalf("resume state=%s accepts=%d", got.Status.State, a.accepted)
				}
			}
		})
	}
}

func TestPermanentAdapterAcceptanceRejectionFailsRun(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateEnvironmentReady,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	adapter := &scriptedAdapter{acceptErr: fmt.Errorf("%w: unsupported task configuration", agent.ErrAdapterTaskRejected)}
	r := reconciler(t, adapter, run, env)
	got := reconcileRun(t, r, run.Name)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionAdapterAccepted)
	if got.Status.State != platformv1alpha1.RunStateFailed || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "AdapterRejected" || !strings.Contains(condition.Message, "unsupported task configuration") || adapter.accepted != 1 {
		t.Fatalf("Run status = %#v, AdapterAccepted = %#v, accepts = %d", got.Status, condition, adapter.accepted)
	}
}

func TestDifferentAdapterShapesDriveSameLifecycleContract(t *testing.T) {
	readyRun := func(name string) *platformv1alpha1.Run {
		return &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name), Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateEnvironmentReady, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e-" + name, UID: types.UID("e-" + name), Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	}
	readyEnvironment := func(run *platformv1alpha1.Run) *platformv1alpha1.Environment {
		return &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: run.Status.EnvironmentRef.Name, Namespace: "ns", UID: run.Status.EnvironmentRef.UID}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	}

	foregroundRun := readyRun("foreground")
	foreground := &foregroundAdapter{process: agent.AdapterObservationRunning}
	foregroundReconciler := reconciler(t, foreground, foregroundRun, readyEnvironment(foregroundRun))
	reconcileRun(t, foregroundReconciler, foregroundRun.Name) // acceptance attempt marker
	reconcileRun(t, foregroundReconciler, foregroundRun.Name) // acceptance
	if got := reconcileRun(t, foregroundReconciler, foregroundRun.Name); got.Status.State != platformv1alpha1.RunStateRunning {
		t.Fatalf("foreground state = %s", got.Status.State)
	}
	foreground.process = agent.AdapterObservationSucceeded
	if got := reconcileRun(t, foregroundReconciler, foregroundRun.Name); got.Status.State != platformv1alpha1.RunStateSucceeded {
		t.Fatalf("foreground terminal state = %s", got.Status.State)
	}

	serviceRun := readyRun("service")
	service := &serviceAdapter{event: agent.AdapterObservationRunning}
	serviceReconciler := reconciler(t, service, serviceRun, readyEnvironment(serviceRun))
	reconcileRun(t, serviceReconciler, serviceRun.Name) // acceptance attempt marker
	reconcileRun(t, serviceReconciler, serviceRun.Name) // task acknowledgement
	reconcileRun(t, serviceReconciler, serviceRun.Name) // running event
	service.event = agent.AdapterObservationNeedsInput
	if got := reconcileRun(t, serviceReconciler, serviceRun.Name); got.Status.State != platformv1alpha1.RunStateNeedsInput || !service.serviceRunning {
		t.Fatalf("service state = %s, serviceRunning = %v", got.Status.State, service.serviceRunning)
	}
}

func TestNonterminalAdapterObservationSchedulesPolling(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAdapterAccepted, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	r := reconciler(t, &foregroundAdapter{process: agent.AdapterObservationRunning}, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != adapterPollInterval {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, adapterPollInterval)
	}
}

func TestEnvironmentReachableRequiresCurrentGenerationReady(t *testing.T) {
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Generation: 2}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration:  1,
		ExecutionGeneration: 1,
		Phase:               platformv1alpha1.EnvironmentPhaseReady,
		PodName:             "env-test",
		Endpoints:           platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		Conditions: []metav1.Condition{{
			Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
			ObservedGeneration: 1, Reason: "SandboxdReady", Message: "stale readiness",
		}},
	}}
	if environmentReachable(env) {
		t.Fatal("stale-generation Ready condition was accepted")
	}
	applyEnvironmentStatus(env, platformv1alpha1.EnvironmentPhaseReady, env.Status.PodName, env.Status.Endpoints.Sandboxd, "SandboxdReady", "current readiness", nil)
	if !environmentReachable(env) {
		t.Fatal("current-generation Ready condition was rejected")
	}
}

func TestRunDoesNotFailOnStaleEnvironmentFailure(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateAllocating,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid", Generation: 2}, Status: platformv1alpha1.EnvironmentStatus{
		ObservedGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseFailed,
	}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	got := reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateAllocating {
		t.Fatalf("Run state = %s, stale Environment failure became terminal", got.Status.State)
	}
}

func TestEnvironmentReadyConditionIsIndependentFromTaskOutcome(t *testing.T) {
	for _, tc := range []struct {
		name         string
		runState     platformv1alpha1.RunState
		envPhase     platformv1alpha1.EnvironmentPhase
		adapter      agent.AdapterLifecycle
		cancel       bool
		accepted     bool
		wantReady    metav1.ConditionStatus
		wantRunState platformv1alpha1.RunState
	}{
		{
			name: "adapter failure leaves ready Environment", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, adapter: &scriptedAdapter{observations: []agent.AdapterObservation{agent.AdapterObservationFailed}},
			accepted: true, wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "unavailable adapter leaves ready Environment", runState: platformv1alpha1.RunStateEnvironmentReady,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "Environment failure clears readiness", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseFailed, adapter: &scriptedAdapter{}, accepted: true, wantReady: metav1.ConditionFalse, wantRunState: platformv1alpha1.RunStateFailed,
		},
		{
			name: "cancellation leaves reachable Environment ready", runState: platformv1alpha1.RunStateAdapterAccepted,
			envPhase: platformv1alpha1.EnvironmentPhaseReady, adapter: &scriptedAdapter{}, cancel: true, accepted: true,
			wantReady: metav1.ConditionTrue, wantRunState: platformv1alpha1.RunStateCancelled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: tc.cancel}, Status: platformv1alpha1.RunStatus{
				State:          tc.runState,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionEnvironmentReady, Status: metav1.ConditionTrue}},
			}}
			if tc.accepted {
				run.Status.Conditions = append(run.Status.Conditions, metav1.Condition{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue})
			}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: tc.envPhase, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			if tc.envPhase == platformv1alpha1.EnvironmentPhaseReady {
				env.Status.PodName = "env-shared"
				env.Status.Endpoints.Sandboxd = "10.0.0.1:50051"
			}
			r := reconciler(t, tc.adapter, run, env)
			if tc.adapter == nil {
				r.Adapters = map[string]agent.AdapterLifecycle{}
			}
			got := reconcileRun(t, r, run.Name)
			condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionEnvironmentReady)
			if got.Status.State != tc.wantRunState || condition == nil || condition.Status != tc.wantReady {
				t.Fatalf("Run status = %#v, EnvironmentReady = %#v", got.Status, condition)
			}
			if tc.wantReady == metav1.ConditionTrue {
				cleaned := reconcileRun(t, r, run.Name)
				condition = apiMeta.FindStatusCondition(cleaned.Status.Conditions, runConditionEnvironmentReady)
				if condition == nil || condition.Status != metav1.ConditionFalse {
					t.Fatalf("EnvironmentReady after terminal claim release = %#v", condition)
				}
				var released platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &released); err != nil || released.Status.ClaimedBy != nil {
					t.Fatalf("terminal claimed Environment = %#v, error = %v", released, err)
				}
			}
		})
	}
}

func TestExplicitSuspendedClaimPublishesReasonScopedWakeDuringAllocationAndRecovery(t *testing.T) {
	for _, reason := range []platformv1alpha1.EnvironmentSuspensionReason{platformv1alpha1.EnvironmentSuspensionReasonIdle, platformv1alpha1.EnvironmentSuspensionReasonRequested} {
		for _, tc := range []struct {
			name     string
			recovery bool
		}{
			{name: "allocation"},
			{name: "claim-before-status recovery", recovery: true},
		} {
			t.Run(string(reason)+"/"+tc.name, func(t *testing.T) {
				run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
				env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
					Suspended: true, SuspensionReason: reason, Epoch: 1,
				}}}
				if tc.recovery {
					env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
					run.Finalizers = []string{runFinalizer}
				}
				r := reconciler(t, &scriptedAdapter{}, run, env)
				var ref *platformv1alpha1.RunEnvironmentReference
				var err error
				if tc.recovery {
					got := reconcileRun(t, r, run.Name)
					ref = got.Status.EnvironmentRef
				} else {
					ref, err = r.allocateEnvironment(context.Background(), run)
				}
				if err != nil || ref == nil || ref.Ownership != platformv1alpha1.EnvironmentOwnershipClaimed {
					t.Fatalf("reference = %#v, error = %v", ref, err)
				}
				var got platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
					t.Fatal(err)
				}
				if got.Spec.Lifecycle.Wake == nil || got.Spec.Lifecycle.Wake.EnvironmentUID != env.UID || got.Spec.Lifecycle.Wake.ExpectedSuspensionReason != reason || got.Status.ClaimedBy == nil || got.Status.ClaimedBy.UID != run.UID {
					t.Fatalf("claimed environment = %#v", got)
				}
			})
		}
	}
}

func TestExplicitHeldClaimFailsImmediatelyDuringAllocationAndRecovery(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "allocation"
		if recovery {
			name = "claim-before-status recovery"
		}
		t.Run(name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
			env := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"},
				Spec: platformv1alpha1.EnvironmentSpec{Lifecycle: platformv1alpha1.EnvironmentLifecycleSpec{Hold: &platformv1alpha1.EnvironmentHoldPolicy{
					Enabled: true, Revision: 3,
				}}},
			}
			if recovery {
				env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
				run.Finalizers = []string{runFinalizer}
			}
			r := reconciler(t, &scriptedAdapter{}, run, env)
			if recovery {
				got := reconcileRun(t, r, run.Name)
				if got.Status.State != platformv1alpha1.RunStateFailed || got.Status.EnvironmentRef != nil {
					t.Fatalf("recovered held claim Run status = %#v", got.Status)
				}
			} else {
				if ref, err := r.allocateEnvironment(context.Background(), run); ref != nil || !errors.Is(err, errExplicitEnvironmentHeld) {
					t.Fatalf("held allocation = (%#v, %v)", ref, err)
				}
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if retained.Status.ClaimedBy != nil || retained.Spec.Lifecycle.Wake != nil {
				t.Fatalf("held Environment was claimed or woken: %#v", retained)
			}
		})
	}
}

func TestExplicitNonWakeableSuspensionFailsBeforeRetainingClaim(t *testing.T) {
	for _, reason := range []platformv1alpha1.EnvironmentSuspensionReason{"", platformv1alpha1.EnvironmentSuspensionReasonHold} {
		for _, recovery := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recovery=%t", reason, recovery), func(t *testing.T) {
				run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{EnvironmentRef: "shared", Agent: "test"}}
				env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
					Suspended: true, SuspensionReason: reason, Epoch: 1,
				}}}
				if recovery {
					env.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
					run.Finalizers = []string{runFinalizer}
				}
				r := reconciler(t, &scriptedAdapter{}, run, env)
				if recovery {
					got := reconcileRun(t, r, run.Name)
					if got.Status.State != platformv1alpha1.RunStateFailed || got.Status.EnvironmentRef != nil {
						t.Fatalf("non-wakeable recovery status = %#v", got.Status)
					}
				} else if ref, err := r.allocateEnvironment(context.Background(), run); ref != nil || !errors.Is(err, errExplicitEnvironmentSuspensionNotWakeable) {
					t.Fatalf("non-wakeable allocation = (%#v, %v)", ref, err)
				}
				var retained platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
					t.Fatal(err)
				}
				if retained.Status.ClaimedBy != nil || retained.Spec.Lifecycle.Wake != nil {
					t.Fatalf("non-wakeable Environment retained traffic: status=%#v spec=%#v", retained.Status, retained.Spec.Lifecycle)
				}
			})
		}
	}
}

func TestCancellationWhilePausedRequiresNoAdapterRPC(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStatePaused,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
	reconcileRun(t, r, run.Name)
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("claimed Environment was deleted")
	}
	if retained.Status.ClaimedBy != nil || adapter.cancelled != 0 {
		t.Fatalf("claim = %#v, adapter cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
	}
}

func TestCancellationBypassesRepositoryCredentialIssuance(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true, RepositoryCredential: platformv1alpha1.RepositoryCredentialGitHubApp}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStatePaused,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	provider := &fakeRepositoryCredentialProvider{canonical: "https://github.com/acme/repo", issueErr: &repositorycredential.Error{Operation: "issue", Reason: "ProviderUnavailable", RetryAfter: time.Minute}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	r.RepositoryCredentials = provider
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
	if len(provider.issued) != 0 {
		t.Fatalf("cancellation issued repository credentials: %v", provider.issued)
	}
}

func TestLostAcceptanceStatusCancelsBeforeClaimRelease(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "cancel"
		if deleting {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				State:          platformv1alpha1.RunStateEnvironmentReady,
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
			}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
				ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, env)
			fault := &failAcceptedStatusClient{Client: r.Client, fail: true}
			r.Client = fault
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err == nil {
				t.Fatal("acceptance status update unexpectedly succeeded")
			}
			if adapter.accepted != 1 {
				t.Fatalf("acceptance calls = %d, want 1", adapter.accepted)
			}
			var storedRun platformv1alpha1.Run
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &storedRun); err != nil {
				t.Fatal(err)
			}
			if storedRun.Status.State != platformv1alpha1.RunStateEnvironmentReady || !acceptanceAttempted(&storedRun) || runAccepted(&storedRun) {
				t.Fatalf("stored status after lost update = %#v", storedRun.Status)
			}
			if deleting {
				if err := r.Delete(context.Background(), &storedRun); err != nil {
					t.Fatal(err)
				}
			} else {
				storedRun.Spec.Cancel = true
				if err := r.Update(context.Background(), &storedRun); err != nil {
					t.Fatal(err)
				}
			}
			adapter.onCancel = func() {
				var current platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil || current.Status.ClaimedBy == nil {
					t.Fatal("claim was released before uncertain acceptance was cancelled")
				}
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
				t.Fatal(err)
			}
			if !deleting {
				reconcileRun(t, r, run.Name)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if adapter.cancelled == 0 || retained.Status.ClaimedBy != nil {
				t.Fatalf("cancellations = %d, claim = %#v", adapter.cancelled, retained.Status.ClaimedBy)
			}
		})
	}
}

func TestAcceptedUnreachableClaimIsFencedBeforeCancellationCleanup(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateAdapterAccepted,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseFailed, PodName: "env-shared", ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if fencing.Spec.Lifecycle.Suspend == nil || fencing.Status.ClaimedBy == nil || adapter.cancelled != 0 {
		t.Fatalf("fencing Environment = %#v, cancellations = %d", fencing, adapter.cancelled)
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.Lifecycle.Suspended = true
	fencing.Status.PodName = ""
	fencing.Status.Endpoints = platformv1alpha1.EnvironmentEndpoints{}
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state after fence = %s", got.Status.State)
	}
	result, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("cleanup before suspend acknowledgement = (%#v, %v)", result, err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Status.ClaimedBy == nil {
		t.Fatalf("claim released before suspend acknowledgement: environment=%#v error=%v", fencing, err)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := r.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, run.Name)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	if fencing.Status.ClaimedBy != nil || adapter.cancelled != 0 {
		t.Fatalf("post-fence claim = %#v, cancellations = %d", fencing.Status.ClaimedBy, adapter.cancelled)
	}
}

func TestCancellationPendingRetainsRunAndClaim(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", Cancel: true}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateAdapterAccepted,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{cancelErr: agent.ErrAdapterCancellationPending}
	r := reconciler(t, adapter, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("pending cancellation = (%#v, %v)", result, err)
	}
	var retainedRun platformv1alpha1.Run
	var retainedEnv platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retainedEnv); err != nil {
		t.Fatal(err)
	}
	if retainedRun.Status.State != platformv1alpha1.RunStateAdapterAccepted || retainedEnv.Status.ClaimedBy == nil {
		t.Fatalf("pending cancellation released state/claim: %s/%#v", retainedRun.Status.State, retainedEnv.Status.ClaimedBy)
	}
	adapter.cancelErr = nil
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("completed cancellation state = %s", got.Status.State)
	}
}

func TestAcceptedUnreachableOwnedFinalizerWaitsForFence(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseCreating}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.finalize(context.Background(), run)
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	var retainedRun platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) || fencing.Spec.Lifecycle.Suspend == nil {
		t.Fatal("owned Run finalizer or fence request was not retained")
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.Lifecycle.Suspended = true
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := r.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.finalize(context.Background(), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || adapter.cancelled != 0 || !exactControllerOwner(&fencing, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) {
		t.Fatalf("owned Environment after fenced finalization = %#v, cancellations = %d", fencing, adapter.cancelled)
	}
}

func TestAcceptedUnreachableOwnedTerminalCleanupWaitsForFence(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateSucceeded,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed, PodName: "env-owned"}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("terminal fence request = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Spec.Lifecycle.Suspend == nil || adapter.cancelled != 0 {
		t.Fatalf("terminal fencing Environment = %#v, cancellations = %d, error = %v", fencing, adapter.cancelled, err)
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.Lifecycle.Suspended = true
	fencing.Status.PodName = ""
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := r.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Spec.Lifecycle.Suspend != nil || adapter.cancelled != 0 {
		t.Fatalf("terminal cleanup after fence = %#v, cancellations = %d, error = %v", fencing, adapter.cancelled, err)
	}
}

func TestOwnedEnvironmentFenceUsesNextSuspendSequence(t *testing.T) {
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid"},
		Status: platformv1alpha1.EnvironmentStatus{Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{
			Epoch: 1, LastSuspendRequestID: "environment/env-uid/fence/4", LastSuspendRequestSequence: 4,
		}},
	}
	r := reconciler(t, &scriptedAdapter{}, environment)
	result, err := r.requestEnvironmentFence(context.Background(), environment)
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("requestEnvironmentFence() = (%#v, %v)", result, err)
	}
	var updated platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Lifecycle.Suspend == nil || updated.Spec.Lifecycle.Suspend.ID != "environment/env-uid/fence/5" || updated.Spec.Lifecycle.Suspend.Sequence != 5 {
		t.Fatalf("next fence request = %#v", updated.Spec.Lifecycle.Suspend)
	}
	if _, err := r.requestEnvironmentFence(context.Background(), &updated); err != nil {
		t.Fatal(err)
	}
	var replay platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &replay); err != nil {
		t.Fatal(err)
	}
	if replay.Spec.Lifecycle.Suspend == nil || replay.Spec.Lifecycle.Suspend.ID != "environment/env-uid/fence/5" || replay.Spec.Lifecycle.Suspend.Sequence != 5 {
		t.Fatalf("retried fence request = %#v", replay.Spec.Lifecycle.Suspend)
	}
}

func TestFinalizeClaimedEnvironmentOrdersCancellationBeforeRelease(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	adapter.onCancel = func() {
		var current platformv1alpha1.Environment
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &current); err != nil || current.Status.ClaimedBy == nil {
			t.Fatal("claim was released before adapter cancellation")
		}
	}
	if result, err := r.finalize(context.Background(), run); err != nil || result.RequeueAfter != 0 {
		t.Fatalf("finalize = (%#v, %v)", result, err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("claimed Environment was deleted")
	}
	if retained.Status.ClaimedBy != nil || adapter.cancelled != 1 {
		t.Fatalf("claim = %#v, cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
	}
	var deletedRun platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &deletedRun); err == nil {
		if controllerutil.ContainsFinalizer(&deletedRun, runFinalizer) {
			t.Fatal("Run finalizer was not removed")
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

func TestFinalizeOwnedEnvironmentLeavesGCReference(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-owned", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"}}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if _, err := r.finalize(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("owned Environment was directly deleted instead of left to GC")
	}
	if !metav1.IsControlledBy(&retained, run) || adapter.cancelled != 1 {
		t.Fatalf("owner = %#v, cancellations = %d", metav1.GetControllerOf(&retained), adapter.cancelled)
	}
}

func TestFinalizePausedAndTransientlyUnreachableAcceptedClaims(t *testing.T) {
	for _, tc := range []struct {
		name        string
		phase       platformv1alpha1.EnvironmentPhase
		specPaused  bool
		wantRequeue bool
		wantClaim   bool
	}{
		{name: "paused is fenced", phase: platformv1alpha1.EnvironmentPhasePaused, specPaused: true},
		{name: "stale paused status is not fenced", phase: platformv1alpha1.EnvironmentPhasePaused, wantRequeue: true, wantClaim: true},
		{name: "setup is ambiguous", phase: platformv1alpha1.EnvironmentPhaseSetup, wantRequeue: true, wantClaim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := metav1.Now()
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
				EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
				Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
			}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: tc.specPaused}, Status: platformv1alpha1.EnvironmentStatus{Phase: tc.phase, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, env)
			result, err := r.finalize(context.Background(), run)
			if err != nil || (result.RequeueAfter != 0) != tc.wantRequeue {
				t.Fatalf("finalize = (%#v, %v)", result, err)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if (retained.Status.ClaimedBy != nil) != tc.wantClaim || adapter.cancelled != 0 {
				t.Fatalf("claim = %#v, cancellations = %d", retained.Status.ClaimedBy, adapter.cancelled)
			}
			if tc.wantRequeue {
				var retainedRun platformv1alpha1.Run
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) {
					t.Fatal("ambiguous accepted work did not retain Run finalizer")
				}
			}
		})
	}
}

func TestMissingAdapterCleanupFallsBackToBackendFence(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "removed-adapter"}, Status: platformv1alpha1.RunStatus{
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "shared", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-shared", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	r := reconciler(t, &scriptedAdapter{}, run, env)
	r.Adapters = map[string]agent.AdapterLifecycle{}
	result, err := r.finalize(context.Background(), run)
	if err != nil || result.RequeueAfter != adapterPollInterval {
		t.Fatalf("missing-adapter fence = (%#v, %v)", result, err)
	}
	var fencing platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil {
		t.Fatal(err)
	}
	var retainedRun platformv1alpha1.Run
	if fencing.Spec.Lifecycle.Suspend == nil || fencing.Status.ClaimedBy == nil || r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun) != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) {
		t.Fatal("missing adapter released ownership before backend fencing")
	}
	fencing.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	fencing.Status.Lifecycle.Suspended = true
	fencing.Status.PodName = ""
	fencing.Status.Endpoints = platformv1alpha1.EnvironmentEndpoints{}
	if err := r.Status().Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	fencing.Spec.Lifecycle.Suspend = nil
	if err := r.Update(context.Background(), &fencing); err != nil {
		t.Fatal(err)
	}
	if _, err := r.finalize(context.Background(), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &fencing); err != nil || fencing.Status.ClaimedBy != nil {
		t.Fatalf("fenced missing-adapter claim = %#v, error = %v", fencing.Status.ClaimedBy, err)
	}
}

func TestFinalizeRecoversLostClaimStatusAndOnlyReleasesMatchingUID(t *testing.T) {
	for _, tc := range []struct {
		name      string
		claimUID  types.UID
		wantClaim bool
	}{
		{name: "matching recovery", claimUID: "run-uid"},
		{name: "same-name replacement claim", claimUID: "other-run", wantClaim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := metav1.Now()
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test", EnvironmentRef: "shared"}}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: tc.claimUID}}}
			r := reconciler(t, &scriptedAdapter{}, run, env)
			if _, err := r.finalize(context.Background(), run); err != nil {
				t.Fatal(err)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
				t.Fatal(err)
			}
			if (retained.Status.ClaimedBy != nil) != tc.wantClaim {
				t.Fatalf("claim = %#v", retained.Status.ClaimedBy)
			}
			if !retained.Spec.Paused {
				t.Fatal("deletion recovery unexpectedly woke the paused Environment")
			}
		})
	}
}

func TestCancelRecoveryDoesNotWakePausedClaim(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", EnvironmentRef: "shared", Cancel: true}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "ns", UID: "env-uid"}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{
		Phase: platformv1alpha1.EnvironmentPhasePaused, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
	}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	reconcileRun(t, r, run.Name)
	if got := reconcileRun(t, r, run.Name); got.Status.State != platformv1alpha1.RunStateCancelled {
		t.Fatalf("state = %s, want Cancelled", got.Status.State)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal(err)
	}
	if !retained.Spec.Paused || adapter.cancelled != 0 {
		t.Fatalf("paused = %t, cancellations = %d", retained.Spec.Paused, adapter.cancelled)
	}
}

func TestFinalizeRecoversOwnedEnvironmentWithLostRunStatus(t *testing.T) {
	now := metav1.Now()
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now}, Spec: platformv1alpha1.RunSpec{Agent: "test", TemplateRef: "small"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "run-run-uid", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: run.Name, UID: run.UID, Controller: ptr(true),
	}}}, Spec: platformv1alpha1.EnvironmentSpec{Paused: true}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused}}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env)
	if _, err := r.finalize(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal("owned Environment should remain for API-server GC")
	}
	if !exactControllerOwner(&retained, platformv1alpha1.GroupVersion.String(), "Run", run.Name, run.UID) || adapter.cancelled != 0 {
		t.Fatalf("owner = %#v, cancellations = %d", metav1.GetControllerOf(&retained), adapter.cancelled)
	}
}

func TestCleanupRecoveryDoesNotPromoteWarmClaim(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "cancel"
		if deleting {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test", TemplateRef: "small", ProjectRef: "project", Cancel: !deleting}}
			template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: "ns", UID: "template-uid"}}
			if deleting {
				now := metav1.Now()
				run.DeletionTimestamp = &now
			}
			warm := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "warm-small-1", Namespace: "ns", UID: "warm-uid", Labels: map[string]string{warmPoolLabel: "small"}, OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "EnvironmentTemplate", Name: "small", UID: "template-uid", Controller: ptr(true),
			}}}, Spec: platformv1alpha1.EnvironmentSpec{TemplateRef: "small"}, Status: platformv1alpha1.EnvironmentStatus{
				Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-warm-small-1", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
				ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
			}}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, template, warm)
			if deleting {
				if _, err := r.finalize(context.Background(), run); err != nil {
					t.Fatal(err)
				}
			} else {
				reconcileRun(t, r, run.Name)
				reconcileRun(t, r, run.Name)
			}
			var retained platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(warm), &retained); err != nil {
				t.Fatal(err)
			}
			owner := metav1.GetControllerOf(&retained)
			if retained.Labels[warmPoolLabel] != "small" || owner == nil || owner.Kind != "EnvironmentTemplate" || owner.Name != "small" ||
				retained.Spec.ProjectRef != "" || retained.Status.ClaimedBy != nil || retained.Status.Phase != platformv1alpha1.EnvironmentPhaseReady || retained.Status.PodName == "" || retained.Status.Endpoints.Sandboxd == "" || adapter.cancelled != 0 {
				t.Fatalf("warm Environment was promoted during cleanup recovery: %#v, cancellations = %d", retained, adapter.cancelled)
			}
		})
	}
}

func TestOwnershipMismatchNeverCancelsOrMutatesEnvironment(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
		State:          platformv1alpha1.RunStateSucceeded,
		EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "owned", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
		Conditions:     []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue}},
	}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "ns", UID: "env-uid", OwnerReferences: []metav1.OwnerReference{{
		APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run", Name: "other", UID: run.UID, Controller: ptr(true),
	}}}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-owned", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"}}}
	s := runScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&platformv1alpha1.Run{}, &platformv1alpha1.Environment{}).WithObjects(run, env).Build()
	adapter := &scriptedAdapter{}
	r := &RunReconciler{Client: c, Scheme: s, Adapters: map[string]agent.AdapterLifecycle{"test": adapter}}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	var retained platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Spec.Paused || adapter.cancelled != 0 || metav1.GetControllerOf(&retained).Name != "other" {
		t.Fatalf("environment mutated or cancelled: %#v, cancellations = %d", retained, adapter.cancelled)
	}
}

func TestCredentialProfileBindsExactUIDBeforeAllocation(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"},
		Spec:       platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile"},
	}
	profile, secret := credentialProfileAndSecret(run, []byte("!!BOUND-KEY-FIXTURE!!"))
	r := reconciler(t, &scriptedAdapter{}, run, profile, secret)

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || !result.Requeue {
		t.Fatalf("binding reconcile = (%#v, %v)", result, err)
	}
	var bound platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &bound); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(bound.Status.Conditions, runConditionCredentialProfileBound)
	if bound.Status.CredentialProfileRef == nil || bound.Status.CredentialProfileRef.Name != profile.Name || bound.Status.CredentialProfileRef.UID != profile.UID ||
		condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Bound" {
		t.Fatalf("binding status = %#v, condition = %#v", bound.Status.CredentialProfileRef, condition)
	}
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments, client.InNamespace(run.Namespace)); err != nil || len(environments.Items) != 0 {
		t.Fatalf("binding allocated environments = %d, error = %v", len(environments.Items), err)
	}
}

type credentiallessOnlyAdapter struct{ scriptedAdapter }

func (*credentiallessOnlyAdapter) SupportsCredentialProfiles() bool { return false }

type countingReader struct{ reads int }

func (r *countingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	r.reads++
	return errors.New("unexpected credential read")
}
func (r *countingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	r.reads++
	return errors.New("unexpected credential list")
}

func TestUnsupportedAdapterCredentialProfileFailsBeforeReadsOrAllocation(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "must-not-read"}}
	r := reconciler(t, &credentiallessOnlyAdapter{}, run)
	reader := &countingReader{}
	r.APIReader = reader
	got := reconcileRun(t, r, run.Name)
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(context.Background(), &environments, client.InNamespace(run.Namespace)); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionCredentialProfileBound)
	if got.Status.State != platformv1alpha1.RunStateFailed || got.Status.EnvironmentRef != nil || len(environments.Items) != 0 || reader.reads != 0 || condition == nil || condition.Reason != "CredentialProfilesUnsupported" {
		t.Fatalf("status=%#v environments=%d reads=%d", got.Status, len(environments.Items), reader.reads)
	}
}

func TestCredentiallessRunPreservesAllocationBehavior(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test"}}
	r := reconciler(t, &scriptedAdapter{}, run)
	reconcileRun(t, r, run.Name) // Finalizer.
	got := reconcileRun(t, r, run.Name)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionCredentialProfileBound)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Credentialless" || got.Status.EnvironmentRef == nil {
		t.Fatalf("credentialless status = %#v", got.Status)
	}
}

func TestCredentialBindingWaitsBoundedlyAndRecovers(t *testing.T) {
	t.Run("profile", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile"}}
		r := reconciler(t, &scriptedAdapter{}, run)
		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
		if err != nil || result.RequeueAfter <= 0 {
			t.Fatalf("missing profile = (%#v, %v)", result, err)
		}
		var waiting platformv1alpha1.Run
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &waiting); err != nil {
			t.Fatal(err)
		}
		condition := apiMeta.FindStatusCondition(waiting.Status.Conditions, runConditionCredentialProfileBound)
		if condition == nil || condition.Reason != "ProfileNotFound" || waiting.Status.EnvironmentRef != nil {
			t.Fatalf("waiting status = %#v", waiting.Status)
		}
		profile, secret := credentialProfileAndSecret(&waiting, []byte("!!RECOVERED-KEY-FIXTURE!!"))
		if err := r.Create(context.Background(), profile); err != nil {
			t.Fatal(err)
		}
		if err := r.Create(context.Background(), secret); err != nil {
			t.Fatal(err)
		}
		got := reconcileRun(t, r, run.Name)
		if got.Status.CredentialProfileRef == nil || got.Status.CredentialProfileRef.UID != profile.UID || got.Status.EnvironmentRef != nil {
			t.Fatalf("recovered binding = %#v", got.Status)
		}
	})

	t.Run("secret timeout", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile"}}
		profile, _ := credentialProfileAndSecret(run, nil)
		r := reconciler(t, &scriptedAdapter{}, run, profile)
		reconcileRun(t, r, run.Name) // Bind the profile UID.
		waiting := reconcileRun(t, r, run.Name)
		condition := apiMeta.FindStatusCondition(waiting.Status.Conditions, runConditionCredentialProfileBound)
		if condition == nil || condition.Reason != "SecretNotReady" || waiting.Status.EnvironmentRef != nil {
			t.Fatalf("secret waiting status = %#v", waiting.Status)
		}
		condition.LastTransitionTime = metav1.NewTime(time.Now().Add(-credentialReadyTimeout - time.Second))
		if err := r.Status().Update(context.Background(), &waiting); err != nil {
			t.Fatal(err)
		}
		failed := reconcileRun(t, r, run.Name)
		condition = apiMeta.FindStatusCondition(failed.Status.Conditions, runConditionCredentialProfileBound)
		if failed.Status.State != platformv1alpha1.RunStateFailed || condition == nil || condition.Reason != "SecretNotReady" || failed.Status.EnvironmentRef != nil {
			t.Fatalf("timed out status = %#v", failed.Status)
		}
	})
}

func TestCredentialBindingRejectsAdapterTypeAndReplacement(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*platformv1alpha1.Run, *platformv1alpha1.AgentCredentialProfile)
		reason string
	}{
		{name: "adapter mismatch", mutate: func(_ *platformv1alpha1.Run, profile *platformv1alpha1.AgentCredentialProfile) {
			profile.Spec.Adapter = "other"
		}, reason: "AdapterMismatch"},
		{name: "unsupported type", mutate: func(_ *platformv1alpha1.Run, profile *platformv1alpha1.AgentCredentialProfile) {
			profile.Spec.CredentialType = "FutureType"
		}, reason: "UnsupportedCredentialType"},
		{name: "same-name replacement", mutate: func(run *platformv1alpha1.Run, _ *platformv1alpha1.AgentCredentialProfile) {
			run.Status.CredentialProfileRef = &platformv1alpha1.RunCredentialProfileReference{Name: run.Spec.CredentialProfileRef, UID: "old-profile-uid"}
		}, reason: "ProfileReplaced"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile"}}
			profile, secret := credentialProfileAndSecret(run, []byte("!!REJECTED-KEY-FIXTURE!!"))
			test.mutate(run, profile)
			r := reconciler(t, &scriptedAdapter{}, run, profile, secret)
			got := reconcileRun(t, r, run.Name)
			condition := apiMeta.FindStatusCondition(got.Status.Conditions, runConditionCredentialProfileBound)
			if got.Status.State != platformv1alpha1.RunStateFailed || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != test.reason || got.Status.EnvironmentRef != nil {
				t.Fatalf("rejected status = %#v", got.Status)
			}
		})
	}
}

func TestResolveCredentialRejectsMalformedForeignAndWrongNamespaceSecrets(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*platformv1alpha1.AgentCredentialProfile, *corev1.Secret)
		reason string
	}{
		{name: "wrong type", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Type = corev1.SecretTypeOpaque
		}, reason: "MalformedSecret"},
		{name: "extra key", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data["extra"] = []byte("x")
		}, reason: "MalformedSecret"},
		{name: "empty", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = nil
		}, reason: "MalformedSecret"},
		{name: "invalid utf8", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = []byte{0xff}
		}, reason: "MalformedSecret"},
		{name: "nul", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = []byte{'x', 0}
		}, reason: "MalformedSecret"},
		{name: "oversize", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = make([]byte, platformv1alpha1.AgentCredentialAPIKeyMaxBytes+1)
		}, reason: "MalformedSecret"},
		{name: "foreign owner", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.OwnerReferences[0].UID = "foreign-profile-uid"
		}, reason: "ForeignSecret"},
		{name: "extra owner", mutate: func(_ *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			secret.OwnerReferences = append(secret.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "other", UID: "other"})
		}, reason: "ForeignSecret"},
		{name: "wrong namespace", mutate: func(profile *platformv1alpha1.AgentCredentialProfile, secret *corev1.Secret) {
			profile.Namespace = "other"
			secret.Namespace = "other"
		}, reason: "ProfileNotFound"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"},
				Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
				Status: platformv1alpha1.RunStatus{CredentialProfileRef: &platformv1alpha1.RunCredentialProfileReference{
					Name: "profile", UID: "profile-uid",
				}},
			}
			profile, secret := credentialProfileAndSecret(run, []byte("!!VALIDATION-KEY-FIXTURE!!"))
			test.mutate(profile, secret)
			r := reconciler(t, &scriptedAdapter{}, run, profile, secret)
			credential, reason, err := r.resolveCredential(context.Background(), run)
			if credential != nil || err == nil || reason != test.reason {
				t.Fatalf("resolve = (%#v, %q, %v), want reason %q", credential, reason, err, test.reason)
			}
		})
	}
}

func TestResolveCredentialRejectsForeignMetadataWithoutReadingSecretData(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"},
		Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
		Status: platformv1alpha1.RunStatus{CredentialProfileRef: &platformv1alpha1.RunCredentialProfileReference{
			Name: "profile", UID: "profile-uid",
		}},
	}
	profile, secret := credentialProfileAndSecret(run, []byte("!!FOREIGN-METADATA-KEY-FIXTURE!!"))
	secret.OwnerReferences[0].UID = "foreign-profile"
	r := reconciler(t, &scriptedAdapter{}, run)
	base := fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(profile, secret).Build()
	secretValueReads := 0
	r.APIReader = interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
		if _, ok := object.(*corev1.Secret); ok {
			secretValueReads++
		}
		return underlying.Get(ctx, key, object, options...)
	}})
	credential, reason, err := r.resolveCredential(context.Background(), run)
	if credential != nil || err == nil || reason != "ForeignSecret" || secretValueReads != 0 {
		t.Fatalf("resolve = (%#v, %q, %v), Secret value reads = %d", credential, reason, err, secretValueReads)
	}
}

func TestCredentialAcceptanceUsesUncachedCurrentKeyAndClearsCopy(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Generation: 1, Finalizers: []string{runFinalizer}},
		Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
		Status: platformv1alpha1.RunStatus{
			State: platformv1alpha1.RunStateEnvironmentReady,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
				Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned,
			},
			CredentialProfileRef: &platformv1alpha1.RunCredentialProfileReference{Name: "profile", UID: "profile-uid"},
			Conditions: []metav1.Condition{
				{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Bound", ObservedGeneration: 1},
				{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue, Reason: "AcceptancePending", ObservedGeneration: 1},
			},
		},
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid"},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	profile, staleSecret := credentialProfileAndSecret(run, []byte("!!STALE-CACHED-KEY!!"))
	_, currentSecret := credentialProfileAndSecret(run, []byte("!!CURRENT-UNCACHED-KEY!!"))
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env, profile, staleSecret)
	liveObjects := append(runExecutionBackendObjects(t, r, env), profile.DeepCopy(), currentSecret)
	liveReader := fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(liveObjects...).Build()
	r.APIReader = liveReader

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}); err != nil {
		t.Fatal(err)
	}
	if len(adapter.acceptedCredentials) != 1 || string(adapter.acceptedCredentials[0]) != "!!CURRENT-UNCACHED-KEY!!" {
		t.Fatalf("accepted credentials = %#v", adapter.acceptedCredentials)
	}
	if !reflect.DeepEqual(adapter.retainedCredentialKey, make([]byte, len(adapter.retainedCredentialKey))) {
		t.Fatal("Run controller retained credential copy after EnsureAccepted")
	}
	if !reflect.DeepEqual(staleSecret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey], []byte("!!STALE-CACHED-KEY!!")) {
		t.Fatal("cached Secret fixture was mutated")
	}
}

func TestCredentialRotationIsRematerializedAfterResumeEpoch(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Generation: 1, Finalizers: []string{runFinalizer}},
		Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
		Status: platformv1alpha1.RunStatus{
			State:                platformv1alpha1.RunStateEnvironmentReady,
			EnvironmentRef:       &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
			CredentialProfileRef: &platformv1alpha1.RunCredentialProfileReference{Name: "profile", UID: "profile-uid"},
			Conditions: []metav1.Condition{
				{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Bound", ObservedGeneration: 1},
				{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue, Reason: "AcceptancePending", ObservedGeneration: 1},
			},
		},
	}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	profile, secret := credentialProfileAndSecret(run, []byte("!!FIRST-EPOCH-KEY!!"))
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, env, profile, secret)
	reconcileRun(t, r, run.Name)

	var rotated corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(secret), &rotated); err != nil {
		t.Fatal(err)
	}
	rotated.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = []byte("!!SECOND-EPOCH-KEY!!")
	if err := r.Update(context.Background(), &rotated); err != nil {
		t.Fatal(err)
	}
	var currentEnv platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &currentEnv); err != nil {
		t.Fatal(err)
	}
	currentEnv.Spec.Paused = true
	if err := r.Update(context.Background(), &currentEnv); err != nil {
		t.Fatal(err)
	}
	applyEnvironmentStatus(&currentEnv, platformv1alpha1.EnvironmentPhasePaused, "", "", "Paused", "paused", nil)
	currentEnv.Status.Lifecycle.Suspended = true
	currentEnv.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
	currentEnv.Status.Lifecycle.Epoch = 1
	if err := r.Status().Update(context.Background(), &currentEnv); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, run.Name)
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &currentEnv); err != nil {
		t.Fatal(err)
	}
	currentEnv.Spec.Paused = false
	if err := r.Update(context.Background(), &currentEnv); err != nil {
		t.Fatal(err)
	}
	applyEnvironmentStatus(&currentEnv, platformv1alpha1.EnvironmentPhaseReady, "env-e", "10.0.0.2:50051", "SandboxdReady", "ready", nil)
	currentEnv.Status.ExecutionGeneration = 2
	currentEnv.Status.Lifecycle.Suspended = false
	currentEnv.Status.Lifecycle.SuspensionReason = ""
	if err := r.Status().Update(context.Background(), &currentEnv); err != nil {
		t.Fatal(err)
	}
	var oldPod corev1.Pod
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: currentEnv.Namespace, Name: "env-e"}, &oldPod); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), &oldPod); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), runExecutionPod(&currentEnv, "pod-e-resumed", "10.0.0.2")); err != nil {
		t.Fatal(err)
	}
	reconcileRun(t, r, run.Name) // Paused -> EnvironmentReady.
	reconcileRun(t, r, run.Name) // Reaccept in the fresh sandbox epoch.
	adapter.observations = []agent.AdapterObservation{agent.AdapterObservationAccepted}
	reconcileRun(t, r, run.Name) // Observe without accepting the same epoch again.
	if len(adapter.acceptedCredentials) != 2 || string(adapter.acceptedCredentials[0]) != "!!FIRST-EPOCH-KEY!!" || string(adapter.acceptedCredentials[1]) != "!!SECOND-EPOCH-KEY!!" {
		t.Fatalf("epoch credentials = %#v", adapter.acceptedCredentials)
	}
}

func TestMissedPauseTransitionReacceptsFreshEnvironmentEpoch(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       platformv1alpha1.RunState
		observation agent.AdapterObservation
	}{
		{name: "running", state: platformv1alpha1.RunStateRunning, observation: agent.AdapterObservationRunning},
		{name: "needs input", state: platformv1alpha1.RunStateNeedsInput, observation: agent.AdapterObservationNeedsInput},
	} {
		t.Run(test.name, func(t *testing.T) {
			epoch0 := int64(0)
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid", Generation: 1, Finalizers: []string{runFinalizer}},
				Spec:       platformv1alpha1.RunSpec{Agent: "test", CredentialProfileRef: "profile"},
				Status: platformv1alpha1.RunStatus{
					State:                    test.state,
					AcceptedEnvironmentEpoch: &epoch0,
					EnvironmentRef:           &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
					CredentialProfileRef:     &platformv1alpha1.RunCredentialProfileReference{Name: "profile", UID: "profile-uid"},
					Conditions: []metav1.Condition{
						{Type: runConditionCredentialProfileBound, Status: metav1.ConditionTrue, Reason: "Bound", ObservedGeneration: 1},
						{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue, Reason: "AcceptancePending", ObservedGeneration: 1},
						{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted", ObservedGeneration: 1},
					},
				},
			}
			env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "env-uid", Generation: 1}}
			applyEnvironmentStatus(env, platformv1alpha1.EnvironmentPhaseReady, "env-e-0", "10.0.0.1:50051", "SandboxdReady", "ready", nil)
			profile, staleSecret := credentialProfileAndSecret(run, []byte("!!EPOCH-ZERO-KEY!!"))
			_, rotatedSecret := credentialProfileAndSecret(run, []byte("!!EPOCH-ONE-KEY!!"))
			adapter := &scriptedAdapter{observations: []agent.AdapterObservation{test.observation}}
			r := reconciler(t, adapter, run, env, profile, staleSecret)

			// The Environment completes an entire pause/resume while the Run
			// controller is unavailable. No Run reconcile occurs in this window.
			var currentEnv platformv1alpha1.Environment
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &currentEnv); err != nil {
				t.Fatal(err)
			}
			currentEnv.Generation++
			currentEnv.Spec.Paused = true
			if err := r.Update(context.Background(), &currentEnv); err != nil {
				t.Fatal(err)
			}
			applyEnvironmentStatus(&currentEnv, platformv1alpha1.EnvironmentPhasePaused, "", "", "Paused", "paused", nil)
			currentEnv.Status.Lifecycle.Suspended = true
			currentEnv.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
			currentEnv.Status.Lifecycle.Epoch = 1
			if err := r.Status().Update(context.Background(), &currentEnv); err != nil {
				t.Fatal(err)
			}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(env), &currentEnv); err != nil {
				t.Fatal(err)
			}
			currentEnv.Generation++
			currentEnv.Spec.Paused = false
			if err := r.Update(context.Background(), &currentEnv); err != nil {
				t.Fatal(err)
			}
			applyEnvironmentStatus(&currentEnv, platformv1alpha1.EnvironmentPhaseReady, "env-e-1", "10.0.0.2:50051", "SandboxdReady", "ready", nil)
			currentEnv.Status.ExecutionGeneration = 2
			currentEnv.Status.Lifecycle.Suspended = false
			currentEnv.Status.Lifecycle.SuspensionReason = ""
			currentEnv.Status.Lifecycle.Epoch = 1
			if err := r.Status().Update(context.Background(), &currentEnv); err != nil {
				t.Fatal(err)
			}
			livePod := runExecutionPod(&currentEnv, "pod-e-1", "10.0.0.2")
			template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: currentEnv.Spec.TemplateRef, Namespace: currentEnv.Namespace, UID: currentEnv.Status.Provisioning.Template.UID, Generation: currentEnv.Status.Provisioning.Template.Generation}}
			r.APIReader = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(currentEnv.DeepCopy(), template, livePod, profile.DeepCopy(), rotatedSecret).Build()

			fenced := reconcileRun(t, r, run.Name)
			if fenced.Status.State != platformv1alpha1.RunStateEnvironmentReady || adapter.accepted != 0 || adapter.observed != 0 {
				t.Fatalf("epoch fence = state %s, acceptances %d, observations %d", fenced.Status.State, adapter.accepted, adapter.observed)
			}
			accepted := reconcileRun(t, r, run.Name)
			if accepted.Status.State != platformv1alpha1.RunStateAdapterAccepted || adapter.accepted != 1 || adapter.observed != 0 ||
				len(adapter.acceptedCredentials) != 1 || string(adapter.acceptedCredentials[0]) != "!!EPOCH-ONE-KEY!!" ||
				accepted.Status.AcceptedEnvironmentEpoch == nil || *accepted.Status.AcceptedEnvironmentEpoch != 1 {
				t.Fatalf("fresh epoch acceptance = status %#v, acceptances %d, observations %d, credentials %#v", accepted.Status, adapter.accepted, adapter.observed, adapter.acceptedCredentials)
			}
			observed := reconcileRun(t, r, run.Name)
			if observed.Status.State != test.state || adapter.accepted != 1 || adapter.observed != 1 {
				t.Fatalf("post-accept observation = state %s, acceptances %d, observations %d", observed.Status.State, adapter.accepted, adapter.observed)
			}
		})
	}
}

func TestObserveDiscardsStaleExecutionResults(t *testing.T) {
	for _, ownership := range []platformv1alpha1.EnvironmentOwnership{
		platformv1alpha1.EnvironmentOwnershipOwned,
		platformv1alpha1.EnvironmentOwnershipClaimed,
	} {
		for _, observation := range []agent.AdapterObservation{agent.AdapterObservationNeedsInput, agent.AdapterObservationSucceeded} {
			t.Run(string(ownership)+"/"+string(observation), func(t *testing.T) {
				epoch := int64(0)
				executionGeneration := int64(1)
				run := &platformv1alpha1.Run{
					ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}},
					Spec:       platformv1alpha1.RunSpec{Agent: "test"},
					Status: platformv1alpha1.RunStatus{
						State:                                  platformv1alpha1.RunStateRunning,
						EnvironmentRef:                         &platformv1alpha1.RunEnvironmentReference{Name: "environment", UID: "env-uid", Ownership: ownership},
						AcceptedEnvironmentEpoch:               &epoch,
						AcceptedEnvironmentExecutionGeneration: &executionGeneration,
						Conditions: []metav1.Condition{{
							Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted",
						}},
					},
				}
				environment := &platformv1alpha1.Environment{
					ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid"},
					Status: platformv1alpha1.EnvironmentStatus{
						ExecutionGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseReady,
					},
				}
				if ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
					environment.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
				}
				adapter := &blockingObserveAdapter{observation: observation, started: make(chan struct{}), release: make(chan struct{})}
				r := reconciler(t, adapter, run, environment)
				done := make(chan struct{})
				var result ctrl.Result
				var reconcileErr error
				go func() {
					result, reconcileErr = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
					close(done)
				}()
				<-adapter.started

				var replaced platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &replaced); err != nil {
					t.Fatal(err)
				}
				replaced.Status.ExecutionGeneration = 2
				if err := r.Status().Update(context.Background(), &replaced); err != nil {
					t.Fatal(err)
				}
				close(adapter.release)
				<-done
				if reconcileErr != nil || !result.Requeue {
					t.Fatalf("stale Observe reconcile = (%#v, %v)", result, reconcileErr)
				}
				var retained platformv1alpha1.Run
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retained); err != nil {
					t.Fatal(err)
				}
				if retained.Status.State != platformv1alpha1.RunStateRunning || retained.Status.FinishedAt != nil ||
					retained.Status.AcceptedEnvironmentExecutionGeneration == nil || *retained.Status.AcceptedEnvironmentExecutionGeneration != 1 {
					t.Fatalf("stale %s observation mutated Run status: %#v", observation, retained.Status)
				}
			})
		}
	}
}

func TestAdapterResultsRequireSameLivePodWithUnchangedStatus(t *testing.T) {
	for _, ownership := range []platformv1alpha1.EnvironmentOwnership{
		platformv1alpha1.EnvironmentOwnershipOwned,
		platformv1alpha1.EnvironmentOwnershipClaimed,
	} {
		for _, acceptErr := range []error{nil, agent.ErrAdapterTaskRejected} {
			name := "successful accept"
			if acceptErr != nil {
				name = "rejected accept"
			}
			t.Run(string(ownership)+"/"+name+" Pod disappears", func(t *testing.T) {
				run, environment := adapterRaceObjects(ownership, platformv1alpha1.RunStateEnvironmentReady)
				apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue, Reason: "AcceptancePending"})
				adapter := &scriptedAdapter{acceptErr: acceptErr}
				r := reconciler(t, adapter, run, environment)
				adapter.onAccept = func() { deleteRunExecutionPod(t, r, environment) }

				result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
				if err != nil || !result.Requeue || adapter.accepted != 1 {
					t.Fatalf("stale acceptance = (%#v, %v), calls %d", result, err, adapter.accepted)
				}
				var retained platformv1alpha1.Run
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retained); err != nil {
					t.Fatal(err)
				}
				if retained.Status.State != platformv1alpha1.RunStateEnvironmentReady || retained.Status.AcceptedEnvironmentExecutionGeneration != nil {
					t.Fatalf("stale acceptance mutated Run: %#v", retained.Status)
				}
			})
		}

		t.Run(string(ownership)+"/observe Pod replaced", func(t *testing.T) {
			run, environment := adapterRaceObjects(ownership, platformv1alpha1.RunStateRunning)
			epoch, generation := int64(0), int64(1)
			run.Status.AcceptedEnvironmentEpoch = &epoch
			run.Status.AcceptedEnvironmentExecutionGeneration = &generation
			apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted"})
			adapter := &scriptedAdapter{observations: []agent.AdapterObservation{agent.AdapterObservationSucceeded}}
			r := reconciler(t, adapter, run, environment)
			// scripted Observe is synchronous, so replace the Pod from its
			// observation hook by using a one-shot wrapper.
			wrapped := &mutatingObserveAdapter{scriptedAdapter: adapter, mutate: func() { replaceRunExecutionPod(t, r, environment) }}
			r.Adapters["test"] = wrapped

			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			if err != nil || !result.Requeue || adapter.observed != 1 {
				t.Fatalf("stale observation = (%#v, %v), calls %d", result, err, adapter.observed)
			}
			var retained platformv1alpha1.Run
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retained); err != nil {
				t.Fatal(err)
			}
			if retained.Status.State != platformv1alpha1.RunStateRunning || retained.Status.FinishedAt != nil {
				t.Fatalf("stale observation mutated Run: %#v", retained.Status)
			}
		})
	}
}

type mutatingObserveAdapter struct {
	*scriptedAdapter
	mutate func()
}

func (a *mutatingObserveAdapter) Observe(ctx context.Context, task agent.AdapterTask, sandbox agent.AdapterSandbox) (agent.AdapterObservation, string, error) {
	observation, message, err := a.scriptedAdapter.Observe(ctx, task, sandbox)
	a.mutate()
	return observation, message, err
}

func adapterRaceObjects(ownership platformv1alpha1.EnvironmentOwnership, state platformv1alpha1.RunState) (*platformv1alpha1.Run, *platformv1alpha1.Environment) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}},
		Spec:       platformv1alpha1.RunSpec{Agent: "test"},
		Status: platformv1alpha1.RunStatus{State: state, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
			Name: "environment", UID: "env-uid", Ownership: ownership,
		}},
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid"},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
	}
	if ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
		environment.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
	}
	return run, environment
}

func deleteRunExecutionPod(t *testing.T, r *RunReconciler, environment *platformv1alpha1.Environment) {
	t.Helper()
	var pod corev1.Pod
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: environment.Namespace, Name: "env-environment"}, &pod); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
}

func replaceRunExecutionPod(t *testing.T, r *RunReconciler, environment *platformv1alpha1.Environment) {
	t.Helper()
	deleteRunExecutionPod(t, r, environment)
	var current platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &current); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), runExecutionPod(&current, "replacement-pod", "10.0.0.1")); err != nil {
		t.Fatal(err)
	}
}

func TestCancelDiscardsResultWhenLiveExecutionDisappears(t *testing.T) {
	for _, ownership := range []platformv1alpha1.EnvironmentOwnership{
		platformv1alpha1.EnvironmentOwnershipOwned,
		platformv1alpha1.EnvironmentOwnershipClaimed,
	} {
		t.Run(string(ownership), func(t *testing.T) {
			epoch, executionGeneration := int64(0), int64(1)
			run := &platformv1alpha1.Run{
				ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}},
				Spec:       platformv1alpha1.RunSpec{Agent: "test", Cancel: true},
				Status: platformv1alpha1.RunStatus{
					State: platformv1alpha1.RunStateRunning,
					EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
						Name: "environment", UID: "env-uid", Ownership: ownership,
					},
					AcceptedEnvironmentEpoch:               &epoch,
					AcceptedEnvironmentExecutionGeneration: &executionGeneration,
					Conditions:                             []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted"}},
				},
			}
			environment := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid"},
				Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady},
			}
			if ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
				environment.Status.ClaimedBy = &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID}
			}
			adapter := &scriptedAdapter{}
			r := reconciler(t, adapter, run, environment)
			adapter.onCancel = func() {
				var pod corev1.Pod
				if err := r.Get(context.Background(), types.NamespacedName{Namespace: environment.Namespace, Name: "env-environment"}, &pod); err != nil {
					t.Fatal(err)
				}
				if err := r.Delete(context.Background(), &pod); err != nil {
					t.Fatal(err)
				}
			}

			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
			if err != nil || !result.Requeue || adapter.cancelled != 1 {
				t.Fatalf("stale Cancel reconcile = (%#v, %v), calls %d", result, err, adapter.cancelled)
			}
			var retainedRun platformv1alpha1.Run
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil {
				t.Fatal(err)
			}
			if retainedRun.Status.State != platformv1alpha1.RunStateRunning || retainedRun.Status.FinishedAt != nil || !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) {
				t.Fatalf("stale Cancel published terminal state or removed finalizer: %#v", retainedRun)
			}
			if ownership == platformv1alpha1.EnvironmentOwnershipClaimed {
				var retainedEnvironment platformv1alpha1.Environment
				if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &retainedEnvironment); err != nil {
					t.Fatal(err)
				}
				if retainedEnvironment.Status.ClaimedBy == nil || retainedEnvironment.Status.ClaimedBy.UID != run.UID {
					t.Fatalf("stale Cancel released claim: %#v", retainedEnvironment.Status.ClaimedBy)
				}
			}
		})
	}
}

func TestTerminalCleanupRetainsClaimWhenExecutionChangesDuringCancel(t *testing.T) {
	run, environment := adapterRaceObjects(platformv1alpha1.EnvironmentOwnershipClaimed, platformv1alpha1.RunStateSucceeded)
	epoch, generation := int64(0), int64(1)
	run.Status.AcceptedEnvironmentEpoch = &epoch
	run.Status.AcceptedEnvironmentExecutionGeneration = &generation
	apiMeta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted"})
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, environment)
	adapter.onCancel = func() { replaceRunExecutionPod(t, r, environment) }

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || !result.Requeue || adapter.cancelled != 1 {
		t.Fatalf("terminal cleanup stale Cancel = (%#v, %v), calls %d", result, err, adapter.cancelled)
	}
	var retainedEnvironment platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &retainedEnvironment); err != nil {
		t.Fatal(err)
	}
	if retainedEnvironment.Status.ClaimedBy == nil || retainedEnvironment.Status.ClaimedBy.UID != run.UID {
		t.Fatalf("terminal cleanup released claim after stale Cancel: %#v", retainedEnvironment.Status.ClaimedBy)
	}
	var retainedRun platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(retainedRun.Status.Conditions, runConditionEnvironmentReady)
	if condition != nil && condition.Reason == "EnvironmentReleased" {
		t.Fatalf("terminal cleanup published release after stale Cancel: %#v", condition)
	}
}

func TestFinalizerRetainsClaimWhenExecutionChangesDuringCancel(t *testing.T) {
	now := metav1.Now()
	epoch, executionGeneration := int64(0), int64(1)
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run-uid", Finalizers: []string{runFinalizer}, DeletionTimestamp: &now},
		Spec:       platformv1alpha1.RunSpec{Agent: "test"},
		Status: platformv1alpha1.RunStatus{
			State: platformv1alpha1.RunStateRunning,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{
				Name: "environment", UID: "env-uid", Ownership: platformv1alpha1.EnvironmentOwnershipClaimed,
			},
			AcceptedEnvironmentEpoch:               &epoch,
			AcceptedEnvironmentExecutionGeneration: &executionGeneration,
			Conditions:                             []metav1.Condition{{Type: runConditionAdapterAccepted, Status: metav1.ConditionTrue, Reason: "AdapterAccepted"}},
		},
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase: platformv1alpha1.EnvironmentPhaseReady, ClaimedBy: &platformv1alpha1.RunReference{Name: run.Name, UID: run.UID},
		},
	}
	adapter := &scriptedAdapter{}
	r := reconciler(t, adapter, run, environment)
	adapter.onCancel = func() {
		var pod corev1.Pod
		if err := r.Get(context.Background(), types.NamespacedName{Namespace: environment.Namespace, Name: "env-environment"}, &pod); err != nil {
			t.Fatal(err)
		}
		pod.UID = "replacement-pod"
		if err := r.Update(context.Background(), &pod); err != nil {
			t.Fatal(err)
		}
	}

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
	if err != nil || !result.Requeue || adapter.cancelled != 1 {
		t.Fatalf("finalizer stale Cancel reconcile = (%#v, %v), calls %d", result, err, adapter.cancelled)
	}
	var retainedRun platformv1alpha1.Run
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &retainedRun); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&retainedRun, runFinalizer) {
		t.Fatal("stale finalizer Cancel removed finalizer")
	}
	var retainedEnvironment platformv1alpha1.Environment
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(environment), &retainedEnvironment); err != nil {
		t.Fatal(err)
	}
	if retainedEnvironment.Status.ClaimedBy == nil || retainedEnvironment.Status.ClaimedBy.UID != run.UID {
		t.Fatalf("stale finalizer Cancel released claim: %#v", retainedEnvironment.Status.ClaimedBy)
	}
}

func TestCancellationBeforeAllocationDoesNotReadCredentialProfileOrSecret(t *testing.T) {
	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"},
		Spec:       platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", CredentialProfileRef: "profile", Cancel: true},
	}
	r := reconciler(t, &scriptedAdapter{}, run)
	reads := 0
	r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
		reads++
		return errors.New("credential reader must not be called")
	}})
	got := reconcileRun(t, r, run.Name)
	if got.Status.State != platformv1alpha1.RunStateCancelled || reads != 0 {
		t.Fatalf("cancellation state = %s, credential reads = %d", got.Status.State, reads)
	}
}

func TestLifecycleTimestampsSetOnceAcrossPauseResumeAndTerminal(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAllocating, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	r := reconciler(t, &scriptedAdapter{observations: []agent.AdapterObservation{agent.AdapterObservationRunning}}, run, env)

	// Drive through allocation to AdapterAccepted.
	reconcileRun(t, r, "r")        // EnvironmentReady
	reconcileRun(t, r, "r")        // acceptance attempt marker
	got := reconcileRun(t, r, "r") // AdapterAccepted
	if got.Status.State != platformv1alpha1.RunStateAdapterAccepted {
		t.Fatalf("state = %s, want AdapterAccepted", got.Status.State)
	}
	if got.Status.StartedAt == nil {
		t.Fatal("StartedAt is nil after AdapterAccepted")
	}
	if got.Status.FinishedAt != nil {
		t.Fatal("FinishedAt is set before terminal")
	}
	startedAt := *got.Status.StartedAt

	// Running: StartedAt must not change.
	got = reconcileRun(t, r, "r") // Running
	if got.Status.State != platformv1alpha1.RunStateRunning {
		t.Fatalf("state = %s, want Running", got.Status.State)
	}
	if got.Status.StartedAt == nil || !got.Status.StartedAt.Equal(&startedAt) {
		t.Fatalf("StartedAt changed at Running: got %v, want %v", got.Status.StartedAt, &startedAt)
	}

	// Pause and resume: StartedAt must not change.
	_ = r.Get(context.Background(), client.ObjectKeyFromObject(env), env)
	env.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
	_ = r.Status().Update(context.Background(), env)
	got = reconcileRun(t, r, "r")
	if got.Status.State != platformv1alpha1.RunStatePaused {
		t.Fatalf("state = %s, want Paused", got.Status.State)
	}
	if got.Status.StartedAt == nil || !got.Status.StartedAt.Equal(&startedAt) {
		t.Fatalf("StartedAt changed during pause: got %v, want %v", got.Status.StartedAt, &startedAt)
	}

	// Resume: re-accept and return to Running. StartedAt still unchanged.
	env.Status.Phase = platformv1alpha1.EnvironmentPhaseReady
	_ = r.Status().Update(context.Background(), env)
	reconcileRun(t, r, "r")       // EnvironmentReady
	got = reconcileRun(t, r, "r") // AdapterAccepted (re-accept)
	if got.Status.State != platformv1alpha1.RunStateAdapterAccepted {
		t.Fatalf("state = %s after resume, want AdapterAccepted", got.Status.State)
	}
	if got.Status.StartedAt == nil || !got.Status.StartedAt.Equal(&startedAt) {
		t.Fatalf("StartedAt changed after resume: got %v, want %v", got.Status.StartedAt, &startedAt)
	}
	got = reconcileRun(t, r, "r") // Running again
	if got.Status.State != platformv1alpha1.RunStateRunning {
		t.Fatalf("state = %s after resume, want Running", got.Status.State)
	}
	if got.Status.StartedAt == nil || !got.Status.StartedAt.Equal(&startedAt) {
		t.Fatalf("StartedAt changed after resume Running: got %v, want %v", got.Status.StartedAt, &startedAt)
	}

	// Terminal: FinishedAt is set once, StartedAt unchanged.
	adapter := r.Adapters["test"].(*scriptedAdapter)
	adapter.observations = []agent.AdapterObservation{agent.AdapterObservationSucceeded}
	got = reconcileRun(t, r, "r")
	if got.Status.State != platformv1alpha1.RunStateSucceeded {
		t.Fatalf("state = %s, want Succeeded", got.Status.State)
	}
	if got.Status.FinishedAt == nil {
		t.Fatal("FinishedAt is nil after terminal")
	}
	if got.Status.StartedAt == nil || !got.Status.StartedAt.Equal(&startedAt) {
		t.Fatalf("StartedAt changed at terminal: got %v, want %v", got.Status.StartedAt, &startedAt)
	}
}

func TestLifecycleTimestampsSetOnImmediateTerminalSuccess(t *testing.T) {
	// A foreground process that exits before the first Observe poll returns
	// Running: the Run goes directly from AdapterAccepted to Succeeded.
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAllocating, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
	r := reconciler(t, &scriptedAdapter{observations: []agent.AdapterObservation{agent.AdapterObservationSucceeded}}, run, env)

	reconcileRun(t, r, "r")        // EnvironmentReady
	reconcileRun(t, r, "r")        // acceptance attempt marker
	got := reconcileRun(t, r, "r") // AdapterAccepted — StartedAt set here
	if got.Status.State != platformv1alpha1.RunStateAdapterAccepted {
		t.Fatalf("state = %s, want AdapterAccepted", got.Status.State)
	}
	if got.Status.StartedAt == nil {
		t.Fatal("StartedAt is nil after AdapterAccepted")
	}

	got = reconcileRun(t, r, "r") // Succeeded (immediate terminal)
	if got.Status.State != platformv1alpha1.RunStateSucceeded {
		t.Fatalf("state = %s, want Succeeded", got.Status.State)
	}
	if got.Status.StartedAt == nil {
		t.Fatal("StartedAt is nil after immediate terminal Succeeded")
	}
	if got.Status.FinishedAt == nil {
		t.Fatal("FinishedAt is nil after terminal Succeeded")
	}
}

func TestLifecycleTimestampsNilForNeverAcceptedFailureAndCancellation(t *testing.T) {
	t.Run("environment failure before acceptance", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{State: platformv1alpha1.RunStateAllocating, EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned}}}
		env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseFailed}}
		r := reconciler(t, &scriptedAdapter{}, run, env)

		got := reconcileRun(t, r, "r")
		if got.Status.State != platformv1alpha1.RunStateFailed {
			t.Fatalf("state = %s, want Failed", got.Status.State)
		}
		if got.Status.StartedAt != nil {
			t.Fatalf("StartedAt = %v, want nil (never accepted)", got.Status.StartedAt)
		}
		if got.Status.FinishedAt == nil {
			t.Fatal("FinishedAt is nil after terminal Failed")
		}
	})

	t.Run("adapter rejection before acceptance", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid", Finalizers: []string{runFinalizer}}, Spec: platformv1alpha1.RunSpec{Agent: "test"}, Status: platformv1alpha1.RunStatus{
			State:          platformv1alpha1.RunStateEnvironmentReady,
			EnvironmentRef: &platformv1alpha1.RunEnvironmentReference{Name: "e", UID: "euid", Ownership: platformv1alpha1.EnvironmentOwnershipOwned},
			Conditions:     []metav1.Condition{{Type: runConditionAdapterAcceptanceAttempted, Status: metav1.ConditionTrue}},
		}}
		env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "ns", UID: "euid"}, Status: platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhaseReady}}
		adapter := &scriptedAdapter{acceptErr: fmt.Errorf("%w: unsupported task configuration", agent.ErrAdapterTaskRejected)}
		r := reconciler(t, adapter, run, env)

		got := reconcileRun(t, r, "r")
		if got.Status.State != platformv1alpha1.RunStateFailed {
			t.Fatalf("state = %s, want Failed", got.Status.State)
		}
		if got.Status.StartedAt != nil {
			t.Fatalf("StartedAt = %v, want nil (adapter rejected, never accepted)", got.Status.StartedAt)
		}
		if got.Status.FinishedAt == nil {
			t.Fatal("FinishedAt is nil after terminal Failed")
		}
	})

	t.Run("cancellation before allocation", func(t *testing.T) {
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "uid"}, Spec: platformv1alpha1.RunSpec{TemplateRef: "small", Agent: "test", Cancel: true}}
		r := reconciler(t, &scriptedAdapter{}, run)

		got := reconcileRun(t, r, "r")
		if got.Status.State != platformv1alpha1.RunStateCancelled {
			t.Fatalf("state = %s, want Cancelled", got.Status.State)
		}
		if got.Status.StartedAt != nil {
			t.Fatalf("StartedAt = %v, want nil (cancelled before acceptance)", got.Status.StartedAt)
		}
		if got.Status.FinishedAt == nil {
			t.Fatal("FinishedAt is nil after terminal Cancelled")
		}
	})
}

func TestAdapterSandboxEmitEventSendsExactRunUID(t *testing.T) {
	run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "run-uid"}, Spec: platformv1alpha1.RunSpec{Agent: "test"}}
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "ns", UID: "env-uid"}}

	var gotNamespace, gotName, gotUID string
	sink := eventSinkFunc(func(ctx context.Context, namespace, name, runUID string, event agent.AdapterEvent) error {
		gotNamespace, gotName, gotUID = namespace, name, runUID
		return nil
	})
	r := &RunReconciler{EventSink: sink}
	sandbox := r.adapterSandbox(run, env, lifecycle.CaptureExecutionFence(env), &fenceRejectionRecorder{callSite: fenceCallSiteEnsureAccepted})
	if sandbox.EmitEvent == nil {
		t.Fatal("EmitEvent closure was not wired")
	}
	if err := sandbox.EmitEvent(context.Background(), agent.AdapterEvent{Source: "adapter", IdempotencyKey: "k", Type: "output", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if gotNamespace != "ns" || gotName != "r" || gotUID != "run-uid" {
		t.Fatalf("sink received namespace/name/uid = %q/%q/%q, want ns/r/run-uid", gotNamespace, gotName, gotUID)
	}
}

type eventSinkFunc func(context.Context, string, string, string, agent.AdapterEvent) error

func (f eventSinkFunc) Append(ctx context.Context, namespace, name, runUID string, event agent.AdapterEvent) error {
	return f(ctx, namespace, name, runUID, event)
}
