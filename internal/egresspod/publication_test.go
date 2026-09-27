package egresspod

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/Chris-Cullins/swe-platform/internal/egressidentity"
)

// These hooks exercise transaction control flow, not API-server semantics.
// publication_envtest_test.go separately checks actual UID/RV/gate writes.
type publicationClient struct {
	client.Client
	create                  func(context.Context, client.Object) error
	get                     func(client.Object)
	patch                   func(context.Context, client.Object, client.Patch) error
	reads, creates, patches int
	readFailure             int
}

func (c *publicationClient) Create(ctx context.Context, obj client.Object, _ ...client.CreateOption) error {
	c.creates++
	if c.create != nil {
		return c.create(ctx, obj)
	}
	return c.Client.Create(ctx, obj)
}
func (c *publicationClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.reads++
	if c.reads == c.readFailure {
		return errors.New("SECRET-SENTINEL read failure")
	}
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if c.get != nil {
		c.get(obj)
	}
	return nil
}
func (c *publicationClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	c.patches++
	if c.patch != nil {
		return c.patch(ctx, obj, patch)
	}
	return c.Client.Patch(ctx, obj, patch)
}

func publicationFixture(t *testing.T) (*publicationClient, *corev1.Pod, RestrictedEgress, egressidentity.Claims) {
	t.Helper()
	pod, in := fixture()
	if err := PrepareRestrictedEgress(pod, in); err != nil {
		t.Fatal(err)
	}
	pod.UID, pod.ResourceVersion = "pod-uid", "7"
	pod.Annotations["other.example/note"] = "preserve-me"
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := &publicationClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()}
	c.create = func(ctx context.Context, obj client.Object) error {
		obj.SetUID("secret-uid")
		return c.Client.Create(ctx, obj)
	}
	return c, pod, in, publicationClaims(pod)
}

func publicationClaims(pod *corev1.Pod) egressidentity.Claims {
	return egressidentity.Claims{InstallationNamespace: "system", InstallationName: "main", InstallationUID: "i", ProjectNamespace: pod.Namespace, ProjectName: "project", ProjectUID: "p", EnvironmentNamespace: pod.Namespace, EnvironmentName: "one", EnvironmentUID: "e", PodName: pod.Name, PodUID: pod.UID, ExecutionGeneration: 1, RuntimePolicyRevision: "policy-revision", ForwarderRevision: ForwarderRevision}
}

func assertPublished(t *testing.T, c client.Reader, pod *corev1.Pod, in RestrictedEgress) {
	t.Helper()
	var got corev1.Pod
	var secret corev1.Secret
	ctx := context.Background()
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &got); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: in.CredentialSecret}, &secret); err != nil {
		t.Fatal(err)
	}
	claims, err := egressidentity.Parse(secret.Data[egressidentity.CanonicalClaimsKey])
	if err != nil {
		t.Fatal("invalid persisted claims")
	}
	if got.UID != pod.UID || got.ResourceVersion == pod.ResourceVersion || len(got.Spec.SchedulingGates) != 0 || got.Spec.NodeName != "" {
		t.Fatal("exact Pod gate was not released")
	}
	if len(got.Annotations) != 5 || got.Annotations["other.example/note"] != "preserve-me" || got.Annotations[ExecutionGenerationAnnotation] != "1" || got.Annotations[PolicyRevisionAnnotation] != "policy-revision" || got.Annotations[ForwarderRevisionAnnotation] != ForwarderRevision || got.Annotations[CertificateFingerprintAnnotation] != claims.CertificateFingerprint {
		t.Fatal("publication annotations incorrect")
	}
	if claims.PodUID != pod.UID || secret.UID == "" || secret.ResourceVersion == "" || secret.Immutable == nil || !*secret.Immutable || metav1.GetControllerOf(&secret).UID != pod.UID {
		t.Fatal("credential not bound to exact Pod")
	}
}

func TestPublishCredential(t *testing.T) {
	c, pod, in, claims := publicationFixture(t)
	if err := PublishCredential(context.Background(), c, c, time.Now(), pod, in, claims); err != nil {
		t.Fatal(err)
	}
	if c.creates != 1 || c.patches != 1 || c.reads != 3 {
		t.Fatalf("calls: create=%d patch=%d read=%d", c.creates, c.patches, c.reads)
	}
	assertPublished(t, c.Client, pod, in)
	if len(pod.Spec.SchedulingGates) != 1 || pod.Annotations[CertificateFingerprintAnnotation] != "" {
		t.Fatal("caller input mutated")
	}
}

func TestPublicationFailures(t *testing.T) {
	sentinel := errors.New("SECRET-SENTINEL must never appear")
	for _, tc := range []struct {
		name             string
		setup            func(*publicationClient, *corev1.Pod)
		stage            string
		uncertain        bool
		creates, patches int
	}{
		{"missing Pod UID", func(_ *publicationClient, p *corev1.Pod) { p.UID = "" }, "input", false, 0, 0},
		{"missing Pod RV", func(_ *publicationClient, p *corev1.Pod) { p.ResourceVersion = "" }, "input", false, 0, 0},
		{"initial read failure", func(c *publicationClient, _ *corev1.Pod) { c.readFailure = 1 }, "read Pod", false, 0, 0},
		{"Secret read failure", func(c *publicationClient, _ *corev1.Pod) { c.readFailure = 2 }, "read Secret", false, 1, 0},
		{"Pod reread failure", func(c *publicationClient, _ *corev1.Pod) { c.readFailure = 3 }, "read Pod", false, 1, 0},
		{"deleting input", func(_ *publicationClient, p *corev1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now }, "input", false, 0, 0},
		{"replaced Pod", func(c *publicationClient, _ *corev1.Pod) { c.get = func(o client.Object) { o.SetUID("foreign") } }, "validate Pod", false, 0, 0},
		{"create forbidden", func(c *publicationClient, _ *corev1.Pod) {
			c.create = func(context.Context, client.Object) error {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "s", sentinel)
			}
		}, "create", false, 1, 0},
		{"create uncertain", func(c *publicationClient, _ *corev1.Pod) {
			c.create = func(context.Context, client.Object) error { return sentinel }
		}, "create", true, 1, 0},
		{"create response UID missing", func(c *publicationClient, _ *corev1.Pod) {
			before := c.create
			c.create = func(ctx context.Context, o client.Object) error { err := before(ctx, o); o.SetUID(""); return err }
		}, "create", true, 1, 0},
		{"create response RV missing", func(c *publicationClient, _ *corev1.Pod) {
			before := c.create
			c.create = func(ctx context.Context, o client.Object) error {
				err := before(ctx, o)
				o.SetResourceVersion("")
				return err
			}
		}, "create", true, 1, 0},
		{"Secret replaced", func(c *publicationClient, _ *corev1.Pod) {
			c.get = func(o client.Object) {
				if _, ok := o.(*corev1.Secret); ok {
					o.SetUID("foreign")
				}
			}
		}, "validate binding", false, 1, 0},
		{"Secret RV changed", func(c *publicationClient, _ *corev1.Pod) {
			c.get = func(o client.Object) {
				if _, ok := o.(*corev1.Secret); ok {
					o.SetResourceVersion("other")
				}
			}
		}, "validate binding", false, 1, 0},
		{"Secret tampered", func(c *publicationClient, _ *corev1.Pod) {
			c.get = func(o client.Object) {
				if s, ok := o.(*corev1.Secret); ok {
					s.Data[egressidentity.ClientPrivateKeyKey] = []byte("SECRET-SENTINEL")
				}
			}
		}, "validate binding", false, 1, 0},
		{"Secret deleting", func(c *publicationClient, _ *corev1.Pod) {
			c.get = func(o client.Object) {
				if _, ok := o.(*corev1.Secret); ok {
					now := metav1.Now()
					o.SetDeletionTimestamp(&now)
				}
			}
		}, "validate binding", false, 1, 0},
		{"Pod deleting after create", func(c *publicationClient, _ *corev1.Pod) {
			c.get = func(o client.Object) {
				if c.reads == 3 {
					now := metav1.Now()
					o.SetDeletionTimestamp(&now)
				}
			}
		}, "validate binding", false, 1, 0},
		{"Pod replaced after create", func(c *publicationClient, _ *corev1.Pod) {
			c.get = func(o client.Object) {
				if c.reads == 3 {
					o.SetUID("foreign")
				}
			}
		}, "validate binding", false, 1, 0},
		{"admission mutation", func(c *publicationClient, _ *corev1.Pod) {
			c.get = func(o client.Object) {
				if c.reads == 3 {
					o.(*corev1.Pod).Spec.Containers[0].Image = "changed"
				}
			}
		}, "validate binding", false, 1, 0},
		{"patch conflict", func(c *publicationClient, _ *corev1.Pod) {
			c.patch = func(context.Context, client.Object, client.Patch) error {
				return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, "p", sentinel)
			}
		}, "release", false, 1, 1},
		{"patch uncertain", func(c *publicationClient, _ *corev1.Pod) {
			c.patch = func(context.Context, client.Object, client.Patch) error { return sentinel }
		}, "release", true, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, pod, in, claims := publicationFixture(t)
			tc.setup(c, pod)
			err := PublishCredential(context.Background(), c, c, time.Now(), pod, in, claims)
			var failure *PublicationError
			if !errors.As(err, &failure) || failure.Stage != tc.stage || failure.Uncertain != tc.uncertain {
				t.Fatalf("unexpected outcome: %v", err)
			}
			if strings.Contains(err.Error(), "SECRET-SENTINEL") {
				t.Fatal("sensitive cause exposed")
			}
			if c.creates != tc.creates || c.patches != tc.patches {
				t.Fatalf("unexpected writes: %d %d", c.creates, c.patches)
			}
			var stored corev1.Pod
			if err := c.Client.Get(context.Background(), client.ObjectKeyFromObject(pod), &stored); err != nil {
				t.Fatal(err)
			}
			if len(stored.Spec.SchedulingGates) != 1 || stored.Annotations[CertificateFingerprintAnnotation] != "" {
				t.Fatal("failed transaction changed stored Pod")
			}
		})
	}
}

func TestPublicationLostResponsesAndBinding(t *testing.T) {
	for _, stage := range []string{"create", "release"} {
		t.Run(stage, func(t *testing.T) {
			c, pod, in, claims := publicationFixture(t)
			if stage == "create" {
				create := c.create
				c.create = func(ctx context.Context, o client.Object) error {
					if err := create(ctx, o); err != nil {
						return err
					}
					return errors.New("lost response")
				}
			} else {
				c.patch = func(ctx context.Context, o client.Object, p client.Patch) error {
					if err := c.Client.Patch(ctx, o, p); err != nil {
						return err
					}
					return errors.New("lost response")
				}
			}
			err := PublishCredential(context.Background(), c, c, time.Now(), pod, in, claims)
			var failure *PublicationError
			if !errors.As(err, &failure) || !failure.Uncertain || failure.Stage != stage {
				t.Fatalf("lost response reported incorrectly: %v", err)
			}
			if c.creates != 1 || c.patches > 1 {
				t.Fatal("write retried")
			}
			if stage == "release" {
				assertPublished(t, c.Client, pod, in)
				return
			}
			// A fresh call after losing the private binding cannot adopt the
			// stored Secret, even when it was created by this exact execution.
			err = PublishCredential(context.Background(), c, c, time.Now(), pod, in, claims)
			if !errors.As(err, &failure) || failure.Uncertain || failure.Stage != "create" || c.patches != 0 {
				t.Fatalf("lost binding adopted: %v", err)
			}
		})
	}
}

func TestPublicationPreservesForeignSecret(t *testing.T) {
	c, pod, in, claims := publicationFixture(t)
	ctx := context.Background()
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: in.CredentialSecret, Namespace: pod.Namespace, UID: "foreign-secret"}, Data: map[string][]byte{"foreign": []byte("SECRET-SENTINEL")}}
	if err := c.Client.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	err := PublishCredential(ctx, c, c, time.Now(), pod, in, claims)
	var failure *PublicationError
	if !errors.As(err, &failure) || failure.Stage != "create" || failure.Uncertain || c.creates != 1 || c.patches != 0 || c.reads != 1 {
		t.Fatalf("collision outcome: %v", err)
	}
	var retained corev1.Secret
	if err := c.Client.Get(ctx, client.ObjectKeyFromObject(foreign), &retained); err != nil {
		t.Fatal(err)
	}
	if retained.UID != foreign.UID || retained.ResourceVersion != foreign.ResourceVersion || string(retained.Data["foreign"]) != "SECRET-SENTINEL" || len(retained.Data) != 1 {
		t.Fatal("foreign Secret mutated")
	}
}

func TestPublicationInvalidReleaseResponseIsUncertain(t *testing.T) {
	for _, mutate := range []func(client.Object){
		func(o client.Object) { o.SetUID("") },
		func(o client.Object) { o.SetResourceVersion("") },
		func(o client.Object) { o.(*corev1.Pod).Spec.Containers[0].Image = "admission-change" },
	} {
		c, pod, in, claims := publicationFixture(t)
		c.patch = func(ctx context.Context, o client.Object, patch client.Patch) error {
			if err := c.Client.Patch(ctx, o, patch); err != nil {
				return err
			}
			mutate(o)
			return nil
		}
		err := PublishCredential(context.Background(), c, c, time.Now(), pod, in, claims)
		var failure *PublicationError
		if !errors.As(err, &failure) || failure.Stage != "release" || !failure.Uncertain || c.patches != 1 {
			t.Fatalf("invalid response reported as success: %v", err)
		}
		assertPublished(t, c.Client, pod, in)
	}
}
