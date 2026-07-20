package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

const cliKeyFixture = "!!CLI-API-KEY-FIXTURE!!"

func credentialCLIScheme(t *testing.T) *runtime.Scheme {
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

func cliCredentialObjects(value []byte) (*platformv1alpha1.AgentCredentialProfile, *corev1.Secret) {
	profile := &platformv1alpha1.AgentCredentialProfile{
		ObjectMeta: metav1.ObjectMeta{Name: "profile", Namespace: "ns", UID: "profile-uid"},
		Spec: platformv1alpha1.AgentCredentialProfileSpec{
			Adapter: "claude-code", CredentialType: platformv1alpha1.AgentCredentialTypeAPIKey,
		},
	}
	return profile, credentialSecret(profile, value)
}

func TestReadAPIKeyIsBoundedAndStripsAtMostOneLineEnding(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []byte
		wantErr bool
	}{
		{name: "no ending", input: []byte("key"), want: []byte("key")},
		{name: "LF", input: []byte("key\n"), want: []byte("key")},
		{name: "CRLF", input: []byte("key\r\n"), want: []byte("key")},
		{name: "lone CR retained", input: []byte("key\r"), want: []byte("key\r")},
		{name: "one of two endings", input: []byte("key\n\n"), want: []byte("key\n")},
		{name: "empty", input: nil, wantErr: true},
		{name: "line ending only", input: []byte("\r\n"), wantErr: true},
		{name: "invalid UTF-8", input: []byte{0xff}, wantErr: true},
		{name: "NUL", input: []byte{'k', 0}, wantErr: true},
		{name: "exact maximum", input: bytes.Repeat([]byte{'x'}, platformv1alpha1.AgentCredentialAPIKeyMaxBytes), want: bytes.Repeat([]byte{'x'}, platformv1alpha1.AgentCredentialAPIKeyMaxBytes)},
		{name: "exact maximum CRLF", input: append(bytes.Repeat([]byte{'x'}, platformv1alpha1.AgentCredentialAPIKeyMaxBytes), '\r', '\n'), want: bytes.Repeat([]byte{'x'}, platformv1alpha1.AgentCredentialAPIKeyMaxBytes)},
		{name: "oversize", input: bytes.Repeat([]byte{'x'}, platformv1alpha1.AgentCredentialAPIKeyMaxBytes+1), wantErr: true},
		{name: "bounded overflow", input: bytes.Repeat([]byte{'x'}, platformv1alpha1.AgentCredentialAPIKeyMaxBytes+3), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := readAPIKey(bytes.NewReader(test.input))
			if (err != nil) != test.wantErr || !test.wantErr && !bytes.Equal(got, test.want) {
				t.Fatalf("readAPIKey() = (%d bytes, %v), want %d bytes, error=%t", len(got), err, len(test.want), test.wantErr)
			}
		})
	}
}

func TestCreateCredentialCreatesAndRepairsWithoutOverwrite(t *testing.T) {
	t.Run("new profile", func(t *testing.T) {
		base := fake.NewClientBuilder().WithScheme(credentialCLIScheme(t)).Build()
		assigned := interceptor.NewClient(base, interceptor.Funcs{Create: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.CreateOption) error {
			if profile, ok := object.(*platformv1alpha1.AgentCredentialProfile); ok && profile.UID == "" {
				profile.UID = "profile-uid"
			}
			return underlying.Create(ctx, object, options...)
		}})
		if err := createCredential(context.Background(), assigned, "ns", "profile", "claude-code", []byte(cliKeyFixture)); err != nil {
			t.Fatal(err)
		}
		profile, _ := cliCredentialObjects(nil)
		var stored corev1.Secret
		if err := base.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: platformv1alpha1.AgentCredentialSecretName(profile.UID)}, &stored); err != nil {
			t.Fatal(err)
		}
		if string(stored.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey]) != cliKeyFixture || validateCredentialSecret(profile, &stored) != nil {
			t.Fatalf("stored credential was not exact and safely owned: %#v", stored)
		}
	})

	t.Run("repair missing Secret", func(t *testing.T) {
		profile, _ := cliCredentialObjects(nil)
		c := fake.NewClientBuilder().WithScheme(credentialCLIScheme(t)).WithObjects(profile).Build()
		if err := createCredential(context.Background(), c, "ns", profile.Name, profile.Spec.Adapter, []byte(cliKeyFixture)); err != nil {
			t.Fatal(err)
		}
		var stored corev1.Secret
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: platformv1alpha1.AgentCredentialSecretName(profile.UID)}, &stored); err != nil || string(stored.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey]) != cliKeyFixture {
			t.Fatalf("repair = %#v, error = %v", stored.Data, err)
		}
	})

	t.Run("existing value is not overwritten", func(t *testing.T) {
		profile, secret := cliCredentialObjects([]byte("!!EXISTING-KEY-FIXTURE!!"))
		c := fake.NewClientBuilder().WithScheme(credentialCLIScheme(t)).WithObjects(profile, secret).Build()
		if err := createCredential(context.Background(), c, "ns", profile.Name, profile.Spec.Adapter, []byte("!!RETRY-KEY-MUST-NOT-WRITE!!")); err != nil {
			t.Fatal(err)
		}
		var stored corev1.Secret
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(secret), &stored); err != nil {
			t.Fatal(err)
		}
		if got := string(stored.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey]); got != "!!EXISTING-KEY-FIXTURE!!" {
			t.Fatalf("existing value was overwritten: %q", got)
		}
	})
}

func TestCreateCredentialRejectsForeignAndMalformedCollisionsWithoutLeaking(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.Secret)
	}{
		{name: "foreign", mutate: func(secret *corev1.Secret) { secret.OwnerReferences[0].UID = "other-profile" }},
		{name: "extra owner", mutate: func(secret *corev1.Secret) {
			secret.OwnerReferences = append(secret.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "other", UID: "other"})
		}},
		{name: "wrong type", mutate: func(secret *corev1.Secret) { secret.Type = corev1.SecretTypeOpaque }},
		{name: "extra key", mutate: func(secret *corev1.Secret) { secret.Data["extra"] = []byte("x") }},
		{name: "empty", mutate: func(secret *corev1.Secret) { secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = nil }},
		{name: "invalid UTF-8", mutate: func(secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = []byte{0xff}
		}},
		{name: "NUL", mutate: func(secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = []byte{'x', 0}
		}},
		{name: "oversize", mutate: func(secret *corev1.Secret) {
			secret.Data[platformv1alpha1.AgentCredentialAPIKeySecretKey] = make([]byte, platformv1alpha1.AgentCredentialAPIKeyMaxBytes+1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile, secret := cliCredentialObjects([]byte("!!COLLISION-KEY-FIXTURE!!"))
			test.mutate(secret)
			base := fake.NewClientBuilder().WithScheme(credentialCLIScheme(t)).WithObjects(profile, secret).Build()
			secretValueReads := 0
			c := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
				if _, ok := object.(*corev1.Secret); ok {
					secretValueReads++
				}
				return underlying.Get(ctx, key, object, options...)
			}})
			err := createCredential(context.Background(), c, "ns", profile.Name, profile.Spec.Adapter, []byte(cliKeyFixture))
			if err == nil || strings.Contains(err.Error(), cliKeyFixture) || strings.Contains(err.Error(), secret.Name) {
				t.Fatalf("unsafe collision error = %v", err)
			}
			if (test.name == "foreign" || test.name == "extra owner") && secretValueReads != 0 {
				t.Fatalf("foreign collision caused %d Secret value reads", secretValueReads)
			}
		})
	}
}

func TestRotateCredentialRejectsForeignMetadataWithoutReadingSecretData(t *testing.T) {
	profile, secret := cliCredentialObjects([]byte("!!FOREIGN-KEY-FIXTURE!!"))
	secret.OwnerReferences[0].UID = "foreign-profile"
	base := fake.NewClientBuilder().WithScheme(credentialCLIScheme(t)).WithObjects(profile, secret).Build()
	secretValueReads := 0
	c := interceptor.NewClient(base, interceptor.Funcs{Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
		if _, ok := object.(*corev1.Secret); ok {
			secretValueReads++
		}
		return underlying.Get(ctx, key, object, options...)
	}})
	err := rotateCredential(context.Background(), c, profile.Namespace, profile.Name, []byte(cliKeyFixture))
	if err == nil || secretValueReads != 0 || strings.Contains(err.Error(), cliKeyFixture) || strings.Contains(err.Error(), secret.Name) {
		t.Fatalf("foreign rotate = error %v, Secret value reads %d", err, secretValueReads)
	}
}

func TestRotateCredentialUsesResourceVersionAndSurfacesConflictSafely(t *testing.T) {
	profile, secret := cliCredentialObjects([]byte("!!OLD-KEY-FIXTURE!!"))
	secret.ResourceVersion = "17"
	base := fake.NewClientBuilder().WithScheme(credentialCLIScheme(t)).WithObjects(profile, secret).Build()
	updates := 0
	seenResourceVersion := ""
	c := interceptor.NewClient(base, interceptor.Funcs{Update: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.UpdateOption) error {
		updates++
		seenResourceVersion = object.GetResourceVersion()
		return apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, object.GetName(), errors.New("simulated rotation conflict"))
	}})
	err := rotateCredential(context.Background(), c, "ns", profile.Name, []byte(cliKeyFixture))
	if !apierrors.IsConflict(err) || updates != 1 || seenResourceVersion != "17" || strings.Contains(err.Error(), cliKeyFixture) || strings.Contains(err.Error(), secret.Name) {
		t.Fatalf("rotate = error %v, updates %d, resourceVersion %q", err, updates, seenResourceVersion)
	}
}

func TestListAndDeleteCredentialsNeverAccessSecrets(t *testing.T) {
	profile, secret := cliCredentialObjects([]byte(cliKeyFixture))
	base := fake.NewClientBuilder().WithScheme(credentialCLIScheme(t)).WithObjects(profile, secret).Build()
	secretAccesses := 0
	profileLists := 0
	profileDeletes := 0
	c := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
			if _, ok := object.(*corev1.Secret); ok {
				secretAccesses++
			}
			return underlying.Get(ctx, key, object, options...)
		},
		List: func(ctx context.Context, underlying client.WithWatch, list client.ObjectList, options ...client.ListOption) error {
			switch list.(type) {
			case *corev1.SecretList:
				secretAccesses++
			case *platformv1alpha1.AgentCredentialProfileList:
				profileLists++
			}
			return underlying.List(ctx, list, options...)
		},
		Delete: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.DeleteOption) error {
			if _, ok := object.(*corev1.Secret); ok {
				secretAccesses++
			}
			if _, ok := object.(*platformv1alpha1.AgentCredentialProfile); ok {
				profileDeletes++
			}
			return underlying.Delete(ctx, object, options...)
		},
	})
	var out bytes.Buffer
	if err := listCredentials(context.Background(), c, "ns", &out); err != nil {
		t.Fatal(err)
	}
	if err := deleteCredential(context.Background(), c, "ns", profile.Name); err != nil {
		t.Fatal(err)
	}
	if secretAccesses != 0 || profileLists != 1 || profileDeletes != 1 || out.String() != "NAME\tAGENT\tTYPE\nprofile\tclaude-code\tAPIKey\n" || strings.Contains(out.String(), secret.Name) || strings.Contains(out.String(), cliKeyFixture) {
		t.Fatalf("list/delete accesses=%d/%d/%d output=%q", secretAccesses, profileLists, profileDeletes, out.String())
	}
}

func TestCredentialCommandContractAndRunCollisionIncludeProfile(t *testing.T) {
	root := NewRootCommand()
	credentials, _, err := root.Find([]string{"credentials"})
	if err != nil || credentials == nil {
		t.Fatalf("credentials command: %v", err)
	}
	for _, name := range []string{"create", "rotate", "list", "delete"} {
		if command, _, findErr := root.Find([]string{"credentials", name}); findErr != nil || command == credentials {
			t.Fatalf("credentials %s command: %v", name, findErr)
		}
	}

	scheme := credentialCLIScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	clients := &kubeClients{Client: base}
	if err := createRunWithCredential(context.Background(), clients, "ns", "stable", "small", "", "", "claude-code", "task", "profile-a", false, 0); err != nil {
		t.Fatal(err)
	}
	if err := createRunWithCredential(context.Background(), clients, "ns", "stable", "small", "", "", "claude-code", "task", "profile-b", false, 0); err == nil {
		t.Fatal("same Run name with a different credential profile was reused")
	}
	var run platformv1alpha1.Run
	if err := base.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "stable"}, &run); err != nil || run.Spec.CredentialProfileRef != "profile-a" {
		t.Fatalf("stored profile = %q, error = %v", run.Spec.CredentialProfileRef, err)
	}
}
