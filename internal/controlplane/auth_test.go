package controlplane

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestKubernetesAccessControllerReviewsExactResourceIdentity(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		review := action.(ktesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		if review.Spec.Token != "reader-token" {
			t.Fatalf("reviewed token = %q, want reader-token", review.Spec.Token)
		}
		if len(review.Spec.Audiences) != 1 || review.Spec.Audiences[0] != defaultTokenAudience {
			t.Fatalf("reviewed audiences = %v, want [%s]", review.Spec.Audiences, defaultTokenAudience)
		}
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     []string{defaultTokenAudience},
			User: authenticationv1.UserInfo{
				Username: "reader",
				UID:      "reader-uid",
				Groups:   []string{"readers"},
				Extra:    map[string]authenticationv1.ExtraValue{"credential-id": {"session-1"}},
			},
		}}, nil
	})
	client.PrependReactor("create", "subjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		review := action.(ktesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		attributes := review.Spec.ResourceAttributes
		if review.Spec.User != "reader" || review.Spec.UID != "reader-uid" || len(review.Spec.Groups) != 1 || review.Spec.Groups[0] != "readers" {
			t.Fatalf("subject = %+v, want authenticated reader identity", review.Spec)
		}
		if values := review.Spec.Extra["credential-id"]; len(values) != 1 || values[0] != "session-1" {
			t.Fatalf("subject extra = %v, want credential-id session-1", review.Spec.Extra)
		}
		want := ResourceAccess{Namespace: "project-a", Verb: "get", Resource: "runs", Subresource: "transcript", Name: "shared"}
		if attributes.Namespace != want.Namespace || attributes.Verb != want.Verb || attributes.Group != "swe.dev" || attributes.Version != "v1alpha1" || attributes.Resource != want.Resource || attributes.Subresource != want.Subresource || attributes.Name != want.Name {
			t.Fatalf("resource attributes = %+v, want %+v", attributes, want)
		}
		return true, &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
	})

	request := httptest.NewRequest(http.MethodGet, transcriptURL, nil)
	request.Header.Set("Authorization", "Bearer reader-token")
	err := (KubernetesAccessController{Client: client}).Authorize(request, ResourceAccess{
		Namespace: "project-a", Verb: "get", Resource: "runs", Subresource: "transcript", Name: "shared",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestKubernetesAccessControllerRejectsAnonymousAndDeniedUsers(t *testing.T) {
	controller := KubernetesAccessController{}
	request := httptest.NewRequest(http.MethodGet, transcriptURL, nil)
	if err := controller.Authorize(request, ResourceAccess{}, true); !errors.Is(err, errUnauthenticated) {
		t.Fatalf("anonymous error = %v, want unauthenticated", err)
	}

	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     []string{defaultTokenAudience},
			User:          authenticationv1.UserInfo{Username: "producer-a"},
		}}, nil
	})
	client.PrependReactor("create", "subjectaccessreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: false}}, nil
	})
	request.Header.Set("Authorization", "Bearer producer-token")
	if err := (KubernetesAccessController{Client: client}).Authorize(request, ResourceAccess{}, false); !errors.Is(err, errForbidden) {
		t.Fatalf("denied error = %v, want forbidden", err)
	}
}

func TestBootstrapAndBrowserSessionCredentials(t *testing.T) {
	bootstrapToken := "bootstrap-secret-at-least-32-bytes"
	controller := KubernetesAccessController{BootstrapToken: bootstrapToken}

	bootstrap := httptest.NewRequest(http.MethodPost, transcriptURL, nil)
	bootstrap.Header.Set("Authorization", "Bearer "+bootstrapToken)
	if err := controller.Authorize(bootstrap, ResourceAccess{}, false); err != nil {
		t.Fatalf("bootstrap token rejected: %v", err)
	}

	session := httptest.NewRequest(http.MethodGet, transcriptURL, nil)
	session.AddCookie(&http.Cookie{Name: sessionCookieName, Value: bootstrapToken})
	if err := controller.Authorize(session, ResourceAccess{}, true); !errors.Is(err, errUnauthenticated) {
		t.Fatalf("bootstrap session error = %v, want unauthenticated", err)
	}
	if err := controller.Authorize(session, ResourceAccess{}, false); !errors.Is(err, errUnauthenticated) {
		t.Fatalf("producer session error = %v, want unauthenticated", err)
	}
}

func TestKubernetesAccessControllerRejectsWrongAudience(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     []string{"other-service"},
			User:          authenticationv1.UserInfo{Username: "reader"},
		}}, nil
	})
	request := httptest.NewRequest(http.MethodGet, transcriptURL, nil)
	request.Header.Set("Authorization", "Bearer wrong-audience")
	if err := (KubernetesAccessController{Client: client}).Authorize(request, ResourceAccess{}, true); !errors.Is(err, errUnauthenticated) {
		t.Fatalf("wrong-audience error = %v, want unauthenticated", err)
	}
}

func TestBrowserSessionAuthenticatesReadsButNotProducerWrites(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		review := action.(ktesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		if review.Spec.Token != "browser-session-token" {
			t.Fatalf("reviewed token = %q, want browser-session-token", review.Spec.Token)
		}
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     []string{defaultTokenAudience},
			User:          authenticationv1.UserInfo{Username: "browser-user"},
		}}, nil
	})
	client.PrependReactor("create", "subjectaccessreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
	})
	controller := KubernetesAccessController{Client: client}
	request := httptest.NewRequest(http.MethodGet, transcriptURL, nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "browser-session-token"})
	if err := controller.Authorize(request, ResourceAccess{}, true); err != nil {
		t.Fatalf("browser reader session rejected: %v", err)
	}
	if err := controller.Authorize(request, ResourceAccess{}, false); !errors.Is(err, errUnauthenticated) {
		t.Fatalf("browser producer session error = %v, want unauthenticated", err)
	}
}
