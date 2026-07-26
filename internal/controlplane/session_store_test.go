package controlplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type fakeSessionStore struct {
	validate func(string) error
	create   func(context.Context, string) (string, error)
	resolve  func(context.Context, string) (string, error)
	delete   func(context.Context, string) error
}

func (s *fakeSessionStore) ValidateToken(token string) error {
	if s.validate == nil {
		return nil
	}
	return s.validate(token)
}

func (s *fakeSessionStore) Create(ctx context.Context, token string) (string, error) {
	if s.create == nil {
		return "", ErrSessionUnavailable
	}
	return s.create(ctx, token)
}

func (s *fakeSessionStore) Resolve(ctx context.Context, id string) (string, error) {
	if s.resolve == nil {
		return "", ErrSessionUnavailable
	}
	return s.resolve(ctx, id)
}

func (s *fakeSessionStore) Delete(ctx context.Context, id string) error {
	if s.delete == nil {
		return nil
	}
	return s.delete(ctx, id)
}

func TestMemorySessionStoreBoundsAndAbsoluteExpiry(t *testing.T) {
	now := time.Date(2026, 7, 19, 19, 0, 0, 0, time.UTC)
	store := NewMemorySessionStore(MemorySessionStoreOptions{
		AbsoluteTTL:       time.Minute,
		MaxTokenBytes:     5,
		MaxActiveSessions: 2,
		Now:               func() time.Time { return now },
		Random:            bytes.NewReader(append(bytes.Repeat([]byte{1}, 32), append(bytes.Repeat([]byte{2}, 32), bytes.Repeat([]byte{3}, 32)...)...)),
	})
	ctx := context.Background()
	first, err := store.Create(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Create(ctx, "two")
	if err != nil || first == second {
		t.Fatalf("second session = %q, err %v", second, err)
	}
	if _, err := store.Create(ctx, "three"); !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	if _, err := store.Create(ctx, "123456"); !errors.Is(err, ErrSessionUnauthenticated) {
		t.Fatalf("oversized token error = %v", err)
	}
	key := sha256SessionKey(first)
	if entry := store.sessions[key]; !entry.createdAt.Equal(now) || !entry.expiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("stored times = %s/%s", entry.createdAt, entry.expiresAt)
	}
	now = now.Add(time.Minute)
	if _, err := store.Resolve(ctx, first); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("expired resolve error = %v", err)
	}
	if _, err := store.Resolve(ctx, first); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("purged resolve error = %v", err)
	}
	if err := store.Delete(ctx, first); err != nil {
		t.Fatalf("idempotent delete error = %v", err)
	}
	if _, err := store.Create(ctx, "new"); err != nil {
		t.Fatalf("expired sessions did not release capacity: %v", err)
	}
}

func TestSessionCreateUsesConfiguredStoreTokenBoundBeforeReview(t *testing.T) {
	t.Run("below default", func(t *testing.T) {
		reviews := 0
		client := fake.NewClientset()
		client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
			reviews++
			return true, nil, errors.New("unexpected TokenReview")
		})
		controller := KubernetesAccessController{
			Client:   client,
			Sessions: NewMemorySessionStore(MemorySessionStoreOptions{MaxTokenBytes: 5}),
		}
		request := httptest.NewRequest(http.MethodPost, "https://console.test/api/v1/session", nil)
		request.Header.Set("Authorization", "Bearer 123456")
		if _, _, err := controller.CreateSession(request); !errors.Is(err, errUnauthenticated) {
			t.Fatalf("create error = %v", err)
		}
		if reviews != 0 {
			t.Fatalf("invalid token caused %d TokenReviews", reviews)
		}
	})

	t.Run("above default", func(t *testing.T) {
		token := strings.Repeat("x", defaultMaxSessionTokenBytes+1)
		client := fake.NewClientset()
		client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				Audiences:     []string{defaultTokenAudience},
				User:          authenticationv1.UserInfo{Username: "console-user"},
			}}, nil
		})
		controller := KubernetesAccessController{
			Client: client,
			Sessions: NewMemorySessionStore(MemorySessionStoreOptions{
				MaxTokenBytes: len(token),
			}),
		}
		request := httptest.NewRequest(http.MethodPost, "https://console.test/api/v1/session", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		if _, _, err := controller.CreateSession(request); err != nil {
			t.Fatalf("create error = %v", err)
		}
	})
}

func TestOpaqueSessionLogoutRevocationAndBearerPreference(t *testing.T) {
	authenticated := true
	reviewedTokens := []string{}
	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		token := action.(ktesting.CreateAction).GetObject().(*authenticationv1.TokenReview).Spec.Token
		reviewedTokens = append(reviewedTokens, token)
		allowed := authenticated || token == "service-token"
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: allowed,
			Audiences:     []string{defaultTokenAudience},
			User:          authenticationv1.UserInfo{Username: "console-user"},
		}}, nil
	})
	client.PrependReactor("create", "subjectaccessreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
	})
	store := NewMemorySessionStore(MemorySessionStoreOptions{})
	controller := KubernetesAccessController{Client: client, Sessions: store}
	server := NewServer(nil, ServerOptions{Access: controller, Sessions: controller})

	post := httptest.NewRequest(http.MethodPost, "https://console.test/api/v1/session", nil)
	post.Header.Set("Authorization", "Bearer kubernetes-token")
	postResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(postResponse, post)
	if postResponse.Code != http.StatusOK {
		t.Fatalf("session create = %d: %s", postResponse.Code, postResponse.Body.String())
	}
	cookie := postResponse.Result().Cookies()[0]
	if cookie.Value == "kubernetes-token" || len(cookie.Value) != 43 {
		t.Fatalf("cookie contains non-opaque value %q", cookie.Value)
	}

	get := httptest.NewRequest(http.MethodGet, "https://console.test/api/v1/session", nil)
	get.AddCookie(cookie)
	getResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK || reviewedTokens[len(reviewedTokens)-1] != "kubernetes-token" {
		t.Fatalf("session validation = %d, reviewed %v", getResponse.Code, reviewedTokens)
	}

	authenticated = false
	revokedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(revokedResponse, get.Clone(get.Context()))
	if revokedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("revoked upstream token status = %d", revokedResponse.Code)
	}
	authenticated = true
	reviewsAfterRevocation := len(reviewedTokens)
	replayResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(replayResponse, get.Clone(get.Context()))
	if replayResponse.Code != http.StatusUnauthorized || len(reviewedTokens) != reviewsAfterRevocation {
		t.Fatalf("revoked session replay = %d, reviews %v", replayResponse.Code, reviewedTokens)
	}

	bearer := httptest.NewRequest(http.MethodGet, "https://console.test/api/v1/namespaces/ns/runs/r", nil)
	bearer.Header.Set("Authorization", "Bearer service-token")
	bearer.AddCookie(cookie)
	if err := controller.Authorize(bearer, ResourceAccess{Namespace: "ns", Verb: "get", Resource: "runs", Name: "r"}, true); err != nil {
		t.Fatalf("explicit bearer was not preferred: %v", err)
	}

	// Mint a replacement and verify logout deletes server-side state, not only
	// the browser cookie.
	postResponse = httptest.NewRecorder()
	server.Handler().ServeHTTP(postResponse, post.Clone(post.Context()))
	logoutCookie := postResponse.Result().Cookies()[0]
	logout := httptest.NewRequest(http.MethodDelete, "https://console.test/api/v1/session", nil)
	logout.Header.Set("Origin", "https://console.test")
	logout.AddCookie(logoutCookie)
	logoutResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(logoutResponse, logout)
	if logoutResponse.Code != http.StatusNoContent {
		t.Fatalf("logout = %d: %s", logoutResponse.Code, logoutResponse.Body.String())
	}
	replay := httptest.NewRequest(http.MethodGet, "https://console.test/api/v1/session", nil)
	replay.AddCookie(logoutCookie)
	replayResponse = httptest.NewRecorder()
	server.Handler().ServeHTTP(replayResponse, replay)
	if replayResponse.Code != http.StatusUnauthorized {
		t.Fatalf("logout replay status = %d", replayResponse.Code)
	}
	if strings.Contains(postResponse.Body.String(), "kubernetes-token") {
		t.Fatalf("session response leaked Kubernetes token: %s", postResponse.Body.String())
	}
}

func TestCookieResourceAuthorizationReviewsEveryRequestAndRevokes(t *testing.T) {
	authenticated := true
	events := []string{}
	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		events = append(events, "tokenreview")
		review := action.(ktesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		if review.Spec.Token != "resource-token" {
			t.Fatalf("reviewed token = %q", review.Spec.Token)
		}
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: authenticated,
			Audiences:     []string{defaultTokenAudience},
			User:          authenticationv1.UserInfo{Username: "console-user"},
		}}, nil
	})
	wantAccess := ResourceAccess{Namespace: "project-a", Verb: "update", Resource: "runs", Name: "run-a"}
	client.PrependReactor("create", "subjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		events = append(events, "sar")
		attributes := action.(ktesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview).Spec.ResourceAttributes
		if attributes.Namespace != wantAccess.Namespace || attributes.Verb != wantAccess.Verb || attributes.Resource != wantAccess.Resource || attributes.Name != wantAccess.Name || attributes.Subresource != "" {
			t.Fatalf("SAR attributes = %#v", attributes)
		}
		return true, &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
	})
	store := NewMemorySessionStore(MemorySessionStoreOptions{})
	sessionID, err := store.Create(context.Background(), "resource-token")
	if err != nil {
		t.Fatal(err)
	}
	controller := KubernetesAccessController{Client: client, Sessions: store}
	request := httptest.NewRequest(http.MethodPost, "https://console.test/api/v1/namespaces/project-a/runs/run-a/cancel", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	for range 2 {
		if err := controller.Authorize(request, wantAccess, true); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Join(events, ","); got != "tokenreview,sar,tokenreview,sar" {
		t.Fatalf("review order = %s", got)
	}
	authenticated = false
	if err := controller.Authorize(request, wantAccess, true); !errors.Is(err, errUnauthenticated) {
		t.Fatalf("revocation error = %v", err)
	}
	eventCount := len(events)
	authenticated = true
	if err := controller.Authorize(request, wantAccess, true); !errors.Is(err, errUnauthenticated) {
		t.Fatalf("replay error = %v", err)
	}
	if len(events) != eventCount {
		t.Fatalf("deleted session replay reached Kubernetes: %v", events[eventCount:])
	}
}

func TestSessionResolveFailureSkipsTokenReview(t *testing.T) {
	reviews := 0
	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		reviews++
		return true, nil, errors.New("unexpected TokenReview")
	})
	store := &fakeSessionStore{resolve: func(context.Context, string) (string, error) {
		return "", ErrSessionUnavailable
	}}
	controller := KubernetesAccessController{Client: client, Sessions: store}
	request := httptest.NewRequest(http.MethodGet, "https://console.test/api/v1/namespaces/ns/runs", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "session"})
	if err := controller.Authorize(request, ResourceAccess{}, true); !errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("resolve error = %v", err)
	}
	if reviews != 0 {
		t.Fatalf("resolve failure caused %d TokenReviews", reviews)
	}
}

func TestTransientTokenReviewFailureRetainsSessionForRetry(t *testing.T) {
	for _, tc := range []struct {
		name        string
		firstResult *authenticationv1.TokenReview
		firstError  error
	}{
		{name: "transport", firstError: errors.New("apiserver unavailable")},
		{name: "status error", firstResult: &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{Error: "webhook unavailable"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reviews := 0
			client := fake.NewClientset()
			client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
				reviews++
				if reviews == 1 {
					return true, tc.firstResult, tc.firstError
				}
				return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
					Authenticated: true,
					Audiences:     []string{defaultTokenAudience},
					User:          authenticationv1.UserInfo{Username: "console-user"},
				}}, nil
			})
			client.PrependReactor("create", "subjectaccessreviews", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
			})
			store := NewMemorySessionStore(MemorySessionStoreOptions{})
			sessionID, err := store.Create(context.Background(), "resource-token")
			if err != nil {
				t.Fatal(err)
			}
			controller := KubernetesAccessController{Client: client, Sessions: store}
			request := httptest.NewRequest(http.MethodGet, "https://console.test/api/v1/namespaces/ns/runs", nil)
			request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
			if err := controller.Authorize(request, ResourceAccess{}, true); err == nil || errors.Is(err, errUnauthenticated) {
				t.Fatalf("transient TokenReview error = %v", err)
			}
			if err := controller.Authorize(request, ResourceAccess{}, true); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			if reviews != 2 {
				t.Fatalf("TokenReviews = %d, want 2", reviews)
			}
		})
	}
}

func TestAudienceMismatchRevokesSession(t *testing.T) {
	for _, tc := range []struct {
		name      string
		operation func(KubernetesAccessController, *http.Request) error
	}{
		{name: "resource authorization", operation: func(controller KubernetesAccessController, request *http.Request) error {
			return controller.Authorize(request, ResourceAccess{}, true)
		}},
		{name: "current session", operation: func(controller KubernetesAccessController, request *http.Request) error {
			_, err := controller.CurrentSession(request)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset()
			client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
					Authenticated: true,
					Audiences:     []string{"other-service"},
					User:          authenticationv1.UserInfo{Username: "console-user"},
				}}, nil
			})
			deletes := 0
			contextKey := struct{}{}
			store := &fakeSessionStore{
				resolve: func(context.Context, string) (string, error) { return "resource-token", nil },
				delete: func(ctx context.Context, id string) error {
					deletes++
					if id != "session" || ctx.Value(contextKey) != "request" {
						t.Fatalf("delete id/context = %q/%v", id, ctx.Value(contextKey))
					}
					return nil
				},
			}
			controller := KubernetesAccessController{Client: client, Sessions: store}
			ctx := context.WithValue(context.Background(), contextKey, "request")
			request := httptest.NewRequest(http.MethodGet, "https://console.test/api/v1/namespaces/ns/runs", nil).WithContext(ctx)
			request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "session"})
			if err := tc.operation(controller, request); !errors.Is(err, errUnauthenticated) {
				t.Fatalf("audience mismatch error = %v", err)
			}
			if deletes != 1 {
				t.Fatalf("deletes = %d, want 1", deletes)
			}
		})
	}
}

func TestDefinitiveRejectionDeleteFailureIsUnavailable(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{Authenticated: false}}, nil
	})
	store := &fakeSessionStore{
		resolve: func(context.Context, string) (string, error) { return "resource-token", nil },
		delete:  func(context.Context, string) error { return ErrSessionUnavailable },
	}
	controller := KubernetesAccessController{Client: client, Sessions: store}
	request := httptest.NewRequest(http.MethodGet, "https://console.test/api/v1/namespaces/ns/runs", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "session"})
	if err := controller.Authorize(request, ResourceAccess{}, true); !errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("delete failure error = %v", err)
	}
}

func TestSubjectAccessReviewFailureRetainsSession(t *testing.T) {
	for _, tc := range []struct {
		name       string
		first      authorizationv1.SubjectAccessReviewStatus
		firstError error
	}{
		{name: "denied", first: authorizationv1.SubjectAccessReviewStatus{Allowed: false}},
		{name: "transport", firstError: errors.New("apiserver unavailable")},
		{name: "evaluation error", first: authorizationv1.SubjectAccessReviewStatus{EvaluationError: "webhook unavailable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset()
			reviews := 0
			client.PrependReactor("create", "tokenreviews", func(ktesting.Action) (bool, runtime.Object, error) {
				reviews++
				return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
					Authenticated: true,
					Audiences:     []string{defaultTokenAudience},
					User:          authenticationv1.UserInfo{Username: "console-user"},
				}}, nil
			})
			sars := 0
			client.PrependReactor("create", "subjectaccessreviews", func(ktesting.Action) (bool, runtime.Object, error) {
				sars++
				if sars == 1 {
					return true, &authorizationv1.SubjectAccessReview{Status: tc.first}, tc.firstError
				}
				return true, &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
			})
			store := NewMemorySessionStore(MemorySessionStoreOptions{})
			sessionID, err := store.Create(context.Background(), "resource-token")
			if err != nil {
				t.Fatal(err)
			}
			controller := KubernetesAccessController{Client: client, Sessions: store}
			request := httptest.NewRequest(http.MethodGet, "https://console.test/api/v1/namespaces/ns/runs", nil)
			request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
			if err := controller.Authorize(request, ResourceAccess{}, true); err == nil {
				t.Fatal("first SAR unexpectedly succeeded")
			}
			if err := controller.Authorize(request, ResourceAccess{}, true); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			if reviews != 2 || sars != 2 {
				t.Fatalf("reviews = TokenReview %d, SAR %d; want 2 each", reviews, sars)
			}
		})
	}
}

func sha256SessionKey(id string) [32]byte {
	return sha256.Sum256([]byte(id))
}
