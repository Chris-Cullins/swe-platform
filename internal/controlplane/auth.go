package controlplane

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const sessionCookieName = "swe-platform-session"
const defaultTokenAudience = "swe-platform"

var (
	errUnauthenticated = errors.New("authentication required")
	errForbidden       = errors.New("access denied")
)

// ResourceAccess identifies one namespaced API authorization decision.
type ResourceAccess struct {
	Namespace   string
	Verb        string
	Resource    string
	Subresource string
	Name        string
}

type principal struct {
	name      string
	uid       string
	groups    []string
	extra     map[string]authorizationv1.ExtraValue
	bootstrap bool
}

// AccessController authenticates credentials and authorizes resource access.
type AccessController interface {
	Authorize(*http.Request, ResourceAccess, bool) error
}

// SessionAuthenticator validates Kubernetes credentials for browser session
// exchange without issuing or retaining a platform credential.
type SessionAuthenticator interface {
	CreateSession(*http.Request) (Session, string, error)
	CurrentSession(*http.Request) (Session, error)
	DeleteSession(*http.Request)
}

// KubernetesAccessController uses TokenReview and SubjectAccessReview. BootstrapToken,
// when non-empty, is an all-access credential intended only for initial self-hosted setup.
type KubernetesAccessController struct {
	Client         kubernetes.Interface
	BootstrapToken string
	Audience       string
	Sessions       *MemorySessionStore
}

// Authorize authenticates a bearer token (or, for browser reads, a session cookie) and
// authorizes the exact namespaced resource through Kubernetes RBAC.
func (a KubernetesAccessController) Authorize(r *http.Request, access ResourceAccess, allowSession bool) error {
	token, bearer, sessionID, err := a.requestCredential(r, allowSession)
	if err != nil {
		return err
	}
	user, err := a.authenticate(r.Context(), token, bearer)
	if err != nil {
		if sessionID != "" {
			a.Sessions.Delete(sessionID)
		}
		return err
	}
	if user.bootstrap {
		return nil
	}
	if a.Client == nil {
		return fmt.Errorf("authorize request: Kubernetes client is unavailable")
	}
	review, err := a.Client.AuthorizationV1().SubjectAccessReviews().Create(r.Context(), &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User:   user.name,
			UID:    user.uid,
			Groups: user.groups,
			Extra:  user.extra,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace:   access.Namespace,
				Verb:        access.Verb,
				Group:       "swe.dev",
				Version:     "v1alpha1",
				Resource:    access.Resource,
				Subresource: access.Subresource,
				Name:        access.Name,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("authorize request: %w", err)
	}
	if review.Status.EvaluationError != "" {
		return fmt.Errorf("authorize request: %s", review.Status.EvaluationError)
	}
	if !review.Status.Allowed {
		return errForbidden
	}
	return nil
}

// CreateSession validates an explicit bearer credential and stores it behind a
// random opaque browser session identifier. Bootstrap access can never be
// exchanged for a browser session.
func (a KubernetesAccessController) CreateSession(r *http.Request) (Session, string, error) {
	token, bearer, err := requestBearerToken(r)
	if err != nil {
		return Session{}, "", err
	}
	if a.Sessions == nil {
		return Session{}, "", fmt.Errorf("create session: session store is unavailable")
	}
	if !a.Sessions.acceptsToken(token) {
		return Session{}, "", errUnauthenticated
	}
	user, err := a.authenticate(r.Context(), token, bearer)
	if err != nil {
		return Session{}, "", err
	}
	if user.bootstrap {
		return Session{}, "", errForbidden
	}
	sessionID, err := a.Sessions.Create(token)
	if err != nil {
		return Session{}, "", err
	}
	return Session{Authenticated: true, Username: user.name}, sessionID, nil
}

// CurrentSession validates the current cookie credential through TokenReview.
func (a KubernetesAccessController) CurrentSession(r *http.Request) (Session, error) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" || a.Sessions == nil {
		return Session{}, errUnauthenticated
	}
	token, err := a.Sessions.Resolve(cookie.Value)
	if err != nil {
		return Session{}, err
	}
	user, err := a.authenticate(r.Context(), token, false)
	if err != nil {
		a.Sessions.Delete(cookie.Value)
		return Session{}, err
	}
	return Session{Authenticated: true, Username: user.name}, nil
}

func (a KubernetesAccessController) DeleteSession(r *http.Request) {
	if a.Sessions == nil {
		return
	}
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		a.Sessions.Delete(cookie.Value)
	}
}

func (a KubernetesAccessController) requestCredential(r *http.Request, allowSession bool) (token string, bearer bool, sessionID string, err error) {
	if r.Header.Get("Authorization") != "" {
		token, bearer, err = requestBearerToken(r)
		return token, bearer, "", err
	}
	if !allowSession || a.Sessions == nil {
		return "", false, "", errUnauthenticated
	}
	cookie, cookieErr := r.Cookie(sessionCookieName)
	if cookieErr != nil || cookie.Value == "" {
		return "", false, "", errUnauthenticated
	}
	token, err = a.Sessions.Resolve(cookie.Value)
	if err != nil {
		return "", false, "", err
	}
	return token, false, cookie.Value, nil
}

func (a KubernetesAccessController) authenticate(ctx context.Context, token string, bearer bool) (principal, error) {
	if a.BootstrapToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(a.BootstrapToken)) == 1 {
		if !bearer {
			return principal{}, errUnauthenticated
		}
		return principal{name: "swe-platform:bootstrap", bootstrap: true}, nil
	}
	if a.Client == nil {
		return principal{}, fmt.Errorf("authenticate request: Kubernetes client is unavailable")
	}
	audience := a.Audience
	if audience == "" {
		audience = defaultTokenAudience
	}
	review, err := a.Client.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: token, Audiences: []string{audience}},
	}, metav1.CreateOptions{})
	if err != nil {
		return principal{}, fmt.Errorf("authenticate request: %w", err)
	}
	if review.Status.Error != "" {
		return principal{}, fmt.Errorf("authenticate request: %s", review.Status.Error)
	}
	if !review.Status.Authenticated {
		return principal{}, errUnauthenticated
	}
	audienceMatched := false
	for _, reviewedAudience := range review.Status.Audiences {
		if reviewedAudience == audience {
			audienceMatched = true
			break
		}
	}
	if !audienceMatched {
		return principal{}, errUnauthenticated
	}
	extra := make(map[string]authorizationv1.ExtraValue, len(review.Status.User.Extra))
	for key, values := range review.Status.User.Extra {
		extra[key] = append(authorizationv1.ExtraValue(nil), values...)
	}
	return principal{
		name:   review.Status.User.Username,
		uid:    review.Status.User.UID,
		groups: append([]string(nil), review.Status.User.Groups...),
		extra:  extra,
	}, nil
}

func requestBearerToken(r *http.Request) (string, bool, error) {
	authorization := r.Header.Get("Authorization")
	if authorization != "" {
		scheme, token, ok := strings.Cut(authorization, " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
			return "", false, errUnauthenticated
		}
		return strings.TrimSpace(token), true, nil
	}
	return "", false, errUnauthenticated
}

func writeAccessError(w http.ResponseWriter, err error) {
	if errors.Is(err, errUnauthenticated) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	if errors.Is(err, errForbidden) {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}
	http.Error(w, "authorization service unavailable", http.StatusServiceUnavailable)
}

func writeRESTAccessError(w http.ResponseWriter, err error) {
	if errors.Is(err, errUnauthenticated) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeProblem(w, http.StatusUnauthorized, "authentication-required", "Authentication required", "provide a valid Kubernetes credential")
		return
	}
	if errors.Is(err, errForbidden) {
		writeProblem(w, http.StatusForbidden, "access-denied", "Access denied", "Kubernetes authorization denied this operation")
		return
	}
	writeProblem(w, http.StatusServiceUnavailable, "authorization-unavailable", "Authorization unavailable", "Kubernetes authentication or authorization is unavailable")
}
