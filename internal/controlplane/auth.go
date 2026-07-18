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

// KubernetesAccessController uses TokenReview and SubjectAccessReview. BootstrapToken,
// when non-empty, is an all-access credential intended only for initial self-hosted setup.
type KubernetesAccessController struct {
	Client         kubernetes.Interface
	BootstrapToken string
	Audience       string
}

// Authorize authenticates a bearer token (or, for browser reads, a session cookie) and
// authorizes the exact namespaced resource through Kubernetes RBAC.
func (a KubernetesAccessController) Authorize(r *http.Request, access ResourceAccess, allowSession bool) error {
	token, bearer, err := requestToken(r, allowSession)
	if err != nil {
		return err
	}
	user, err := a.authenticate(r.Context(), token, bearer)
	if err != nil {
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

func requestToken(r *http.Request, allowSession bool) (string, bool, error) {
	authorization := r.Header.Get("Authorization")
	if authorization != "" {
		scheme, token, ok := strings.Cut(authorization, " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
			return "", false, errUnauthenticated
		}
		return strings.TrimSpace(token), true, nil
	}
	if allowSession {
		cookie, err := r.Cookie(sessionCookieName)
		if err == nil && cookie.Value != "" {
			return cookie.Value, false, nil
		}
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
