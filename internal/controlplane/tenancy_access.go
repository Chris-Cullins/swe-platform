package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/types"

	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

type namespaceUIDContextKey struct{}

// TenancyAccessController preserves the existing authentication/SAR ordering,
// then rejects namespaced work outside the exact active Installation claim.
type TenancyAccessController struct {
	Access     AccessController
	Verifier   *tenancy.Verifier
	Namespaces map[string]struct{}
}

func (a TenancyAccessController) Authorize(r *http.Request, access ResourceAccess, allowSession bool) error {
	_, err := a.AuthorizePrincipal(r, access, allowSession)
	return err
}

// AuthenticatePrincipal delegates credential validation without guessing a
// namespace. Exact authorization below performs tenancy verification once the
// resource identity is known.
func (a TenancyAccessController) AuthenticatePrincipal(r *http.Request, allowSession bool) (string, error) {
	if authenticator, ok := a.Access.(principalAuthenticator); ok {
		return authenticator.AuthenticatePrincipal(r, allowSession)
	}
	return "", errUnauthenticated
}

func (a TenancyAccessController) AuthorizePrincipal(r *http.Request, access ResourceAccess, allowSession bool) (string, error) {
	var principalKey string
	var err error
	if principalAccess, ok := a.Access.(principalAccessController); ok {
		principalKey, err = principalAccess.AuthorizePrincipal(r, access, allowSession)
	} else if a.Access == nil {
		err = errUnauthenticated
	} else {
		err = a.Access.Authorize(r, access, allowSession)
		principalKey = "authorized"
	}
	if err != nil || access.Namespace == "" {
		return principalKey, err
	}
	if a.Verifier == nil {
		return "", fmt.Errorf("authorize namespace scope: verifier is unavailable")
	}
	if a.Verifier.Mode == tenancy.ModeScoped {
		if _, configured := a.Namespaces[access.Namespace]; !configured {
			return "", errForbidden
		}
	}
	claim, err := a.Verifier.VerifyNamespace(r.Context(), access.Namespace)
	if err != nil {
		if errors.Is(err, tenancy.ErrOutOfScope) {
			return "", errForbidden
		}
		return "", fmt.Errorf("authorize namespace scope: %w", err)
	}
	if claim.Lifecycle != tenancy.LifecycleActive {
		return "", errForbidden
	}
	*r = *r.WithContext(context.WithValue(r.Context(), namespaceUIDContextKey{}, claim.NamespaceUID))
	return principalKey, nil
}

func namespaceUIDFromRequest(r *http.Request) types.UID {
	uid, _ := r.Context().Value(namespaceUIDContextKey{}).(types.UID)
	return uid
}
