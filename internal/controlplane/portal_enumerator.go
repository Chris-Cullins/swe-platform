package controlplane

import (
	"context"
	"errors"
	"sort"

	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

// KubernetesPortalEnvironmentEnumerator confines locator discovery to the
// installation's freshly verified active namespace claims.
type KubernetesPortalEnvironmentEnumerator struct {
	Client     client.Client
	Verifier   *tenancy.Verifier
	Namespaces map[string]struct{}
}

func (e KubernetesPortalEnvironmentEnumerator) ListPortalEnvironments(ctx context.Context) ([]platformv1alpha1.Environment, error) {
	if e.Client == nil || e.Verifier == nil {
		return nil, tenancy.ErrOutOfScope
	}
	if e.Verifier.Mode == tenancy.ModeScoped {
		names := make([]string, 0, len(e.Namespaces))
		for name := range e.Namespaces {
			names = append(names, name)
		}
		sort.Strings(names)
		var result []platformv1alpha1.Environment
		for _, namespace := range names {
			claim, err := e.Verifier.VerifyNamespace(ctx, namespace)
			if errors.Is(err, tenancy.ErrOutOfScope) || err == nil && claim.Lifecycle != tenancy.LifecycleActive {
				continue
			}
			if err != nil {
				return nil, err
			}
			var list platformv1alpha1.EnvironmentList
			if err := e.Client.List(ctx, &list, client.InNamespace(namespace)); err != nil {
				return nil, err
			}
			result = append(result, list.Items...)
		}
		return result, nil
	}
	var list platformv1alpha1.EnvironmentList
	if err := e.Client.List(ctx, &list); err != nil {
		return nil, err
	}
	result := make([]platformv1alpha1.Environment, 0, len(list.Items))
	for i := range list.Items {
		claim, err := e.Verifier.VerifyNamespace(ctx, list.Items[i].Namespace)
		if err == nil && claim.Lifecycle == tenancy.LifecycleActive {
			result = append(result, list.Items[i])
		} else if err != nil && !errors.Is(err, tenancy.ErrOutOfScope) {
			return nil, err
		}
	}
	return result, nil
}
