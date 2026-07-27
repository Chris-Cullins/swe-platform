package tenancy

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

type mutationLeaseKey struct{}

type mutationLease struct {
	namespace       string
	claim           Claim
	fenceOnly       bool
	environmentName string
	environmentUID  types.UID
}

// BeginEnvironmentStaleProjectFence grants narrowly scoped teardown authority
// only when an otherwise-current namespace claim has lost its exact Project
// incarnation. It never grants fallback authority for reader failures or a
// still-valid Project claim.
func (s *ReconcileScope) BeginEnvironmentStaleProjectFence(ctx context.Context, namespace, environmentName string, environmentUID types.UID, permitted ...Lifecycle) (context.Context, Claim, error) {
	if s == nil || s.Verifier == nil {
		return ctx, Claim{}, errors.New("stale-Project fence requires a tenancy verifier")
	}
	if namespace == "" || environmentName == "" || environmentUID == "" {
		return ctx, Claim{}, fmt.Errorf("%w: complete Environment identity is required for stale-Project fence", ErrOutOfScope)
	}
	claim, err := s.Verifier.verifyNamespaceIdentity(ctx, namespace)
	if err != nil {
		return ctx, Claim{}, err
	}
	if !permitsLifecycle(claim.Lifecycle, permitted) {
		return ctx, claim, fmt.Errorf("%w: Namespace %q lifecycle is %s", ErrOutOfScope, namespace, claim.Lifecycle)
	}
	projectErr := s.Verifier.verifyProjectClaim(ctx, namespace, claim)
	if projectErr == nil {
		return ctx, claim, fmt.Errorf("%w: Project claim remains valid; stale-Project fence is unavailable", ErrOutOfScope)
	}
	if !errors.Is(projectErr, ErrOutOfScope) {
		return ctx, Claim{}, projectErr
	}
	lease := mutationLease{namespace: namespace, claim: claim, fenceOnly: true, environmentName: environmentName, environmentUID: environmentUID}
	return context.WithValue(ctx, mutationLeaseKey{}, lease), claim, nil
}

// ReconcileScope validates a namespace at reconcile entry and issues the
// context-bound lease required by GuardedClient for each later mutation.
type ReconcileScope struct {
	Verifier *Verifier
}

func (s *ReconcileScope) Begin(ctx context.Context, namespace string, permitted ...Lifecycle) (context.Context, Claim, error) {
	if s == nil || s.Verifier == nil {
		return ctx, Claim{Lifecycle: LifecycleActive}, nil
	}
	claim, err := s.Verifier.VerifyNamespace(ctx, namespace)
	if err != nil {
		return ctx, Claim{}, err
	}
	if !permitsLifecycle(claim.Lifecycle, permitted) {
		return ctx, claim, fmt.Errorf("%w: Namespace %q lifecycle is %s", ErrOutOfScope, namespace, claim.Lifecycle)
	}
	lease := mutationLease{namespace: namespace, claim: claim}
	return context.WithValue(ctx, mutationLeaseKey{}, lease), claim, nil
}

type GuardedClient struct {
	client.Client
	Verifier *Verifier
}

func (c GuardedClient) authorize(ctx context.Context, namespace string) error {
	if namespace == "" {
		return errors.New("tenancy guard refuses mutation without a namespace")
	}
	lease, ok := ctx.Value(mutationLeaseKey{}).(mutationLease)
	if !ok || lease.namespace != namespace {
		return errors.New("tenancy guard refuses mutation without a matching reconcile lease")
	}
	claim, err := c.Verifier.VerifyNamespace(ctx, namespace)
	if err != nil {
		return err
	}
	if claim != lease.claim {
		return fmt.Errorf("%w: Namespace %q claim changed during reconciliation", ErrOutOfScope, namespace)
	}
	return nil
}

func (c GuardedClient) authorizeFence(ctx context.Context, obj client.Object, status, delete bool) error {
	lease, ok := ctx.Value(mutationLeaseKey{}).(mutationLease)
	if !ok || !lease.fenceOnly || obj.GetNamespace() == "" || obj.GetNamespace() != lease.namespace {
		return errors.New("tenancy guard refuses fence-only mutation without a matching lease")
	}
	claim, err := c.Verifier.verifyNamespaceIdentity(ctx, lease.namespace)
	if err != nil {
		return err
	}
	if claim != lease.claim {
		return fmt.Errorf("%w: Namespace %q claim changed during fence", ErrOutOfScope, lease.namespace)
	}
	if status {
		env, ok := obj.(*platformv1alpha1.Environment)
		if !ok || env.Name != lease.environmentName || env.UID != lease.environmentUID {
			return errors.New("tenancy guard fence lease only permits status mutation of the exact Environment")
		}
		return nil
	}
	if delete {
		switch obj.(type) {
		case *corev1.Pod, *corev1.Secret:
		default:
			return errors.New("tenancy guard fence lease only permits Pod or Secret deletion")
		}
		for _, owner := range obj.GetOwnerReferences() {
			if owner.Controller != nil && *owner.Controller && owner.APIVersion == platformv1alpha1.GroupVersion.String() && owner.Kind == "Environment" && owner.Name == lease.environmentName && owner.UID == lease.environmentUID {
				return nil
			}
		}
		return errors.New("tenancy guard fence lease refuses child without exact Environment controller owner")
	}
	return errors.New("tenancy guard fence lease refuses this mutation")
}

func (c GuardedClient) Apply(context.Context, runtime.ApplyConfiguration, ...client.ApplyOption) error {
	return errors.New("tenancy guarded apply is unsupported")
}

func (c GuardedClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if lease, _ := ctx.Value(mutationLeaseKey{}).(mutationLease); lease.fenceOnly {
		return c.authorizeFence(ctx, obj, false, false)
	}
	if err := c.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c GuardedClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if lease, _ := ctx.Value(mutationLeaseKey{}).(mutationLease); lease.fenceOnly {
		if err := c.authorizeFence(ctx, obj, false, true); err != nil {
			return err
		}
		return c.Client.Delete(ctx, obj, opts...)
	}
	if err := c.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c GuardedClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if lease, _ := ctx.Value(mutationLeaseKey{}).(mutationLease); lease.fenceOnly {
		return c.authorizeFence(ctx, obj, false, false)
	}
	if err := c.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c GuardedClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if lease, _ := ctx.Value(mutationLeaseKey{}).(mutationLease); lease.fenceOnly {
		return c.authorizeFence(ctx, obj, false, false)
	}
	if err := c.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c GuardedClient) DeleteAllOf(context.Context, client.Object, ...client.DeleteAllOfOption) error {
	return errors.New("tenancy guarded DeleteAllOf is unsupported")
}

func (c GuardedClient) Status() client.SubResourceWriter {
	return guardedSubResourceWriter{SubResourceWriter: c.Client.Status(), client: c, fenceStatus: true}
}

func (c GuardedClient) SubResource(name string) client.SubResourceClient {
	return guardedSubResourceClient{SubResourceClient: c.Client.SubResource(name), client: c, fenceStatus: name == "status"}
}

type guardedSubResourceWriter struct {
	client.SubResourceWriter
	client      GuardedClient
	fenceStatus bool
}

func (w guardedSubResourceWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	if lease, _ := ctx.Value(mutationLeaseKey{}).(mutationLease); lease.fenceOnly {
		return errors.New("tenancy guard fence lease refuses subresource create")
	}
	if err := w.client.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return w.SubResourceWriter.Create(ctx, obj, subResource, opts...)
}

func (w guardedSubResourceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if lease, _ := ctx.Value(mutationLeaseKey{}).(mutationLease); lease.fenceOnly {
		if !w.fenceStatus {
			return errors.New("tenancy guard fence lease refuses arbitrary subresource update")
		}
		if err := w.client.authorizeFence(ctx, obj, true, false); err != nil {
			return err
		}
		return w.SubResourceWriter.Update(ctx, obj, opts...)
	}
	if err := w.client.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func (w guardedSubResourceWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if lease, _ := ctx.Value(mutationLeaseKey{}).(mutationLease); lease.fenceOnly {
		if !w.fenceStatus {
			return errors.New("tenancy guard fence lease refuses arbitrary subresource patch")
		}
		if err := w.client.authorizeFence(ctx, obj, true, false); err != nil {
			return err
		}
		return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}
	if err := w.client.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func (w guardedSubResourceWriter) Apply(context.Context, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
	return errors.New("tenancy guarded subresource apply is unsupported")
}

type guardedSubResourceClient struct {
	client.SubResourceClient
	client      GuardedClient
	fenceStatus bool
}

func (c guardedSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return guardedSubResourceWriter{SubResourceWriter: c.SubResourceClient, client: c.client, fenceStatus: c.fenceStatus}.Create(ctx, obj, subResource, opts...)
}

func (c guardedSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return guardedSubResourceWriter{SubResourceWriter: c.SubResourceClient, client: c.client, fenceStatus: c.fenceStatus}.Update(ctx, obj, opts...)
}

func (c guardedSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return guardedSubResourceWriter{SubResourceWriter: c.SubResourceClient, client: c.client, fenceStatus: c.fenceStatus}.Patch(ctx, obj, patch, opts...)
}

func (c guardedSubResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return guardedSubResourceWriter{SubResourceWriter: c.SubResourceClient, client: c.client, fenceStatus: c.fenceStatus}.Apply(ctx, obj, opts...)
}
