package tenancy

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mutationLeaseKey struct{}

type mutationLease struct {
	namespace string
	claim     Claim
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

func (c GuardedClient) Apply(context.Context, runtime.ApplyConfiguration, ...client.ApplyOption) error {
	return errors.New("tenancy guarded apply is unsupported")
}

func (c GuardedClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := c.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c GuardedClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if err := c.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c GuardedClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c GuardedClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if err := c.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c GuardedClient) DeleteAllOf(context.Context, client.Object, ...client.DeleteAllOfOption) error {
	return errors.New("tenancy guarded DeleteAllOf is unsupported")
}

func (c GuardedClient) Status() client.SubResourceWriter {
	return guardedSubResourceWriter{SubResourceWriter: c.Client.Status(), client: c}
}

func (c GuardedClient) SubResource(name string) client.SubResourceClient {
	return guardedSubResourceClient{SubResourceClient: c.Client.SubResource(name), client: c}
}

type guardedSubResourceWriter struct {
	client.SubResourceWriter
	client GuardedClient
}

func (w guardedSubResourceWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	if err := w.client.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return w.SubResourceWriter.Create(ctx, obj, subResource, opts...)
}

func (w guardedSubResourceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if err := w.client.authorize(ctx, obj.GetNamespace()); err != nil {
		return err
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func (w guardedSubResourceWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
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
	client GuardedClient
}

func (c guardedSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return guardedSubResourceWriter{SubResourceWriter: c.SubResourceClient, client: c.client}.Create(ctx, obj, subResource, opts...)
}

func (c guardedSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return guardedSubResourceWriter{SubResourceWriter: c.SubResourceClient, client: c.client}.Update(ctx, obj, opts...)
}

func (c guardedSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return guardedSubResourceWriter{SubResourceWriter: c.SubResourceClient, client: c.client}.Patch(ctx, obj, patch, opts...)
}

func (c guardedSubResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return guardedSubResourceWriter{SubResourceWriter: c.SubResourceClient, client: c.client}.Apply(ctx, obj, opts...)
}
