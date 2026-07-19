package controllers

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

// WarmPoolReconciler keeps the requested number of unclaimed environments
// provisioned for each EnvironmentTemplate.
type WarmPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=swe.dev,resources=environmenttemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=environmenttemplates/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=environments,verbs=get;list;watch;create;delete

func (r *WarmPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var tmpl platformv1alpha1.EnvironmentTemplate
	if err := r.Get(ctx, req.NamespacedName, &tmpl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	minimum := int32(0)
	if tmpl.Spec.WarmPool != nil {
		minimum = tmpl.Spec.WarmPool.Min
	}
	if platformv1alpha1.EffectiveEnvironmentBackend(&platformv1alpha1.Environment{}, &tmpl) != platformv1alpha1.EnvironmentBackendPod {
		if tmpl.Status.WarmPoolReady != 0 {
			before := tmpl.DeepCopy()
			tmpl.Status.WarmPoolReady = 0
			if err := r.Status().Patch(ctx, &tmpl, client.MergeFrom(before)); err != nil {
				return ctrl.Result{}, fmt.Errorf("clear unsupported warm pool status: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	var environments platformv1alpha1.EnvironmentList
	if err := r.List(ctx, &environments,
		client.InNamespace(tmpl.Namespace),
		client.MatchingLabels{warmPoolLabel: tmpl.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list warm environments: %w", err)
	}

	ready := int32(0)
	for i := range environments.Items {
		if platformv1alpha1.IsEnvironmentReady(&environments.Items[i]) && environments.Items[i].Status.ClaimedBy == nil {
			ready++
		}
	}
	if tmpl.Status.WarmPoolReady != ready {
		before := tmpl.DeepCopy()
		tmpl.Status.WarmPoolReady = ready
		if err := r.Status().Patch(ctx, &tmpl, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, fmt.Errorf("update warm pool status: %w", err)
		}
	}

	active := make([]*platformv1alpha1.Environment, 0, len(environments.Items))
	for i := range environments.Items {
		env := &environments.Items[i]
		if env.Status.ClaimedBy != nil || !env.DeletionTimestamp.IsZero() {
			continue
		}
		statusCurrent := env.Status.ObservedGeneration == env.Generation
		if env.Spec.Paused || statusCurrent && (env.Status.Phase == platformv1alpha1.EnvironmentPhaseFailed || env.Status.Phase == platformv1alpha1.EnvironmentPhaseTerminated) {
			resourceVersion := env.ResourceVersion
			if err := r.Delete(ctx, env, client.Preconditions{ResourceVersion: &resourceVersion}); errors.IsConflict(err) || errors.IsNotFound(err) {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete unusable warm environment %q: %w", env.Name, err)
			}
			continue
		}
		active = append(active, env)
	}
	for int32(len(active)) < minimum {
		env := &platformv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:    tmpl.Namespace,
				GenerateName: "warm-" + tmpl.Name + "-",
				Labels:       map[string]string{warmPoolLabel: tmpl.Name},
			},
			Spec: platformv1alpha1.EnvironmentSpec{
				TemplateRef: tmpl.Name,
			},
		}
		if err := controllerutil.SetControllerReference(&tmpl, env, r.Scheme, controllerutil.WithBlockOwnerDeletion(false)); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, env); err != nil {
			return ctrl.Result{}, fmt.Errorf("create warm environment: %w", err)
		}
		active = append(active, env)
	}

	if int32(len(active)) > minimum {
		sort.Slice(active, func(i, j int) bool {
			return active[i].CreationTimestamp.After(active[j].CreationTimestamp.Time)
		})
		for _, env := range active[:int32(len(active))-minimum] {
			resourceVersion := env.ResourceVersion
			if err := r.Delete(ctx, env, client.Preconditions{ResourceVersion: &resourceVersion}); errors.IsConflict(err) || errors.IsNotFound(err) {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete excess warm environment %q: %w", env.Name, err)
			}
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers the warm-pool controller with the manager.
func (r *WarmPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.EnvironmentTemplate{}).
		Watches(&platformv1alpha1.Environment{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			template := object.(*platformv1alpha1.Environment).Spec.TemplateRef
			if template == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: object.GetNamespace(), Name: template}}}
		})).
		Complete(r)
}
