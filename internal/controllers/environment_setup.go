package controllers

import (
	"context"
	"fmt"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func environmentTemplateRefIndex(object client.Object) []string {
	environment := object.(*platformv1alpha1.Environment)
	if environment.Spec.TemplateRef == "" {
		return nil
	}
	return []string{environment.Spec.TemplateRef}
}

func environmentProjectRefIndex(object client.Object) []string {
	environment := object.(*platformv1alpha1.Environment)
	if environment.Spec.ProjectRef == "" {
		return nil
	}
	return []string{environment.Spec.ProjectRef}
}

func environmentRuntimeClassIndex(object client.Object) []string {
	environment := object.(*platformv1alpha1.Environment)
	if environment.Status.Provisioning == nil || environment.Status.Provisioning.RuntimeClassName == "" {
		return nil
	}
	return []string{environment.Status.Provisioning.RuntimeClassName}
}

func (r *EnvironmentReconciler) environmentReferenceRequests(ctx context.Context, namespace, field, name string) []reconcile.Request {
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(ctx, &environments, client.InNamespace(namespace), client.MatchingFields{field: name}); err != nil {
		log.FromContext(ctx).Error(err, "list environments for reference", "field", field, "name", name)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(environments.Items))
	for i := range environments.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&environments.Items[i])})
	}
	return requests
}

func (r *EnvironmentReconciler) runtimeClassReferenceRequests(ctx context.Context, name string) []reconcile.Request {
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(ctx, &environments, client.MatchingFields{provisioningRuntimeClassField: name}); err != nil {
		log.FromContext(ctx).Error(err, "list environments for RuntimeClass", "runtimeClass", name)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(environments.Items))
	for i := range environments.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&environments.Items[i])})
	}
	return requests
}

func (r *EnvironmentReconciler) installationIsolationRequests(ctx context.Context, _ client.Object) []reconcile.Request {
	var environments platformv1alpha1.EnvironmentList
	if err := r.List(ctx, &environments); err != nil {
		log.FromContext(ctx).Error(err, "list environments for Installation isolation")
		return nil
	}
	requests := make([]reconcile.Request, 0, len(environments.Items))
	for i := range environments.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&environments.Items[i])})
	}
	return requests
}

// SetupWithManager registers the controller with the manager.
func (r *EnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &platformv1alpha1.Environment{}, templateRefField, environmentTemplateRefIndex); err != nil {
		return fmt.Errorf("index environments by template: %w", err)
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &platformv1alpha1.Environment{}, projectRefField, environmentProjectRefIndex); err != nil {
		return fmt.Errorf("index environments by project: %w", err)
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &platformv1alpha1.Environment{}, provisioningRuntimeClassField, environmentRuntimeClassIndex); err != nil {
		return fmt.Errorf("index environments by RuntimeClass: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.Environment{}, builder.WithPredicates(predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
			old, ok1 := e.ObjectOld.(*platformv1alpha1.Environment)
			new, ok2 := e.ObjectNew.(*platformv1alpha1.Environment)
			return !ok1 || !ok2 || observationRelevantEnvironmentUpdate(old, new)
		}})).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Secret{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Watches(&platformv1alpha1.Run{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			run := object.(*platformv1alpha1.Run)
			if run.Status.EnvironmentRef == nil {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: run.Status.EnvironmentRef.Name}}}
		})).
		Watches(&platformv1alpha1.EnvironmentTemplate{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			return r.environmentReferenceRequests(ctx, object.GetNamespace(), templateRefField, object.GetName())
		})).
		Watches(&platformv1alpha1.Project{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			return r.environmentReferenceRequests(ctx, object.GetNamespace(), projectRefField, object.GetName())
		})).
		Watches(&nodev1.RuntimeClass{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			return r.runtimeClassReferenceRequests(ctx, object.GetName())
		})).
		Watches(&platformv1alpha1.Installation{}, handler.EnqueueRequestsFromMapFunc(r.installationIsolationRequests)).
		Complete(r)
}
