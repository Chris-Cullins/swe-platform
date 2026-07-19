package controllers

import (
	"context"
	"fmt"
	"sort"
	"time"

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

const (
	warmPoolCleanupAnnotation = "swe.dev/warm-pool-unusable-since"
	warmPoolCleanupGrace      = 5 * time.Minute
)

// WarmPoolReconciler keeps the requested number of unclaimed environments
// provisioned for each EnvironmentTemplate.
type WarmPoolReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Now       func() time.Time
}

// +kubebuilder:rbac:groups=swe.dev,resources=environmenttemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=environmenttemplates/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=environments,verbs=get;list;watch;create;patch;delete

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
			if current, err := r.templateCurrent(ctx, &tmpl); err != nil {
				return ctrl.Result{}, err
			} else if !current {
				return ctrl.Result{Requeue: true}, nil
			}
			before := tmpl.DeepCopy()
			tmpl.Status.WarmPoolReady = 0
			if err := r.Status().Patch(ctx, &tmpl, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); errors.IsConflict(err) || errors.IsNotFound(err) {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, fmt.Errorf("clear unsupported warm pool status: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	var environments platformv1alpha1.EnvironmentList
	reader := r.APIReader
	if reader == nil {
		// Unit tests construct reconcilers without a manager. Production always
		// installs the uncached API reader in SetupWithManager.
		reader = r.Client
	}
	if err := reader.List(ctx, &environments,
		client.InNamespace(tmpl.Namespace),
		client.MatchingLabels{warmPoolLabel: tmpl.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list warm environments: %w", err)
	}

	ready := int32(0)
	for i := range environments.Items {
		env := &environments.Items[i]
		if warmPoolMember(env, &tmpl) && platformv1alpha1.IsEnvironmentReady(env) && env.Status.ClaimedBy == nil {
			ready++
		}
	}
	if tmpl.Status.WarmPoolReady != ready {
		if current, err := r.templateCurrent(ctx, &tmpl); err != nil {
			return ctrl.Result{}, err
		} else if !current {
			return ctrl.Result{Requeue: true}, nil
		}
		before := tmpl.DeepCopy()
		tmpl.Status.WarmPoolReady = ready
		if err := r.Status().Patch(ctx, &tmpl, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); errors.IsConflict(err) || errors.IsNotFound(err) {
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			return ctrl.Result{}, fmt.Errorf("update warm pool status: %w", err)
		}
	}

	active := make([]*platformv1alpha1.Environment, 0, len(environments.Items))
	quarantined := int32(0)
	var requeueAfter time.Duration
	now := r.now()
	for i := range environments.Items {
		env := &environments.Items[i]
		if !warmPoolMember(env, &tmpl) || env.Status.ClaimedBy != nil || !env.DeletionTimestamp.IsZero() {
			continue
		}
		statusCurrent := env.Status.ObservedGeneration == env.Generation
		if env.Spec.Paused || statusCurrent && (env.Status.Phase == platformv1alpha1.EnvironmentPhaseFailed || env.Status.Phase == platformv1alpha1.EnvironmentPhaseTerminated) {
			quarantined++
			unusableSince, marked := warmPoolUnusableSince(env)
			if !marked || unusableSince.After(now) {
				if current, err := r.templateCurrent(ctx, &tmpl); err != nil {
					return ctrl.Result{}, err
				} else if !current {
					return ctrl.Result{Requeue: true}, nil
				}
				before := env.DeepCopy()
				if env.Annotations == nil {
					env.Annotations = make(map[string]string)
				}
				env.Annotations[warmPoolCleanupAnnotation] = now.UTC().Format(time.RFC3339Nano)
				if err := r.Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); errors.IsConflict(err) || errors.IsNotFound(err) {
					return ctrl.Result{Requeue: true}, nil
				} else if err != nil {
					return ctrl.Result{}, fmt.Errorf("mark unusable warm environment %q: %w", env.Name, err)
				}
				unusableSince = now
			}
			remaining := unusableSince.Add(warmPoolCleanupGrace).Sub(now)
			if remaining > 0 {
				if requeueAfter == 0 || remaining < requeueAfter {
					requeueAfter = remaining
				}
				continue
			}
			if current, err := r.templateCurrent(ctx, &tmpl); err != nil {
				return ctrl.Result{}, err
			} else if !current {
				return ctrl.Result{Requeue: true}, nil
			}
			resourceVersion := env.ResourceVersion
			uid := env.UID
			if err := r.Delete(ctx, env, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); errors.IsConflict(err) || errors.IsNotFound(err) {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete unusable warm environment %q: %w", env.Name, err)
			}
			quarantined--
			continue
		}
		if _, marked := warmPoolUnusableSince(env); marked {
			if current, err := r.templateCurrent(ctx, &tmpl); err != nil {
				return ctrl.Result{}, err
			} else if !current {
				return ctrl.Result{Requeue: true}, nil
			}
			before := env.DeepCopy()
			delete(env.Annotations, warmPoolCleanupAnnotation)
			if err := r.Patch(ctx, env, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); errors.IsConflict(err) || errors.IsNotFound(err) {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, fmt.Errorf("clear warm environment cleanup marker %q: %w", env.Name, err)
			}
		}
		active = append(active, env)
	}
	// Keep replacement capacity bounded while unusable members remain during
	// grace. maxSurge=minimum permits one complete replacement set, bounding
	// exact unclaimed membership at 2*minimum even when every replacement fails.
	maxSurge := minimum
	for int32(len(active)) < minimum && int32(len(active))+quarantined < minimum+maxSurge {
		if current, err := r.templateCurrent(ctx, &tmpl); err != nil {
			return ctrl.Result{}, err
		} else if !current {
			return ctrl.Result{Requeue: true}, nil
		}
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
			if current, err := r.templateCurrent(ctx, &tmpl); err != nil {
				return ctrl.Result{}, err
			} else if !current {
				return ctrl.Result{Requeue: true}, nil
			}
			resourceVersion := env.ResourceVersion
			uid := env.UID
			if err := r.Delete(ctx, env, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); errors.IsConflict(err) || errors.IsNotFound(err) {
				return ctrl.Result{Requeue: true}, nil
			} else if err != nil {
				return ctrl.Result{}, fmt.Errorf("delete excess warm environment %q: %w", env.Name, err)
			}
		}
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *WarmPoolReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func warmPoolMember(env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) bool {
	return env.Labels[warmPoolLabel] == tmpl.Name && env.Spec.TemplateRef == tmpl.Name &&
		exactControllerOwner(env, platformv1alpha1.GroupVersion.String(), "EnvironmentTemplate", tmpl.Name, tmpl.UID)
}

func warmPoolUnusableSince(env *platformv1alpha1.Environment) (time.Time, bool) {
	value := env.Annotations[warmPoolCleanupAnnotation]
	if value == "" {
		return time.Time{}, false
	}
	markedAt, err := time.Parse(time.RFC3339Nano, value)
	return markedAt, err == nil
}

func (r *WarmPoolReconciler) templateCurrent(ctx context.Context, observed *platformv1alpha1.EnvironmentTemplate) (bool, error) {
	reader := r.APIReader
	if reader == nil {
		// Unit tests construct reconcilers without a manager. Production always
		// installs the uncached API reader in SetupWithManager.
		reader = r.Client
	}
	var current platformv1alpha1.EnvironmentTemplate
	if err := reader.Get(ctx, client.ObjectKeyFromObject(observed), &current); errors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("revalidate environment template: %w", err)
	}
	return current.UID == observed.UID && current.Generation == observed.Generation && current.DeletionTimestamp.IsZero(), nil
}

func warmPoolTemplateRequests(object client.Object) []reconcile.Request {
	env := object.(*platformv1alpha1.Environment)
	templates := make(map[string]struct{}, 3)
	for _, name := range []string{env.Spec.TemplateRef, env.Labels[warmPoolLabel]} {
		if name != "" {
			templates[name] = struct{}{}
		}
	}
	if owner := metav1.GetControllerOf(env); owner != nil && owner.APIVersion == platformv1alpha1.GroupVersion.String() && owner.Kind == "EnvironmentTemplate" && owner.Name != "" {
		templates[owner.Name] = struct{}{}
	}
	requests := make([]reconcile.Request, 0, len(templates))
	for name := range templates {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: env.Namespace, Name: name}})
	}
	return requests
}

// SetupWithManager registers the warm-pool controller with the manager.
func (r *WarmPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.EnvironmentTemplate{}).
		// EnqueueRequestsFromMapFunc applies the mapper to both old and new
		// objects on updates, so identity changes replenish both pools.
		Watches(&platformv1alpha1.Environment{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			return warmPoolTemplateRequests(object)
		})).
		Complete(r)
}
