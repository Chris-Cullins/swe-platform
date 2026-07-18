package controllers

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

// sizePresets maps EnvironmentTemplate size names to pod resource requests/limits.
var sizePresets = map[string]corev1.ResourceList{
	"tiny":   {corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")},
	"small":  {corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
	"medium": {corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi")},
	"large":  {corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("32Gi")},
}

const (
	defaultDiskSize    = "40Gi"
	defaultIdleTimeout = 15 * time.Minute
	templateRefField   = "spec.templateRef"
)

const projectSetupScript = `set -eu
if [ ! -d /workspace/.git ]; then
	git clone -- "$SWE_REPOSITORY" /workspace
fi
if ! git -c safe.directory=/workspace -C /workspace config --local --get swe.setup-complete >/dev/null 2>&1; then
	if [ -f /workspace/.agents/setup ]; then
		/bin/sh /workspace/.agents/setup
	fi
	git -c safe.directory=/workspace -C /workspace config --local swe.setup-complete true
fi
if [ "${SWE_RESUMING:-false}" = true ] && [ -f /workspace/.agents/resume ]; then
	/bin/sh /workspace/.agents/resume
fi
`

// EnvironmentReconciler reconciles Environment objects into pods + workspace volumes.
type EnvironmentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=swe.dev,resources=environments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=swe.dev,resources=environments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swe.dev,resources=environmenttemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=swe.dev,resources=projects,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives an Environment toward its desired state:
// pod + PVC present when active, pod deleted (PVC retained) when paused.
func (r *EnvironmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var env platformv1alpha1.Environment
	if err := r.Get(ctx, req.NamespacedName, &env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !env.DeletionTimestamp.IsZero() {
		// Owned pods/PVCs are garbage-collected via owner references.
		return ctrl.Result{}, nil
	}

	var tmpl platformv1alpha1.EnvironmentTemplate
	if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.TemplateRef}, &tmpl); err != nil {
		return ctrl.Result{}, r.fail(ctx, &env, fmt.Errorf("get template %q: %w", env.Spec.TemplateRef, err))
	}

	if err := r.ensureWorkspacePVC(ctx, &env, &tmpl); err != nil {
		return ctrl.Result{}, r.fail(ctx, &env, fmt.Errorf("ensure workspace PVC: %w", err))
	}

	if env.Spec.Paused {
		return r.reconcilePaused(ctx, &env)
	}
	if env.Status.Phase == platformv1alpha1.EnvironmentPhasePaused {
		now := metav1.Now()
		env.Status.LastActiveAt = &now
		return ctrl.Result{Requeue: true}, r.setPhase(ctx, &env, platformv1alpha1.EnvironmentPhaseResuming, "")
	}

	pod, err := r.ensurePod(ctx, &env, &tmpl)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, &env, fmt.Errorf("ensure pod: %w", err))
	}

	if err := r.syncStatus(ctx, &env, pod); err != nil {
		return ctrl.Result{}, err
	}
	if pod.Status.Phase != corev1.PodRunning {
		return ctrl.Result{}, nil
	}
	return r.reconcileIdle(ctx, &env, &tmpl)
}

// reconcileIdle schedules the next activity check or requests a pause once the
// template's idle timeout has elapsed.
func (r *EnvironmentReconciler) reconcileIdle(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) (ctrl.Result, error) {
	timeout := defaultIdleTimeout
	if tmpl.Spec.IdleTimeout != nil {
		timeout = tmpl.Spec.IdleTimeout.Duration
	}
	remaining := timeout
	if env.Status.LastActiveAt != nil {
		remaining = time.Until(env.Status.LastActiveAt.Add(timeout))
	}
	if remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	before := env.DeepCopy()
	env.Spec.Paused = true
	patch := client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})
	if err := r.Patch(ctx, env, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("request idle pause: %w", err)
	}
	if err := r.setPhase(ctx, env, platformv1alpha1.EnvironmentPhaseIdle, env.Status.PodName); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// reconcilePaused deletes the pod (if any) and keeps the workspace volume.
func (r *EnvironmentReconciler) reconcilePaused(ctx context.Context, env *platformv1alpha1.Environment) (ctrl.Result, error) {
	podName := envPodName(env)
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: podName}, &pod)
	if err == nil {
		if delErr := r.Delete(ctx, &pod); delErr != nil && !errors.IsNotFound(delErr) {
			return ctrl.Result{}, fmt.Errorf("delete pod for pause: %w", delErr)
		}
		return ctrl.Result{Requeue: true}, nil
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, r.setPhase(ctx, env, platformv1alpha1.EnvironmentPhasePaused, "")
}

func (r *EnvironmentReconciler) ensureWorkspacePVC(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) error {
	pvcName := envPVCName(env)
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: pvcName}, &pvc)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	size := resource.MustParse(defaultDiskSize)
	if tmpl.Spec.DiskSize != nil {
		size = *tmpl.Spec.DiskSize
	}

	pvc = corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: env.Namespace,
			Name:      pvcName,
			Labels:    envLabels(env),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
	if err := controllerutil.SetControllerReference(env, &pvc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, &pvc)
}

// ensurePod returns the backing pod, creating it if necessary.
func (r *EnvironmentReconciler) ensurePod(ctx context.Context, env *platformv1alpha1.Environment, tmpl *platformv1alpha1.EnvironmentTemplate) (*corev1.Pod, error) {
	podName := envPodName(env)
	var pod corev1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: podName}, &pod)
	if err == nil {
		return &pod, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	resources, ok := sizePresets[tmpl.Spec.Size]
	if !ok {
		resources = sizePresets["medium"]
	}

	pod = corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: env.Namespace,
			Name:      podName,
			Labels:    envLabels(env),
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			SecurityContext: &corev1.PodSecurityContext{
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:            "environment",
				Image:           tmpl.Spec.Image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sandboxd", "serve"},
				Resources: corev1.ResourceRequirements{
					Requests: resources,
					Limits:   resources,
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr(false),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "workspace",
					MountPath: "/workspace",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "workspace",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: envPVCName(env),
					},
				},
			}},
		},
	}
	if tmpl.Spec.RuntimeClass != "" {
		pod.Spec.RuntimeClassName = &tmpl.Spec.RuntimeClass
	}
	if env.Spec.ProjectRef != "" {
		var project platformv1alpha1.Project
		if err := r.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: env.Spec.ProjectRef}, &project); err != nil {
			return nil, fmt.Errorf("get project %q: %w", env.Spec.ProjectRef, err)
		}
		if len(project.Spec.Repositories) == 0 {
			return nil, fmt.Errorf("project %q has no repositories", env.Spec.ProjectRef)
		}
		projectEnv := []corev1.EnvVar{{Name: "SWE_REPOSITORY", Value: project.Spec.Repositories[0]}}
		if env.Status.Phase == platformv1alpha1.EnvironmentPhaseResuming {
			projectEnv = append(projectEnv, corev1.EnvVar{Name: "SWE_RESUMING", Value: "true"})
		}
		pod.Spec.InitContainers = []corev1.Container{{
			Name:            "project-setup",
			Image:           tmpl.Spec.Image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/bin/sh", "-c", projectSetupScript},
			Env:             projectEnv,
			Resources: corev1.ResourceRequirements{
				Requests: resources,
				Limits:   resources,
			},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr(false),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
			VolumeMounts: pod.Spec.Containers[0].VolumeMounts,
		}}
		if project.Spec.SecretRef != nil {
			projectSecret := []corev1.EnvFromSource{{
				SecretRef: &corev1.SecretEnvSource{LocalObjectReference: *project.Spec.SecretRef},
			}}
			pod.Spec.InitContainers[0].EnvFrom = projectSecret
			pod.Spec.Containers[0].EnvFrom = projectSecret
		}
	}
	if err := controllerutil.SetControllerReference(env, &pod, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, &pod); err != nil {
		return nil, err
	}
	return &pod, nil
}

// syncStatus maps pod state onto the Environment phase.
func (r *EnvironmentReconciler) syncStatus(ctx context.Context, env *platformv1alpha1.Environment, pod *corev1.Pod) error {
	phase := platformv1alpha1.EnvironmentPhaseCreating
	initializeActivity := false
	switch pod.Status.Phase {
	case corev1.PodPending:
		if env.Status.Phase == platformv1alpha1.EnvironmentPhaseResuming {
			phase = platformv1alpha1.EnvironmentPhaseResuming
		} else if len(pod.Spec.InitContainers) > 0 {
			phase = platformv1alpha1.EnvironmentPhaseSetup
		}
	case corev1.PodRunning:
		phase = platformv1alpha1.EnvironmentPhaseReady
		initializeActivity = env.Status.LastActiveAt == nil
	case corev1.PodFailed:
		phase = platformv1alpha1.EnvironmentPhaseFailed
	case corev1.PodSucceeded:
		phase = platformv1alpha1.EnvironmentPhaseTerminated
	}
	if initializeActivity {
		now := metav1.Now()
		env.Status.LastActiveAt = &now
	}
	if env.Status.Phase == phase && env.Status.PodName == pod.Name && !initializeActivity {
		return nil
	}
	env.Status.Phase = phase
	env.Status.PodName = pod.Name
	return r.Status().Update(ctx, env)
}

func (r *EnvironmentReconciler) setPhase(ctx context.Context, env *platformv1alpha1.Environment, phase platformv1alpha1.EnvironmentPhase, podName string) error {
	if env.Status.Phase == phase && env.Status.PodName == podName {
		return nil
	}
	env.Status.Phase = phase
	env.Status.PodName = podName
	return r.Status().Update(ctx, env)
}

func (r *EnvironmentReconciler) fail(ctx context.Context, env *platformv1alpha1.Environment, err error) error {
	log.FromContext(ctx).Error(err, "reconcile failed", "environment", env.Name)
	env.Status.Phase = platformv1alpha1.EnvironmentPhaseFailed
	if statusErr := r.Status().Update(ctx, env); statusErr != nil {
		return statusErr
	}
	return err
}

func envPodName(env *platformv1alpha1.Environment) string { return "env-" + env.Name }
func envPVCName(env *platformv1alpha1.Environment) string { return "env-" + env.Name }

func envLabels(env *platformv1alpha1.Environment) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "swe-platform",
		"swe.dev/environment":          env.Name,
	}
}

func ptr[T any](v T) *T { return &v }

// SetupWithManager registers the controller with the manager.
func (r *EnvironmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &platformv1alpha1.Environment{}, templateRefField, func(object client.Object) []string {
		environment := object.(*platformv1alpha1.Environment)
		return []string{environment.Spec.TemplateRef}
	}); err != nil {
		return fmt.Errorf("index environments by template: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.Environment{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Watches(&platformv1alpha1.EnvironmentTemplate{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			var environments platformv1alpha1.EnvironmentList
			if err := r.List(ctx, &environments, client.InNamespace(object.GetNamespace()), client.MatchingFields{templateRefField: object.GetName()}); err != nil {
				log.FromContext(ctx).Error(err, "list environments for template", "template", object.GetName())
				return nil
			}
			requests := make([]reconcile.Request, 0, len(environments.Items))
			for i := range environments.Items {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&environments.Items[i])})
			}
			return requests
		})).
		Complete(r)
}
