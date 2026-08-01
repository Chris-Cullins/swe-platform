package controllers

import (
	"context"
	stderrors "errors"
	"fmt"
	"net"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *EnvironmentReconciler) syncStatus(ctx context.Context, env *platformv1alpha1.Environment, pod *corev1.Pod) error {
	executionGeneration, ok := podExecutionGeneration(pod)
	if !ok || executionGeneration != env.Status.ExecutionGeneration {
		return fmt.Errorf("environment pod execution generation is not current")
	}
	phase, reason, message := environmentPodState(env, pod)
	sandboxdEndpoint := ""
	if phase == platformv1alpha1.EnvironmentPhaseReady {
		sandboxdEndpoint = net.JoinHostPort(pod.Status.PodIP, "50051")
		if env.Status.LastActiveAt == nil || (env.Status.Phase != platformv1alpha1.EnvironmentPhaseReady && env.Status.Phase != platformv1alpha1.EnvironmentPhaseRunning) {
			now := metav1.Now()
			env.Status.LastActiveAt = &now
		}
	}
	executionChanged := false
	err := r.updateEnvironmentStatus(ctx, env, func(current *platformv1alpha1.Environment) {
		if current.Status.ExecutionGeneration != executionGeneration {
			executionChanged = true
			return
		}
		applyEnvironmentStatus(current, phase, pod.Name, sandboxdEndpoint, reason, message, env.Status.LastActiveAt)
		current.Status.ImageID = environmentImageID(pod)
		if phase == platformv1alpha1.EnvironmentPhaseReady {
			current.Status.Recovery = platformv1alpha1.EnvironmentRecoveryStatus{}
			current.Status.PodRecoveryAttempts = 0
			current.Status.PodRecoveryExhausted = false
			current.Status.PodRecoveryUID = ""
			current.Status.PodRecoveryNextAttemptAt = nil
		}
		clearChildOwnershipCollision(current)
	})
	if err != nil {
		return err
	}
	if executionChanged {
		return errEnvironmentExecutionChanged
	}
	return nil
}

func environmentImageID(pod *corev1.Pod) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "environment" {
			return status.ImageID
		}
	}
	return ""
}

func environmentPodState(env *platformv1alpha1.Environment, pod *corev1.Pod) (platformv1alpha1.EnvironmentPhase, string, string) {
	resuming := podIsResume(pod) || env.Status.Phase == platformv1alpha1.EnvironmentPhaseResuming
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse && condition.Reason == corev1.PodReasonUnschedulable {
			return platformv1alpha1.EnvironmentPhaseCreating, "Unschedulable", messageOr(condition.Message, "the scheduler cannot currently place the environment pod")
		}
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if terminated := status.State.Terminated; terminated != nil && terminated.ExitCode != 0 {
			return initContainerFailure(status.Name, terminated, resuming)
		}
		if status.State.Waiting != nil {
			waiting := status.State.Waiting
			if terminated := status.LastTerminationState.Terminated; terminated != nil && terminated.ExitCode != 0 {
				return initContainerFailure(status.Name, terminated, resuming)
			}
			if imagePullFailure(waiting.Reason) {
				return platformv1alpha1.EnvironmentPhaseFailed, "ImagePullFailed", containerStatusMessage(status.Name, waiting.Reason, waiting.Message)
			}
			if waiting.Reason == "CrashLoopBackOff" {
				return platformv1alpha1.EnvironmentPhaseFailed, setupReason(resuming, "Failed"), containerStatusMessage(status.Name, waiting.Reason, waiting.Message)
			}
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil {
			waiting := status.State.Waiting
			if imagePullFailure(waiting.Reason) {
				return platformv1alpha1.EnvironmentPhaseFailed, "ImagePullFailed", containerStatusMessage(status.Name, waiting.Reason, waiting.Message)
			}
			if waiting.Reason == "CrashLoopBackOff" {
				return platformv1alpha1.EnvironmentPhaseFailed, "SandboxdCrashLoopBackOff", containerStatusMessage(status.Name, waiting.Reason, waiting.Message)
			}
		}
	}

	switch pod.Status.Phase {
	case corev1.PodPending:
		if resuming {
			return platformv1alpha1.EnvironmentPhaseResuming, "ResumeInProgress", "repository resume and sandboxd startup are in progress"
		}
		if len(pod.Spec.InitContainers) > 0 {
			return platformv1alpha1.EnvironmentPhaseSetup, "SetupInProgress", "repository setup and sandboxd startup are in progress"
		}
		return platformv1alpha1.EnvironmentPhaseCreating, "Provisioning", "environment pod is provisioning"
	case corev1.PodRunning:
		if podReady(pod) && pod.Status.PodIP != "" {
			return platformv1alpha1.EnvironmentPhaseReady, "SandboxdReady", "setup is complete and sandboxd is ready"
		}
		return platformv1alpha1.EnvironmentPhaseCreating, "SandboxdNotReady", "sandboxd has not passed its readiness probe"
	case corev1.PodFailed:
		return platformv1alpha1.EnvironmentPhaseFailed, "PodFailed", messageOr(pod.Status.Message, "environment pod failed")
	case corev1.PodSucceeded:
		return platformv1alpha1.EnvironmentPhaseTerminated, "PodTerminated", "environment pod terminated"
	default:
		return platformv1alpha1.EnvironmentPhaseCreating, "Provisioning", "environment pod is provisioning"
	}
}

func initContainerFailure(name string, terminated *corev1.ContainerStateTerminated, resuming bool) (platformv1alpha1.EnvironmentPhase, string, string) {
	reason := setupReason(resuming, "Failed")
	if terminated.ExitCode == 124 || terminated.ExitCode == 137 {
		reason = setupReason(resuming, "HookTimedOut")
	}
	message := fmt.Sprintf("init container %s exited with code %d", name, terminated.ExitCode)
	if terminated.Message != "" {
		message += ": " + terminated.Message
	}
	return platformv1alpha1.EnvironmentPhaseFailed, reason, message
}

func messageOr(message, fallback string) string {
	if message != "" {
		return message
	}
	return fallback
}

func podIsResume(pod *corev1.Pod) bool {
	for _, container := range pod.Spec.InitContainers {
		for _, variable := range container.Env {
			if variable.Name == "SWE_RESUMING" && variable.Value == "true" {
				return true
			}
		}
	}
	return false
}

func imagePullFailure(reason string) bool {
	return reason == "ErrImagePull" || reason == "ImagePullBackOff" || reason == "InvalidImageName"
}

func setupReason(resuming bool, suffix string) string {
	if resuming {
		return "Resume" + suffix
	}
	return "Setup" + suffix
}

func containerStatusMessage(name, reason, message string) string {
	if message == "" {
		return fmt.Sprintf("container %s: %s", name, reason)
	}
	return fmt.Sprintf("container %s: %s: %s", name, reason, message)
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *EnvironmentReconciler) setPhase(ctx context.Context, env *platformv1alpha1.Environment, phase platformv1alpha1.EnvironmentPhase, podName, sandboxdEndpoint string) error {
	reason, message := phaseReadiness(phase)
	return r.setEnvironmentStatus(ctx, env, phase, podName, sandboxdEndpoint, reason, message)
}

func (r *EnvironmentReconciler) fail(ctx context.Context, env *platformv1alpha1.Environment, err error) error {
	log.FromContext(ctx).Error(err, "reconcile failed", "environment", env.Name)
	var collision *childOwnershipCollisionError
	var terminal *terminalEnvironmentError
	phase := platformv1alpha1.EnvironmentPhaseCreating
	reason := "OperationalError"
	if stderrors.As(err, &terminal) || errors.IsInvalid(err) || errors.IsBadRequest(err) {
		phase = platformv1alpha1.EnvironmentPhaseFailed
		reason = "InvalidConfiguration"
	} else if stderrors.As(err, &collision) {
		phase = platformv1alpha1.EnvironmentPhaseFailed
		reason = "ResourceCollision"
	}
	statusErr := r.updateEnvironmentStatus(ctx, env, func(current *platformv1alpha1.Environment) {
		applyEnvironmentStatus(current, phase, "", "", reason, err.Error(), env.Status.LastActiveAt)
		if stderrors.As(err, &collision) {
			apimeta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
				Type:               "ChildOwnershipConflict",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: current.Generation,
				Reason:             "ResourceCollision",
				Message:            collision.Error(),
			})
		}
	})
	if statusErr != nil {
		return statusErr
	}
	if collision != nil || terminal != nil || errors.IsInvalid(err) || errors.IsBadRequest(err) {
		return nil
	}
	return err
}

func (r *EnvironmentReconciler) setEnvironmentStatus(ctx context.Context, env *platformv1alpha1.Environment, phase platformv1alpha1.EnvironmentPhase, podName, sandboxdEndpoint, reason, message string) error {
	return r.updateEnvironmentStatus(ctx, env, func(current *platformv1alpha1.Environment) {
		applyEnvironmentStatus(current, phase, podName, sandboxdEndpoint, reason, message, env.Status.LastActiveAt)
		clearChildOwnershipCollision(current)
	})
}

func (r *EnvironmentReconciler) updateEnvironmentStatus(ctx context.Context, env *platformv1alpha1.Environment, mutate func(*platformv1alpha1.Environment)) error {
	key := client.ObjectKeyFromObject(env)
	expectedUID := env.UID
	expectedGeneration := env.Generation
	var updated platformv1alpha1.Environment
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.apiReader().Get(ctx, key, &updated); err != nil {
			return err
		}
		if updated.UID != expectedUID {
			return errEnvironmentIncarnationChanged
		}
		if updated.Generation != expectedGeneration {
			return nil
		}
		before := updated.DeepCopy()
		mutate(&updated)
		if apiequality.Semantic.DeepEqual(before.Status, updated.Status) {
			return nil
		}
		return r.Status().Update(ctx, &updated)
	})
	if err == nil {
		env.Status = updated.Status
	}
	return err
}
