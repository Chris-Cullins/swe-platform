package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const WorkspaceIncarnationAnnotation = "swe.dev/workspace-incarnation"

// WorkspacePVCMatches proves a bound storage object, not its contents. Pending
// identity cannot authorize execution or historical Run association.
func WorkspacePVCMatches(env *Environment, pvc *corev1.PersistentVolumeClaim) bool {
	w := env.Status.Workspace
	if w == nil || w.Backend != EnvironmentBackendPod || w.Incarnation == "" || w.PVCRef == nil || w.PVCRef.UID == "" ||
		pvc.Namespace != env.Namespace || pvc.Name != w.PVCRef.Name || pvc.UID != w.PVCRef.UID || !pvc.DeletionTimestamp.IsZero() ||
		pvc.Annotations[WorkspaceIncarnationAnnotation] != w.Incarnation {
		return false
	}
	owner := metav1.GetControllerOf(pvc)
	return owner != nil && owner.APIVersion == GroupVersion.String() && owner.Kind == "Environment" && owner.Name == env.Name && owner.UID == env.UID
}
