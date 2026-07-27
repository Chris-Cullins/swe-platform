package controllers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func TestEnvironmentPredicatesIgnoreOnlyObservationBookkeeping(t *testing.T) {
	old := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "n", UID: "u", ResourceVersion: "1"}}
	newEnv := old.DeepCopy()
	newEnv.ResourceVersion = "2"
	newEnv.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "observer"}}
	newEnv.Status.ServiceObservations = &platformv1alpha1.EnvironmentServiceObservations{ObservedAt: metav1.Now()}
	if observationRelevantEnvironmentUpdate(old, newEnv) || runRelevantEnvironmentUpdate(old, newEnv) || warmPoolRelevantEnvironmentUpdate(old, newEnv) {
		t.Fatal("observation-only update was relevant")
	}
	mutations := map[string]func(*platformv1alpha1.Environment){
		"service spec": func(e *platformv1alpha1.Environment) {
			e.Spec.Services = []platformv1alpha1.EnvironmentServiceDeclaration{{Name: "web", Revision: 1, TargetPort: 3000}}
		},
		"finalizer": func(e *platformv1alpha1.Environment) { e.Finalizers = []string{"f"} },
		"label":     func(e *platformv1alpha1.Environment) { e.Labels = map[string]string{"x": "y"} },
		"owner":     func(e *platformv1alpha1.Environment) { e.OwnerReferences = []metav1.OwnerReference{{Name: "owner"}} },
		"lifecycle": func(e *platformv1alpha1.Environment) { e.Status.Lifecycle.Epoch++ },
		"readiness": func(e *platformv1alpha1.Environment) { e.Status.Phase = platformv1alpha1.EnvironmentPhaseReady },
		"endpoint":  func(e *platformv1alpha1.Environment) { e.Status.Endpoints.Sandboxd = "host:1" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := old.DeepCopy()
			mutate(changed)
			if !observationRelevantEnvironmentUpdate(old, changed) || !warmPoolRelevantEnvironmentUpdate(old, changed) {
				t.Fatal("substantive update ignored")
			}
		})
	}
}
