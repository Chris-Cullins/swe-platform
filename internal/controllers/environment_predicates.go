package controllers

import (
	"reflect"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

// observationRelevantEnvironmentUpdate suppresses only bookkeeping and a sole
// service-observation status delta. Every substantive object delta remains.
func observationRelevantEnvironmentUpdate(oldEnv, newEnv *platformv1alpha1.Environment) bool {
	oldCopy, newCopy := oldEnv.DeepCopy(), newEnv.DeepCopy()
	oldCopy.ResourceVersion, newCopy.ResourceVersion = "", ""
	oldCopy.ManagedFields, newCopy.ManagedFields = nil, nil
	oldCopy.Status.ServiceObservations, newCopy.Status.ServiceObservations = nil, nil
	return !reflect.DeepEqual(oldCopy, newCopy)
}
