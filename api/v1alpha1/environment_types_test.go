package v1alpha1

import (
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnvironmentRecoveryStatusJSONContractIsNestedAndBackendNeutral(t *testing.T) {
	nextAttemptAt := metav1.NewTime(time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC))
	status := EnvironmentStatus{Recovery: EnvironmentRecoveryStatus{
		Attempts: 2, Exhausted: true, ExecutionGeneration: 7, NextAttemptAt: &nextAttemptAt,
	}}

	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	for _, oldField := range []string{
		"pod" + "RecoveryAttempts",
		"pod" + "RecoveryExhausted",
		"pod" + "RecoveryUID",
		"pod" + "RecoveryNextAttemptAt",
	} {
		if _, exists := object[oldField]; exists {
			t.Fatalf("legacy field %q remains in Environment status JSON: %s", oldField, encoded)
		}
	}
	var recovery map[string]json.RawMessage
	if err := json.Unmarshal(object["recovery"], &recovery); err != nil {
		t.Fatalf("decode recovery status from %s: %v", encoded, err)
	}
	for _, field := range []string{"attempts", "exhausted", "executionGeneration", "nextAttemptAt"} {
		if _, exists := recovery[field]; !exists {
			t.Fatalf("recovery field %q is absent from JSON: %s", field, encoded)
		}
	}
	for field := range recovery {
		if field == "podUID" || field == "podName" || field == "uid" {
			t.Fatalf("backend-private identity field %q is exposed in recovery JSON: %s", field, encoded)
		}
	}
}
