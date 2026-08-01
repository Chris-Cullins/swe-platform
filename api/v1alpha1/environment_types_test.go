package v1alpha1

import (
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnvironmentRecoveryStatusJSONContractRetainsLegacyCompatibility(t *testing.T) {
	nextAttemptAt := metav1.NewTime(time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC))
	status := EnvironmentStatus{Recovery: EnvironmentRecoveryStatus{
		Attempts: 2, Exhausted: true, ExecutionGeneration: 7, NextAttemptAt: &nextAttemptAt,
	}, PodRecoveryAttempts: 1, PodRecoveryExhausted: true, PodRecoveryUID: "pod-uid", PodRecoveryNextAttemptAt: &nextAttemptAt}

	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"podRecoveryAttempts", "podRecoveryExhausted", "podRecoveryUID", "podRecoveryNextAttemptAt",
	} {
		if _, exists := object[field]; !exists {
			t.Fatalf("compatibility field %q is absent from Environment status JSON: %s", field, encoded)
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

	var decoded EnvironmentStatus
	if err := json.Unmarshal([]byte(`{"podRecoveryAttempts":2,"podRecoveryExhausted":true,"podRecoveryUID":"legacy-pod","podRecoveryNextAttemptAt":"2026-07-27T12:00:00Z"}`), &decoded); err != nil {
		t.Fatalf("decode legacy compatibility status: %v", err)
	}
	if decoded.PodRecoveryAttempts != 2 || !decoded.PodRecoveryExhausted || decoded.PodRecoveryUID != "legacy-pod" || decoded.PodRecoveryNextAttemptAt == nil || !decoded.PodRecoveryNextAttemptAt.Equal(&nextAttemptAt) {
		t.Fatalf("decoded legacy compatibility status = %#v", decoded)
	}
}
