package v1alpha1

import (
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

func TestResolveEnvironmentProvisioningDefaultsAndFreezesProjection(t *testing.T) {
	template := &EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "template-uid", Generation: 4}, Spec: EnvironmentTemplateSpec{Image: "image:v1"}}
	project := &Project{ObjectMeta: metav1.ObjectMeta{Name: "project", UID: "project-uid", Generation: 3}, Spec: ProjectSpec{Repositories: []string{"https://example.test/one.git"}}}
	env := &Environment{Spec: EnvironmentSpec{TemplateRef: template.Name, ProjectRef: project.Name}}

	snapshot := ResolveEnvironmentProvisioning(env, template, project)
	if snapshot.Backend != EnvironmentBackendPod || snapshot.Size != "medium" || snapshot.DiskSize.Cmp(resource.MustParse("40Gi")) != 0 {
		t.Fatalf("resolved defaults = backend %q size %q disk %s", snapshot.Backend, snapshot.Size, snapshot.DiskSize.String())
	}
	cpu, memory := snapshot.Resources[string(corev1.ResourceCPU)], snapshot.Resources[string(corev1.ResourceMemory)]
	if cpu.Cmp(resource.MustParse("8")) != 0 || memory.Cmp(resource.MustParse("16Gi")) != 0 {
		t.Fatalf("resolved resources = %v", snapshot.Resources)
	}
	if snapshot.Project == nil || snapshot.Project.Repository != project.Spec.Repositories[0] {
		t.Fatalf("resolved project = %#v", snapshot.Project)
	}
	template.Spec.Image = "image:v2"
	project.Spec.Repositories[0] = "https://example.test/two.git"
	if snapshot.Image != "image:v1" || snapshot.Project.Repository != "https://example.test/one.git" {
		t.Fatalf("snapshot followed source edits: %#v", snapshot)
	}
}

func TestProvisioningSnapshotCurrentTemplateIgnoresPolicyButNotProvisioning(t *testing.T) {
	template := &EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "small", UID: "uid", Generation: 1}, Spec: EnvironmentTemplateSpec{Image: "image", Size: "small"}}
	snapshot := ResolveEnvironmentProvisioning(&Environment{}, template, nil)
	snapshot.TemplateVerified = true
	template.Generation++
	template.Spec.IdleTimeout = &metav1.Duration{Duration: time.Hour}
	template.Spec.WarmPool = &WarmPoolSpec{Min: 10}
	if !ProvisioningSnapshotCurrentTemplate(snapshot, template) {
		t.Fatal("policy-only edits made provisioning snapshot stale")
	}
	template.Spec.Image = "changed"
	if ProvisioningSnapshotCurrentTemplate(snapshot, template) {
		t.Fatal("image edit did not make provisioning snapshot stale")
	}
	if _, ok := snapshot.Resources[string(corev1.ResourceCPU)]; !ok {
		t.Fatal("snapshot omitted CPU resource key")
	}

	malformed := snapshot.DeepCopy()
	malformed.Template.Generation = 0
	// Make the live source match the malformed field to ensure structural
	// validation, rather than projection inequality, rejects it.
	template.Generation = 0
	template.Spec.Image = snapshot.Image
	if ProvisioningSnapshotCurrentTemplate(malformed, template) {
		t.Fatal("structurally malformed snapshot was accepted as current")
	}
}
