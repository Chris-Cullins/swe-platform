package controllers

import (
	"slices"
	"testing"
)

func TestEnvironmentReconcilePhaseOrderingContract(t *testing.T) {
	want := []string{
		"tenancy-fencing",
		"deletion",
		"recovery-migration",
		"lifecycle",
		"legacy-provisioning-migration",
		"provisioning-fence",
		"project-resolution",
		"suspension",
		"project-validation",
		"template",
		"recovery",
		"backend-runtime",
		"provisioning",
		"status-idle",
	}
	got := make([]string, 0, len(environmentReconcilePhases))
	for _, phase := range environmentReconcilePhases {
		got = append(got, phase.name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("environment reconcile phase order = %v, want %v", got, want)
	}

	// Teardown and execution fencing must never wait for dependency resolution.
	for _, gate := range []string{"tenancy-fencing", "deletion", "recovery-migration", "lifecycle", "legacy-provisioning-migration", "provisioning-fence"} {
		for _, dependency := range []string{"project-resolution", "project-validation", "template", "recovery", "backend-runtime", "provisioning"} {
			if slices.Index(got, gate) >= slices.Index(got, dependency) {
				t.Errorf("gate %q must run before dependency phase %q", gate, dependency)
			}
		}
	}
	if slices.Index(got, "deletion") >= slices.Index(got, "recovery-migration") || slices.Index(got, "recovery-migration") >= slices.Index(got, "lifecycle") {
		t.Errorf("deletion, recovery migration, and lifecycle are out of order: %v", got)
	}

	// Preserve the base's split Project ordering: read enough to reject a
	// non-empty egress allowlist, suspend without depending on a successful
	// Project read, then classify any deferred Project error.
	if slices.Index(got, "project-resolution") >= slices.Index(got, "suspension") ||
		slices.Index(got, "suspension") >= slices.Index(got, "project-validation") {
		t.Errorf("project resolution, suspension, and project validation are out of order: %v", got)
	}
}
