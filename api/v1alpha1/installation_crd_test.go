package v1alpha1

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestGeneratedInstallationCRDIsolationContract(t *testing.T) {
	contents, err := os.Open(filepath.Join("..", "..", "config", "crd", "bases", "swe.dev_installations.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer contents.Close()

	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema generatedCRDSchema `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	if err := yaml.NewYAMLToJSONDecoder(contents).Decode(&crd); err != nil {
		t.Fatal(err)
	}
	if len(crd.Spec.Versions) != 1 {
		t.Fatalf("generated Installation CRD versions = %d, want 1", len(crd.Spec.Versions))
	}
	schema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	isolation := schema.Properties["spec"].Properties["isolation"]
	if isolation.Default != nil {
		t.Fatalf("spec.isolation has a default: %#v", isolation.Default)
	}
	wantModes := []string{"UnrestrictedDevelopment", "RestrictedProductionCalicoV3_32_1"}
	if got := isolation.Properties["mode"].Enum; !slices.Equal(got, wantModes) {
		t.Fatalf("spec.isolation.mode enum = %#v, want %#v", got, wantModes)
	}
	if len(isolation.XValidations) != 1 {
		t.Fatalf("spec.isolation cross-field validations = %#v, want one", isolation.XValidations)
	}

	status := schema.Properties["status"]
	wantStates := []string{"LegacyUnclassified", "Fencing", "Blocked", "Active"}
	if got := status.Properties["isolationState"].Enum; !slices.Equal(got, wantStates) {
		t.Fatalf("status.isolationState enum = %#v, want %#v", got, wantStates)
	}
	if csiDriver, ok := status.Properties["csiDriver"]; !ok || len(csiDriver.XValidations) != 1 {
		t.Fatalf("status.csiDriver exact-identity validation = present %v, validations %#v", ok, csiDriver.XValidations)
	}
	conditions := status.Properties["conditions"]
	if conditions.MaxItems == nil || *conditions.MaxItems != 16 {
		t.Fatalf("status.conditions maxItems = %v, want 16", conditions.MaxItems)
	}
}
