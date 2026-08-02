package v1alpha1

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"
)

type generatedCRDSchema struct {
	Properties   map[string]generatedCRDSchema `json:"properties"`
	Items        *generatedCRDSchema           `json:"items"`
	XValidations []struct {
		Message         string `json:"message"`
		Rule            string `json:"rule"`
		OptionalOldSelf bool   `json:"optionalOldSelf"`
	} `json:"x-kubernetes-validations"`
}

func TestGeneratedEnvironmentCRDServiceIdentityRuleHasBoundedItemScope(t *testing.T) {
	contents, err := os.Open(filepath.Join("..", "..", "config", "crd", "bases", "swe.dev_environments.yaml"))
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
		t.Fatalf("generated Environment CRD versions = %d, want 1", len(crd.Spec.Versions))
	}
	services := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["services"]
	if len(services.XValidations) != 0 {
		t.Fatalf("spec.services has list-scoped CEL rules with multiplicative cost: %#v", services.XValidations)
	}
	if services.Items == nil {
		t.Fatal("spec.services has no item schema")
	}
	found := false
	for _, validation := range services.Items.XValidations {
		if strings.HasPrefix(validation.Message, "new services require instanceID") {
			found = true
			if !validation.OptionalOldSelf || strings.Contains(validation.Rule, "self.all(") || strings.Contains(validation.Rule, ".exists(") {
				t.Fatalf("service identity migration rule is not bounded item validation: %#v", validation)
			}
		}
	}
	if !found {
		t.Fatal("service item schema has no instanceID migration rule")
	}
}
