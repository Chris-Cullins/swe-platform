package v1alpha1

import (
	"context"
	"os"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

func TestGeneratedProjectEgressAllowlistSchema(t *testing.T) {
	data, err := os.ReadFile("../../config/crd/bases/swe.dev_projects.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	var internalCRD apiextensions.CustomResourceDefinition
	if err := apiextensionsv1.Convert_v1_CustomResourceDefinition_To_apiextensions_CustomResourceDefinition(&crd, &internalCRD, nil); err != nil {
		t.Fatal(err)
	}
	for _, version := range internalCRD.Spec.Versions {
		if version.Storage {
			internalCRD.Status.StoredVersions = append(internalCRD.Status.StoredVersions, version.Name)
		}
	}
	if errs := apiextensionsvalidation.ValidateCustomResourceDefinition(context.Background(), &internalCRD); len(errs) != 0 {
		t.Fatalf("generated Project CRD is not establishable: %v", errs)
	}

	version := crd.Spec.Versions[0]
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Name == GroupVersion.Version {
			version = crd.Spec.Versions[i]
		}
	}
	allowlist := version.Schema.OpenAPIV3Schema.Properties["spec"].Properties["egressAllowlist"]
	if allowlist.MaxItems == nil || *allowlist.MaxItems != 64 || allowlist.XListType == nil || *allowlist.XListType != "set" || allowlist.Items == nil || allowlist.Items.Schema == nil {
		t.Fatalf("generated allowlist collection bounds are incomplete: %#v", allowlist)
	}
	item := allowlist.Items.Schema
	if item.MinLength == nil || *item.MinLength != 1 || item.MaxLength == nil || *item.MaxLength != 253 || item.Pattern == "" {
		t.Fatalf("generated allowlist item bounds are incomplete: %#v", item)
	}

	var internal apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(version.Schema.OpenAPIV3Schema, &internal, nil); err != nil {
		t.Fatal(err)
	}
	structural, err := structuralschema.NewStructural(&internal)
	if err != nil {
		t.Fatal(err)
	}
	openAPI, _, err := apiservervalidation.NewSchemaValidator(&internal)
	if err != nil {
		t.Fatal(err)
	}
	celValidator := cel.NewValidator(structural, true, 1_000_000)
	base := map[string]interface{}{
		"apiVersion": GroupVersion.String(), "kind": "Project",
		"metadata": map[string]interface{}{"name": "project", "namespace": "default"},
		"spec":     map[string]interface{}{"repositories": []interface{}{"https://example.com/repo.git"}},
	}
	tests := []struct {
		name      string
		allowlist []interface{}
		valid     bool
	}{
		{name: "omitted", valid: true},
		{name: "empty", allowlist: []interface{}{}, valid: true},
		{name: "exact FQDN", allowlist: []interface{}{"api.example.com"}, valid: true},
		{name: "uppercase", allowlist: []interface{}{"API.example.com"}},
		{name: "trailing dot", allowlist: []interface{}{"api.example.com."}},
		{name: "IDNA", allowlist: []interface{}{"xn--bcher-kva.example"}},
		{name: "inner IDNA", allowlist: []interface{}{"www.xn--bcher-kva.example"}},
		{name: "IPv4", allowlist: []interface{}{"127.0.0.1"}},
		{name: "invalid octal is a hostname", allowlist: []interface{}{"09.0.0.1"}, valid: true},
		// Legacy IP forms pass the expressible CRD shape and are rejected by the
		// authoritative egresspolicy parser before any later runtime use.
		{name: "legacy IPv4 requires Go authority", allowlist: []interface{}{"0x7f.0x0.0x0.0x1"}, valid: true},
		{name: "one label", allowlist: []interface{}{"localhost"}},
		{name: "too many", allowlist: projectSchemaHosts(65)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			object := runtime.DeepCopyJSONValue(base).(map[string]interface{})
			if test.allowlist != nil {
				object["spec"].(map[string]interface{})["egressAllowlist"] = test.allowlist
			}
			errs := apiservervalidation.ValidateCustomResource(nil, object, openAPI)
			celErrs, _ := celValidator.Validate(context.Background(), nil, structural, object, nil, 10_000_000)
			errs = append(errs, celErrs...)
			if test.valid != (len(errs) == 0) {
				t.Fatalf("valid=%t errors=%v", test.valid, errs)
			}
		})
	}
}

func projectSchemaHosts(count int) []interface{} {
	result := make([]interface{}, count)
	for i := range result {
		result[i] = "host-" + string(rune('a'+i%26)) + ".example.com"
	}
	return result
}
