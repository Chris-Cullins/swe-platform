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

// TestGeneratedEnvironmentSelectorTransitions exercises the installed artifact,
// rather than duplicating or inspecting the marker source that generated it.
func TestGeneratedEnvironmentSelectorTransitions(t *testing.T) {
	data, err := os.ReadFile("../../config/crd/bases/swe.dev_environments.yaml")
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
		t.Fatalf("generated Environment CRD is not establishable: %v", errs)
	}
	version := crd.Spec.Versions[0]
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Name == GroupVersion.Version {
			version = crd.Spec.Versions[i]
		}
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
		"apiVersion": GroupVersion.String(), "kind": "Environment",
		"metadata": map[string]interface{}{"name": "env", "namespace": "default"},
		"spec":     map[string]interface{}{"templateRef": "small"},
	}
	withSpec := func(source map[string]interface{}, changes map[string]interface{}) map[string]interface{} {
		result := runtime.DeepCopyJSONValue(source).(map[string]interface{})
		spec := result["spec"].(map[string]interface{})
		for key, value := range changes {
			if value == nil {
				delete(spec, key)
			} else {
				spec[key] = value
			}
		}
		return result
	}
	tests := []struct {
		name        string
		oldChanges  map[string]interface{}
		newChanges  map[string]interface{}
		create      bool
		wantAllowed bool
	}{
		{name: "all selectors unchanged", oldChanges: map[string]interface{}{"projectRef": "project", "backend": "pod"}, newChanges: map[string]interface{}{"projectRef": "project", "backend": "pod"}, wantAllowed: true},
		{name: "omitted project stays omitted", wantAllowed: true},
		{name: "omitted to nonempty project", newChanges: map[string]interface{}{"projectRef": "project"}, wantAllowed: true},
		{name: "omitted to explicit empty project rejected", newChanges: map[string]interface{}{"projectRef": ""}},
		{name: "explicit empty stays explicit empty", oldChanges: map[string]interface{}{"projectRef": ""}, newChanges: map[string]interface{}{"projectRef": ""}, wantAllowed: true},
		{name: "explicit empty to nonempty project", oldChanges: map[string]interface{}{"projectRef": ""}, newChanges: map[string]interface{}{"projectRef": "project"}, wantAllowed: true},
		{name: "explicit empty to omitted project", oldChanges: map[string]interface{}{"projectRef": ""}, wantAllowed: true},
		{name: "nonempty project unchanged", oldChanges: map[string]interface{}{"projectRef": "project"}, newChanges: map[string]interface{}{"projectRef": "project"}, wantAllowed: true},
		{name: "template change", newChanges: map[string]interface{}{"templateRef": "large"}},
		{name: "backend omitted to pod", newChanges: map[string]interface{}{"backend": "pod"}},
		{name: "backend pod to omitted", oldChanges: map[string]interface{}{"backend": "pod"}},
		{name: "project changed", oldChanges: map[string]interface{}{"projectRef": "one"}, newChanges: map[string]interface{}{"projectRef": "two"}},
		{name: "project removed", oldChanges: map[string]interface{}{"projectRef": "project"}},
		{name: "lifecycle independently changes", newChanges: map[string]interface{}{"lifecycle": map[string]interface{}{"hold": map[string]interface{}{"enabled": true, "revision": int64(1)}}}, wantAllowed: true},
		{name: "services independently change", newChanges: map[string]interface{}{"services": []interface{}{map[string]interface{}{"name": "web", "source": "API", "instanceID": "aaaaaaaaaaaaaaaaaaaa", "revision": int64(1), "protocol": "HTTP", "targetPort": int64(8080), "visibility": "Project", "readiness": "TCPConnect"}}}, wantAllowed: true},
		{name: "deprecated paused independently changes", newChanges: map[string]interface{}{"paused": true}, wantAllowed: true},
		{name: "present blank project rejected on creation by OpenAPI", newChanges: map[string]interface{}{"projectRef": ""}, create: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldObject := withSpec(base, test.oldChanges)
			newObject := withSpec(base, test.newChanges)
			errs := apiservervalidation.ValidateCustomResource(nil, newObject, openAPI)
			var oldForCEL interface{}
			if !test.create {
				errs = apiservervalidation.ValidateCustomResourceUpdate(nil, newObject, oldObject, openAPI, apiservervalidation.WithRatcheting(nil))
				oldForCEL = oldObject
			}
			celErrs, _ := celValidator.Validate(context.Background(), nil, structural, newObject, oldForCEL, 10_000_000)
			errs = append(errs, celErrs...)
			if test.wantAllowed && len(errs) != 0 {
				t.Fatalf("valid transition rejected: %v", errs)
			}
			if !test.wantAllowed && len(errs) == 0 {
				t.Fatal("invalid transition was accepted")
			}
		})
	}

	provisioning := map[string]interface{}{
		"template": map[string]interface{}{"name": "small", "uid": "template-uid", "generation": int64(1)},
		"backend":  "pod", "image": "image", "size": "small",
		"resources":        map[string]interface{}{"cpu": "2", "memory": "4Gi"},
		"runtimeClassName": "", "diskSize": "40Gi", "templateVerified": false, "projectVerified": false,
	}
	withProvisioning := func(snapshot map[string]interface{}) map[string]interface{} {
		object := runtime.DeepCopyJSONValue(base).(map[string]interface{})
		object["status"] = map[string]interface{}{"provisioning": snapshot}
		return object
	}
	transition := func(t *testing.T, oldSnapshot, newSnapshot map[string]interface{}, allowed bool) {
		t.Helper()
		oldObject, newObject := runtime.DeepCopyJSONValue(base).(map[string]interface{}), runtime.DeepCopyJSONValue(base).(map[string]interface{})
		newObject["status"] = map[string]interface{}{}
		if oldSnapshot != nil {
			oldObject = withProvisioning(oldSnapshot)
		}
		if newSnapshot != nil {
			newObject = withProvisioning(newSnapshot)
		}
		errs := apiservervalidation.ValidateCustomResource(nil, newObject, openAPI)
		celErrs, _ := celValidator.Validate(context.Background(), nil, structural, newObject, oldObject, 10_000_000)
		errs = append(errs, celErrs...)
		if allowed != (len(errs) == 0) {
			t.Fatalf("allowed=%t errors=%v", allowed, errs)
		}
	}
	t.Run("provisioning initial add", func(t *testing.T) { transition(t, nil, provisioning, true) })
	legacyProvisioning := runtime.DeepCopyJSONValue(provisioning).(map[string]interface{})
	legacyProvisioning["legacyWorkspacePVCUID"] = "pvc-uid"
	t.Run("legacy workspace UID initial add", func(t *testing.T) { transition(t, nil, legacyProvisioning, true) })
	t.Run("legacy workspace UID rejects empty", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(provisioning).(map[string]interface{})
		next["legacyWorkspacePVCUID"] = ""
		transition(t, nil, next, false)
	})
	t.Run("legacy workspace UID remains unchanged", func(t *testing.T) { transition(t, legacyProvisioning, legacyProvisioning, true) })
	t.Run("legacy workspace UID rejects later addition", func(t *testing.T) { transition(t, provisioning, legacyProvisioning, false) })
	t.Run("legacy workspace UID rejects removal", func(t *testing.T) { transition(t, legacyProvisioning, provisioning, false) })
	t.Run("legacy workspace UID rejects change", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(legacyProvisioning).(map[string]interface{})
		next["legacyWorkspacePVCUID"] = "replacement-pvc"
		transition(t, legacyProvisioning, next, false)
	})
	t.Run("provisioning rejects initially template-verified snapshot", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(provisioning).(map[string]interface{})
		next["templateVerified"] = true
		transition(t, nil, next, false)
	})
	t.Run("provisioning rejects initially project-verified snapshot", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(provisioning).(map[string]interface{})
		next["project"] = map[string]interface{}{"name": "project", "uid": "project-uid", "generation": int64(1), "repository": "https://example.test/repo"}
		next["projectVerified"] = true
		transition(t, nil, next, false)
	})
	t.Run("provisioning template verification", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(provisioning).(map[string]interface{})
		next["templateVerified"] = true
		transition(t, provisioning, next, true)
	})
	verified := runtime.DeepCopyJSONValue(provisioning).(map[string]interface{})
	verified["templateVerified"] = true
	projectAdded := runtime.DeepCopyJSONValue(verified).(map[string]interface{})
	projectAdded["project"] = map[string]interface{}{"name": "project", "uid": "project-uid", "generation": int64(1), "repository": "https://example.test/repo"}
	t.Run("provisioning project add", func(t *testing.T) { transition(t, verified, projectAdded, true) })
	t.Run("provisioning rejects project add with base change", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(projectAdded).(map[string]interface{})
		next["image"] = "changed"
		transition(t, verified, next, false)
	})
	t.Run("provisioning project verification", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(projectAdded).(map[string]interface{})
		next["projectVerified"] = true
		transition(t, projectAdded, next, true)
	})
	projectVerified := runtime.DeepCopyJSONValue(projectAdded).(map[string]interface{})
	projectVerified["projectVerified"] = true
	t.Run("provisioning rejects project change", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(projectVerified).(map[string]interface{})
		next["project"].(map[string]interface{})["uid"] = "replacement-project"
		transition(t, projectVerified, next, false)
	})
	t.Run("provisioning rejects project removal", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(projectVerified).(map[string]interface{})
		delete(next, "project")
		transition(t, projectVerified, next, false)
	})
	t.Run("provisioning rejects project verification reset", func(t *testing.T) {
		transition(t, projectVerified, projectAdded, false)
	})
	for _, field := range []string{"template", "backend", "image", "size", "resources", "runtimeClassName", "diskSize"} {
		t.Run("provisioning rejects changed "+field, func(t *testing.T) {
			next := runtime.DeepCopyJSONValue(verified).(map[string]interface{})
			next[field] = nil
			transition(t, verified, next, false)
		})
	}
	t.Run("provisioning rejects removal", func(t *testing.T) { transition(t, verified, nil, false) })
	t.Run("provisioning rejects template verification reset", func(t *testing.T) { transition(t, verified, provisioning, false) })
	t.Run("provisioning rejects verified project addition", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(projectAdded).(map[string]interface{})
		next["projectVerified"] = true
		transition(t, verified, next, false)
	})
	t.Run("provisioning quantity semantic no-op", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(verified).(map[string]interface{})
		next["diskSize"] = "40960Mi"
		transition(t, verified, next, true)
	})
	t.Run("provisioning resource quantities semantic no-op", func(t *testing.T) {
		next := runtime.DeepCopyJSONValue(verified).(map[string]interface{})
		next["resources"] = map[string]interface{}{"cpu": "2000m", "memory": "4096Mi"}
		transition(t, verified, next, true)
	})
}
