package isolation

import (
	"crypto/sha256"
	"strings"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func restrictedSelection() platformv1alpha1.InstallationIsolationSpec {
	return platformv1alpha1.InstallationIsolationSpec{
		Mode:                platformv1alpha1.InstallationIsolationModeRestrictedProductionCalicoV3_32_1,
		PolicyConfigMapName: "egress-policy",
		RuntimeClass:        &platformv1alpha1.InstallationRuntimeClassExpectation{Name: "gvisor", Handler: "runsc"},
		StorageClass:        &platformv1alpha1.InstallationStorageClassExpectation{Name: "workspace", CSIDriver: "csi.example.com"},
	}
}

func restrictedInputs() RevisionInputs {
	return RevisionInputs{
		InstallationUID: "installation-uid",
		Selection:       restrictedSelection(),
		PolicyConfigMap: &PolicyConfigMapIdentity{UID: "policy-uid", ContentSHA256: sha256.Sum256([]byte("canonical policy"))},
		RuntimeClass:    &RuntimeClassIdentity{UID: "runtime-uid", Handler: "runsc"},
		StorageClass:    &StorageClassIdentity{UID: "storage-uid", CSIDriver: "csi.example.com"},
	}
}

func TestValidateSelectionCrossFieldContract(t *testing.T) {
	development := platformv1alpha1.InstallationIsolationSpec{Mode: platformv1alpha1.InstallationIsolationModeUnrestrictedDevelopment}
	if err := ValidateSelection(nil); err != nil {
		t.Fatalf("legacy omission: %v", err)
	}
	if err := ValidateSelection(&development); err != nil {
		t.Fatalf("development selection: %v", err)
	}
	if err := ValidateSelection(ptr(restrictedSelection())); err != nil {
		t.Fatalf("restricted selection: %v", err)
	}

	tests := map[string]func(*platformv1alpha1.InstallationIsolationSpec){
		"development restricted input": func(s *platformv1alpha1.InstallationIsolationSpec) {
			s.RuntimeClass = restrictedSelection().RuntimeClass
		},
		"restricted missing policy": func(s *platformv1alpha1.InstallationIsolationSpec) { s.PolicyConfigMapName = "" },
		"invalid ConfigMap name":    func(s *platformv1alpha1.InstallationIsolationSpec) { s.PolicyConfigMapName = "Bad_Name" },
		"oversized RuntimeClass name": func(s *platformv1alpha1.InstallationIsolationSpec) {
			s.RuntimeClass.Name = strings.Repeat("a", 254)
		},
		"invalid handler":           func(s *platformv1alpha1.InstallationIsolationSpec) { s.RuntimeClass.Handler = "runsc/unsafe" },
		"invalid StorageClass name": func(s *platformv1alpha1.InstallationIsolationSpec) { s.StorageClass.Name = "workspace_1" },
		"oversized CSI driver": func(s *platformv1alpha1.InstallationIsolationSpec) {
			s.StorageClass.CSIDriver = strings.Repeat("a", 64)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			selection := restrictedSelection()
			if strings.HasPrefix(name, "development") {
				selection = development
			}
			mutate(&selection)
			if err := ValidateSelection(&selection); err == nil {
				t.Fatal("invalid selection accepted")
			}
		})
	}
}

func TestRevisionCanonicalStabilityAndDomain(t *testing.T) {
	inputs := restrictedInputs()
	canonical, err := inputs.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"domain":"swe.dev/isolation/installation-revision/v1","egressPolicyAPIVersion":"swe.dev/egress-policy/v1","egressRuntimeRevisionDomain":"swe.dev/egress-policy/runtime-revision/v1","installationUID":"installation-uid","selection":{"mode":"RestrictedProductionCalicoV3_32_1","policyConfigMapName":"egress-policy","runtimeClassName":"gvisor","runtimeClassHandler":"runsc","storageClassName":"workspace","storageClassDriver":"csi.example.com"},"policyConfigMap":{"apiVersion":"v1","kind":"ConfigMap","name":"egress-policy","uid":"policy-uid","sha256":"6c6f75f2824d3a495045e88a743c5d94fc218fc6b9439acb73174bc316afc5ec"},"runtimeClass":{"apiVersion":"node.k8s.io/v1","kind":"RuntimeClass","name":"gvisor","uid":"runtime-uid","value":"runsc"},"storageClass":{"apiVersion":"storage.k8s.io/v1","kind":"StorageClass","name":"workspace","uid":"storage-uid","value":"csi.example.com"}}`
	if string(canonical) != want {
		t.Fatalf("canonical bytes changed:\n got %s\nwant %s", canonical, want)
	}
	revision, err := inputs.DeriveRevision()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := revision.String(), "ddaaa8cc5533c87f328a9cfc7e273d11f7c00cabab74467fe8df17c6dd3b6b7b"; got != want {
		t.Fatalf("revision = %s, want %s", got, want)
	}
	withoutDomain := strings.Replace(string(canonical), `"domain":"`+RevisionDomain+`",`, "", 1)
	if revision == Revision(sha256.Sum256([]byte(withoutDomain))) {
		t.Fatal("revision is not domain separated")
	}
}

func TestRevisionBindsEveryAuthorityInput(t *testing.T) {
	base := restrictedInputs()
	want, err := base.DeriveRevision()
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*RevisionInputs){
		"Installation UID":  func(in *RevisionInputs) { in.InstallationUID = "other-installation" },
		"policy UID":        func(in *RevisionInputs) { in.PolicyConfigMap.UID = "other-policy" },
		"policy content":    func(in *RevisionInputs) { in.PolicyConfigMap.ContentSHA256 = sha256.Sum256([]byte("other policy")) },
		"RuntimeClass UID":  func(in *RevisionInputs) { in.RuntimeClass.UID = "other-runtime" },
		"StorageClass UID":  func(in *RevisionInputs) { in.StorageClass.UID = "other-storage" },
		"RuntimeClass name": func(in *RevisionInputs) { in.Selection.RuntimeClass.Name = "gvisor-alt" },
		"StorageClass name": func(in *RevisionInputs) { in.Selection.StorageClass.Name = "workspace-alt" },
		"policy ConfigMap":  func(in *RevisionInputs) { in.Selection.PolicyConfigMapName = "egress-policy-alt" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := restrictedInputs()
			mutate(&changed)
			got, err := changed.DeriveRevision()
			if err != nil {
				t.Fatal(err)
			}
			if got == want {
				t.Fatal("revision did not change")
			}
		})
	}
}

func TestRevisionRejectsAmbiguousOrMismatchedInputs(t *testing.T) {
	first := restrictedInputs()
	first.Selection.RuntimeClass.Name = "a"
	first.Selection.RuntimeClass.Handler = "b-c"
	first.RuntimeClass.Handler = "b-c"
	second := restrictedInputs()
	second.Selection.RuntimeClass.Name = "a-b"
	second.Selection.RuntimeClass.Handler = "c"
	second.RuntimeClass.Handler = "c"
	firstRevision, err := first.DeriveRevision()
	if err != nil {
		t.Fatal(err)
	}
	secondRevision, err := second.DeriveRevision()
	if err != nil {
		t.Fatal(err)
	}
	if firstRevision == secondRevision {
		t.Fatal("field-boundary ambiguity produced the same revision")
	}

	for name, mutate := range map[string]func(*RevisionInputs){
		"missing Installation UID": func(in *RevisionInputs) { in.InstallationUID = "" },
		"missing policy identity":  func(in *RevisionInputs) { in.PolicyConfigMap = nil },
		"zero policy hash":         func(in *RevisionInputs) { in.PolicyConfigMap.ContentSHA256 = [sha256.Size]byte{} },
		"handler mismatch":         func(in *RevisionInputs) { in.RuntimeClass.Handler = "other" },
		"CSI driver mismatch":      func(in *RevisionInputs) { in.StorageClass.CSIDriver = "other.example.com" },
	} {
		t.Run(name, func(t *testing.T) {
			inputs := restrictedInputs()
			mutate(&inputs)
			if _, err := inputs.DeriveRevision(); err == nil {
				t.Fatal("invalid revision inputs accepted")
			}
		})
	}
}

func ptr[T any](value T) *T { return &value }
