package sandboxclient

import (
	"encoding/json"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

func TestExactPortalCapability(t *testing.T) {
	token := "portal-secret"
	config := func(grants ...auth.Grant) []byte {
		contents, err := json.Marshal(auth.Config{Grants: grants})
		if err != nil {
			t.Fatal(err)
		}
		return contents
	}
	portal := auth.Grant{TokenHash: auth.TokenVerifier(token), Capabilities: []auth.Capability{auth.CapabilityPortal}}
	for _, tc := range []struct {
		name string
		data []byte
		want bool
	}{
		{"exact", config(portal), true},
		{"extra capability", config(auth.Grant{TokenHash: auth.TokenVerifier(token), Capabilities: []auth.Capability{auth.CapabilityPortal, auth.CapabilityHealth}}), false},
		{"other token portal", config(auth.Grant{TokenHash: auth.TokenVerifier("other"), Capabilities: []auth.Capability{auth.CapabilityPortal}}), false},
		{"duplicate grant", config(portal, portal), false},
		{"malformed", []byte("{"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := exactPortalCapability(tc.data, token); got != tc.want {
				t.Fatalf("exactPortalCapability = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExactServiceDeclaration(t *testing.T) {
	d := platformv1alpha1.EnvironmentServiceDeclaration{Name: "web", InstanceID: "abcdefghijklmnopqrst", Revision: 1, TargetPort: 8080}
	for _, tc := range []struct {
		name    string
		items   []platformv1alpha1.EnvironmentServiceDeclaration
		wantErr bool
	}{
		{"exact", []platformv1alpha1.EnvironmentServiceDeclaration{d}, false},
		{"missing", nil, true},
		{"duplicate", []platformv1alpha1.EnvironmentServiceDeclaration{d, d}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exactServiceDeclaration(ServiceDeclarationSnapshot{declarations: tc.items}, "web")
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
