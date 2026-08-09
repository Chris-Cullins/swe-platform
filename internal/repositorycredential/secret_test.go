package repositorycredential

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func TestActiveSecretRejectsPendingMetadataPresence(t *testing.T) {
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	uid := types.UID("run-uid")
	c := &Credential{Token: []byte("token"), Repository: "https://github.com/acme/repo", InstallationID: 7, ExpiresAt: now.Add(time.Hour)}
	for _, annotation := range []string{AnnotationRevocationState, AnnotationRevocationDisposition} {
		t.Run(annotation, func(t *testing.T) {
			s, err := NewSecret("ns", "run", uid, "github-app", c.Repository, c, 1, "", 0, now)
			if err != nil {
				t.Fatal(err)
			}
			s.Annotations[annotation] = ""
			if _, err := Parse(s, "run", uid); err == nil {
				t.Fatal("Parse accepted pending-only annotation presence")
			}
			if ExactManagedIdentity(s, "run", uid, "github-app", c.Repository, c.Repository) {
				t.Fatal("ExactManagedIdentity accepted pending-only annotation presence")
			}
		})
	}
}

func TestPendingRevocationStateAndDispositionAreStrict(t *testing.T) {
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	uid := types.UID("run-uid")
	c := &Credential{Token: []byte("token"), Repository: "https://github.com/acme/repo", InstallationID: 7, ExpiresAt: now.Add(time.Hour)}
	s, err := NewPendingRevocationSecret("ns", "run", uid, "github-app", c.Repository, c, 1, now, DispositionProviderInvalidToken)
	if err != nil {
		t.Fatal(err)
	}
	p, err := ParsePendingRevocation(s, "run", uid)
	if err != nil || p.State != RevocationStatePending || p.Disposition != DispositionProviderInvalidToken || p.Legacy {
		t.Fatalf("pending parse = %#v, %v", p, err)
	}
	for _, name := range []string{"missing state", "missing disposition", "unknown state", "unknown disposition"} {
		t.Run(name, func(t *testing.T) {
			copy := s.DeepCopy()
			// Apply the named mutation independently to avoid shared table state.
			switch name {
			case "missing state":
				delete(copy.Annotations, AnnotationRevocationState)
			case "missing disposition":
				delete(copy.Annotations, AnnotationRevocationDisposition)
			case "unknown state":
				copy.Annotations[AnnotationRevocationState] = "unknown"
			case "unknown disposition":
				copy.Annotations[AnnotationRevocationDisposition] = "unknown"
			}
			if _, err := ParsePendingRevocation(copy, "run", uid); err == nil {
				t.Fatal("malformed pending metadata accepted")
			}
		})
	}
	legacy := s.DeepCopy()
	delete(legacy.Annotations, AnnotationRevocationState)
	delete(legacy.Annotations, AnnotationRevocationDisposition)
	p, err = ParsePendingRevocation(legacy, "run", uid)
	if err != nil || !p.Legacy || p.State != RevocationStatePending || p.Disposition != "" {
		t.Fatalf("legacy parse = %#v, %v", p, err)
	}
	if _, err := NewPendingRevocationSecret("ns", "run", uid, "github-app", c.Repository, c, 1, now, "unknown"); err == nil {
		t.Fatal("constructor accepted unknown disposition")
	}
}

func TestSecretConstructorsEnforceActiveValidityBoundaryOnly(t *testing.T) {
	now := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	base := Credential{Token: []byte("token"), Repository: "https://github.com/acme/repo", InstallationID: 7}
	for _, tc := range []struct {
		name     string
		expires  time.Time
		activeOK bool
	}{
		{name: "short", expires: now.Add(time.Minute)},
		{name: "exact minimum", expires: now.Add(MinimumValidity)},
		{name: "above minimum", expires: now.Add(MinimumValidity + time.Nanosecond), activeOK: true},
		{name: "expired", expires: now.Add(-time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			credential := base
			credential.ExpiresAt = tc.expires
			active, activeErr := NewSecret("ns", "run", "uid", "github-app", credential.Repository, &credential, 1, "", 0, now)
			if (active != nil) != tc.activeOK || (activeErr == nil) != tc.activeOK {
				t.Fatalf("active = %#v, %v", active, activeErr)
			}
			pending, pendingErr := NewPendingRevocationSecret("ns", "run", "uid", "github-app", credential.Repository, &credential, 1, now, "")
			if pendingErr != nil || pending == nil {
				t.Fatalf("pending = %#v, %v", pending, pendingErr)
			}
		})
	}
}
