package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

func TestDefaultClaudeCodeAdapterIsRegistered(t *testing.T) {
	if registeredAdapters()["claude-code"] == nil {
		t.Fatal("claude-code adapter is not registered")
	}
	if registeredAdapters()["amp"] == nil {
		t.Fatal("amp adapter is not registered")
	}
	if registeredAdapters()["codex"] == nil {
		t.Fatal("codex adapter is not registered")
	}
	if registeredAdapters()["pi"] == nil {
		t.Fatal("pi adapter is not registered")
	}
}

func TestInstallationLeaderElectionIDIsReleaseIsolated(t *testing.T) {
	first := installationLeaderElectionID(types.UID("installation-1"))
	if first == installationLeaderElectionID(types.UID("installation-2")) {
		t.Fatal("distinct Installation UIDs share a leader-election lease")
	}
	if first != installationLeaderElectionID(types.UID("installation-1")) || len(first) > 63 {
		t.Fatalf("leader-election ID is unstable or invalid: %q", first)
	}
}

func TestManagerCacheScopesProjectAndSystemObjects(t *testing.T) {
	options := operatorCacheOptions(tenancy.ModeScoped, []string{"project-b", "project-a"}, "system")
	for _, namespace := range []string{"project-a", "project-b"} {
		if _, ok := options.DefaultNamespaces[namespace]; !ok {
			t.Errorf("scoped manager cache omitted %q", namespace)
		}
	}
	if len(options.DefaultNamespaces) != 2 {
		t.Fatalf("scoped manager cache namespaces = %#v", options.DefaultNamespaces)
	}
	if got := operatorCacheOptions(tenancy.ModeScoped, nil, "system"); len(got.DefaultNamespaces) != 1 {
		t.Fatalf("empty scoped manager cache namespaces = %#v", got.DefaultNamespaces)
	}
	if got := operatorCacheOptions(tenancy.ModeTrustedAdmin, nil, "system"); got.DefaultNamespaces != nil {
		t.Fatalf("trusted-admin unexpectedly restricted cache namespaces = %#v", got.DefaultNamespaces)
	}
	for _, mode := range []tenancy.Mode{tenancy.ModeScoped, tenancy.ModeTrustedAdmin} {
		options := operatorCacheOptions(mode, []string{"project"}, "system")
		seen := make(map[string]bool, 2)
		for object, config := range options.ByObject {
			kind := ""
			switch object.(type) {
			case *corev1.ConfigMap:
				kind = "ConfigMap"
			case *platformv1alpha1.Installation:
				kind = "Installation"
			default:
				t.Fatalf("unexpected cache object restriction %T", object)
			}
			if len(config.Namespaces) != 1 {
				t.Errorf("%s %s cache namespaces = %#v", mode, kind, config.Namespaces)
			} else if _, ok := config.Namespaces["system"]; !ok {
				t.Errorf("%s %s cache omitted system namespace: %#v", mode, kind, config.Namespaces)
			}
			seen[kind] = true
		}
		if !seen["ConfigMap"] || !seen["Installation"] {
			t.Errorf("%s system cache restrictions = %#v", mode, seen)
		}
	}
}
