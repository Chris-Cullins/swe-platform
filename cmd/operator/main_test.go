package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"
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
