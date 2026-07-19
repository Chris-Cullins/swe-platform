package main

import "testing"

func TestDefaultClaudeCodeAdapterIsRegistered(t *testing.T) {
	if registeredAdapters()["claude-code"] == nil {
		t.Fatal("claude-code adapter is not registered")
	}
}
