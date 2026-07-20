package main

import "testing"

func TestDefaultClaudeCodeAdapterIsRegistered(t *testing.T) {
	if registeredAdapters()["claude-code"] == nil {
		t.Fatal("claude-code adapter is not registered")
	}
	if registeredAdapters()["amp"] == nil {
		t.Fatal("amp adapter is not registered")
	}
}
