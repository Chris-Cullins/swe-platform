package main

import "testing"

func TestDefaultClaudeCodeAdapterIsRegistered(t *testing.T) {
	if registeredAdapters()["claude-code"] == nil {
		t.Fatal("claude-code adapter is not registered")
	}
}

func TestPiAdapterIsRegistered(t *testing.T) {
	if registeredAdapters()["pi"] == nil {
		t.Fatal("pi adapter is not registered")
	}
}
