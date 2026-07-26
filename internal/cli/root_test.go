package cli

import (
	"io"
	"strings"
	"testing"
)

func TestExecutableCommandRequiresExplicitNamespace(t *testing.T) {
	command := NewRootCommand()
	command.SetArgs([]string{"attach", "env", "--environment-uid", "uid", "--control-plane", "https://example.test", "--token", "token"})
	command.SetOut(io.Discard)
	command.SetErr(io.Discard)
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "--namespace is required") {
		t.Fatalf("error = %v, want explicit namespace requirement", err)
	}
}

func TestGroupingCommandWorksWithoutNamespace(t *testing.T) {
	command := NewRootCommand()
	command.SetArgs([]string{"project"})
	command.SetOut(io.Discard)
	command.SetErr(io.Discard)
	if err := command.Execute(); err != nil {
		t.Fatalf("project grouping command: %v", err)
	}
}
