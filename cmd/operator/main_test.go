package main

import "testing"

func TestAdaptersAreRegistered(t *testing.T) {
	for _, name := range []string{"amp", "claude-code"} {
		if registeredAdapters()[name] == nil {
			t.Fatalf("%s adapter is not registered", name)
		}
	}
}
