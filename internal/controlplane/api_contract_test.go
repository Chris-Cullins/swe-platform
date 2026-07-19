package controlplane

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestAPIContractFixtures(t *testing.T) {
	tests := []struct {
		path string
		into any
	}{
		{"testdata/contracts/session.json", &Session{}},
		{"testdata/contracts/create-run.json", &CreateRunRequest{}},
		{"testdata/contracts/run.json", &Run{}},
		{"testdata/contracts/run-list.json", &RunList{}},
		{"testdata/contracts/environment.json", &Environment{}},
		{"testdata/contracts/problem.json", &Problem{}},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			fixture, err := os.ReadFile(test.path)
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(bytes.NewReader(fixture))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(test.into); err != nil {
				t.Fatalf("fixture does not match contract: %v", err)
			}
		})
	}
}
