package agent

import (
	"errors"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func TestPrepareLaunchMaterialMergeCopyCleanupAndConflict(t *testing.T) {
	repositoryValue := []byte("repository-secret")
	apiValue := []byte("api-secret")
	input := &AdapterLaunchMaterial{
		AgentCredential:     &AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey, APIKey: apiValue},
		RepositorySecretEnv: map[string][]byte{"REPOSITORY_TOKEN": repositoryValue},
	}
	launch, cleanup, err := PrepareLaunchMaterial(input, "AGENT_API_KEY", true)
	if err != nil {
		t.Fatal(err)
	}
	if string(launch.SecretEnv["REPOSITORY_TOKEN"]) != string(repositoryValue) || string(launch.SecretEnv["AGENT_API_KEY"]) != string(apiValue) {
		t.Fatalf("merged launch material = %#v", launch.SecretEnv)
	}
	launch.SecretEnv["REPOSITORY_TOKEN"][0] = 'x'
	if string(repositoryValue) != "repository-secret" {
		t.Fatal("repository input was not defensively copied")
	}
	cleanup()
	for name, value := range launch.SecretEnv {
		for _, b := range value {
			if b != 0 {
				t.Fatalf("temporary %s value was not cleared", name)
			}
		}
	}

	_, _, err = PrepareLaunchMaterial(&AdapterLaunchMaterial{
		AgentCredential:     &AdapterCredential{Type: platformv1alpha1.AgentCredentialTypeAPIKey},
		RepositorySecretEnv: map[string][]byte{"AGENT_API_KEY": []byte("conflict")},
	}, "AGENT_API_KEY", true)
	if !errors.Is(err, ErrAdapterTaskRejected) {
		t.Fatalf("duplicate conflict = %v", err)
	}
}

func TestAdapterObservationStatusMessagesAreFixedPlatformVocabulary(t *testing.T) {
	tests := map[AdapterObservation]string{
		AdapterObservationAccepted:   "adapter accepted the task",
		AdapterObservationRunning:    "adapter is running",
		AdapterObservationNeedsInput: "adapter needs input",
		AdapterObservationSucceeded:  "adapter completed successfully",
		AdapterObservationFailed:     "adapter reported failure",
	}
	for observation, want := range tests {
		if got := observation.StatusMessage(); got != want {
			t.Errorf("%s status message = %q, want %q", observation, got, want)
		}
	}
	if got := AdapterObservation("provider-controlled").StatusMessage(); got != "" {
		t.Fatalf("unknown observation status message = %q, want empty", got)
	}
}
