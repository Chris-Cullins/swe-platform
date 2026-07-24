package cli

import (
	"context"
	"crypto/rand"
	"fmt"
	"reflect"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func newRunCommand() *cobra.Command {
	var (
		template          string
		project           string
		environment       string
		agent             string
		name              string
		credentialProfile string
		wait              bool
		timeout           time.Duration
	)

	cmd := &cobra.Command{
		Use:   "run [prompt]",
		Short: "Create an environment and start an agent run in it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			return runEnvironmentWithCredential(cmd.Context(), namespace, name, template, project, environment, agent, args[0], credentialProfile, wait, timeout)
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Stable Run name/idempotency key (reuse to retry an uncertain create)")
	cmd.Flags().StringVarP(&template, "template", "t", "", "EnvironmentTemplate to use (defaults to the project's template)")
	cmd.Flags().StringVarP(&project, "project", "p", "", "Project providing the default template and environment configuration")
	cmd.Flags().StringVarP(&environment, "environment", "e", "", "Existing unclaimed Environment to reuse (exclusive with --template/--project)")
	cmd.Flags().StringVar(&agent, "agent", "claude-code", "Agent adapter to run")
	cmd.Flags().StringVar(&credentialProfile, "credential-profile", "", "AgentCredentialProfile to use")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for the adapter to start or the Run to finish")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "How long to wait for the Run")
	return cmd
}

func runEnvironment(ctx context.Context, namespace, name, template, project, environment, agent, prompt string, wait bool, timeout time.Duration) error {
	return runEnvironmentWithCredential(ctx, namespace, name, template, project, environment, agent, prompt, "", wait, timeout)
}

func runEnvironmentWithCredential(ctx context.Context, namespace, name, template, project, environment, agent, prompt, credentialProfile string, wait bool, timeout time.Duration) error {
	clients, err := newKubeClients()
	if err != nil {
		return err
	}
	return createRunWithCredential(ctx, clients, namespace, name, template, project, environment, agent, prompt, credentialProfile, wait, timeout)
}

func createRun(ctx context.Context, clients *kubeClients, namespace, name, template, project, environment, agent, prompt string, wait bool, timeout time.Duration) error {
	return createRunWithCredential(ctx, clients, namespace, name, template, project, environment, agent, prompt, "", wait, timeout)
}

func createRunWithCredential(ctx context.Context, clients *kubeClients, namespace, name, template, project, environment, agent, prompt, credentialProfile string, wait bool, timeout time.Duration) error {
	if credentialProfile != "" && len(validation.IsDNS1123Subdomain(credentialProfile)) != 0 {
		return fmt.Errorf("--credential-profile must be a Kubernetes DNS subdomain")
	}
	if environment == "" && template == "" && project == "" {
		return fmt.Errorf("an environment, template, or project is required")
	}
	if environment != "" && (template != "" || project != "") {
		return fmt.Errorf("--environment cannot be combined with --template or --project")
	}
	if name == "" {
		name = "run-" + randSuffix(10)
	}

	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: platformv1alpha1.RunSpec{
			EnvironmentRef:       environment,
			ProjectRef:           project,
			TemplateRef:          template,
			Agent:                agent,
			Prompt:               prompt,
			CredentialProfileRef: credentialProfile,
		},
	}
	if err := clients.Create(ctx, run); err != nil {
		// A timeout may mean the API server persisted the object but its response
		// was lost. Read the same name and accept only the same immutable intent.
		var existing platformv1alpha1.Run
		getErr := clients.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &existing)
		if getErr != nil {
			return fmt.Errorf("create run %q: %w (retry with --name %s)", name, err, name)
		}
		if !sameRunIntent(existing.Spec, run.Spec) {
			return fmt.Errorf("run %q already exists with different task intent", name)
		}
		run = &existing
		fmt.Printf("run %s already exists with the same intent; reusing it\n", run.Name)
	} else {
		fmt.Printf("run %s created (agent %s)\n", run.Name, agent)
	}

	if !wait {
		return nil
	}
	return waitForRun(ctx, clients, namespace, name, timeout)
}

func sameRunIntent(a, b platformv1alpha1.RunSpec) bool {
	a.Cancel, b.Cancel = false, false
	return reflect.DeepEqual(a, b)
}

// waitForRun polls until the adapter accepts and starts work or the Run ends.
// This direct-Kubernetes compatibility path intentionally retains polling; the
// authenticated control-plane clients use the typed Run resource watch.
func waitForRun(ctx context.Context, clients *kubeClients, namespace, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	lastState := platformv1alpha1.RunState("")
	fmt.Println("waiting for the run to start...")

	for time.Now().Before(deadline) {
		var run platformv1alpha1.Run
		if err := clients.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
			return fmt.Errorf("get run: %w", err)
		}
		if run.Status.State != lastState {
			fmt.Printf("  state: %s\n", run.Status.State)
			lastState = run.Status.State
		}
		switch run.Status.State {
		case platformv1alpha1.RunStateRunning, platformv1alpha1.RunStateNeedsInput, platformv1alpha1.RunStateSucceeded:
			if run.Status.EnvironmentRef != nil {
				fmt.Printf("run %s is %s — attach with: swe attach %s\n", name, run.Status.State, run.Status.EnvironmentRef.Name)
			}
			return nil
		case platformv1alpha1.RunStateFailed:
			return fmt.Errorf("run failed")
		case platformv1alpha1.RunStateCancelled:
			return fmt.Errorf("run was cancelled")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("timed out after %s waiting for run %s", timeout, name)
}

func newCancelCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel RUN",
		Short: "Request idempotent cancellation of a run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			clients, err := newKubeClients()
			if err != nil {
				return err
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			var run platformv1alpha1.Run
			if err := clients.Get(cmd.Context(), types.NamespacedName{Namespace: namespace, Name: args[0]}, &run); err != nil {
				if apierrors.IsNotFound(err) {
					return fmt.Errorf("run %q not found", args[0])
				}
				return err
			}
			if run.Spec.Cancel {
				fmt.Printf("run %s cancellation already requested\n", run.Name)
				return nil
			}
			run.Spec.Cancel = true
			if err := clients.Update(cmd.Context(), &run); err != nil {
				return fmt.Errorf("cancel run: %w", err)
			}
			fmt.Printf("run %s cancellation requested\n", run.Name)
			return nil
		},
	}
}

// randSuffix returns n random lowercase alphanumeric characters.
func randSuffix(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	for i := range buf {
		buf[i] = alphabet[int(buf[i])%len(alphabet)]
	}
	return string(buf)
}
