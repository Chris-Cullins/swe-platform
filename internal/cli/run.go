package cli

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func newRunCommand() *cobra.Command {
	var (
		template string
		project  string
		agent    string
		wait     bool
		timeout  time.Duration
	)

	cmd := &cobra.Command{
		Use:   "run [prompt]",
		Short: "Create an environment and start an agent run in it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			return runEnvironment(cmd.Context(), namespace, template, project, agent, args[0], wait, timeout)
		},
	}

	cmd.Flags().StringVarP(&template, "template", "t", "", "EnvironmentTemplate to use (defaults to the project's template)")
	cmd.Flags().StringVarP(&project, "project", "p", "", "Project providing the default template and environment configuration")
	cmd.Flags().StringVar(&agent, "agent", "claude-code", "Agent adapter to run")
	cmd.Flags().BoolVar(&wait, "wait", true, "Wait for the environment to become Ready")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "How long to wait for readiness")
	return cmd
}

func runEnvironment(ctx context.Context, namespace, template, project, agent, prompt string, wait bool, timeout time.Duration) error {
	clients, err := newKubeClients()
	if err != nil {
		return err
	}
	if project != "" {
		var configuredProject platformv1alpha1.Project
		if err := clients.Get(ctx, types.NamespacedName{Namespace: namespace, Name: project}, &configuredProject); err != nil {
			return fmt.Errorf("get project %q: %w", project, err)
		}
		if template == "" {
			template = configuredProject.Spec.TemplateRef
		}
	}
	if template == "" {
		return fmt.Errorf("an environment template is required: set --template or use a project with spec.templateRef")
	}

	envName := "env-" + randSuffix(6)
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: envName},
		Spec: platformv1alpha1.EnvironmentSpec{
			ProjectRef:  project,
			TemplateRef: template,
		},
	}
	if err := clients.Create(ctx, env); err != nil {
		return fmt.Errorf("create environment: %w", err)
	}
	fmt.Printf("environment %s created (template %s)\n", envName, template)

	run := &platformv1alpha1.Run{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "run-" + randSuffix(6)},
		Spec: platformv1alpha1.RunSpec{
			EnvironmentRef: envName,
			Agent:          agent,
			Prompt:         prompt,
		},
	}
	if err := clients.Create(ctx, run); err != nil {
		return fmt.Errorf("create run: %w", err)
	}
	fmt.Printf("run %s created (agent %s)\n", run.Name, agent)

	if !wait {
		return nil
	}
	return waitForReady(ctx, clients, namespace, envName, timeout)
}

// waitForReady polls the Environment until it reports Ready or the timeout expires.
// TODO(P1): replace polling with a watch once the control plane exists.
func waitForReady(ctx context.Context, clients *kubeClients, namespace, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	lastPhase := platformv1alpha1.EnvironmentPhase("")
	fmt.Println("waiting for environment to become ready...")

	for time.Now().Before(deadline) {
		var env platformv1alpha1.Environment
		if err := clients.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &env); err != nil {
			return fmt.Errorf("get environment: %w", err)
		}
		if env.Status.Phase != lastPhase {
			fmt.Printf("  phase: %s\n", env.Status.Phase)
			lastPhase = env.Status.Phase
		}
		switch env.Status.Phase {
		case platformv1alpha1.EnvironmentPhaseReady, platformv1alpha1.EnvironmentPhaseRunning:
			fmt.Printf("environment %s is ready — attach with: swe attach %s\n", name, name)
			return nil
		case platformv1alpha1.EnvironmentPhaseFailed:
			return fmt.Errorf("environment failed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("timed out after %s waiting for readiness", timeout)
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
