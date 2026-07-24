package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func newEnvironmentCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "environment", Short: "Manage environment lifecycle policy"}
	cmd.AddCommand(newEnvironmentHoldCommand(true), newEnvironmentHoldCommand(false))
	return cmd
}

func newEnvironmentHoldCommand(enabled bool) *cobra.Command {
	use := "hold ENVIRONMENT"
	short := "Explicitly hold an environment"
	verb := "held"
	if !enabled {
		use = "release ENVIRONMENT"
		short = "Release an explicit environment hold"
		verb = "released"
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			clients, err := newKubeClients()
			if err != nil {
				return err
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			revision, err := setEnvironmentHold(cmd.Context(), clients.Client, types.NamespacedName{Namespace: namespace, Name: args[0]}, enabled)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "environment %s %s at hold-policy revision %d\n", args[0], verb, revision)
			return nil
		},
	}
}

func setEnvironmentHold(ctx context.Context, kube client.Client, key types.NamespacedName, enabled bool) (int64, error) {
	revision := int64(0)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var environment platformv1alpha1.Environment
		if err := kube.Get(ctx, key, &environment); err != nil {
			return err
		}
		if environment.Spec.Paused {
			return fmt.Errorf("environment has a legacy pause awaiting controller migration")
		}
		before := environment.DeepCopy()
		if environment.Spec.Lifecycle.Hold == nil {
			if enabled {
				environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: enabled, Revision: 1}
			}
		} else if environment.Spec.Lifecycle.Hold.Enabled != enabled {
			environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: enabled, Revision: environment.Spec.Lifecycle.Hold.Revision + 1}
		}
		if environment.Spec.Lifecycle.Hold != nil {
			revision = environment.Spec.Lifecycle.Hold.Revision
		}
		if holdPoliciesEqual(before.Spec.Lifecycle.Hold, environment.Spec.Lifecycle.Hold) {
			return nil
		}
		return kube.Patch(ctx, &environment, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		return 0, fmt.Errorf("set environment %q hold policy: %w", key.Name, err)
	}
	return revision, nil
}

func holdPoliciesEqual(a, b *platformv1alpha1.EnvironmentHoldPolicy) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
