package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/spf13/cobra"
)

func newFollowUpCommand() *cobra.Command {
	group := &cobra.Command{Use: "follow-up", Short: "Inspect retained-workspace eligibility (execution is not implemented)"}
	var baseURL, token, uid string
	var asJSON bool
	cmd := &cobra.Command{
		Use: "inspect SOURCE", Short: "Read exact retained-workspace eligibility without claiming or waking", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			namespace, _ := cmd.Flags().GetString("namespace")
			c, err := controlplaneclient.New(baseURL, token, nil)
			if err != nil {
				return err
			}
			result, err := c.InspectFollowUp(ctx, namespace, args[0], uid)
			if err != nil {
				return fmt.Errorf("inspect follow-up: %w", err)
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Source: %s/%s (%s)\nEligibility: %s\nEnvironment: %s (%s)\nWorkspace incarnation: %s\nAdvisory only: no reservation, wake or follow-up execution.\n",
				safeText(result.Namespace), safeText(result.SourceName), safeText(result.SourceUID), safeText(result.Reason), safeText(result.EnvironmentName), safeText(result.EnvironmentUID), safeText(result.WorkspaceIncarnation))
			return err
		},
	}
	cmd.Flags().StringVar(&baseURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN)")
	cmd.Flags().StringVar(&uid, "run-uid", "", "Exact source Run UID (required)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Print bounded identity and eligibility fields, never task or credential content")
	_ = cmd.MarkFlagRequired("run-uid")
	group.AddCommand(cmd)
	return group
}
