package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/spf13/cobra"
)

func newDescribeRunCommand() *cobra.Command {
	var baseURL, token, uid string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "describe-run RUN",
		Short: "Inspect an exact Run and its safe diagnostic without changing it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(args[0]) == "" {
				return fmt.Errorf("Run name is required")
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			client, err := controlplaneclient.New(baseURL, token, nil)
			if err != nil {
				return err
			}
			run, err := client.GetRunExact(cmd.Context(), namespace, args[0], uid)
			if err != nil {
				return fmt.Errorf("describe run: %w", err)
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(run)
			}
			var output strings.Builder
			fmt.Fprintf(&output, "Namespace: %s\nRun: %s\nUID: %s\nState: %s\nAgent: %s\n", safeText(namespace), safeText(run.Name), safeText(run.UID), safeText(run.State), safeText(run.Intent.Agent))
			if !run.CreatedAt.IsZero() {
				fmt.Fprintf(&output, "Created: %s\n", run.CreatedAt.UTC().Format(time.RFC3339))
			}
			if run.StartedAt != nil && !run.StartedAt.IsZero() {
				fmt.Fprintf(&output, "Started: %s\n", run.StartedAt.UTC().Format(time.RFC3339))
			}
			if run.FinishedAt != nil && !run.FinishedAt.IsZero() {
				fmt.Fprintf(&output, "Finished: %s\n", run.FinishedAt.UTC().Format(time.RFC3339))
			}
			if run.Diagnostic == nil {
				fmt.Fprintln(&output, "Diagnostic: unknown (no current recognized diagnostic).")
			} else {
				fmt.Fprintf(&output, "Diagnostic: %s\n%s\nNext action: %s\n", safeText(run.Diagnostic.Code), safeText(run.Diagnostic.Message), safeText(run.Diagnostic.NextAction))
			}
			fmt.Fprintln(&output, "Agent outcome is not verification evidence.")
			_, err = fmt.Fprint(cmd.OutOrStdout(), output.String())
			return err
		},
	}
	cmd.Flags().StringVar(&baseURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN)")
	cmd.Flags().StringVar(&uid, "run-uid", "", "Immutable Run UID (required; never follows replacements)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Print the existing typed Run, including its authorized task prompt and other detail")
	_ = cmd.MarkFlagRequired("run-uid")
	return cmd
}
