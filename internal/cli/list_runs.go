package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/spf13/cobra"
)

func newListRunsCommand() *cobra.Command {
	var baseURL, token, state, agent string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "list-runs",
		Short: "List Run summaries in one namespace, optionally filtered by state and agent",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			switch platformv1alpha1.RunState(state) {
			case "", platformv1alpha1.RunStateAllocating, platformv1alpha1.RunStateEnvironmentReady,
				platformv1alpha1.RunStateAdapterAccepted, platformv1alpha1.RunStateRunning,
				platformv1alpha1.RunStateNeedsInput, platformv1alpha1.RunStatePaused,
				platformv1alpha1.RunStateSucceeded, platformv1alpha1.RunStateFailed, platformv1alpha1.RunStateCancelled:
			default:
				return fmt.Errorf("invalid --state: use Allocating, EnvironmentReady, AdapterAccepted, Running, NeedsInput, Paused, Succeeded, Failed, or Cancelled")
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			client, err := controlplaneclient.New(baseURL, token, nil)
			if err != nil {
				return err
			}
			runs, err := client.ListRunSummaries(cmd.Context(), namespace)
			if err != nil {
				return fmt.Errorf("list runs: %w", err)
			}
			items := make([]controlplane.RunSummary, 0, len(runs))
			for _, run := range runs {
				if (state == "" || run.State == state) && (agent == "" || run.Agent == agent) {
					items = append(items, run)
				}
			}
			sort.Slice(items, func(i, j int) bool {
				if items[i].Name == items[j].Name {
					return items[i].UID < items[j].UID
				}
				return items[i].Name < items[j].Name
			})
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(items)
			}
			if len(items) == 0 {
				message := "No Runs found."
				if state != "" || agent != "" {
					message = "No Runs match the filters."
				}
				_, err := fmt.Fprintln(cmd.OutOrStdout(), message)
				return err
			}
			writer := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(writer, "NAME\tUID\tSTATE\tAGENT\tCREATED"); err != nil {
				return err
			}
			for _, run := range items {
				if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\n", safeText(run.Name), safeText(run.UID), safeText(run.State), safeText(run.Agent), run.CreatedAt.UTC().Format(time.RFC3339)); err != nil {
					return err
				}
			}
			return writer.Flush()
		},
	}
	cmd.Flags().StringVar(&baseURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN)")
	cmd.Flags().StringVar(&state, "state", "", "Exact lifecycle state (case-sensitive)")
	cmd.Flags().StringVar(&agent, "agent", "", "Exact agent name (case-sensitive)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Print the filtered Run summary array, including bounded prompt previews")
	return cmd
}
