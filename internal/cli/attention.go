package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/spf13/cobra"
)

// attentionRun deliberately does not embed the prompt-bearing RunSummary.
type attentionRun struct {
	Namespace       string    `json:"namespace"`
	Name            string    `json:"name"`
	UID             string    `json:"uid"`
	ReportedState   string    `json:"reportedState"`
	Agent           string    `json:"agent"`
	CreatedAt       time.Time `json:"createdAt"`
	CancelRequested bool      `json:"cancelRequested"`
	Bucket          string    `json:"bucket"`
}

func attentionBucket(state string, cancelRequested bool) string {
	switch state {
	case "Cancelled":
		return "cancelled"
	case "Failed":
		return "failed"
	case "Succeeded":
		return "review-candidate"
	case "Allocating", "EnvironmentReady", "AdapterAccepted", "Running", "NeedsInput", "Paused":
		if cancelRequested {
			return "cancelling"
		}
		if state == "NeedsInput" {
			return "input-reported"
		}
		if state == "Paused" {
			return "paused"
		}
		return "in-progress"
	default:
		return "unknown"
	}
}

func newAttentionCommand() *cobra.Command {
	var baseURL, token, bucket, agent string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "attention",
		Short: "List prompt-free attention candidates from reported Run states",
		Long: `List reported-state candidates, not proof of current controller status, verified
success, available evidence, unreviewed work, failure cause, input support or wake permission.
Defaults include failed, input-reported, review-candidate, paused and unknown.
Paused work may be intentional; all retained successes remain review candidates.
Use swe describe-run RUN --namespace NS --run-uid UID for an explicit exact explanation.
Reads only summary pages, with a command-local 30-second deadline; never wakes a Run.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("bucket") {
				switch bucket {
				case "unknown", "cancelled", "failed", "review-candidate", "cancelling", "input-reported", "paused", "in-progress":
				default:
					return fmt.Errorf("invalid --bucket: use unknown, cancelled, failed, review-candidate, cancelling, input-reported, paused, or in-progress")
				}
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			client, err := controlplaneclient.New(baseURL, token, nil)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			runs, err := client.ListRunSummaries(ctx, namespace)
			if err != nil {
				return fmt.Errorf("attention: %w", err)
			}
			items := make([]attentionRun, 0, len(runs))
			for _, run := range runs {
				classified := attentionBucket(run.State, run.CancelRequested)
				if bucket != "" && classified != bucket || agent != "" && run.Agent != agent {
					continue
				}
				if bucket == "" && (classified == "cancelled" || classified == "cancelling" || classified == "in-progress") {
					continue
				}
				items = append(items, attentionRun{Namespace: namespace, Name: run.Name, UID: run.UID,
					ReportedState: run.State, Agent: run.Agent, CreatedAt: run.CreatedAt,
					CancelRequested: run.CancelRequested, Bucket: classified})
			}
			sort.Slice(items, func(i, j int) bool {
				if items[i].Name == items[j].Name {
					return items[i].UID < items[j].UID
				}
				return items[i].Name < items[j].Name
			})
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("attention: %w", err)
			}
			if asJSON {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(items)
			}
			if len(items) == 0 {
				message := "No attention candidates found."
				if bucket != "" || agent != "" {
					message = "No Runs match the attention filters."
				}
				_, err := fmt.Fprintln(cmd.OutOrStdout(), message)
				return err
			}
			writer := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(writer, "NAMESPACE\tNAME\tUID\tREPORTED STATE\tAGENT\tCREATED\tCANCEL REQUESTED\tBUCKET"); err != nil {
				return err
			}
			for _, run := range items {
				if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\t%t\t%s\n", safeText(run.Namespace), safeText(run.Name), safeText(run.UID), safeText(run.ReportedState), safeText(run.Agent), run.CreatedAt.UTC().Format(time.RFC3339), run.CancelRequested, run.Bucket); err != nil {
					return err
				}
			}
			return writer.Flush()
		},
	}
	cmd.Flags().StringVar(&baseURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN)")
	cmd.Flags().StringVar(&bucket, "bucket", "", "Select one exact bucket instead of the default candidates")
	cmd.Flags().StringVar(&agent, "agent", "", "Exact agent name (case-sensitive)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Print only the derived prompt-free attention fields")
	return cmd
}
