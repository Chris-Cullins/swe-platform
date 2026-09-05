package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/spf13/cobra"
)

func newChangesCommand() *cobra.Command {
	var baseURL, token, uid, path string
	var asJSON bool
	var offset int
	var revision int64
	cmd := &cobra.Command{Use: "changes <run>", Short: "Review bounded workspace changes since an exact Run started", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		namespace, _ := cmd.Flags().GetString("namespace")
		client, err := controlplaneclient.New(baseURL, token, nil)
		if err != nil {
			return err
		}
		result, err := client.GetRunChanges(cmd.Context(), namespace, args[0], uid, offset, revision, path)
		if err != nil {
			return err
		}
		if asJSON {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Changes: %s · revision %d · final=%t\n", result.State, result.Revision, result.Final)
		if !result.CapturedAt.IsZero() {
			fmt.Fprintf(cmd.OutOrStdout(), "Last verified capture: %s\n", result.CapturedAt.Format(time.RFC3339))
		}
		if result.Unavailable {
			fmt.Fprintln(cmd.OutOrStdout(), "Latest capture unavailable; any files below are the last retained observation, not a current or complete result.")
		}
		if !result.Final {
			fmt.Fprintln(cmd.OutOrStdout(), "Retained observation only; the workspace may have changed since capture (including before pause).")
		}
		for _, file := range result.Files {
			fmt.Fprintf(cmd.OutOrStdout(), "%-8s %-12s %q\n", file.Kind, file.State, file.Path)
			if file.Diff != "" {
				fmt.Fprint(cmd.OutOrStdout(), file.Diff)
			}
		}
		if result.Next > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "More files: --offset %d --revision %d\n", result.Next, result.Revision)
		}
		if path == "" && result.Total > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "Read a diff with --path PATH --revision %d\n", result.Revision)
		}
		return nil
	}}
	cmd.Flags().StringVar(&baseURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token")
	cmd.Flags().StringVar(&uid, "run-uid", "", "Immutable Run UID (required; never follows replacements)")
	cmd.Flags().StringVar(&path, "path", "", "Read one changed file's diff")
	cmd.Flags().IntVar(&offset, "offset", 0, "File-list page offset")
	cmd.Flags().Int64Var(&revision, "revision", 0, "Pin a retained observation revision")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Print the typed JSON response")
	_ = cmd.MarkFlagRequired("run-uid")
	return cmd
}
