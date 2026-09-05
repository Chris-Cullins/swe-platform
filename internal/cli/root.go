// Package cli implements the swe command line interface.
package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// NewRootCommand builds the root `swe` command tree.
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "swe",
		Short: "swe runs coding agents in ephemeral Kubernetes environments",
		Long: `swe is the CLI for swe-platform: it creates environments, starts agent
runs in them, and streams their output back to your terminal.`,
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Grouping commands have no Run function and remain usable for help.
			if cmd.Run == nil && cmd.RunE == nil {
				return nil
			}
			flag := cmd.Flags().Lookup("namespace")
			if flag == nil || !flag.Changed || strings.TrimSpace(flag.Value.String()) == "" {
				return fmt.Errorf("--namespace is required for %s", cmd.CommandPath())
			}
			return nil
		},
	}

	root.PersistentFlags().StringP("namespace", "n", "", "Kubernetes namespace to operate in (required)")

	root.AddCommand(
		newRunCommand(),
		newTUICommand(),
		newMCPCommand(),
		newEnvironmentCommand(),
		newCredentialsCommand(),
		newCancelCommand(),
		newDeleteRunCommand(),
		newLogsCommand(),
		newAttachCommand(),
		newPortalCommand(),
		newProjectCommand(),
	)
	return root
}
