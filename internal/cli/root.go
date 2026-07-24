// Package cli implements the swe command line interface.
package cli

import (
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
	}

	root.PersistentFlags().StringP("namespace", "n", "default", "Kubernetes namespace to operate in")

	root.AddCommand(
		newRunCommand(),
		newEnvironmentCommand(),
		newCredentialsCommand(),
		newCancelCommand(),
		newLogsCommand(),
		newAttachCommand(),
	)
	return root
}
