package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newAttachCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "attach <environment>",
		Short: "Attach a terminal to an environment (via sandboxd)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO(P1): port-forward to the environment's sandboxd and open the
			// shared terminal stream. Never kubectl-exec into the pod — sandboxd
			// is the only contract into an environment.
			return fmt.Errorf("attach is not implemented yet — it will arrive with the sandboxd terminal stream (P1)")
		},
	}
}
