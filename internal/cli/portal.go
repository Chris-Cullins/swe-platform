package cli

import (
	"fmt"
	"os"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/spf13/cobra"
)

func newPortalCommand() *cobra.Command {
	var controlPlaneURL, token string
	cmd := &cobra.Command{Use: "portal <environment> <service>", Short: "Print the authenticated portal URL for an Environment service", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		namespace, _ := cmd.Flags().GetString("namespace")
		client, err := controlplaneclient.New(controlPlaneURL, token, nil)
		if err != nil {
			return err
		}
		route, err := client.GetPortalRoute(cmd.Context(), namespace, args[0], args[1])
		if err != nil {
			return fmt.Errorf("discover portal: %w", err)
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), route.URL)
		return err
	}}
	cmd.Flags().StringVar(&controlPlaneURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN)")
	return cmd
}
