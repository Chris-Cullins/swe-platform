package cli

import (
	"fmt"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/Chris-Cullins/swe-platform/internal/mcpserver"
)

func newMCPCommand() *cobra.Command {
	var controlPlaneURL string
	var token string
	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve authenticated platform tools over local MCP stdio",
		Long: `Serve bounded create_run and read_transcript tools over MCP stdio.
The process acts as the explicit bearer-authenticated control-plane user in one
namespace. Interactive terminal attach is intentionally not exposed as a tool.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			client, err := controlplaneclient.New(controlPlaneURL, token, nil)
			if err != nil {
				return err
			}
			if err := mcpserver.New(client, namespace).Run(cmd.Context(), &mcp.StdioTransport{}); err != nil {
				return fmt.Errorf("serve MCP stdio: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&controlPlaneURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN)")
	return cmd
}
