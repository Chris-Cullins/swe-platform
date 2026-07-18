package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
)

func newLogsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs <environment>",
		Short: "Stream logs from an environment's backing pod",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			return streamLogs(cmd, namespace, args[0])
		},
	}
	return cmd
}

// streamLogs follows the logs of the environment pod.
// TODO(P1): stream the agent transcript via the control plane instead of raw pod logs.
func streamLogs(cmd *cobra.Command, namespace, envName string) error {
	clients, err := newKubeClients()
	if err != nil {
		return err
	}

	podName := "env-" + envName
	req := clients.Clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: "environment",
		Follow:    true,
	})
	stream, err := req.Stream(cmd.Context())
	if err != nil {
		return fmt.Errorf("open log stream for pod %s: %w", podName, err)
	}
	defer stream.Close()

	_, err = io.Copy(cmd.OutOrStdout(), stream)
	return err
}
