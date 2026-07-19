package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
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
// TODO(#57): define a compatible Run-selecting transcript mode before replacing
// this kubeconfig-authenticated Environment log stream.
func streamLogs(cmd *cobra.Command, namespace, envName string) error {
	clients, err := newKubeClients()
	if err != nil {
		return err
	}

	var env platformv1alpha1.Environment
	if err := clients.Get(cmd.Context(), types.NamespacedName{Namespace: namespace, Name: envName}, &env); err != nil {
		return fmt.Errorf("get environment %s: %w", envName, err)
	}
	if env.Status.PodName == "" {
		return fmt.Errorf("environment %s has no active pod", envName)
	}
	podName := env.Status.PodName
	var pod corev1.Pod
	if err := clients.Get(cmd.Context(), types.NamespacedName{Namespace: namespace, Name: podName}, &pod); err != nil {
		return fmt.Errorf("get environment pod %s: %w", podName, err)
	}
	if !metav1.IsControlledBy(&pod, &env) {
		return fmt.Errorf("environment pod %s is not owned by the current environment", podName)
	}
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
