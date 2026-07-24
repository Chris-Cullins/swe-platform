package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func newLogsCommand() *cobra.Command {
	var (
		run             string
		controlPlaneURL string
		token           string
		cursor          string
	)
	cmd := &cobra.Command{
		Use:   "logs [environment]",
		Short: "Stream environment pod logs or a Run transcript",
		Long: `Without --run, stream the backing pod's environment-container logs for
compatibility. With --run RUN, stream that exact namespaced Run's bounded,
process-local transcript as NDJSON through the authenticated control plane.
Transcript and transcript-gap data remains adapter-owned JSON.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			if run != "" {
				if len(args) != 0 {
					return fmt.Errorf("an environment argument cannot be combined with --run")
				}
				return streamRunTranscript(cmd, controlPlaneURL, token, namespace, run, cursor)
			}
			if len(args) == 0 {
				return fmt.Errorf("an environment argument or --run is required")
			}
			for _, flag := range []string{"control-plane", "token", "after"} {
				if cmd.Flags().Changed(flag) {
					return fmt.Errorf("--%s requires --run", flag)
				}
			}
			return streamLogs(cmd, namespace, args[0])
		},
	}
	cmd.Flags().StringVar(&run, "run", "", "Run whose transcript to stream instead of Environment pod logs")
	cmd.Flags().StringVar(&controlPlaneURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL; requires --run)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN; requires --run)")
	cmd.Flags().StringVar(&cursor, "after", "", "Opaque transcript cursor to resume after (requires --run)")
	return cmd
}

// streamLogs follows the logs of the environment pod.
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

func streamRunTranscript(cmd *cobra.Command, controlPlaneURL, token, namespace, run, cursor string) error {
	client, err := controlplaneclient.New(controlPlaneURL, token, nil)
	if err != nil {
		return err
	}
	endpoint := client.Endpoint("api", "v1", "namespaces", namespace, "runs", run, "transcript")
	return client.StreamSSE(cmd.Context(), endpoint, cursor, func(event controlplaneclient.SSEEvent) error {
		return writeTranscriptOutput(cmd.OutOrStdout(), event)
	})
}

func writeTranscriptOutput(output io.Writer, event controlplaneclient.SSEEvent) error {
	if !json.Valid(event.Data) {
		return fmt.Errorf("control plane returned non-JSON %q event data", event.Event)
	}
	eventName, _ := json.Marshal(event.Event)
	id, _ := json.Marshal(event.ID)
	record := make([]byte, 0, len(eventName)+len(id)+len(event.Data)+30)
	record = append(record, `{"event":`...)
	record = append(record, eventName...)
	record = append(record, `,"id":`...)
	record = append(record, id...)
	record = append(record, `,"data":`...)
	record = append(record, event.Data...)
	record = append(record, '}', '\n')
	n, err := output.Write(record)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	return err
}
