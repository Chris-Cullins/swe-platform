package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func newAttachCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "attach <environment>",
		Short: "Attach a terminal to an environment (via sandboxd)",
		Long:  "Attach to an environment's shared terminal via sandboxd. Press Ctrl-] to detach.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			return attachTerminal(cmd, namespace, args[0])
		},
	}
	return cmd
}

func attachTerminal(cmd *cobra.Command, namespace, envName string) error {
	clients, err := newKubeClients()
	if err != nil {
		return err
	}
	env := &platformv1alpha1.Environment{}
	if err := clients.Get(cmd.Context(), types.NamespacedName{Namespace: namespace, Name: envName}, env); err != nil {
		return fmt.Errorf("get environment %s: %w", envName, err)
	}
	if env.Status.PodName == "" {
		return fmt.Errorf("environment %s has no backing pod", envName)
	}

	localPort, stopForward, forwardErr, err := forwardSandboxd(cmd.Context(), clients, namespace, env.Status.PodName, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	defer stopForward()

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", localPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to sandboxd: %w", err)
	}
	defer conn.Close()

	if err := bridgeTerminal(cmd.Context(), sandboxdv1.NewTerminalServiceClient(conn), cmd.InOrStdin(), cmd.OutOrStdout()); err != nil {
		select {
		case portErr := <-forwardErr:
			if portErr != nil {
				return fmt.Errorf("port-forward to pod %s: %w", env.Status.PodName, portErr)
			}
		default:
		}
		return err
	}
	return nil
}

func forwardSandboxd(ctx context.Context, clients *kubeClients, namespace, podName string, errOut io.Writer) (uint16, func(), <-chan error, error) {
	transport, upgrader, err := spdy.RoundTripperFor(clients.RESTConfig)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("build port-forward transport: %w", err)
	}
	requestURL := clients.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(podName).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, requestURL)
	stop := make(chan struct{})
	ready := make(chan struct{})
	forwarder, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"}, []string{"0:50051"}, stop, ready, io.Discard, errOut)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("configure port-forward: %w", err)
	}
	forwardErr := make(chan error, 1)
	go func() { forwardErr <- forwarder.ForwardPorts() }()

	select {
	case <-ctx.Done():
		close(stop)
		return 0, nil, nil, ctx.Err()
	case err := <-forwardErr:
		close(stop)
		return 0, nil, nil, fmt.Errorf("start port-forward to pod %s: %w", podName, err)
	case <-ready:
	}
	ports, err := forwarder.GetPorts()
	if err != nil {
		close(stop)
		return 0, nil, nil, fmt.Errorf("get forwarded sandboxd port: %w", err)
	}
	if len(ports) != 1 {
		close(stop)
		return 0, nil, nil, fmt.Errorf("expected one forwarded sandboxd port, got %d", len(ports))
	}
	var once bool
	stopForward := func() {
		if !once {
			once = true
			close(stop)
		}
	}
	return ports[0].Local, stopForward, forwardErr, nil
}

func bridgeTerminal(ctx context.Context, client sandboxdv1.TerminalServiceClient, input io.Reader, output io.Writer) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cols, rows := 80, 24
	var terminalFile *os.File
	if file, ok := input.(*os.File); ok && term.IsTerminal(int(file.Fd())) {
		terminalFile = file
		if width, height, err := term.GetSize(int(file.Fd())); err == nil {
			cols, rows = width, height
		}
		state, err := term.MakeRaw(int(file.Fd()))
		if err != nil {
			return fmt.Errorf("enter raw terminal mode: %w", err)
		}
		defer term.Restore(int(file.Fd()), state)
	}

	stream, err := client.Terminal(streamCtx)
	if err != nil {
		return fmt.Errorf("open terminal stream: %w", err)
	}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Open{
		Open: &sandboxdv1.TerminalOpen{Cols: uint32(cols), Rows: uint32(rows)},
	}}); err != nil {
		return fmt.Errorf("open terminal: %w", err)
	}

	type inputResult struct {
		data []byte
		err  error
	}
	inputCh := make(chan inputResult)
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := input.Read(buf)
			result := inputResult{data: append([]byte(nil), buf[:n]...), err: err}
			select {
			case inputCh <- result:
			case <-streamCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	resizeCh := make(chan os.Signal, 1)
	if terminalFile != nil {
		signal.Notify(resizeCh, syscall.SIGWINCH)
		defer signal.Stop(resizeCh)
	}
	go func() {
		for {
			select {
			case <-streamCtx.Done():
				return
			case result := <-inputCh:
				if detachAt := bytes.IndexByte(result.data, 0x1d); detachAt >= 0 {
					if detachAt > 0 {
						_ = stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Data{Data: result.data[:detachAt]}})
					}
					cancel()
					return
				}
				if len(result.data) > 0 {
					if stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Data{Data: result.data}}) != nil {
						return
					}
				}
				if result.err != nil {
					return
				}
			case <-resizeCh:
				width, height, err := term.GetSize(int(terminalFile.Fd()))
				if err == nil {
					if stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Resize{
						Resize: &sandboxdv1.TerminalResize{Cols: uint32(width), Rows: uint32(height)},
					}}) != nil {
						return
					}
				}
			}
		}
	}()

	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if streamCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("terminal stream: %w", err)
		}
		if _, err := output.Write(message.GetData()); err != nil {
			return fmt.Errorf("write terminal output: %w", err)
		}
	}
}
