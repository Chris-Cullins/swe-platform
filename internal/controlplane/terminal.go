package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	defaultNamespace = "default"
	sandboxdPort     = "50051"
)

// TerminalDialer resolves an Environment and connects to its sandboxd API.
type TerminalDialer interface {
	DialTerminal(context.Context, string, string) (sandboxdv1.TerminalServiceClient, io.Closer, error)
}

// KubernetesTerminalDialer resolves environment pods through the Kubernetes API.
type KubernetesTerminalDialer struct {
	Client client.Client
}

// DialTerminal connects directly to sandboxd in an active environment pod.
func (d KubernetesTerminalDialer) DialTerminal(ctx context.Context, namespace, name string) (sandboxdv1.TerminalServiceClient, io.Closer, error) {
	var environment platformv1alpha1.Environment
	if err := d.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &environment); err != nil {
		return nil, nil, fmt.Errorf("get environment: %w", err)
	}
	if environment.Status.PodName == "" {
		return nil, nil, fmt.Errorf("environment has no active pod")
	}

	var pod corev1.Pod
	if err := d.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: environment.Status.PodName}, &pod); err != nil {
		return nil, nil, fmt.Errorf("get environment pod: %w", err)
	}
	if pod.Status.PodIP == "" {
		return nil, nil, fmt.Errorf("environment pod has no IP address")
	}

	connection, err := grpc.NewClient(net.JoinHostPort(pod.Status.PodIP, sandboxdPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to sandboxd: %w", err)
	}
	return sandboxdv1.NewTerminalServiceClient(connection), connection, nil
}

type terminalControl struct {
	Type string `json:"type"`
	Cols uint32 `json:"cols"`
	Rows uint32 `json:"rows"`
}

var terminalUpgrader = websocket.Upgrader{
	ReadBufferSize:  32 * 1024,
	WriteBufferSize: 32 * 1024,
}

func (s *Server) handleTerminal(w http.ResponseWriter, r *http.Request) {
	environment, ok := terminalEnvironment(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if s.terminalDialer == nil {
		http.Error(w, "terminal gateway is unavailable", http.StatusServiceUnavailable)
		return
	}
	namespace := r.URL.Query().Get("namespace")
	if namespace == "" {
		namespace = defaultNamespace
	}

	terminal, closer, err := s.terminalDialer.DialTerminal(r.Context(), namespace, environment)
	if err != nil {
		s.log.Warn("resolve terminal backend", "namespace", namespace, "environment", environment, "error", err)
		http.Error(w, "environment terminal is unavailable", http.StatusBadGateway)
		return
	}
	defer closer.Close()

	connection, err := terminalUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer connection.Close()
	connection.SetReadLimit(1 << 20)
	if err := bridgeWebTerminal(r.Context(), connection, terminal); err != nil && r.Context().Err() == nil {
		s.log.Debug("web terminal closed", "namespace", namespace, "environment", environment, "error", err)
	}
}

func terminalEnvironment(path string) (string, bool) {
	remainder := strings.TrimPrefix(path, terminalPathPrefix)
	if remainder == path || !strings.HasSuffix(remainder, "/terminal") {
		return "", false
	}
	environment := strings.TrimSuffix(remainder, "/terminal")
	return environment, environment != "" && !strings.Contains(environment, "/")
}

func bridgeWebTerminal(ctx context.Context, connection *websocket.Conn, client sandboxdv1.TerminalServiceClient) error {
	streamContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Terminal(streamContext)
	if err != nil {
		return fmt.Errorf("open sandboxd terminal: %w", err)
	}

	messageType, payload, err := connection.ReadMessage()
	if err != nil {
		return fmt.Errorf("read terminal open: %w", err)
	}
	control, err := decodeTerminalControl(messageType, payload, "open")
	if err != nil {
		return err
	}
	if err := stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Open{
		Open: &sandboxdv1.TerminalOpen{Cols: control.Cols, Rows: control.Rows},
	}}); err != nil {
		return fmt.Errorf("open sandboxd terminal: %w", err)
	}

	inputDone := make(chan error, 1)
	go func() {
		defer cancel()
		for {
			messageType, payload, err := connection.ReadMessage()
			if err != nil {
				inputDone <- err
				return
			}
			var message *sandboxdv1.TerminalMessage
			switch messageType {
			case websocket.BinaryMessage:
				message = &sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Data{Data: payload}}
			case websocket.TextMessage:
				control, err := decodeTerminalControl(messageType, payload, "resize")
				if err != nil {
					inputDone <- err
					return
				}
				message = &sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Resize{
					Resize: &sandboxdv1.TerminalResize{Cols: control.Cols, Rows: control.Rows},
				}}
			default:
				continue
			}
			if err := stream.Send(message); err != nil {
				inputDone <- err
				return
			}
		}
	}()

	for {
		message, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			select {
			case inputErr := <-inputDone:
				if websocket.IsCloseError(inputErr, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					return nil
				}
				return inputErr
			default:
			}
			return fmt.Errorf("sandboxd terminal: %w", err)
		}
		if err := connection.WriteMessage(websocket.BinaryMessage, message.GetData()); err != nil {
			return fmt.Errorf("write terminal output: %w", err)
		}
	}
}

func decodeTerminalControl(messageType int, payload []byte, want string) (terminalControl, error) {
	if messageType != websocket.TextMessage {
		return terminalControl{}, fmt.Errorf("first terminal message must be a JSON %s message", want)
	}
	var control terminalControl
	if err := json.Unmarshal(payload, &control); err != nil {
		return terminalControl{}, fmt.Errorf("invalid terminal %s message: %w", want, err)
	}
	if control.Type != want || control.Cols == 0 || control.Rows == 0 {
		return terminalControl{}, fmt.Errorf("terminal %s requires non-zero cols and rows", want)
	}
	return control, nil
}
