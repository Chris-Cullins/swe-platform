package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	wakeTimeout              = 2 * time.Minute
	wakePollInterval         = 250 * time.Millisecond
	terminalHandshakeTimeout = 5 * time.Second
)

var errTerminalEnvironmentIncarnationChanged = errors.New("environment incarnation changed")

// TerminalDialer resolves an Environment and connects to its sandboxd API.
type TerminalDialer interface {
	DialTerminal(context.Context, string, string) (sandboxdv1.TerminalServiceClient, io.Closer, error)
}

// KubernetesTerminalDialer resolves environment pods through the Kubernetes API.
type KubernetesTerminalDialer struct {
	Client client.Client
}

type activeTerminalConnection struct {
	io.Closer
	cancel context.CancelFunc
}

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

func (c *activeTerminalConnection) Close() error {
	c.cancel()
	return c.Closer.Close()
}

// DialTerminal records terminal activity, wakes a paused environment, and then
// requests an authenticated sandboxd connection through the environment
// connector. Backend transport details stay out of terminal feature code.
func (d KubernetesTerminalDialer) DialTerminal(ctx context.Context, namespace, name string) (sandboxdv1.TerminalServiceClient, io.Closer, error) {
	var environment platformv1alpha1.Environment
	if err := d.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &environment); err != nil {
		return nil, nil, fmt.Errorf("get environment: %w", err)
	}
	expectedUID := environment.UID
	if err := d.markActive(ctx, &environment, expectedUID); err != nil {
		return nil, nil, err
	}
	if environment.Spec.Paused {
		before := environment.DeepCopy()
		environment.Spec.Paused = false
		if err := d.Client.Patch(ctx, &environment, client.MergeFrom(before)); err != nil {
			return nil, nil, fmt.Errorf("wake environment: %w", err)
		}
		if err := d.waitUntilReady(ctx, namespace, name, expectedUID, &environment); err != nil {
			return nil, nil, err
		}
		if err := d.markActive(ctx, &environment, expectedUID); err != nil {
			return nil, nil, err
		}
	}
	if !platformv1alpha1.IsEnvironmentReady(&environment) {
		return nil, nil, fmt.Errorf("environment is not ready for its current generation")
	}
	terminal, closeConnection, err := (sandboxclient.Connector{Reader: d.Client}).DialTerminal(ctx, namespace, name, expectedUID)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to sandboxd: %w", err)
	}
	heartbeatContext, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatInterval, err := d.activityHeartbeatInterval(ctx, &environment)
	if err != nil {
		cancelHeartbeat()
		_ = closeConnection()
		return nil, nil, err
	}
	go d.heartbeatActivity(heartbeatContext, types.NamespacedName{Namespace: namespace, Name: name}, expectedUID, heartbeatInterval)
	closer := &activeTerminalConnection{Closer: closeFunc(closeConnection), cancel: cancelHeartbeat}
	return terminal, closer, nil
}

func (d KubernetesTerminalDialer) activityHeartbeatInterval(ctx context.Context, environment *platformv1alpha1.Environment) (time.Duration, error) {
	timeout := 15 * time.Minute
	var template platformv1alpha1.EnvironmentTemplate
	key := types.NamespacedName{Namespace: environment.Namespace, Name: environment.Spec.TemplateRef}
	if err := d.Client.Get(ctx, key, &template); err != nil {
		return 0, fmt.Errorf("get environment template: %w", err)
	}
	if template.Spec.IdleTimeout != nil {
		timeout = template.Spec.IdleTimeout.Duration
	}
	if timeout <= 0 {
		return 0, fmt.Errorf("environment template idle timeout must be positive")
	}
	return timeout / 2, nil
}

func (d KubernetesTerminalDialer) heartbeatActivity(ctx context.Context, key types.NamespacedName, expectedUID types.UID, interval time.Duration) {
	retryInterval := interval / 4
	if retryInterval <= 0 || retryInterval > time.Second {
		retryInterval = time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			environment := platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
			if err := d.markActive(ctx, &environment, expectedUID); err != nil {
				if errors.Is(err, errTerminalEnvironmentIncarnationChanged) {
					return
				}
				timer.Reset(retryInterval)
				continue
			}
			timer.Reset(interval)
		}
	}
}

func (d KubernetesTerminalDialer) markActive(ctx context.Context, environment *platformv1alpha1.Environment, expectedUID types.UID) error {
	key := client.ObjectKeyFromObject(environment)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := d.Client.Get(ctx, key, environment); err != nil {
			return err
		}
		if environment.UID != expectedUID {
			return errTerminalEnvironmentIncarnationChanged
		}
		now := metav1.Now()
		if environment.Status.LastActiveAt != nil && !now.After(environment.Status.LastActiveAt.Time) {
			return nil
		}
		environment.Status.LastActiveAt = &now
		return d.Client.Status().Update(ctx, environment)
	}); err != nil {
		return fmt.Errorf("record environment activity: %w", err)
	}
	return nil
}

func (d KubernetesTerminalDialer) waitUntilReady(ctx context.Context, namespace, name string, expectedUID types.UID, environment *platformv1alpha1.Environment) error {
	wakeContext, cancel := context.WithTimeout(ctx, wakeTimeout)
	defer cancel()
	ticker := time.NewTicker(wakePollInterval)
	defer ticker.Stop()
	key := types.NamespacedName{Namespace: namespace, Name: name}
	for {
		if err := d.Client.Get(wakeContext, key, environment); err != nil {
			return fmt.Errorf("wait for environment wake: %w", err)
		}
		if environment.UID != expectedUID {
			return fmt.Errorf("wait for environment wake: environment incarnation changed")
		}
		if platformv1alpha1.IsEnvironmentReady(environment) {
			return nil
		}
		select {
		case <-wakeContext.Done():
			return fmt.Errorf("wait for environment wake: %w", wakeContext.Err())
		case <-ticker.C:
		}
	}
}

type terminalControl struct {
	Type string `json:"type"`
	Cols uint32 `json:"cols"`
	Rows uint32 `json:"rows"`
}

var terminalUpgrader = websocket.Upgrader{
	HandshakeTimeout: terminalHandshakeTimeout,
	ReadBufferSize:   32 * 1024,
	WriteBufferSize:  32 * 1024,
	CheckOrigin:      func(*http.Request) bool { return true }, // checked before backend dial
}

func (s *Server) handleTerminal(w http.ResponseWriter, r *http.Request, namespace, environment string) {
	if s.access == nil {
		writeAccessError(w, errUnauthenticated)
		return
	}
	if err := s.access.Authorize(r, ResourceAccess{Namespace: namespace, Verb: "get", Resource: "environments", Subresource: "terminal", Name: environment}, true); err != nil {
		writeAccessError(w, err)
		return
	}
	if !websocket.IsWebSocketUpgrade(r) {
		http.Error(w, "websocket upgrade is required", http.StatusBadRequest)
		return
	}
	if !s.checkWebSocketOrigin(r) {
		http.Error(w, "websocket origin is not allowed", http.StatusForbidden)
		return
	}
	if s.terminalDialer == nil {
		http.Error(w, "terminal gateway is unavailable", http.StatusServiceUnavailable)
		return
	}
	r, cancelStream := s.withStreamLifecycle(r)
	defer cancelStream()

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
	stopCloseOnCancel := context.AfterFunc(r.Context(), func() { _ = connection.Close() })
	defer stopCloseOnCancel()
	connection.SetReadLimit(1 << 20)
	if err := bridgeWebTerminal(r.Context(), connection, terminal); err != nil {
		if r.Context().Err() == nil {
			s.log.Debug("web terminal closed", "namespace", namespace, "environment", environment, "error", err)
		}
		return
	}
	_ = connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

func (s *Server) checkWebSocketOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		_, _, err := requestToken(r, false)
		return err == nil
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return false
	}
	host := r.Host
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if s.trustProxy {
		forwardedHost := r.Header.Get("X-Forwarded-Host")
		forwardedProto := r.Header.Get("X-Forwarded-Proto")
		if forwardedHost != "" || forwardedProto != "" {
			if forwardedHost == "" || forwardedProto == "" || strings.Contains(forwardedHost, ",") || strings.Contains(forwardedProto, ",") {
				return false
			}
			host = strings.TrimSpace(forwardedHost)
			scheme = strings.ToLower(strings.TrimSpace(forwardedProto))
			if scheme != "http" && scheme != "https" {
				return false
			}
		}
	}
	return parsed.Scheme == scheme && strings.EqualFold(parsed.Host, host)
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
