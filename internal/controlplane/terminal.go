package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	wakeTimeout                = 2 * time.Minute
	wakePollInterval           = 250 * time.Millisecond
	terminalPolicyPollInterval = 5 * time.Second
	terminalHealthTimeout      = 5 * time.Second
	terminalHandshakeTimeout   = 5 * time.Second
)

var errTerminalEnvironmentIncarnationChanged = errors.New("environment incarnation changed")

// TerminalDialer resolves an Environment and connects to its sandboxd API.
type TerminalDialer interface {
	DialTerminal(context.Context, string, string) (sandboxdv1.TerminalServiceClient, sandboxdv1.HealthServiceClient, io.Closer, error)
}

// KubernetesTerminalDialer resolves environment pods through the Kubernetes API.
type KubernetesTerminalDialer struct {
	Client             client.Client
	policyPollInterval time.Duration
}

type activeTerminalConnection struct {
	io.Closer
	cancel context.CancelFunc
}

type terminalConnectionLease struct {
	mu     sync.Mutex
	closer io.Closer
	closed bool
}

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

func (c *activeTerminalConnection) Close() error {
	c.cancel()
	return c.Closer.Close()
}

func (l *terminalConnectionLease) attach(closer io.Closer) bool {
	l.mu.Lock()
	if !l.closed {
		l.closer = closer
		l.mu.Unlock()
		return true
	}
	l.mu.Unlock()
	_ = closer.Close()
	return false
}

func (l *terminalConnectionLease) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	closer := l.closer
	l.mu.Unlock()
	if closer != nil {
		return closer.Close()
	}
	return nil
}

// DialTerminal records terminal activity, wakes a paused environment, and then
// requests an authenticated sandboxd connection through the environment
// connector. Backend transport details stay out of terminal feature code.
func (d KubernetesTerminalDialer) DialTerminal(ctx context.Context, namespace, name string) (sandboxdv1.TerminalServiceClient, sandboxdv1.HealthServiceClient, io.Closer, error) {
	var environment platformv1alpha1.Environment
	if err := d.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &environment); err != nil {
		return nil, nil, nil, fmt.Errorf("get environment: %w", err)
	}
	expectedUID := environment.UID
	policyRevision := lifecycle.HoldPolicyRevision(&environment)
	if err := terminalAccessPolicyError(&environment); err != nil {
		return nil, nil, nil, err
	}
	if err := d.markActive(ctx, &environment, expectedUID, policyRevision); err != nil {
		return nil, nil, nil, err
	}
	heartbeatInterval, err := d.activityHeartbeatInterval(ctx, &environment)
	if err != nil {
		return nil, nil, nil, err
	}
	heartbeatContext, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatTransferred := false
	defer func() {
		if !heartbeatTransferred {
			cancelHeartbeat()
		}
	}()
	connectionLease := &terminalConnectionLease{}
	go d.heartbeatActivity(heartbeatContext, types.NamespacedName{Namespace: namespace, Name: name}, expectedUID, policyRevision, heartbeatInterval, func() { _ = connectionLease.Close() })
	if err := d.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &environment); err != nil {
		return nil, nil, nil, fmt.Errorf("refresh environment lifecycle: %w", err)
	}
	if environment.UID != expectedUID {
		return nil, nil, nil, errTerminalEnvironmentIncarnationChanged
	}
	if err := terminalAccessPolicyError(&environment); err != nil {
		return nil, nil, nil, err
	}
	if environment.Status.Lifecycle.Suspended {
		requestID := fmt.Sprintf("terminal/wake/%d", time.Now().UnixNano())
		if err := lifecycle.RequestWake(ctx, d.Client, types.NamespacedName{Namespace: namespace, Name: name}, expectedUID, policyRevision, requestID); err != nil {
			return nil, nil, nil, fmt.Errorf("wake environment: %w", err)
		}
		if err := d.waitUntilReady(ctx, namespace, name, expectedUID, &environment); err != nil {
			return nil, nil, nil, err
		}
		if err := d.markActive(ctx, &environment, expectedUID, policyRevision); err != nil {
			return nil, nil, nil, err
		}
	}
	// Wake intents advance generation, while activity metadata does not. Do not
	// resolve sandboxd against a stale Ready observation after a wake or a
	// concurrent lifecycle change.
	if err := d.waitUntilReady(ctx, namespace, name, expectedUID, &environment); err != nil {
		return nil, nil, nil, err
	}
	if !platformv1alpha1.IsEnvironmentReady(&environment) {
		return nil, nil, nil, fmt.Errorf("environment is not ready for its current generation")
	}
	terminal, health, closeConnection, err := (sandboxclient.Connector{Reader: d.Client}).DialTerminal(ctx, namespace, name, expectedUID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect to sandboxd: %w", err)
	}
	if !connectionLease.attach(closeFunc(closeConnection)) {
		return nil, nil, nil, fmt.Errorf("environment became explicitly held while opening terminal")
	}
	closer := &activeTerminalConnection{Closer: connectionLease, cancel: cancelHeartbeat}
	heartbeatTransferred = true
	return terminal, health, closer, nil
}

func terminalAccessPolicyError(environment *platformv1alpha1.Environment) error {
	policyRevision := lifecycle.HoldPolicyRevision(environment)
	if environment.Spec.Paused {
		return fmt.Errorf("environment has a legacy pause awaiting hold-policy migration")
	}
	if environment.Spec.Lifecycle.Hold != nil && environment.Spec.Lifecycle.Hold.Enabled {
		return fmt.Errorf("environment is explicitly held at policy revision %d", policyRevision)
	}
	if environment.Status.Lifecycle.Suspended && environment.Status.Lifecycle.SuspensionReason != platformv1alpha1.EnvironmentSuspensionReasonIdle {
		return fmt.Errorf("environment suspension reason %q is not terminal-wakeable", environment.Status.Lifecycle.SuspensionReason)
	}
	return nil
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

func (d KubernetesTerminalDialer) heartbeatActivity(ctx context.Context, key types.NamespacedName, expectedUID types.UID, policyRevision int64, interval time.Duration, revoke func()) {
	retryInterval := interval / 4
	if retryInterval <= 0 || retryInterval > time.Second {
		retryInterval = time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	policyTicker := time.NewTicker(d.holdPolicyPollInterval())
	defer policyTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-policyTicker.C:
			revision, held, err := d.readHoldPolicy(ctx, key, expectedUID)
			if err != nil {
				if errors.Is(err, errTerminalEnvironmentIncarnationChanged) {
					revoke()
					return
				}
				continue
			}
			if held || revision < policyRevision {
				revoke()
				return
			}
			if revision > policyRevision {
				policyRevision = revision
			}
		case <-timer.C:
			for {
				environment := platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
				err := d.markActive(ctx, &environment, expectedUID, policyRevision)
				if err == nil {
					timer.Reset(interval)
					break
				}
				if errors.Is(err, errTerminalEnvironmentIncarnationChanged) {
					revoke()
					return
				}
				if errors.Is(err, lifecycle.ErrHoldPolicyChanged) {
					revision, held, refreshErr := d.refreshHoldPolicy(ctx, key, expectedUID, policyRevision)
					if refreshErr != nil {
						if errors.Is(refreshErr, errTerminalEnvironmentIncarnationChanged) {
							revoke()
							return
						}
						timer.Reset(retryInterval)
						break
					}
					if held || revision <= policyRevision {
						revoke()
						return
					}
					policyRevision = revision
					continue
				}
				timer.Reset(retryInterval)
				break
			}
		}
	}
}

func (d KubernetesTerminalDialer) holdPolicyPollInterval() time.Duration {
	if d.policyPollInterval > 0 {
		return d.policyPollInterval
	}
	return terminalPolicyPollInterval
}

func (d KubernetesTerminalDialer) readHoldPolicy(ctx context.Context, key types.NamespacedName, expectedUID types.UID) (int64, bool, error) {
	var environment platformv1alpha1.Environment
	if err := d.Client.Get(ctx, key, &environment); err != nil {
		return 0, false, err
	}
	if environment.UID != expectedUID {
		return 0, false, errTerminalEnvironmentIncarnationChanged
	}
	revision := lifecycle.HoldPolicyRevision(&environment)
	return revision, environment.Spec.Lifecycle.Hold != nil && environment.Spec.Lifecycle.Hold.Enabled, nil
}

func (d KubernetesTerminalDialer) refreshHoldPolicy(ctx context.Context, key types.NamespacedName, expectedUID types.UID, previousRevision int64) (int64, bool, error) {
	revision, held, err := d.readHoldPolicy(ctx, key, expectedUID)
	if err != nil {
		return 0, false, err
	}
	if revision <= previousRevision {
		return revision, true, nil
	}
	return revision, held, nil
}

func (d KubernetesTerminalDialer) markActive(ctx context.Context, environment *platformv1alpha1.Environment, expectedUID types.UID, policyRevision int64) error {
	key := client.ObjectKeyFromObject(environment)
	requestID := fmt.Sprintf("terminal/activity/%d", time.Now().UnixNano())
	if err := lifecycle.RecordActivity(ctx, d.Client, key, expectedUID, policyRevision, platformv1alpha1.EnvironmentActivitySourceTerminal, requestID); err != nil {
		if errors.Is(err, lifecycle.ErrEnvironmentIncarnationChanged) {
			return fmt.Errorf("record environment activity: %w", errTerminalEnvironmentIncarnationChanged)
		}
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
		if err := terminalAccessPolicyError(environment); err != nil {
			return fmt.Errorf("wait for environment wake: %w", err)
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

	terminal, health, closer, err := s.terminalDialer.DialTerminal(r.Context(), namespace, environment)
	if err != nil {
		s.log.Warn("resolve terminal backend", "namespace", namespace, "environment", environment, "error", err)
		http.Error(w, "environment terminal is unavailable", http.StatusBadGateway)
		return
	}
	defer closer.Close()
	if err := checkTerminalHealth(r.Context(), health, terminalHealthTimeout); err != nil {
		s.log.Warn("check terminal backend health", "namespace", namespace, "environment", environment, "error", err)
		http.Error(w, "environment terminal is unavailable", http.StatusBadGateway)
		return
	}

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

func checkTerminalHealth(ctx context.Context, health sandboxdv1.HealthServiceClient, timeout time.Duration) error {
	checkContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	response, err := health.Check(checkContext, &sandboxdv1.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("check sandboxd health: %w", err)
	}
	if !response.GetOk() {
		return fmt.Errorf("sandboxd reported unhealthy")
	}
	return nil
}

func (s *Server) checkWebSocketOrigin(r *http.Request) bool {
	origins := r.Header.Values("Origin")
	if len(origins) == 0 || (len(origins) == 1 && origins[0] == "") {
		_, _, err := requestBearerToken(r)
		return err == nil
	}
	return s.sameOrigin(r)
}

func bridgeWebTerminal(ctx context.Context, connection *websocket.Conn, client sandboxdv1.TerminalServiceClient) error {
	streamContext, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stream, err := client.Terminal(streamContext)
	if err != nil {
		return fmt.Errorf("open sandboxd terminal: %w", err)
	}

	messageType, payload, err := connection.ReadMessage()
	if err != nil {
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			return nil
		}
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

	go func() {
		for {
			messageType, payload, err := connection.ReadMessage()
			if err != nil {
				cancel(err)
				return
			}
			var message *sandboxdv1.TerminalMessage
			switch messageType {
			case websocket.BinaryMessage:
				message = &sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Data{Data: payload}}
			case websocket.TextMessage:
				control, err := decodeTerminalControl(messageType, payload, "resize")
				if err != nil {
					cancel(err)
					return
				}
				message = &sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Resize{
					Resize: &sandboxdv1.TerminalResize{Cols: control.Cols, Rows: control.Rows},
				}}
			default:
				continue
			}
			if err := stream.Send(message); err != nil {
				cancel(err)
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
			if inputErr := context.Cause(streamContext); inputErr != nil {
				if websocket.IsCloseError(inputErr, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					return nil
				}
				return inputErr
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
