package controlplane

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func TestWebTerminalBridgesSandboxd(t *testing.T) {
	backend := &terminalTestServer{}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, backend)
	sandboxdv1.RegisterHealthServiceServer(grpcServer, backend)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	dialer := &terminalTestDialer{
		client: sandboxdv1.NewTerminalServiceClient(connection),
		health: sandboxdv1.NewHealthServiceClient(connection),
	}
	server := httptest.NewServer(NewServer(nil, ServerOptions{Access: &fakeAccess{}, TerminalDialer: dialer}).Handler())
	t.Cleanup(server.Close)

	websocketURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/namespaces/project-1/environments/env-1/terminal"
	header := http.Header{"Authorization": []string{"Bearer reader"}}
	websocketConnection, _, err := websocket.DefaultDialer.Dial(websocketURL, header)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = websocketConnection.Close() })
	if err := websocketConnection.WriteJSON(terminalControl{Type: "open", Cols: 100, Rows: 40}); err != nil {
		t.Fatal(err)
	}
	if err := websocketConnection.WriteJSON(terminalControl{Type: "resize", Cols: 120, Rows: 50}); err != nil {
		t.Fatal(err)
	}
	if err := websocketConnection.WriteMessage(websocket.BinaryMessage, []byte("echo hello\n")); err != nil {
		t.Fatal(err)
	}
	messageType, payload, err := websocketConnection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.BinaryMessage || string(payload) != "terminal output" {
		t.Fatalf("terminal output = (%d, %q), want binary terminal output", messageType, payload)
	}

	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	if dialer.namespace != "project-1" || dialer.environment != "env-1" {
		t.Fatalf("resolved terminal %s/%s, want project-1/env-1", dialer.namespace, dialer.environment)
	}
	if backend.open == nil || backend.open.Cols != 100 || backend.open.Rows != 40 {
		t.Fatalf("sandboxd open = %+v, want 100x40", backend.open)
	}
	if backend.resize == nil || backend.resize.Cols != 120 || backend.resize.Rows != 50 {
		t.Fatalf("sandboxd resize = %+v, want 120x50", backend.resize)
	}
	if string(backend.input) != "echo hello\n" {
		t.Fatalf("sandboxd input = %q, want echo hello", backend.input)
	}
}

func TestBridgeWebTerminalTreatsCausalNormalClientCloseAsClean(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, &terminalCancelServer{})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	backendConnection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backendConnection.Close() })

	bridgeResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := terminalUpgrader.Upgrade(w, r, nil)
		if err != nil {
			bridgeResult <- err
			return
		}
		defer connection.Close()
		bridgeResult <- bridgeWebTerminal(r.Context(), connection, sandboxdv1.NewTerminalServiceClient(backendConnection))
	}))
	t.Cleanup(server.Close)

	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.WriteJSON(terminalControl{Type: "open", Cols: 80, Rows: 24}); err != nil {
		t.Fatal(err)
	}
	if err := connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-bridgeResult:
		if err != nil {
			t.Fatalf("bridge error after normal client close = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bridge did not stop after normal client close")
	}
}

func TestWebTerminalClosesWhileWaitingForOpenWhenContextCanceled(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, &terminalTestServer{})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	backendConnection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backendConnection.Close() })

	closed := make(chan struct{})
	dialer := &terminalTestDialer{client: sandboxdv1.NewTerminalServiceClient(backendConnection), closer: closeFunc(func() error {
		close(closed)
		return nil
	})}
	streamLifecycle, cancelStreams := context.WithCancel(context.Background())
	server := httptest.NewServer(NewServer(nil, ServerOptions{Access: &fakeAccess{}, TerminalDialer: dialer, StreamLifecycle: streamLifecycle}).Handler())
	t.Cleanup(server.Close)

	websocketURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/namespaces/project-1/environments/env-1/terminal"
	connection, _, err := websocket.DefaultDialer.Dial(websocketURL, http.Header{"Authorization": []string{"Bearer reader"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	select {
	case <-closed:
		t.Fatal("terminal backend closed before request cancellation")
	default:
	}

	cancelStreams()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("terminal backend was not closed after request cancellation")
	}
}

func TestTerminalHandshakeTimeoutBoundsPostHijackWrite(t *testing.T) {
	if terminalUpgrader.HandshakeTimeout != terminalHandshakeTimeout || terminalHandshakeTimeout >= shutdownTimeoutForTest {
		t.Fatalf("terminal handshake timeout = %v, want positive and below shutdown budget", terminalUpgrader.HandshakeTimeout)
	}
	upgrader := terminalUpgrader
	upgrader.HandshakeTimeout = 10 * time.Millisecond
	serverConnection, stalledPeer := net.Pipe()
	defer serverConnection.Close()
	defer stalledPeer.Close()
	w := &hijackResponseWriter{header: make(http.Header), connection: serverConnection}
	request := httptest.NewRequest(http.MethodGet, "/terminal", nil)
	setWebSocketUpgrade(request)
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	request.Header.Set("Sec-WebSocket-Version", "13")

	done := make(chan error, 1)
	go func() {
		_, err := upgrader.Upgrade(w, request, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled post-hijack handshake unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("stalled post-hijack handshake ignored its timeout")
	}
}

const shutdownTimeoutForTest = 10 * time.Second

type hijackResponseWriter struct {
	header     http.Header
	connection net.Conn
}

func (w *hijackResponseWriter) Header() http.Header             { return w.header }
func (*hijackResponseWriter) Write(payload []byte) (int, error) { return len(payload), nil }
func (*hijackResponseWriter) WriteHeader(int)                   {}
func (w *hijackResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.connection, bufio.NewReadWriter(bufio.NewReader(w.connection), bufio.NewWriter(w.connection)), nil
}

func TestWebTerminalRequiresDialer(t *testing.T) {
	request := httptest.NewRequest("GET", "/api/v1/namespaces/project-1/environments/env-1/terminal", nil)
	setWebSocketUpgrade(request)
	response := httptest.NewRecorder()
	NewServer(nil, ServerOptions{Access: &fakeAccess{}}).Handler().ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatalf("status = %d, want 503", response.Code)
	}
}

func TestWebTerminalChecksSandboxdHealthBeforeUpgrade(t *testing.T) {
	checked := make(chan struct{})
	closed := make(chan struct{})
	dialer := &terminalTestDialer{
		health: terminalTestHealthClient{check: func(ctx context.Context) (*sandboxdv1.HealthCheckResponse, error) {
			defer close(checked)
			deadline, ok := ctx.Deadline()
			remaining := time.Until(deadline)
			if !ok || remaining <= 0 || remaining > terminalHealthTimeout {
				return nil, errors.New("health check has no bounded deadline")
			}
			return nil, errors.New("sandboxd unavailable")
		}},
		closer: closeFunc(func() error {
			close(closed)
			return nil
		}),
	}
	server := httptest.NewServer(NewServer(nil, ServerOptions{Access: &fakeAccess{}, TerminalDialer: dialer}).Handler())
	t.Cleanup(server.Close)

	websocketURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/namespaces/project-1/environments/env-1/terminal"
	connection, response, err := websocket.DefaultDialer.Dial(websocketURL, http.Header{"Authorization": []string{"Bearer reader"}})
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil {
		t.Fatal("terminal upgraded despite failed sandboxd health check")
	}
	if response == nil || response.StatusCode != http.StatusBadGateway {
		t.Fatalf("pre-upgrade response = %#v, want HTTP 502", response)
	}
	select {
	case <-checked:
	default:
		t.Fatal("sandboxd health was not checked")
	}
	select {
	case <-closed:
	default:
		t.Fatal("sandboxd connection was not closed after failed health check")
	}
}

func TestKubernetesTerminalDialerMarksEnvironmentActive(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	oldActivity := metav1.NewTime(time.Now().Add(-time.Hour))
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-1", Namespace: "project-1"},
		Status:     platformv1alpha1.EnvironmentStatus{LastActiveAt: &oldActivity},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	dialer := KubernetesTerminalDialer{Client: kubeClient}

	if err := dialer.markActive(context.Background(), environment, environment.UID); err != nil {
		t.Fatalf("markActive() error = %v", err)
	}
	var updated platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.LastActiveAt == nil || !updated.Status.LastActiveAt.After(oldActivity.Time) {
		t.Fatalf("LastActiveAt = %v, want after %s", updated.Status.LastActiveAt, oldActivity.Time)
	}
}

func TestKubernetesTerminalDialerDoesNotMarkReplacementEnvironmentActive(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	original := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env-1", Namespace: "project-1", UID: "old-uid"}}
	replacement := original.DeepCopy()
	replacement.UID = "new-uid"
	kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(replacement).WithObjects(replacement).Build()
	dialer := KubernetesTerminalDialer{Client: kubeClient}
	if err := dialer.markActive(context.Background(), original, original.UID); err == nil || !strings.Contains(err.Error(), "incarnation changed") {
		t.Fatalf("markActive() replacement error = %v", err)
	}
	var updated platformv1alpha1.Environment
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(replacement), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.LastActiveAt != nil {
		t.Fatal("replacement Environment was marked active")
	}
}

func TestTerminalHeartbeatRecoversAfterTransientGetFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	oldActivity := metav1.NewTime(time.Now().Add(-time.Hour))
	environment := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "env-1", Namespace: "project-1", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{LastActiveAt: &oldActivity}}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment).Build()
	transient := &transientTerminalGetClient{Client: baseClient, getFailures: 1, updateFailures: 1}
	dialer := KubernetesTerminalDialer{Client: transient}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dialer.heartbeatActivity(ctx, client.ObjectKeyFromObject(environment), environment.UID, 5*time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var updated platformv1alpha1.Environment
		if err := baseClient.Get(context.Background(), client.ObjectKeyFromObject(environment), &updated); err != nil {
			t.Fatal(err)
		}
		if updated.Status.LastActiveAt != nil && updated.Status.LastActiveAt.After(oldActivity.Time) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("terminal heartbeat stopped after a transient Get failure")
}

type transientTerminalGetClient struct {
	client.Client
	mu             sync.Mutex
	getFailures    int
	updateFailures int
}

func (c *transientTerminalGetClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getFailures > 0 {
		c.getFailures--
		return errors.New("transient API failure")
	}
	return c.Client.Get(ctx, key, object, options...)
}

func (c *transientTerminalGetClient) Status() client.SubResourceWriter {
	return &transientTerminalStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type transientTerminalStatusWriter struct {
	client.SubResourceWriter
	client *transientTerminalGetClient
}

func (w *transientTerminalStatusWriter) Update(ctx context.Context, object client.Object, options ...client.SubResourceUpdateOption) error {
	w.client.mu.Lock()
	defer w.client.mu.Unlock()
	if w.client.updateFailures > 0 {
		w.client.updateFailures--
		return errors.New("transient status failure")
	}
	return w.SubResourceWriter.Update(ctx, object, options...)
}

func TestKubernetesTerminalDialerUsesRecreatedPodCredentialsAfterWake(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-1", Namespace: "project-1", UID: "current-environment"},
		Spec: platformv1alpha1.EnvironmentSpec{
			Paused:      true,
			TemplateRef: "default",
		},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase:   platformv1alpha1.EnvironmentPhasePaused,
			PodName: "old-pod",
		},
	}
	template := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: environment.Namespace},
	}
	newPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "new-pod",
			Namespace: environment.Namespace,
			UID:       "new-pod-uid",
			Annotations: map[string]string{
				sandboxdauth.IdentityAnnotation: "new-incarnation.sandboxd.swe.dev",
				sandboxdauth.TrustAnnotation:    testCertificatePEM(t, "new-incarnation.sandboxd.swe.dev"),
				sandboxdauth.TokenAnnotation:    "new-terminal-token",
			},
		},
		Status: corev1.PodStatus{PodIP: "192.0.2.10", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	if err := controllerutil.SetControllerReference(environment, newPod, scheme); err != nil {
		t.Fatal(err)
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment, template, newPod).Build()
	dialer := KubernetesTerminalDialer{Client: wakeReadyClient{Client: baseClient, podName: newPod.Name}}

	_, _, closer, err := dialer.DialTerminal(context.Background(), environment.Namespace, environment.Name)
	if err != nil {
		t.Fatalf("DialTerminal() error = %v; wake did not use the recreated pod credential bundle", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalHeartbeatProtectsSlowWakeBeforeConnection(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-1", Namespace: "project-1", UID: "current-environment"},
		Spec:       platformv1alpha1.EnvironmentSpec{Paused: true, TemplateRef: "default"},
		Status:     platformv1alpha1.EnvironmentStatus{Phase: platformv1alpha1.EnvironmentPhasePaused},
	}
	template := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: environment.Namespace},
		Spec:       platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: 40 * time.Millisecond}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "new-pod", Namespace: environment.Namespace, UID: "new-pod-uid",
			Annotations: map[string]string{
				sandboxdauth.IdentityAnnotation: "new-incarnation.sandboxd.swe.dev",
				sandboxdauth.TrustAnnotation:    testCertificatePEM(t, "new-incarnation.sandboxd.swe.dev"),
				sandboxdauth.TokenAnnotation:    "new-terminal-token",
			},
		},
		Status: corev1.PodStatus{PodIP: "192.0.2.10", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	if err := controllerutil.SetControllerReference(environment, pod, scheme); err != nil {
		t.Fatal(err)
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment, template, pod).Build()
	activityClient := &activityCountingClient{Client: baseClient}
	wakeClient := &heartbeatWakeReadyClient{activityCountingClient: activityClient, podName: pod.Name, minimumActivityWrites: 3}
	dialer := KubernetesTerminalDialer{Client: wakeClient}

	_, _, closer, err := dialer.DialTerminal(context.Background(), environment.Namespace, environment.Name)
	if err != nil {
		t.Fatalf("DialTerminal() error = %v", err)
	}
	if writes := activityClient.count(); writes < wakeClient.minimumActivityWrites {
		t.Fatalf("activity writes before ready = %d, want at least %d", writes, wakeClient.minimumActivityWrites)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	writesAfterClose := waitForActivityWritesToQuiesce(t, activityClient, 60*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if writes := activityClient.count(); writes != writesAfterClose {
		t.Fatalf("activity writes continued after terminal close = %d, want %d", writes, writesAfterClose)
	}
}

func TestTerminalHeartbeatCancelsWhenWakeOrDialFails(t *testing.T) {
	for _, test := range []struct {
		name   string
		paused bool
	}{
		{name: "wake", paused: true},
		{name: "dial"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := platformv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			environment := &platformv1alpha1.Environment{
				ObjectMeta: metav1.ObjectMeta{Name: "env-1", Namespace: "project-1", UID: "env-uid"},
				Spec:       platformv1alpha1.EnvironmentSpec{Paused: test.paused, TemplateRef: "default"},
			}
			if test.paused {
				environment.Status.Phase = platformv1alpha1.EnvironmentPhasePaused
			} else {
				applyReadyTerminalStatus(environment, "missing-pod")
			}
			template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: environment.Namespace}, Spec: platformv1alpha1.EnvironmentTemplateSpec{IdleTimeout: &metav1.Duration{Duration: 40 * time.Millisecond}}}
			baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment, template).Build()
			activityClient := &activityCountingClient{Client: baseClient}
			var kubeClient client.Client = activityClient
			if test.paused {
				kubeClient = &failingWakeClient{activityCountingClient: activityClient}
			}
			dialer := KubernetesTerminalDialer{Client: kubeClient}

			if _, _, _, err := dialer.DialTerminal(context.Background(), environment.Namespace, environment.Name); err == nil {
				t.Fatalf("DialTerminal() succeeded during %s failure", test.name)
			}
			writesAfterFailure := waitForActivityWritesToQuiesce(t, activityClient, 60*time.Millisecond)
			time.Sleep(60 * time.Millisecond)
			if writes := activityClient.count(); writes != writesAfterFailure {
				t.Fatalf("activity writes continued after %s failure = %d, want %d", test.name, writes, writesAfterFailure)
			}
		})
	}
}

func waitForActivityWritesToQuiesce(t *testing.T, activityClient *activityCountingClient, quietFor time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	writes := activityClient.count()
	for time.Now().Before(deadline) {
		time.Sleep(quietFor)
		next := activityClient.count()
		if next == writes {
			return next
		}
		writes = next
	}
	t.Fatal("terminal activity writes did not quiesce after cancellation")
	return 0
}

type activityCountingClient struct {
	client.Client
	mu             sync.Mutex
	activityWrites int
}

func (c *activityCountingClient) Status() client.SubResourceWriter {
	return &activityCountingStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

func (c *activityCountingClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activityWrites
}

type activityCountingStatusWriter struct {
	client.SubResourceWriter
	client *activityCountingClient
}

func (w *activityCountingStatusWriter) Update(ctx context.Context, object client.Object, options ...client.SubResourceUpdateOption) error {
	if err := w.SubResourceWriter.Update(ctx, object, options...); err != nil {
		return err
	}
	if _, ok := object.(*platformv1alpha1.Environment); ok {
		w.client.mu.Lock()
		w.client.activityWrites++
		w.client.mu.Unlock()
	}
	return nil
}

type heartbeatWakeReadyClient struct {
	*activityCountingClient
	podName               string
	minimumActivityWrites int
	mu                    sync.Mutex
	readyPublished        bool
}

func (c *heartbeatWakeReadyClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*platformv1alpha1.Environment); ok && c.count() >= c.minimumActivityWrites {
		c.mu.Lock()
		if !c.readyPublished {
			var current platformv1alpha1.Environment
			if err := c.Client.Get(ctx, key, &current, options...); err != nil {
				c.mu.Unlock()
				return err
			}
			applyReadyTerminalStatus(&current, c.podName)
			if err := c.Client.Status().Update(ctx, &current); err != nil {
				c.mu.Unlock()
				return err
			}
			c.readyPublished = true
		}
		c.mu.Unlock()
	}
	return c.Client.Get(ctx, key, object, options...)
}

type failingWakeClient struct{ *activityCountingClient }

func (*failingWakeClient) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return errors.New("wake failed")
}

func applyReadyTerminalStatus(environment *platformv1alpha1.Environment, podName string) {
	environment.Status.Phase = platformv1alpha1.EnvironmentPhaseReady
	environment.Status.PodName = podName
	environment.Status.Endpoints.Sandboxd = "192.0.2.10:50051"
	environment.Status.ObservedGeneration = environment.Generation
	apimeta.SetStatusCondition(&environment.Status.Conditions, metav1.Condition{
		Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
		ObservedGeneration: environment.Generation, Reason: "SandboxdReady", Message: "sandboxd is ready",
	})
}

type wakeReadyClient struct {
	client.Client
	podName string
}

func (c wakeReadyClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if err := c.Client.Patch(ctx, object, patch, options...); err != nil {
		return err
	}
	environment, ok := object.(*platformv1alpha1.Environment)
	if !ok {
		return nil
	}
	var current platformv1alpha1.Environment
	if err := c.Client.Get(ctx, client.ObjectKeyFromObject(environment), &current); err != nil {
		return err
	}
	current.Status.Phase = platformv1alpha1.EnvironmentPhaseReady
	current.Status.PodName = c.podName
	var pod corev1.Pod
	if err := c.Client.Get(ctx, types.NamespacedName{Namespace: current.Namespace, Name: c.podName}, &pod); err != nil {
		return err
	}
	current.Status.Endpoints.Sandboxd = net.JoinHostPort(pod.Status.PodIP, "50051")
	current.Status.ObservedGeneration = current.Generation
	apimeta.SetStatusCondition(&current.Status.Conditions, metav1.Condition{
		Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
		ObservedGeneration: current.Generation, Reason: "SandboxdReady", Message: "sandboxd is ready",
	})
	return c.Client.Status().Update(ctx, &current)
}

func testCertificatePEM(t *testing.T, serverName string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: serverName},
		DNSNames:     []string{serverName},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}))
}

func TestWebTerminalAuthorizesBeforeDial(t *testing.T) {
	dialer := &terminalTestDialer{}
	handler := NewServer(nil, ServerOptions{Access: &fakeAccess{err: errUnauthenticated}, TerminalDialer: dialer}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/project-a/environments/shared/terminal?namespace=project-b", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if dialer.calls != 0 {
		t.Fatalf("terminal dialed %d times before authorization", dialer.calls)
	}
}

func TestWebTerminalSameNamedEnvironmentCannotCrossNamespace(t *testing.T) {
	dialer := &terminalTestDialer{}
	access := &fakeAccess{allow: func(resource ResourceAccess) bool { return resource.Namespace == "project-a" }}
	handler := NewServer(nil, ServerOptions{Access: access, TerminalDialer: dialer}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/project-b/environments/shared/terminal", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if dialer.calls != 0 {
		t.Fatal("cross-namespace terminal request reached dialer")
	}
}

func TestWebSocketOriginPolicy(t *testing.T) {
	tests := []struct {
		name           string
		host           string
		origin         string
		forwardedHost  string
		forwardedProto string
		trustProxy     bool
		want           bool
	}{
		{name: "non-browser bearer client", host: "control.internal", want: true},
		{name: "same origin", host: "console.example.com", origin: "http://console.example.com", want: true},
		{name: "scheme mismatch", host: "console.example.com", origin: "https://console.example.com", want: false},
		{name: "cross origin", host: "console.example.com", origin: "http://evil.example.com", want: false},
		{name: "same origin behind trusted proxy", host: "control.internal", origin: "https://console.example.com", forwardedHost: "console.example.com", forwardedProto: "https", trustProxy: true, want: true},
		{name: "forwarded headers ignored by default", host: "control.internal", origin: "https://console.example.com", forwardedHost: "console.example.com", forwardedProto: "https", want: false},
		{name: "cross origin behind proxy", host: "control.internal", origin: "https://evil.example.com", forwardedHost: "console.example.com", forwardedProto: "https", trustProxy: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "http://"+tt.host+"/terminal", nil)
			request.Header.Set("Authorization", "Bearer reader")
			request.Header.Set("Origin", tt.origin)
			request.Header.Set("X-Forwarded-Host", tt.forwardedHost)
			request.Header.Set("X-Forwarded-Proto", tt.forwardedProto)
			server := &Server{trustProxy: tt.trustProxy}
			if got := server.checkWebSocketOrigin(request); got != tt.want {
				t.Fatalf("checkWebSocketOrigin() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestWebTerminalRejectsCookieWithoutOriginAndCrossOriginBeforeDial(t *testing.T) {
	for _, origin := range []string{"", "https://evil.example.com"} {
		dialer := &terminalTestDialer{}
		handler := NewServer(nil, ServerOptions{Access: &fakeAccess{}, TerminalDialer: dialer}).Handler()
		request := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/project-a/environments/env-1/terminal", nil)
		setWebSocketUpgrade(request)
		request.Header.Del("Authorization")
		request.Header.Set("Origin", origin)
		request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "reader-session"})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("origin %q status = %d, want %d", origin, response.Code, http.StatusForbidden)
		}
		if dialer.calls != 0 {
			t.Fatalf("origin %q reached terminal dialer", origin)
		}
	}
}

func setWebSocketUpgrade(request *http.Request) {
	request.Header.Set("Authorization", "Bearer reader")
	request.Header.Set("Connection", "upgrade")
	request.Header.Set("Upgrade", "websocket")
}

func TestKubernetesTerminalDialerRejectsPodOwnedByAnotherEnvironmentIncarnation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env-1", Namespace: "project-1", UID: "current-environment"},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "default"},
		Status: platformv1alpha1.EnvironmentStatus{
			Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-env-1", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "192.0.2.10:50051"},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, Reason: "SandboxdReady", Message: "sandboxd is ready"}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "env-env-1", Namespace: "project-1",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: "env-1", UID: "old-environment", Controller: ptrTo(true),
			}},
		},
		Status: corev1.PodStatus{PodIP: "192.0.2.10"},
	}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "project-1"}}
	dialer := KubernetesTerminalDialer{Client: fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(environment).WithObjects(environment, pod, template).Build()}

	_, _, _, err := dialer.DialTerminal(context.Background(), "project-1", "env-1")
	if err == nil || !strings.Contains(err.Error(), "not owned by the current environment") {
		t.Fatalf("DialTerminal() error = %v, want stale pod rejection", err)
	}
}

func ptrTo[T any](value T) *T { return &value }

type terminalTestDialer struct {
	mu          sync.Mutex
	client      sandboxdv1.TerminalServiceClient
	health      sandboxdv1.HealthServiceClient
	closer      io.Closer
	namespace   string
	environment string
	calls       int
}

func (d *terminalTestDialer) DialTerminal(_ context.Context, namespace, environment string) (sandboxdv1.TerminalServiceClient, sandboxdv1.HealthServiceClient, io.Closer, error) {
	d.mu.Lock()
	d.calls++
	d.namespace = namespace
	d.environment = environment
	d.mu.Unlock()
	health := d.health
	if health == nil {
		health = terminalTestHealthClient{}
	}
	closer := d.closer
	if closer == nil {
		closer = io.NopCloser(strings.NewReader(""))
	}
	return d.client, health, closer, nil
}

type terminalTestServer struct {
	sandboxdv1.UnimplementedTerminalServiceServer
	sandboxdv1.UnimplementedHealthServiceServer
	open   *sandboxdv1.TerminalOpen
	resize *sandboxdv1.TerminalResize
	input  []byte
}

func (*terminalTestServer) Check(context.Context, *sandboxdv1.HealthCheckRequest) (*sandboxdv1.HealthCheckResponse, error) {
	return &sandboxdv1.HealthCheckResponse{Ok: true}, nil
}

func (s *terminalTestServer) Terminal(stream sandboxdv1.TerminalService_TerminalServer) error {
	message, err := stream.Recv()
	if err != nil {
		return err
	}
	s.open = message.GetOpen()
	message, err = stream.Recv()
	if err != nil {
		return err
	}
	s.resize = message.GetResize()
	message, err = stream.Recv()
	if err != nil {
		return err
	}
	s.input = append([]byte(nil), message.GetData()...)
	return stream.Send(&sandboxdv1.TerminalMessage{Kind: &sandboxdv1.TerminalMessage_Data{Data: []byte("terminal output")}})
}

type terminalCancelServer struct {
	sandboxdv1.UnimplementedTerminalServiceServer
}

func (*terminalCancelServer) Terminal(stream sandboxdv1.TerminalService_TerminalServer) error {
	message, err := stream.Recv()
	if err != nil {
		return err
	}
	if message.GetOpen() == nil {
		return errors.New("missing terminal open")
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

type terminalTestHealthClient struct {
	check func(context.Context) (*sandboxdv1.HealthCheckResponse, error)
}

func (c terminalTestHealthClient) Check(ctx context.Context, _ *sandboxdv1.HealthCheckRequest, _ ...grpc.CallOption) (*sandboxdv1.HealthCheckResponse, error) {
	if c.check != nil {
		return c.check(ctx)
	}
	return &sandboxdv1.HealthCheckResponse{Ok: true}, nil
}
