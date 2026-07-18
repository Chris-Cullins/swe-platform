package controlplane

import (
	"context"
	"io"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func TestWebTerminalBridgesSandboxd(t *testing.T) {
	backend := &terminalTestServer{}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sandboxdv1.RegisterTerminalServiceServer(grpcServer, backend)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	dialer := &terminalTestDialer{client: sandboxdv1.NewTerminalServiceClient(connection)}
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

func TestWebTerminalRequiresDialer(t *testing.T) {
	request := httptest.NewRequest("GET", "/api/v1/namespaces/project-1/environments/env-1/terminal", nil)
	setWebSocketUpgrade(request)
	response := httptest.NewRecorder()
	NewServer(nil, ServerOptions{Access: &fakeAccess{}}).Handler().ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatalf("status = %d, want 503", response.Code)
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

	if err := dialer.markActive(context.Background(), environment); err != nil {
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
		Status:     platformv1alpha1.EnvironmentStatus{PodName: "env-env-1"},
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
	dialer := KubernetesTerminalDialer{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment, pod).Build()}

	_, _, err := dialer.DialTerminal(context.Background(), "project-1", "env-1")
	if err == nil || !strings.Contains(err.Error(), "not owned by the current environment") {
		t.Fatalf("DialTerminal() error = %v, want stale pod rejection", err)
	}
}

func ptrTo[T any](value T) *T { return &value }

type terminalTestDialer struct {
	mu          sync.Mutex
	client      sandboxdv1.TerminalServiceClient
	namespace   string
	environment string
	calls       int
}

func (d *terminalTestDialer) DialTerminal(_ context.Context, namespace, environment string) (sandboxdv1.TerminalServiceClient, io.Closer, error) {
	d.mu.Lock()
	d.calls++
	d.namespace = namespace
	d.environment = environment
	d.mu.Unlock()
	return d.client, io.NopCloser(strings.NewReader("")), nil
}

type terminalTestServer struct {
	sandboxdv1.UnimplementedTerminalServiceServer
	open   *sandboxdv1.TerminalOpen
	resize *sandboxdv1.TerminalResize
	input  []byte
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
