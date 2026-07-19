package controlplane

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
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
	server := httptest.NewServer(NewServer(nil, dialer).Handler())
	t.Cleanup(server.Close)

	websocketURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/environments/env-1/terminal?namespace=project-1"
	websocketConnection, _, err := websocket.DefaultDialer.Dial(websocketURL, nil)
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
	request := httptest.NewRequest("GET", "/api/v1/environments/env-1/terminal", nil)
	response := httptest.NewRecorder()
	NewServer(nil).Handler().ServeHTTP(response, request)
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

type terminalTestDialer struct {
	mu          sync.Mutex
	client      sandboxdv1.TerminalServiceClient
	namespace   string
	environment string
}

func (d *terminalTestDialer) DialTerminal(_ context.Context, namespace, environment string) (sandboxdv1.TerminalServiceClient, io.Closer, error) {
	d.mu.Lock()
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
