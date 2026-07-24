package cli

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

func TestTerminalGatewayURL(t *testing.T) {
	tests := []struct {
		base string
		want string
	}{
		{base: "http://control.example", want: "ws://control.example/api/v1/namespaces/project-a/environments/env-1/terminal"},
		{base: "https://control.example/platform/", want: "wss://control.example/platform/api/v1/namespaces/project-a/environments/env-1/terminal"},
		{base: "HTTPS://control.example", want: "wss://control.example/api/v1/namespaces/project-a/environments/env-1/terminal"},
	}
	for _, test := range tests {
		got, err := terminalGatewayURL(test.base, "project-a", "env-1")
		if err != nil {
			t.Fatalf("terminalGatewayURL(%q): %v", test.base, err)
		}
		if got != test.want {
			t.Errorf("terminalGatewayURL(%q) = %q, want %q", test.base, got, test.want)
		}
	}
	for _, invalid := range []string{"", "ftp://control.example", "https://control.example?token=secret"} {
		if _, err := terminalGatewayURL(invalid, "project-a", "env-1"); err == nil {
			t.Errorf("terminalGatewayURL(%q) succeeded", invalid)
		}
	}
}

func TestAttachTerminalUsesAuthenticatedGateway(t *testing.T) {
	serverErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/namespaces/project-a/environments/env-1/terminal" || r.Header.Get("Authorization") != "Bearer terminal-token" {
			serverErrors <- errors.New("gateway request did not carry the environment identity and bearer token")
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer connection.Close()
		if _, _, err := connection.ReadMessage(); err != nil {
			serverErrors <- err
			return
		}
		messageType, input, err := connection.ReadMessage()
		if err != nil || messageType != websocket.BinaryMessage || string(input) != "echo hello\n" {
			serverErrors <- errors.New("gateway did not receive terminal input")
			return
		}
		if err := connection.WriteMessage(websocket.BinaryMessage, []byte("terminal output")); err != nil {
			serverErrors <- err
			return
		}
		serverErrors <- connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	}))
	defer server.Close()

	command := &cobra.Command{}
	command.SetContext(context.Background())
	command.SetIn(strings.NewReader("echo hello\n"))
	var output bytes.Buffer
	command.SetOut(&output)
	if err := attachTerminal(command, server.URL, "terminal-token", "project-a", "env-1"); err != nil {
		t.Fatalf("attachTerminal() error = %v", err)
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
	if output.String() != "terminal output" {
		t.Fatalf("terminal output = %q", output.String())
	}
}

func TestAttachTerminalReturnsWhenContextIsCancelled(t *testing.T) {
	opened := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if _, _, err := connection.ReadMessage(); err != nil {
			return
		}
		close(opened)
		_, _, _ = connection.ReadMessage()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	command := &cobra.Command{}
	command.SetContext(ctx)
	command.SetIn(strings.NewReader(""))
	done := make(chan error, 1)
	go func() { done <- attachTerminal(command, server.URL, "terminal-token", "default", "env-1") }()
	<-opened
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("attachTerminal did not return after context cancellation")
	}
}

type uncertainCreateClient struct {
	client.Client
	lostResponse bool
}

func (c *uncertainCreateClient) Create(ctx context.Context, object client.Object, opts ...client.CreateOption) error {
	if err := c.Client.Create(ctx, object, opts...); err != nil {
		return err
	}
	if !c.lostResponse {
		c.lostResponse = true
		return errors.New("API response lost after persistence")
	}
	return nil
}

func TestCreateRunIsDeclarativeAndIdempotent(t *testing.T) {
	s := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).Build()
	clients := &kubeClients{Client: c}
	call := func(prompt string) error {
		return createRun(context.Background(), clients, "ns", "stable", "small", "", "", "test", prompt, false, 0)
	}
	if err := call("do it"); err != nil {
		t.Fatal(err)
	}
	if err := call("do it"); err != nil {
		t.Fatalf("same intent: %v", err)
	}
	if err := call("different"); err == nil {
		t.Fatal("mismatched intent succeeded")
	}
	var runs platformv1alpha1.RunList
	if err := c.List(context.Background(), &runs); err != nil {
		t.Fatal(err)
	}
	var envs platformv1alpha1.EnvironmentList
	if err := c.List(context.Background(), &envs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || len(envs.Items) != 0 {
		t.Fatalf("runs=%d environments=%d", len(runs.Items), len(envs.Items))
	}
}

func TestCreateRunRecoversUncertainCreateResponse(t *testing.T) {
	s := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	base := fake.NewClientBuilder().WithScheme(s).Build()
	clients := &kubeClients{Client: &uncertainCreateClient{Client: base}}
	if err := createRun(context.Background(), clients, "ns", "stable-timeout", "small", "", "", "test", "do it", false, 0); err != nil {
		t.Fatalf("uncertain create was not recovered: %v", err)
	}
	var runs platformv1alpha1.RunList
	if err := base.List(context.Background(), &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Name != "stable-timeout" {
		t.Fatalf("runs = %#v", runs.Items)
	}
}
