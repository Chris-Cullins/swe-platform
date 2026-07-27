package server

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func portalTestClient(t *testing.T, controlPort uint32) sandboxdv1.PortalServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	sandboxdv1.RegisterPortalServiceServer(server, NewPortalServer(controlPort))
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough://portal", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return sandboxdv1.NewPortalServiceClient(conn)
}

func TestPortalHandshakeEchoAndHalfClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		request, err := io.ReadAll(conn)
		if err == nil {
			_, err = conn.Write(append([]byte("response:"), request...))
		}
		serverDone <- err
	}()
	stream, err := portalTestClient(t, 50051).Tunnel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	port := uint32(listener.Addr().(*net.TCPAddr).Port)
	if err := stream.Send(&sandboxdv1.PortalFrame{TargetPort: port}); err != nil {
		t.Fatal(err)
	}
	ack, err := stream.Recv()
	if err != nil || !ack.Opened || len(ack.Data) != 0 {
		t.Fatalf("ack = %#v, %v", ack, err)
	}
	if err := stream.Send(&sandboxdv1.PortalFrame{Data: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&sandboxdv1.PortalFrame{WriteEof: true}); err != nil {
		t.Fatal(err)
	}
	frame, err := stream.Recv()
	if err != nil || string(frame.Data) != "response:hello" {
		t.Fatalf("response = %q, %v", frame.GetData(), err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("final receive = %v, want EOF", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestPortalRejectsInvalidControlUnavailableAndOversizedFrames(t *testing.T) {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedPort := uint32(closed.Addr().(*net.TCPAddr).Port)
	_ = closed.Close()
	for name, test := range map[string]struct {
		frame *sandboxdv1.PortalFrame
		want  codes.Code
	}{
		"zero":        {&sandboxdv1.PortalFrame{}, codes.InvalidArgument},
		"control":     {&sandboxdv1.PortalFrame{TargetPort: 50051}, codes.InvalidArgument},
		"client ack":  {&sandboxdv1.PortalFrame{TargetPort: 1234, Opened: true}, codes.InvalidArgument},
		"oversized":   {&sandboxdv1.PortalFrame{TargetPort: 1234, Data: make([]byte, portalMaxFrame+1)}, codes.InvalidArgument},
		"unavailable": {&sandboxdv1.PortalFrame{TargetPort: closedPort}, codes.Unavailable},
	} {
		t.Run(name, func(t *testing.T) {
			stream, err := portalTestClient(t, 50051).Tunnel(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(test.frame); err != nil {
				t.Fatal(err)
			}
			_, err = stream.Recv()
			if status.Code(err) != test.want {
				t.Fatalf("status = %v, want %v", err, test.want)
			}
		})
	}
}
