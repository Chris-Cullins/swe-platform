package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const portalMaxFrame = 64 * 1024

var portalLoopbacks = []string{"127.0.0.1", "::1"}

// PortalServer tunnels bytes only to logical loopback. The semaphore bounds
// daemon-wide tunnel concurrency and is deliberately independent of HTTP.
type PortalServer struct {
	sandboxdv1.UnimplementedPortalServiceServer
	controlPort uint32
	active      chan struct{}
	dialContext dialContextFunc
}

func NewPortalServer(controlPort uint32) *PortalServer {
	dialer := &net.Dialer{}
	return &PortalServer{controlPort: controlPort, active: make(chan struct{}, 64), dialContext: dialer.DialContext}
}

func (s *PortalServer) Tunnel(stream sandboxdv1.PortalService_TunnelServer) error {
	select {
	case s.active <- struct{}{}:
		defer func() { <-s.active }()
	default:
		return status.Error(codes.ResourceExhausted, "portal tunnel limit reached")
	}
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "initial portal frame required")
	}
	if first.TargetPort == 0 || first.TargetPort > 65535 || first.TargetPort == s.controlPort || len(first.Data) > portalMaxFrame || first.Opened {
		return status.Error(codes.InvalidArgument, "invalid portal target or frame")
	}
	conn, err := s.dialLoopback(stream.Context(), first.TargetPort)
	if err != nil {
		return status.Error(codes.Unavailable, "portal target unavailable")
	}
	defer conn.Close()
	// A target that stops reading can block a synchronous write. Closing the
	// connection is the portable way to unblock both directions on cancellation
	// and ensures the bounded tunnel slot is released.
	stopCloseOnCancel := context.AfterFunc(stream.Context(), func() { _ = conn.Close() })
	defer stopCloseOnCancel()
	// This acknowledgement is the open handshake: clients must not expose a
	// connection before receiving it.
	if err := stream.Send(&sandboxdv1.PortalFrame{Opened: true}); err != nil {
		return err
	}
	if len(first.Data) > 0 {
		if _, err := conn.Write(first.Data); err != nil {
			return status.Error(codes.Unavailable, "portal target write failed")
		}
	}
	if first.WriteEof {
		closeWrite(conn)
	}

	targetDone := make(chan error, 1)
	go func() {
		buf := make([]byte, portalMaxFrame)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if sendErr := stream.Send(&sandboxdv1.PortalFrame{Data: append([]byte(nil), buf[:n]...)}); sendErr != nil {
					targetDone <- sendErr
					return
				}
			}
			if err != nil {
				targetDone <- err
				return
			}
		}
	}()
	type receivedFrame struct {
		frame *sandboxdv1.PortalFrame
		err   error
	}
	received := make(chan receivedFrame, 1)
	go func() {
		for {
			frame, err := stream.Recv()
			select {
			case received <- receivedFrame{frame: frame, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	clientWriteClosed := first.WriteEof
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err = <-targetDone:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case item := <-received:
			if item.err != nil {
				if !errors.Is(item.err, io.EOF) {
					return item.err
				}
				if !clientWriteClosed {
					closeWrite(conn)
					clientWriteClosed = true
				}
				continue
			}
			frame := item.frame
			if frame.TargetPort != 0 || len(frame.Data) > portalMaxFrame || frame.Opened || clientWriteClosed {
				return status.Error(codes.InvalidArgument, "invalid portal frame")
			}
			if len(frame.Data) > 0 {
				if _, err := conn.Write(frame.Data); err != nil {
					return status.Error(codes.Unavailable, "portal target write failed")
				}
			}
			if frame.WriteEof {
				closeWrite(conn)
				clientWriteClosed = true
			}
		}
	}
}

func (s *PortalServer) dialLoopback(ctx context.Context, port uint32) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var lastErr error
	for _, loopback := range portalLoopbacks {
		conn, err := s.dialContext(dialCtx, "tcp", net.JoinHostPort(loopback, strconv.Itoa(int(port))))
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if dialCtx.Err() != nil {
			break
		}
	}
	return nil, lastErr
}

func closeWrite(conn net.Conn) {
	if c, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
	}
}
