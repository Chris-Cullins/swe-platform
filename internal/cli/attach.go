package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func newAttachCommand() *cobra.Command {
	var controlPlaneURL string
	var token string
	cmd := &cobra.Command{
		Use:   "attach <environment>",
		Short: "Attach a terminal to an environment through the control plane",
		Long:  "Attach to an environment's shared terminal through the authenticated control-plane gateway. Press Ctrl-] to detach.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			return attachTerminal(cmd, controlPlaneURL, token, namespace, args[0])
		},
	}
	cmd.Flags().StringVar(&controlPlaneURL, "control-plane", os.Getenv("SWE_CONTROL_PLANE_URL"), "Control-plane base URL (or SWE_CONTROL_PLANE_URL)")
	cmd.Flags().StringVar(&token, "token", os.Getenv("SWE_CONTROL_PLANE_TOKEN"), "Control-plane bearer token (or SWE_CONTROL_PLANE_TOKEN)")
	return cmd
}

func attachTerminal(cmd *cobra.Command, controlPlaneURL, token, namespace, envName string) error {
	endpoint, err := terminalGatewayURL(controlPlaneURL, namespace, envName)
	if err != nil {
		return err
	}
	if token == "" {
		return fmt.Errorf("control-plane token is required (set --token or SWE_CONTROL_PLANE_TOKEN)")
	}
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	connection, response, err := websocket.DefaultDialer.DialContext(cmd.Context(), endpoint, header)
	if err != nil {
		if response != nil {
			return fmt.Errorf("connect to environment terminal: control plane returned %s", response.Status)
		}
		return fmt.Errorf("connect to environment terminal: %w", err)
	}
	defer connection.Close()
	return bridgeTerminal(cmd.Context(), connection, cmd.InOrStdin(), cmd.OutOrStdout())
}

func terminalGatewayURL(baseURL, namespace, environment string) (string, error) {
	if baseURL == "" {
		return "", fmt.Errorf("control-plane URL is required (set --control-plane or SWE_CONTROL_PLANE_URL)")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse control-plane URL: %w", err)
	}
	if parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("control-plane URL must be an HTTP(S) base URL without a query or fragment")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", fmt.Errorf("control-plane URL scheme must be http or https")
	}
	parsed.Path = path.Join(parsed.Path, "api/v1/namespaces", namespace, "environments", environment, "terminal")
	return parsed.String(), nil
}

func bridgeTerminal(ctx context.Context, connection *websocket.Conn, input io.Reader, output io.Writer) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-streamCtx.Done()
		_ = connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = connection.Close()
	}()
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

	if err := connection.WriteJSON(map[string]any{"type": "open", "cols": cols, "rows": rows}); err != nil {
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
						_ = connection.WriteMessage(websocket.BinaryMessage, result.data[:detachAt])
					}
					_ = connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
					cancel()
					return
				}
				if len(result.data) > 0 {
					if connection.WriteMessage(websocket.BinaryMessage, result.data) != nil {
						return
					}
				}
				if result.err != nil {
					return
				}
			case <-resizeCh:
				width, height, err := term.GetSize(int(terminalFile.Fd()))
				if err == nil {
					if connection.WriteJSON(map[string]any{"type": "resize", "cols": width, "rows": height}) != nil {
						return
					}
				}
			}
		}
	}()

	for {
		messageType, message, err := connection.ReadMessage()
		if err != nil {
			if streamCtx.Err() != nil {
				return nil
			}
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}
			return fmt.Errorf("terminal stream: %w", err)
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		if _, err := output.Write(message); err != nil {
			return fmt.Errorf("write terminal output: %w", err)
		}
	}
}
