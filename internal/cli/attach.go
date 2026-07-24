package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/gorilla/websocket"
	"github.com/muesli/cancelreader"
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
	client, err := controlplaneclient.New(controlPlaneURL, token, nil)
	if err != nil {
		return err
	}
	return attachTerminalWithClient(cmd.Context(), client, namespace, envName, cmd.InOrStdin(), cmd.OutOrStdout())
}

func attachTerminalWithClient(ctx context.Context, client *controlplaneclient.Client, namespace, environment string, input io.Reader, output io.Writer) error {
	if err := validateTerminalInput(input); err != nil {
		return err
	}
	endpoint := client.WebSocketEndpoint("api", "v1", "namespaces", namespace, "environments", environment, "terminal")
	connection, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, client.AuthorizationHeader())
	if err != nil {
		if response != nil {
			return fmt.Errorf("connect to environment terminal: control plane returned %s", response.Status)
		}
		return fmt.Errorf("connect to environment terminal: %w", err)
	}
	defer connection.Close()
	return bridgeTerminal(ctx, connection, input, output)
}

func terminalGatewayURL(baseURL, namespace, environment string) (string, error) {
	return controlplaneclient.WebSocketEndpoint(baseURL, "api", "v1", "namespaces", namespace, "environments", environment, "terminal")
}

func bridgeTerminal(ctx context.Context, connection *websocket.Conn, input io.Reader, output io.Writer) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
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
	cancelInput, err := cancelreader.NewReader(input)
	if err != nil {
		return fmt.Errorf("prepare terminal input: %w", err)
	}
	defer cancelInput.Close()

	type inputResult struct {
		data   []byte
		err    error
		detach bool
	}
	inputCh := make(chan inputResult)
	inputDone := make(chan struct{})
	go func() {
		defer close(inputDone)
		buf := make([]byte, 32*1024)
		for {
			n, err := cancelInput.Read(buf)
			data := append([]byte(nil), buf[:n]...)
			detachAt := bytes.IndexByte(data, 0x1d)
			result := inputResult{data: data, err: err, detach: detachAt >= 0}
			if detachAt >= 0 {
				result.data = data[:detachAt]
			}
			select {
			case inputCh <- result:
			case <-streamCtx.Done():
				return
			}
			if err != nil || result.detach {
				return
			}
		}
	}()
	resizeCh := make(chan os.Signal, 1)
	if terminalFile != nil {
		signal.Notify(resizeCh, syscall.SIGWINCH)
		defer signal.Stop(resizeCh)
	}
	writeDone := make(chan error, 1)
	go func() {
		terminalInput := inputCh
		for {
			select {
			case <-streamCtx.Done():
				writeDone <- nil
				return
			case result := <-terminalInput:
				if len(result.data) > 0 {
					if err := connection.WriteMessage(websocket.BinaryMessage, result.data); err != nil {
						writeDone <- fmt.Errorf("send terminal input: %w", err)
						return
					}
				}
				if result.detach {
					_ = connection.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
					writeDone <- nil
					return
				}
				if result.err != nil {
					terminalInput = nil
				}
			case <-resizeCh:
				width, height, err := term.GetSize(int(terminalFile.Fd()))
				if err == nil {
					if err := connection.WriteJSON(map[string]any{"type": "resize", "cols": width, "rows": height}); err != nil {
						writeDone <- fmt.Errorf("resize terminal: %w", err)
						return
					}
				}
			}
		}
	}()

	readDone := make(chan error, 1)
	go func() {
		for {
			messageType, message, err := connection.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					readDone <- nil
				} else {
					readDone <- fmt.Errorf("terminal stream: %w", err)
				}
				return
			}
			if messageType != websocket.BinaryMessage {
				continue
			}
			if _, err := output.Write(message); err != nil {
				readDone <- fmt.Errorf("write terminal output: %w", err)
				return
			}
		}
	}()

	var result error
	readFinished, writeFinished := false, false
	select {
	case result = <-readDone:
		readFinished = true
	case result = <-writeDone:
		writeFinished = true
	case <-ctx.Done():
		result = ctx.Err()
	}
	cancel()
	cancelInput.Cancel()
	_ = connection.Close()
	if !readFinished {
		<-readDone
	}
	if !writeFinished {
		<-writeDone
	}
	// Input ownership must be fully returned before Bubble Tea resumes and
	// installs its own reader. A timeout here would abandon a goroutine that
	// could consume future dashboard keystrokes on fallback platforms.
	<-inputDone
	if result != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return result
}

func validateTerminalInput(input io.Reader) error {
	switch value := input.(type) {
	case *os.File:
		if value.Fd() == os.Stdin.Fd() {
			return nil
		}
	case *bytes.Buffer, *bytes.Reader, *strings.Reader:
		return nil
	}
	// Do not accept structural lookalikes: a custom Len method says nothing
	// about whether Read can block or be canceled and joined before the TUI
	// regains terminal input ownership.
	return fmt.Errorf("terminal input must be standard input or a finite in-memory reader")
}
