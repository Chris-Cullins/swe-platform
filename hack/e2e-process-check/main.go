package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"

	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const ordinaryMarker = "ordinary-process-credential-absent"

const listenerScript = `const http=require("http"),crypto=require("crypto"); const server=http.createServer((request,response)=>{response.setHeader("Content-Type","application/json");response.end(JSON.stringify({marker:"portal-listener",authorization:request.headers.authorization||"",cookie:request.headers.cookie||"",connection:request.headers.connection||"",portalHeader:request.headers["x-portal-check"]||""}))}); server.on("upgrade",(request,socket)=>{const key=request.headers["sec-websocket-key"];if(!key){socket.destroy();return}const accept=crypto.createHash("sha1").update(key+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "+accept+"\r\n\r\n")});server.listen(Number(process.argv[1]),"127.0.0.1")`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sandboxd e2e process check failed:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) >= 2 && (os.Args[1] == "service-start" || os.Args[1] == "service-stop" || os.Args[1] == "service-state") {
		return runServiceProcess(os.Args[1:])
	}
	if len(os.Args) != 6 {
		return fmt.Errorf("usage: e2e-process-check ADDRESS SERVER_NAME CERT TOKEN RUN_UID")
	}
	forbidden, err := io.ReadAll(io.LimitReader(os.Stdin, 32*1024))
	if err != nil || len(forbidden) == 0 {
		return fmt.Errorf("read nonempty forbidden fixture")
	}
	defer clear(forbidden)
	certificate, err := os.ReadFile(os.Args[3])
	if err != nil {
		return fmt.Errorf("read trust certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return fmt.Errorf("parse trust certificate")
	}
	token, err := os.ReadFile(os.Args[4])
	if err != nil {
		return fmt.Errorf("read process capability: %w", err)
	}
	defer clear(token)
	connection, err := grpc.NewClient(os.Args[1],
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs: roots, ServerName: os.Args[2], MinVersion: tls.VersionTLS13,
		})),
		grpc.WithPerRPCCredentials(sandboxdauth.BearerCredentials{Token: strings.TrimSpace(string(token))}),
	)
	if err != nil {
		return fmt.Errorf("create process client: %w", err)
	}
	defer connection.Close()
	client := sandboxdv1.NewProcessServiceClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	selected, err := client.Get(ctx, &sandboxdv1.GetProcessRequest{Key: &sandboxdv1.ProcessKey{OwnerId: os.Args[5], Role: "agent"}})
	if err != nil {
		return fmt.Errorf("get selected process: %w", err)
	}
	if err := checkPublicProcess(selected, forbidden); err != nil {
		return fmt.Errorf("selected process: %w", err)
	}

	ordinary, err := client.Start(ctx, &sandboxdv1.StartProcessRequest{
		Key: &sandboxdv1.ProcessKey{OwnerId: "e2e-ordinary-" + os.Args[5], Role: "credential-check"},
		Spec: &sandboxdv1.ProcessSpec{
			Argv: []string{"sh", "-c", `if [ -n "${ANTHROPIC_API_KEY+x}" ] || [ -n "${AMP_API_KEY+x}" ]; then exit 86; fi; printf ordinary-process-credential-absent`},
		},
	})
	if err != nil {
		return fmt.Errorf("start ordinary process: %w", err)
	}
	if err := checkPublicProcess(ordinary, forbidden); err != nil {
		return fmt.Errorf("ordinary Start response: %w", err)
	}
	key := ordinary.Key
	for ordinary.State == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING || ordinary.State == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING {
		time.Sleep(25 * time.Millisecond)
		ordinary, err = client.Get(ctx, &sandboxdv1.GetProcessRequest{Key: key})
		if err != nil {
			return fmt.Errorf("get ordinary process: %w", err)
		}
		if err := checkPublicProcess(ordinary, forbidden); err != nil {
			return fmt.Errorf("ordinary process: %w", err)
		}
	}
	if ordinary.State != sandboxdv1.ProcessState_PROCESS_STATE_EXITED || ordinary.ExitCode == nil || ordinary.GetExitCode() != 0 {
		return fmt.Errorf("ordinary process did not exit successfully")
	}

	var output []byte
	var offset uint64
	for {
		page, err := client.ReadOutput(ctx, &sandboxdv1.ReadOutputRequest{
			Key: key, ExecutionId: ordinary.ExecutionId, Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT,
			Offset: offset, MaxBytes: 4096,
		})
		if err != nil {
			return fmt.Errorf("read ordinary process output: %w", err)
		}
		if bytes.Contains(page.Data, forbidden) {
			return fmt.Errorf("ordinary process output exposed launch material")
		}
		output = append(output, page.Data...)
		offset = page.NextOffset
		if page.Eof {
			break
		}
	}
	if string(output) != ordinaryMarker {
		return fmt.Errorf("ordinary process did not report credential absence")
	}
	return nil
}

func runServiceProcess(args []string) error {
	wantArgs := 7
	if args[0] == "service-start" || args[0] == "service-state" {
		wantArgs = 8
	}
	if len(args) != wantArgs {
		return fmt.Errorf("usage: e2e-process-check service-{start|stop|state} ADDRESS SERVER_NAME CERT TOKEN OWNER ROLE [PORT|EXPECTED_STATE]")
	}
	certificate, err := os.ReadFile(args[3])
	if err != nil {
		return fmt.Errorf("read trust certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return fmt.Errorf("parse trust certificate")
	}
	token, err := os.ReadFile(args[4])
	if err != nil {
		return fmt.Errorf("read process capability: %w", err)
	}
	defer clear(token)
	connection, err := grpc.NewClient(args[1],
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: args[2], MinVersion: tls.VersionTLS13})),
		grpc.WithPerRPCCredentials(sandboxdauth.BearerCredentials{Token: strings.TrimSpace(string(token))}),
	)
	if err != nil {
		return fmt.Errorf("create process client: %w", err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	processes := sandboxdv1.NewProcessServiceClient(connection)
	key := &sandboxdv1.ProcessKey{OwnerId: args[5], Role: args[6]}
	if args[0] == "service-state" {
		process, err := processes.Get(ctx, &sandboxdv1.GetProcessRequest{Key: key})
		if err != nil {
			return fmt.Errorf("get managed service: %w", err)
		}
		running := process.State == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING
		if args[7] == "running" && !running {
			return fmt.Errorf("managed service is %s, want running", process.State)
		}
		if args[7] == "stopped" && (running || process.State == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING) {
			return fmt.Errorf("managed service is %s, want stopped", process.State)
		}
		if args[7] != "running" && args[7] != "stopped" {
			return fmt.Errorf("expected state must be running or stopped")
		}
		return nil
	}
	if args[0] == "service-stop" {
		process, err := processes.Stop(ctx, &sandboxdv1.StopProcessRequest{Key: key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE})
		if err != nil {
			return fmt.Errorf("stop listener: %w", err)
		}
		for process.State == sandboxdv1.ProcessState_PROCESS_STATE_RUNNING || process.State == sandboxdv1.ProcessState_PROCESS_STATE_STOPPING {
			time.Sleep(25 * time.Millisecond)
			process, err = processes.Get(ctx, &sandboxdv1.GetProcessRequest{Key: key})
			if err != nil {
				return fmt.Errorf("get stopping listener: %w", err)
			}
		}
		if process.State != sandboxdv1.ProcessState_PROCESS_STATE_EXITED && process.State != sandboxdv1.ProcessState_PROCESS_STATE_FAILED {
			return fmt.Errorf("listener did not stop: %s", process.State)
		}
		return nil
	}
	port := args[7]
	for _, character := range port {
		if character < '0' || character > '9' {
			return fmt.Errorf("invalid listener port")
		}
	}
	process, err := processes.Start(ctx, &sandboxdv1.StartProcessRequest{Key: key, Spec: &sandboxdv1.ProcessSpec{Argv: []string{
		"node", "-e", listenerScript, port,
	}}})
	if err != nil {
		return fmt.Errorf("start listener: %w", err)
	}
	if process.State != sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
		return fmt.Errorf("listener did not start: %s", process.State)
	}
	time.Sleep(250 * time.Millisecond)
	process, err = processes.Get(ctx, &sandboxdv1.GetProcessRequest{Key: key})
	if err != nil {
		return fmt.Errorf("confirm listener: %w", err)
	}
	if process.State != sandboxdv1.ProcessState_PROCESS_STATE_RUNNING {
		return fmt.Errorf("listener exited before observation: %s: %s", process.State, process.Error)
	}
	return nil
}

func checkPublicProcess(process *sandboxdv1.Process, forbidden []byte) error {
	encoded, err := protojson.Marshal(process)
	if err != nil {
		return fmt.Errorf("encode process: %w", err)
	}
	if bytes.Contains(encoded, forbidden) {
		return fmt.Errorf("public Process contains launch material")
	}
	if spec := process.GetSpec(); spec != nil {
		for _, name := range []string{"ANTHROPIC_API_KEY", "AMP_API_KEY", "CODEX_API_KEY", "PORT", "PUBLIC_URL"} {
			if _, exposed := spec.Env[name]; exposed {
				return fmt.Errorf("non-service ProcessSpec contains isolated environment name %q", name)
			}
		}
		for _, value := range spec.Env {
			if bytes.Contains([]byte(value), forbidden) {
				return fmt.Errorf("public ProcessSpec contains launch material")
			}
		}
	}
	return nil
}
