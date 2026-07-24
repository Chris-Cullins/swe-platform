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

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sandboxd e2e process check failed:", err)
		os.Exit(1)
	}
}

func run() error {
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

func checkPublicProcess(process *sandboxdv1.Process, forbidden []byte) error {
	encoded, err := protojson.Marshal(process)
	if err != nil {
		return fmt.Errorf("encode process: %w", err)
	}
	if bytes.Contains(encoded, forbidden) {
		return fmt.Errorf("public Process contains launch material")
	}
	if spec := process.GetSpec(); spec != nil {
		for _, name := range []string{"ANTHROPIC_API_KEY", "AMP_API_KEY"} {
			if _, exposed := spec.Env[name]; exposed {
				return fmt.Errorf("public ProcessSpec contains secret environment name")
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
