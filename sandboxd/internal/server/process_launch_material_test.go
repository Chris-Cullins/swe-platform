package server

import (
	"bytes"
	"context"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const launchFixture = "!!LAUNCH-MATERIAL-FIXTURE!!"

func launchRequest(key string, env map[string][]byte) *sandboxdv1.StartProcessWithLaunchMaterialRequest {
	return &sandboxdv1.StartProcessWithLaunchMaterialRequest{
		Key:            &sandboxdv1.ProcessKey{OwnerId: key, Role: "agent"},
		Spec:           &sandboxdv1.ProcessSpec{Argv: []string{os.Args[0], "-test.run=^TestLaunchMaterialChild$"}, EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_REPLACE},
		LaunchMaterial: &sandboxdv1.LaunchMaterial{SecretEnv: env},
	}
}

func TestLaunchMaterialChild(t *testing.T) {
	if os.Getenv("LAUNCH_TEST_REPORT_PRESENCE") == "1" {
		if _, ok := os.LookupEnv("SAND_TOKEN"); ok {
			_, _ = os.Stdout.WriteString("present")
		}
		os.Exit(0)
	}
	if value, ok := os.LookupEnv("SAND_TOKEN"); ok {
		_, _ = os.Stdout.WriteString(value)
		os.Exit(0)
	}
}

func TestLaunchMaterialDeliveryRedactionAndCleanup(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	value := []byte(launchFixture)
	p, err := s.StartWithLaunchMaterial(context.Background(), launchRequest("delivery", map[string][]byte{"SAND_TOKEN": value}))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains([]byte(p.String()), []byte(launchFixture)) || len(p.Spec.Env) != 0 {
		t.Fatalf("public process leaked launch material: %v", p)
	}
	if !bytes.Equal(value, make([]byte, len(value))) {
		t.Fatal("request launch-material value was not cleared")
	}
	s.mu.Lock()
	managed := s.processes[processKey{"delivery", "agent"}]
	if managed == nil || !managed.secretLaunch || managed.cmd == nil || managed.cmd.Env != nil {
		t.Fatalf("unexpected retained process launch state: %#v", managed)
	}
	s.mu.Unlock()

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, readErr := s.ReadOutput(context.Background(), &sandboxdv1.ReadOutputRequest{Key: p.Key, ExecutionId: p.ExecutionId, Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, MaxBytes: 1024})
		if readErr != nil {
			t.Fatal(readErr)
		}
		if got.Eof {
			if string(got.Data) != launchFixture {
				t.Fatalf("child received %q", got.Data)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for child output")
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := s.Get(context.Background(), &sandboxdv1.GetProcessRequest{Key: p.Key})
	if err != nil || bytes.Contains([]byte(got.String()), []byte(launchFixture)) {
		t.Fatalf("Get leaked launch material: process=%v err=%v", got, err)
	}
	stopped, err := s.Stop(context.Background(), &sandboxdv1.StopProcessRequest{Key: p.Key, Mode: sandboxdv1.StopMode_STOP_MODE_FORCE})
	if err != nil || bytes.Contains([]byte(stopped.String()), []byte(launchFixture)) {
		t.Fatalf("Stop leaked launch material: process=%v err=%v", stopped, err)
	}
}

func TestLaunchMaterialOverridesInheritedEnvironment(t *testing.T) {
	t.Setenv("SAND_TOKEN", "!!AMBIENT-VALUE-MUST-NOT-WIN!!")
	s := NewProcessServer(t.TempDir())
	req := launchRequest("override", map[string][]byte{"SAND_TOKEN": []byte(launchFixture)})
	req.Spec.EnvMode = sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT
	p, err := s.StartWithLaunchMaterial(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, readErr := s.ReadOutput(context.Background(), &sandboxdv1.ReadOutputRequest{Key: p.Key, ExecutionId: p.ExecutionId, Stream: sandboxdv1.OutputStream_OUTPUT_STREAM_STDOUT, MaxBytes: 1024})
		if readErr != nil {
			t.Fatal(readErr)
		}
		if got.Eof {
			if string(got.Data) != launchFixture {
				t.Fatalf("child inherited the wrong environment value: %q", got.Data)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for child output")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLaunchMaterialIsNotAmbientForOrdinaryExec(t *testing.T) {
	original, wasSet := os.LookupEnv("SAND_TOKEN")
	if err := os.Unsetenv("SAND_TOKEN"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv("SAND_TOKEN", original)
		} else {
			_ = os.Unsetenv("SAND_TOKEN")
		}
	})

	connection := newConn(t, t.TempDir())
	if _, err := sandboxdv1.NewProcessServiceClient(connection).StartWithLaunchMaterial(context.Background(), launchRequest("isolated", map[string][]byte{"SAND_TOKEN": []byte(launchFixture)})); err != nil {
		t.Fatal(err)
	}
	stream, err := sandboxdv1.NewExecServiceClient(connection).Exec(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&sandboxdv1.ExecRequest{Kind: &sandboxdv1.ExecRequest_Start{Start: &sandboxdv1.ExecStart{
		Argv: []string{os.Args[0], "-test.run=^TestLaunchMaterialChild$"},
		Env:  map[string]string{"LAUNCH_TEST_REPORT_PRESENCE": "1"},
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var stdout []byte
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if output := response.GetStdout(); output != nil {
			stdout = append(stdout, output.Data...)
		}
	}
	if len(stdout) != 0 {
		t.Fatalf("ordinary Exec inherited launch material: %q", stdout)
	}
}

func TestLaunchMaterialIdempotencyAndModeFencing(t *testing.T) {
	s := NewProcessServer(t.TempDir())
	first, err := s.StartWithLaunchMaterial(context.Background(), launchRequest("secret", map[string][]byte{"SAND_TOKEN": []byte("!!FIRST!!")}))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := s.StartWithLaunchMaterial(context.Background(), launchRequest("secret", map[string][]byte{"SAND_TOKEN": []byte("!!ROTATED!!")}))
	if err != nil || retry.ExecutionId != first.ExecutionId {
		t.Fatalf("rotated retry: process=%v err=%v", retry, err)
	}
	plain := &sandboxdv1.StartProcessRequest{Key: first.Key, Spec: first.Spec}
	if _, err := s.Start(context.Background(), plain); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("secret-to-plain collision = %v", err)
	}
	plain.Key = &sandboxdv1.ProcessKey{OwnerId: "plain", Role: "agent"}
	p, err := s.Start(context.Background(), plain)
	if err != nil {
		t.Fatal(err)
	}
	secret := launchRequest("plain", map[string][]byte{"SAND_TOKEN": []byte("!!OTHER!!")})
	secret.Spec = p.Spec
	if _, err := s.StartWithLaunchMaterial(context.Background(), secret); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("plain-to-secret collision = %v", err)
	}
}

func TestLaunchMaterialValidationBeforePublication(t *testing.T) {
	tests := map[string]map[string][]byte{
		"invalid name": {"BAD-NAME": []byte("x")},
		"invalid utf8": {string([]byte{0xff}): []byte("x")},
		"nul value":    {"GOOD_NAME": []byte{'x', 0}},
		"long name":    {"A" + string(bytes.Repeat([]byte{'A'}, maxLaunchMaterialName)): []byte("x")},
		"long value":   {"GOOD_NAME": bytes.Repeat([]byte{'x'}, maxLaunchMaterialValue+1)},
	}
	many := make(map[string][]byte, maxLaunchMaterialEntries+1)
	for i := 0; i <= maxLaunchMaterialEntries; i++ {
		many["ENTRY_"+string(rune('A'+i))] = []byte("x")
	}
	tests["too many"] = many
	aggregate := make(map[string][]byte)
	for _, name := range []string{"BIG_A", "BIG_B", "BIG_C", "BIG_D", "BIG_E"} {
		aggregate[name] = bytes.Repeat([]byte{'x'}, 60*1024)
	}
	tests["aggregate"] = aggregate
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewProcessServer(t.TempDir())
			if _, err := s.StartWithLaunchMaterial(context.Background(), launchRequest(name, env)); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v", err)
			}
			if len(s.processes) != 0 {
				t.Fatal("invalid launch was published")
			}
		})
	}
	s := NewProcessServer(t.TempDir())
	req := launchRequest("conflict", map[string][]byte{"SAND_TOKEN": []byte("x")})
	req.Spec.Env = map[string]string{"SAND_TOKEN": "public"}
	if _, err := s.StartWithLaunchMaterial(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("public conflict = %v", err)
	}
	caseEnv := map[string][]byte{"SAND_TOKEN": []byte("x"), "sand_token": []byte("y")}
	err := validateLaunchMaterial(caseEnv, nil)
	if runtime.GOOS == "windows" && status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Windows case collision = %v", err)
	}
	if runtime.GOOS != "windows" && err != nil {
		t.Fatalf("case-distinct Unix names rejected: %v", err)
	}
}
