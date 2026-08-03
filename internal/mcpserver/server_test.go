package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
)

type fakeControlPlaneClient struct {
	createdNamespace string
	createdRequest   controlplane.CreateRunRequest
	createResult     controlplane.Run
	createErr        error
	streamNamespace  string
	streamRun        string
	streamRunUID     string
	streamCursor     string
	streamEvents     []controlplaneclient.SSEEvent
	streamCalls      int
	streamErr        error
}

func (f *fakeControlPlaneClient) CreateRun(_ context.Context, namespace string, request controlplane.CreateRunRequest) (controlplane.Run, error) {
	f.createdNamespace, f.createdRequest = namespace, request
	return f.createResult, f.createErr
}

func (f *fakeControlPlaneClient) StreamRunTranscript(ctx context.Context, namespace, run, runUID, cursor string, handle func(controlplaneclient.SSEEvent) error) error {
	f.streamCalls++
	f.streamNamespace, f.streamRun, f.streamRunUID, f.streamCursor = namespace, run, runUID, cursor
	if f.streamErr != nil {
		return f.streamErr
	}
	for _, event := range f.streamEvents {
		if err := handle(event); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestMCPCreateRunUsesFixedNamespaceAndReturnsUID(t *testing.T) {
	fake := &fakeControlPlaneClient{createResult: controlplane.Run{Name: "task-1", UID: "run-uid", State: "Allocating"}}
	server := newServer(fake, "team-a")
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	result, err := clientSession.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "create_run",
		Arguments: map[string]any{
			"name": "task-1", "prompt": "do the work", "project": "repo", "repositoryCredential": "GitHubApp",
		},
	})
	if err != nil || result.IsError {
		t.Fatalf("CallTool() = %#v, %v", result, err)
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if fake.createdNamespace != "team-a" || fake.createdRequest.Agent != "claude-code" || fake.createdRequest.Selector.Project != "repo" || fake.createdRequest.RepositoryCredential != "GitHubApp" {
		t.Fatalf("create = %q/%#v", fake.createdNamespace, fake.createdRequest)
	}
	if !strings.Contains(string(encoded), `"namespace":"team-a"`) || !strings.Contains(string(encoded), `"uid":"run-uid"`) || strings.Contains(string(encoded), "do the work") {
		t.Fatalf("structured result = %s", encoded)
	}
}

func TestMCPInputSchemasEnforceBoundsBeforeControlPlaneCalls(t *testing.T) {
	fake := &fakeControlPlaneClient{}
	server := newServer(fake, "team-a")
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer clientSession.Close()

	result, err := clientSession.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "read_transcript",
		Arguments: map[string]any{
			"runName": "task-1", "runUID": "uid", "maxEvents": maxTranscriptEvents + 1,
		},
	})
	if err != nil || result == nil || !result.IsError || fake.streamCalls != 0 {
		t.Fatalf("CallTool() = %#v, %v; stream calls = %d", result, err, fake.streamCalls)
	}
	if text, ok := result.Content[0].(*mcp.TextContent); !ok || !strings.Contains(text.Text, "maximum") {
		t.Fatalf("validation result = %#v", result.Content)
	}
}

func TestReadTranscriptReturnsBoundedOpaqueBatchAndCursor(t *testing.T) {
	fake := &fakeControlPlaneClient{
		streamEvents: []controlplaneclient.SSEEvent{
			{Event: "transcript-gap", Data: []byte(`{"resumeAfter":"gap-cursor"}`)},
			{Event: "transcript", ID: "cursor-2", HasID: true, Data: []byte(`{"source":"adapter","type":"owned","data":{"value":1}}`)},
			{Event: "transcript", ID: "cursor-3", HasID: true, Data: []byte(`{"ignored":true}`)},
		},
	}
	output, err := readTranscript(context.Background(), fake, "team-a", ReadTranscriptInput{
		RunName: "task-1", RunUID: "run-uid", After: "cursor-1", MaxEvents: 2, WaitMilliseconds: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if fake.streamNamespace != "team-a" || fake.streamRun != "task-1" || fake.streamRunUID != "run-uid" || fake.streamCursor != "cursor-1" {
		t.Fatalf("stream = %q/%q uid %q after %q", fake.streamNamespace, fake.streamRun, fake.streamRunUID, fake.streamCursor)
	}
	if len(output.Events) != 2 || !output.LimitReached || output.TimedOut || output.NextCursor != "cursor-2" {
		t.Fatalf("output = %#v", output)
	}
	if output.Events[0].DataJSON != `{"resumeAfter":"gap-cursor"}` || output.Events[1].DataJSON != `{"source":"adapter","type":"owned","data":{"value":1}}` {
		t.Fatalf("opaque events = %#v", output.Events)
	}
}

func TestReadTranscriptRejectsReplacedRunBeforeEvents(t *testing.T) {
	fake := &fakeControlPlaneClient{
		streamErr: errors.New("control plane returned 409 Conflict: Run UID mismatch"),
	}
	output, err := readTranscript(context.Background(), fake, "team-a", ReadTranscriptInput{
		RunName: "task-1", RunUID: "original-uid", MaxEvents: 1, WaitMilliseconds: 10,
	})
	if err == nil || !strings.Contains(err.Error(), "Run UID mismatch") || len(output.Events) != 0 {
		t.Fatalf("output/error = %#v/%v", output, err)
	}
}

func TestReadTranscriptGapAdvancesSafeCursor(t *testing.T) {
	fake := &fakeControlPlaneClient{
		streamEvents: []controlplaneclient.SSEEvent{
			{Event: "transcript-gap", ID: "stale-cursor", Data: []byte(`{"resumeAfter":"safe-cursor","earliestSequence":4}`)},
		},
	}
	output, err := readTranscript(context.Background(), fake, "team-a", ReadTranscriptInput{
		RunName: "task-1", RunUID: "run-uid", MaxEvents: 1, WaitMilliseconds: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Events) != 1 || output.Events[0].ID != "" || output.NextCursor != "safe-cursor" || !output.LimitReached {
		t.Fatalf("gap output = %#v", output)
	}
}

func TestReadTranscriptRejectsMalformedGapCursor(t *testing.T) {
	fake := &fakeControlPlaneClient{
		streamEvents: []controlplaneclient.SSEEvent{{Event: "transcript-gap", Data: []byte(`{"earliestSequence":4}`)}},
	}
	_, err := readTranscript(context.Background(), fake, "team-a", ReadTranscriptInput{
		RunName: "task-1", RunUID: "run-uid", MaxEvents: 1, WaitMilliseconds: 10,
	})
	if err == nil || !strings.Contains(err.Error(), "gap recovery cursor") {
		t.Fatalf("error = %v", err)
	}
}

func TestReadTranscriptDoesNotAdvancePastOmittedGap(t *testing.T) {
	firstData := []byte(`{"payload":"` + strings.Repeat("x", maxTranscriptResultData-32) + `"}`)
	fake := &fakeControlPlaneClient{
		streamEvents: []controlplaneclient.SSEEvent{
			{Event: "transcript", ID: "accepted-cursor", HasID: true, Data: firstData},
			{Event: "transcript-gap", Data: []byte(`{"resumeAfter":"gap-cursor","earliestSequence":4}`)},
		},
	}
	output, err := readTranscript(context.Background(), fake, "team-a", ReadTranscriptInput{
		RunName: "task-1", RunUID: "run-uid", MaxEvents: 2, WaitMilliseconds: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Events) != 1 || output.NextCursor != "accepted-cursor" || !output.LimitReached {
		t.Fatalf("bounded gap output = %#v", output)
	}
}

func TestValidateCreateRunRejectsUnboundedOrAmbiguousIntent(t *testing.T) {
	for _, input := range []CreateRunInput{
		{Name: "task", Prompt: "work"},
		{Name: "task", Prompt: "work", Environment: "env", Project: "repo"},
		{Name: "task", Prompt: strings.Repeat("x", maxPromptBytes+1), Project: "repo"},
	} {
		if err := validateCreateRun(input); err == nil {
			t.Fatalf("validateCreateRun(%#v) succeeded", input)
		}
	}
	if err := validateCreateRun(CreateRunInput{Name: "task", Prompt: "work", Project: "repo"}); err != nil {
		t.Fatal(err)
	}
}

func TestReadTranscriptPropagatesCallerCancellation(t *testing.T) {
	fake := &fakeControlPlaneClient{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := readTranscript(ctx, fake, "team-a", ReadTranscriptInput{RunName: "task", RunUID: "uid"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}
