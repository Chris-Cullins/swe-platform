// Package mcpserver exposes bounded swe-platform operations over local MCP stdio.
package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/Chris-Cullins/swe-platform/internal/controlplane"
	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
)

const (
	maxPromptBytes          = 128 << 10
	maxTranscriptEvents     = 100
	maxTranscriptResultData = 128 << 10
	maxTranscriptResultJSON = 256 << 10
	maxTranscriptWait       = 10 * time.Second
)

var errBatchComplete = errors.New("transcript batch complete")

type controlPlaneClient interface {
	CreateRun(context.Context, string, controlplane.CreateRunRequest) (controlplane.Run, error)
	StreamRunTranscript(context.Context, string, string, string, string, func(controlplaneclient.SSEEvent) error) error
}

// CreateRunInput is the bounded immutable Run intent exposed to MCP clients.
type CreateRunInput struct {
	Name                 string `json:"name" jsonschema:"Stable Run name and idempotency key."`
	Prompt               string `json:"prompt" jsonschema:"Task for the selected agent."`
	Agent                string `json:"agent,omitempty" jsonschema:"Agent adapter; defaults to claude-code."`
	Environment          string `json:"environment,omitempty" jsonschema:"Existing Environment; exclusive with project and template."`
	Project              string `json:"project,omitempty" jsonschema:"Project supplying repository and default template configuration."`
	Template             string `json:"template,omitempty" jsonschema:"EnvironmentTemplate; may override a Project default."`
	CredentialProfile    string `json:"credentialProfile,omitempty" jsonschema:"Same-namespace AgentCredentialProfile name."`
	RepositoryCredential string `json:"repositoryCredential,omitempty" jsonschema:"Repository credential provider; GitHubApp when selected."`
}

// RunReference is the concise, immutable identity returned by create_run.
type RunReference struct {
	Namespace   string                       `json:"namespace"`
	Name        string                       `json:"name"`
	UID         string                       `json:"uid"`
	State       string                       `json:"state"`
	Environment *controlplane.RunEnvironment `json:"environment,omitempty"`
}

// CreateRunOutput is the structured create_run result.
type CreateRunOutput struct {
	Run RunReference `json:"run"`
}

// ReadTranscriptInput identifies one immutable Run and a bounded cursor read.
type ReadTranscriptInput struct {
	RunName          string `json:"runName" jsonschema:"Run name in the MCP server's configured namespace."`
	RunUID           string `json:"runUID" jsonschema:"Expected immutable Run UID returned by create_run or an exact Run read."`
	After            string `json:"after,omitempty" jsonschema:"Opaque transcript cursor after which to read."`
	MaxEvents        int    `json:"maxEvents,omitempty" jsonschema:"Maximum events to return; defaults to 20 and cannot exceed 100."`
	WaitMilliseconds int    `json:"waitMilliseconds,omitempty" jsonschema:"Bounded wait for events; defaults to 1000 and cannot exceed 10000."`
}

// TranscriptEnvelope preserves the control-plane event name, cursor, and
// adapter-owned JSON without defining a shared transcript payload schema.
type TranscriptEnvelope struct {
	Event    string `json:"event"`
	ID       string `json:"id,omitempty"`
	DataJSON string `json:"dataJSON"`
}

// ReadTranscriptOutput is a finite transcript batch. NextCursor is safe to
// pass back as After; gap data remains in Events for the caller to interpret.
type ReadTranscriptOutput struct {
	Namespace    string               `json:"namespace"`
	RunName      string               `json:"runName"`
	RunUID       string               `json:"runUID"`
	Events       []TranscriptEnvelope `json:"events"`
	NextCursor   string               `json:"nextCursor,omitempty"`
	LimitReached bool                 `json:"limitReached"`
	TimedOut     bool                 `json:"timedOut"`
}

// New constructs an MCP server whose tools act as the explicit bearer-authenticated
// control-plane client in exactly one configured namespace.
func New(client *controlplaneclient.Client, namespace string) *mcp.Server {
	return newServer(client, namespace)
}

func newServer(client controlPlaneClient, namespace string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "swe-platform", Version: "0.1.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name:         "create_run",
		Description:  "Create one Run, or recover an exact-intent same-name retry while that Run exists, in namespace " + namespace + " through the authenticated control-plane API. Returns the immutable Run UID required by other tools.",
		InputSchema:  createRunSchema(),
		OutputSchema: createRunOutputSchema(),
		Annotations:  &mcp.ToolAnnotations{OpenWorldHint: boolPointer(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input CreateRunInput) (*mcp.CallToolResult, CreateRunOutput, error) {
		output, err := createRun(ctx, client, namespace, input)
		return nil, output, err
	})
	mcp.AddTool(server, &mcp.Tool{
		Name:         "read_transcript",
		Description:  "Read a finite cursor batch from one exact Run transcript in namespace " + namespace + ". Event data remains adapter-owned JSON in dataJSON; no shared payload schema is implied.",
		InputSchema:  readTranscriptSchema(),
		OutputSchema: readTranscriptOutputSchema(),
		Annotations:  &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: boolPointer(true)},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input ReadTranscriptInput) (*mcp.CallToolResult, ReadTranscriptOutput, error) {
		output, err := readTranscript(ctx, client, namespace, input)
		return nil, output, err
	})
	return server
}

func createRun(ctx context.Context, client controlPlaneClient, namespace string, input CreateRunInput) (CreateRunOutput, error) {
	if err := validateCreateRun(input); err != nil {
		return CreateRunOutput{}, err
	}
	agent := input.Agent
	if agent == "" {
		agent = "claude-code"
	}
	run, err := client.CreateRun(ctx, namespace, controlplane.CreateRunRequest{
		Name: input.Name,
		Selector: controlplane.RunSelector{
			Environment: input.Environment,
			Project:     input.Project,
			Template:    input.Template,
		},
		Agent:                agent,
		Prompt:               input.Prompt,
		CredentialProfile:    input.CredentialProfile,
		RepositoryCredential: input.RepositoryCredential,
	})
	if err != nil {
		return CreateRunOutput{}, fmt.Errorf("create Run: %w", err)
	}
	if run.Name != input.Name || run.UID == "" || len(run.UID) > 128 || len(run.State) > 128 {
		return CreateRunOutput{}, fmt.Errorf("control plane returned an invalid Run identity")
	}
	if run.Environment != nil && (run.Environment.Name == "" || len(run.Environment.Name) > 253 || len(run.Environment.UID) > 128 || len(run.Environment.Ownership) > 32) {
		return CreateRunOutput{}, fmt.Errorf("control plane returned an invalid Run environment reference")
	}
	return CreateRunOutput{Run: RunReference{
		Namespace: namespace,
		Name:      run.Name, UID: run.UID, State: run.State, Environment: run.Environment,
	}}, nil
}

func validateCreateRun(input CreateRunInput) error {
	for field, value := range map[string]string{
		"name": input.Name, "environment": input.Environment, "project": input.Project,
		"template": input.Template, "credentialProfile": input.CredentialProfile,
	} {
		if value != "" && len(validation.IsDNS1123Subdomain(value)) != 0 {
			return fmt.Errorf("%s must be a Kubernetes DNS subdomain", field)
		}
	}
	if input.Environment == "" && input.Project == "" && input.Template == "" {
		return fmt.Errorf("environment, project, or template is required")
	}
	if input.Environment != "" && (input.Project != "" || input.Template != "") {
		return fmt.Errorf("environment cannot be combined with project or template")
	}
	if input.Agent != "" && len(input.Agent) > 128 {
		return fmt.Errorf("agent exceeds 128 bytes")
	}
	if input.Prompt == "" || len(input.Prompt) > maxPromptBytes {
		return fmt.Errorf("prompt is required and must not exceed %d bytes", maxPromptBytes)
	}
	if input.RepositoryCredential != "" && input.RepositoryCredential != "GitHubApp" {
		return fmt.Errorf("repositoryCredential must be GitHubApp")
	}
	return nil
}

func readTranscript(ctx context.Context, client controlPlaneClient, namespace string, input ReadTranscriptInput) (ReadTranscriptOutput, error) {
	if err := validateReadTranscript(input); err != nil {
		return ReadTranscriptOutput{}, err
	}
	maxEvents := input.MaxEvents
	if maxEvents == 0 {
		maxEvents = 20
	}
	wait := time.Duration(input.WaitMilliseconds) * time.Millisecond
	if wait == 0 {
		wait = time.Second
	}
	output := ReadTranscriptOutput{
		Namespace: namespace, RunName: input.RunName, RunUID: input.RunUID,
		Events: make([]TranscriptEnvelope, 0, maxEvents), NextCursor: input.After,
	}
	batchCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	dataBytes := 0
	err := client.StreamRunTranscript(batchCtx, namespace, input.RunName, input.RunUID, input.After, func(event controlplaneclient.SSEEvent) error {
		if !json.Valid(event.Data) {
			return fmt.Errorf("control plane returned non-JSON %q event data", event.Event)
		}
		if len(event.ID) > 4096 {
			return fmt.Errorf("control plane returned an oversized transcript cursor")
		}
		envelope := TranscriptEnvelope{Event: event.Event, DataJSON: string(event.Data)}
		nextCursor := output.NextCursor
		switch event.Event {
		case "transcript":
			if !event.HasID || event.ID == "" {
				return fmt.Errorf("control plane returned a transcript event without a cursor")
			}
			envelope.ID = event.ID
			nextCursor = event.ID
		case "transcript-gap":
			if event.HasID {
				return fmt.Errorf("control plane returned a transcript gap with an event cursor")
			}
			var gap struct {
				ResumeAfter string `json:"resumeAfter"`
			}
			if err := json.Unmarshal(event.Data, &gap); err != nil || gap.ResumeAfter == "" || len(gap.ResumeAfter) > 4096 {
				return fmt.Errorf("control plane returned an invalid transcript gap recovery cursor")
			}
			nextCursor = gap.ResumeAfter
		default:
			return fmt.Errorf("control plane returned unknown transcript event %q", event.Event)
		}
		if dataBytes+len(event.Data) > maxTranscriptResultData {
			if len(output.Events) == 0 {
				return fmt.Errorf("first transcript event exceeds the %d-byte MCP batch bound; use swe logs --run with its exact --run-uid for streaming output", maxTranscriptResultData)
			}
			output.LimitReached = true
			return errBatchComplete
		}
		candidate := output
		candidate.Events = append(append([]TranscriptEnvelope(nil), output.Events...), envelope)
		candidate.NextCursor = nextCursor
		if encoded, marshalErr := json.Marshal(candidate); marshalErr != nil || len(encoded) > maxTranscriptResultJSON {
			if len(output.Events) == 0 {
				return fmt.Errorf("first transcript event exceeds the %d-byte encoded MCP batch bound", maxTranscriptResultJSON)
			}
			output.LimitReached = true
			return errBatchComplete
		}
		output = candidate
		dataBytes += len(event.Data)
		if len(output.Events) == maxEvents {
			output.LimitReached = true
			return errBatchComplete
		}
		return nil
	})
	switch {
	case errors.Is(err, errBatchComplete):
		return output, nil
	case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
		output.TimedOut = true
		return output, nil
	case err == nil:
		return output, nil
	default:
		if recovery, ok := controlplaneclient.TranscriptCursorRecovery(err); ok {
			return ReadTranscriptOutput{}, fmt.Errorf("transcript cursor is unavailable; retry with after %q", recovery.ResumeAfter)
		}
		return ReadTranscriptOutput{}, fmt.Errorf("read transcript: %w", err)
	}
}

func validateReadTranscript(input ReadTranscriptInput) error {
	if len(validation.IsDNS1123Subdomain(input.RunName)) != 0 {
		return fmt.Errorf("runName must be a Kubernetes DNS subdomain")
	}
	if input.RunUID == "" || len(input.RunUID) > 128 {
		return fmt.Errorf("runUID is required and must not exceed 128 bytes")
	}
	if len(input.After) > 4096 {
		return fmt.Errorf("after must not exceed 4096 bytes")
	}
	if input.MaxEvents < 0 || input.MaxEvents > maxTranscriptEvents {
		return fmt.Errorf("maxEvents must be from 0 through %d", maxTranscriptEvents)
	}
	if input.WaitMilliseconds < 0 || input.WaitMilliseconds > int(maxTranscriptWait/time.Millisecond) {
		return fmt.Errorf("waitMilliseconds must be from 0 through %d", maxTranscriptWait/time.Millisecond)
	}
	return nil
}

func createRunSchema() *jsonschema.Schema {
	schema := inferredSchema[CreateRunInput]()
	setStringBounds(schema, "name", 1, 253)
	setStringBounds(schema, "prompt", 1, maxPromptBytes)
	setStringBounds(schema, "agent", 0, 128)
	for _, field := range []string{"environment", "project", "template", "credentialProfile"} {
		setStringBounds(schema, field, 0, 253)
	}
	setStringBounds(schema, "repositoryCredential", 0, 9)
	return schema
}

func readTranscriptSchema() *jsonschema.Schema {
	schema := inferredSchema[ReadTranscriptInput]()
	setStringBounds(schema, "runName", 1, 253)
	setStringBounds(schema, "runUID", 1, 128)
	setStringBounds(schema, "after", 0, 4096)
	setNumberBounds(schema, "maxEvents", 0, maxTranscriptEvents)
	setNumberBounds(schema, "waitMilliseconds", 0, int(maxTranscriptWait/time.Millisecond))
	return schema
}

func createRunOutputSchema() *jsonschema.Schema {
	schema := inferredSchema[CreateRunOutput]()
	run := schema.Properties["run"]
	setStringBounds(run, "namespace", 1, 253)
	setStringBounds(run, "name", 1, 253)
	setStringBounds(run, "uid", 1, 128)
	setStringBounds(run, "state", 0, 128)
	environment := run.Properties["environment"]
	setStringBounds(environment, "name", 1, 253)
	setStringBounds(environment, "uid", 0, 128)
	setStringBounds(environment, "ownership", 1, 32)
	return schema
}

func readTranscriptOutputSchema() *jsonschema.Schema {
	schema := inferredSchema[ReadTranscriptOutput]()
	setStringBounds(schema, "namespace", 1, 253)
	setStringBounds(schema, "runName", 1, 253)
	setStringBounds(schema, "runUID", 1, 128)
	setStringBounds(schema, "nextCursor", 0, 4096)
	events := schema.Properties["events"]
	maximum := maxTranscriptEvents
	events.MaxItems = &maximum
	setStringBounds(events.Items, "event", 1, 32)
	setStringBounds(events.Items, "id", 0, 4096)
	setStringBounds(events.Items, "dataJSON", 1, maxTranscriptResultData)
	return schema
}

func inferredSchema[T any]() *jsonschema.Schema {
	schema, err := jsonschema.ForType(reflect.TypeFor[T](), nil)
	if err != nil {
		panic(err)
	}
	return schema
}

func setStringBounds(schema *jsonschema.Schema, field string, minimum, maximum int) {
	schema.Properties[field].MinLength = &minimum
	schema.Properties[field].MaxLength = &maximum
}

func setNumberBounds(schema *jsonschema.Schema, field string, minimum, maximum int) {
	min, max := float64(minimum), float64(maximum)
	schema.Properties[field].Minimum = &min
	schema.Properties[field].Maximum = &max
}

func boolPointer(value bool) *bool { return &value }
