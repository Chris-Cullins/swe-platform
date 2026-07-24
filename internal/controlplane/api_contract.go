package controlplane

import "time"

// Problem is the error envelope returned by the resource and session APIs.
type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// Session is the authenticated browser identity. The HttpOnly cookie carries
// only an opaque process-local session identifier and is never included here.
type Session struct {
	Authenticated bool   `json:"authenticated"`
	Username      string `json:"username"`
}

// RunSelector is the caller-owned environment selection intent. Environment is
// exclusive with Project and Template; Project and Template may be combined.
type RunSelector struct {
	Environment string `json:"environment,omitempty"`
	Project     string `json:"project,omitempty"`
	Template    string `json:"template,omitempty"`
}

// CreateRunRequest is the immutable intent accepted by the Run create API.
// Name is required and acts as the retry/idempotency key.
type CreateRunRequest struct {
	Name              string      `json:"name"`
	Selector          RunSelector `json:"selector"`
	Agent             string      `json:"agent"`
	Prompt            string      `json:"prompt"`
	CredentialProfile string      `json:"credentialProfile,omitempty"`
}

// RunIntent is the immutable portion of a Run exposed to API clients.
type RunIntent struct {
	Selector          RunSelector `json:"selector"`
	Agent             string      `json:"agent"`
	Prompt            string      `json:"prompt"`
	CredentialProfile string      `json:"credentialProfile,omitempty"`
}

// RunEnvironment identifies the controller-selected allocation without
// exposing infrastructure endpoints or implementation details.
type RunEnvironment struct {
	Name      string `json:"name"`
	UID       string `json:"uid,omitempty"`
	Ownership string `json:"ownership"`
}

// RunUsage is normalized usage attributed to a Run.
type RunUsage struct {
	CPUSeconds int64 `json:"cpuSeconds"`
	TokensIn   int64 `json:"tokensIn"`
	TokensOut  int64 `json:"tokensOut"`
}

// Run is the stable HTTP representation of a Run CRD.
type Run struct {
	Name              string          `json:"name"`
	UID               string          `json:"uid"`
	Generation        int64           `json:"generation"`
	CreatedAt         time.Time       `json:"createdAt"`
	Intent            RunIntent       `json:"intent"`
	CancelRequested   bool            `json:"cancelRequested"`
	State             string          `json:"state"`
	Environment       *RunEnvironment `json:"environment,omitempty"`
	TerminalAvailable bool            `json:"terminalAvailable,omitempty"`
	Branch            string          `json:"branch,omitempty"`
	Usage             RunUsage        `json:"usage"`
}

// RunList is a bounded page of Runs. Continue is opaque and may be supplied to
// the next list request.
type RunList struct {
	Items           []Run  `json:"items"`
	Continue        string `json:"continue,omitempty"`
	ResourceVersion string `json:"resourceVersion"`
}

// RunSummary is the bounded representation used by operations-console lists.
// Full prompts remain available from the exact Run detail endpoint.
type RunSummary struct {
	Name            string          `json:"name"`
	UID             string          `json:"uid"`
	Generation      int64           `json:"generation"`
	CreatedAt       time.Time       `json:"createdAt"`
	Agent           string          `json:"agent"`
	PromptPreview   string          `json:"promptPreview"`
	CancelRequested bool            `json:"cancelRequested"`
	State           string          `json:"state"`
	Environment     *RunEnvironment `json:"environment,omitempty"`
}

// RunSummaryList is a bounded page of Run list summaries.
type RunSummaryList struct {
	Items           []RunSummary `json:"items"`
	Continue        string       `json:"continue,omitempty"`
	ResourceVersion string       `json:"resourceVersion"`
}

// RunWatchEvent is the bounded, typed representation of a Kubernetes Run watch event.
type RunWatchEvent struct {
	Type            string     `json:"type"`
	ResourceVersion string     `json:"resourceVersion"`
	Run             RunSummary `json:"run"`
}

// RunWatchCheckpoint advances a watch cursor without changing a Run.
type RunWatchCheckpoint struct {
	ResourceVersion string `json:"resourceVersion"`
}

// CancelRunRequest optionally fences cancellation to one immutable Run.
// An empty request remains supported for existing browser clients.
type CancelRunRequest struct {
	RunUID string `json:"runUID,omitempty"`
}

// RunTerminalAssociation is the exact server-validated Run-to-Environment
// relationship required by the Run-scoped terminal gateway.
type RunTerminalAssociation struct {
	RunName              string
	RunUID               string
	EnvironmentName      string
	EnvironmentUID       string
	EnvironmentOwnership string
}

// EnvironmentClaim identifies the Run holding a reusable Environment. The UID
// fences a same-name Run replacement.
type EnvironmentClaim struct {
	RunName string `json:"runName"`
	RunUID  string `json:"runUID"`
}

// Environment is the stable HTTP representation of an Environment CRD.
type Environment struct {
	Name         string            `json:"name"`
	UID          string            `json:"uid"`
	CreatedAt    time.Time         `json:"createdAt"`
	Project      string            `json:"project,omitempty"`
	Template     string            `json:"template"`
	Backend      string            `json:"backend"`
	Paused       bool              `json:"paused"`
	Phase        string            `json:"phase"`
	Ready        bool              `json:"ready"`
	Claim        *EnvironmentClaim `json:"claim,omitempty"`
	LastActiveAt *time.Time        `json:"lastActiveAt,omitempty"`
}
