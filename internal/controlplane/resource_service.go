package controlplane

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	errRunIntentConflict      = errors.New("run already exists with different immutable intent")
	errRunUIDConflict         = errors.New("run UID does not match the current Run")
	errRunTerminalAssociation = errors.New("run is not associated with the exact environment")
)

const (
	runPromptPreviewRunes = 160
	runAgentSummaryRunes  = 128
)

// ResourceService is the Kubernetes-independent resource API used by HTTP
// handlers. Its representations deliberately contain only the frozen API DTOs.
type ResourceService interface {
	ListRuns(ctx context.Context, namespace string, limit int64, continueToken string) (RunList, error)
	ListRunSummaries(ctx context.Context, namespace string, limit int64, continueToken string) (RunSummaryList, error)
	CreateRun(ctx context.Context, namespace string, request CreateRunRequest) (Run, error)
	ResolveRunCreateCollision(ctx context.Context, namespace string, request CreateRunRequest) (Run, error)
	GetRun(ctx context.Context, namespace, name string) (Run, error)
	CancelRun(ctx context.Context, namespace, name, expectedUID string) (Run, error)
	GetEnvironment(ctx context.Context, namespace, name string) (Environment, error)
	ResolveRunTerminal(ctx context.Context, namespace, name, expectedRunUID, expectedEnvironmentUID string) (RunTerminalAssociation, error)
}

func (s *KubernetesResourceService) ListRunSummaries(ctx context.Context, namespace string, limit int64, continueToken string) (RunSummaryList, error) {
	var runs platformv1alpha1.RunList
	if err := s.Client.List(ctx, &runs, &client.ListOptions{Namespace: namespace, Limit: limit, Continue: continueToken}); err != nil {
		return RunSummaryList{}, err
	}
	result := RunSummaryList{Items: make([]RunSummary, 0, len(runs.Items)), Continue: runs.Continue, ResourceVersion: runs.ResourceVersion}
	for i := range runs.Items {
		result.Items = append(result.Items, runSummaryDTO(&runs.Items[i]))
	}
	return result, nil
}

// KubernetesResourceService stores resource intent in swe.dev CRDs.
type KubernetesResourceService struct {
	Client client.WithWatch
}

func (s *KubernetesResourceService) WatchRuns(ctx context.Context, namespace, resourceVersion string, timeout time.Duration) (watch.Interface, error) {
	seconds := int64(timeout / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return s.Client.Watch(ctx, &platformv1alpha1.RunList{}, &client.ListOptions{Namespace: namespace, Raw: &metav1.ListOptions{
		ResourceVersion: resourceVersion, AllowWatchBookmarks: true, TimeoutSeconds: &seconds,
	}})
}

func (s *KubernetesResourceService) ListRuns(ctx context.Context, namespace string, limit int64, continueToken string) (RunList, error) {
	var runs platformv1alpha1.RunList
	if err := s.Client.List(ctx, &runs, &client.ListOptions{
		Namespace: namespace,
		Limit:     limit,
		Continue:  continueToken,
	}); err != nil {
		return RunList{}, err
	}

	result := RunList{Items: make([]Run, 0, len(runs.Items)), Continue: runs.Continue, ResourceVersion: runs.ResourceVersion}
	for i := range runs.Items {
		result.Items = append(result.Items, runDTO(&runs.Items[i]))
	}
	return result, nil
}

func (s *KubernetesResourceService) CreateRun(ctx context.Context, namespace string, request CreateRunRequest) (Run, error) {
	run := &platformv1alpha1.Run{
		TypeMeta: metav1.TypeMeta{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Run"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      request.Name,
		},
		Spec: desiredRunSpec(request),
	}
	if err := s.Client.Create(ctx, run); err != nil {
		// In particular, do not resolve AlreadyExists here: doing so would read an
		// object before the handler has separately authorized that operation.
		return Run{}, err
	}
	return runDTO(run), nil
}

func (s *KubernetesResourceService) ResolveRunCreateCollision(ctx context.Context, namespace string, request CreateRunRequest) (Run, error) {
	var run platformv1alpha1.Run
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: request.Name}, &run); err != nil {
		return Run{}, err
	}
	existing := run.Spec
	existing.Cancel = false
	if len(existing.Notify) == 0 {
		existing.Notify = nil
	}
	if !reflect.DeepEqual(existing, desiredRunSpec(request)) {
		return Run{}, errRunIntentConflict
	}
	return runDTO(&run), nil
}

func (s *KubernetesResourceService) GetRun(ctx context.Context, namespace, name string) (Run, error) {
	var run platformv1alpha1.Run
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		return Run{}, err
	}
	result := runDTO(&run)
	if run.Status.EnvironmentRef != nil {
		var environment platformv1alpha1.Environment
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: run.Status.EnvironmentRef.Name}, &environment); err == nil {
			result.TerminalAvailable = runOwnsOrClaimsEnvironment(&run, &environment)
			if result.TerminalAvailable {
				result.Environment.UID = string(environment.UID)
			}
		}
	}
	return result, nil
}

func desiredRunSpec(request CreateRunRequest) platformv1alpha1.RunSpec {
	return platformv1alpha1.RunSpec{
		EnvironmentRef:       request.Selector.Environment,
		ProjectRef:           request.Selector.Project,
		TemplateRef:          request.Selector.Template,
		Agent:                request.Agent,
		Prompt:               request.Prompt,
		CredentialProfileRef: request.CredentialProfile,
		Notify:               nil,
		ParentRef:            "",
	}
}

func (s *KubernetesResourceService) CancelRun(ctx context.Context, namespace, name, expectedUID string) (Run, error) {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	for attempt := 0; attempt < 5; attempt++ {
		var run platformv1alpha1.Run
		if err := s.Client.Get(ctx, key, &run); err != nil {
			return Run{}, err
		}
		if expectedUID != "" && string(run.UID) != expectedUID {
			return Run{}, errRunUIDConflict
		}
		if run.Spec.Cancel {
			return runDTO(&run), nil
		}
		run.Spec.Cancel = true
		if err := s.Client.Update(ctx, &run); err != nil {
			if apierrors.IsConflict(err) && attempt < 4 {
				continue
			}
			return Run{}, err
		}
		return runDTO(&run), nil
	}
	panic("unreachable")
}

func (s *KubernetesResourceService) ResolveRunTerminal(ctx context.Context, namespace, name, expectedRunUID, expectedEnvironmentUID string) (RunTerminalAssociation, error) {
	var run platformv1alpha1.Run
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		return RunTerminalAssociation{}, err
	}
	if string(run.UID) != expectedRunUID {
		return RunTerminalAssociation{}, errRunUIDConflict
	}
	if run.Status.EnvironmentRef == nil || run.Status.EnvironmentRef.Name == "" {
		return RunTerminalAssociation{}, errRunTerminalAssociation
	}
	var environment platformv1alpha1.Environment
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: run.Status.EnvironmentRef.Name}, &environment); err != nil {
		return RunTerminalAssociation{}, err
	}
	if string(environment.UID) != expectedEnvironmentUID || !runOwnsOrClaimsEnvironment(&run, &environment) {
		return RunTerminalAssociation{}, errRunTerminalAssociation
	}
	return RunTerminalAssociation{
		RunName: name, RunUID: expectedRunUID,
		EnvironmentName: environment.Name, EnvironmentUID: expectedEnvironmentUID,
		EnvironmentOwnership: string(run.Status.EnvironmentRef.Ownership),
	}, nil
}

func runOwnsOrClaimsEnvironment(run *platformv1alpha1.Run, environment *platformv1alpha1.Environment) bool {
	if run.Status.EnvironmentRef == nil || run.Status.EnvironmentRef.Name != environment.Name || run.Status.EnvironmentRef.UID != environment.UID {
		return false
	}
	switch run.Status.EnvironmentRef.Ownership {
	case platformv1alpha1.EnvironmentOwnershipClaimed:
		return environment.Status.ClaimedBy != nil && environment.Status.ClaimedBy.Name == run.Name && environment.Status.ClaimedBy.UID == run.UID
	case platformv1alpha1.EnvironmentOwnershipOwned:
		owner := metav1.GetControllerOf(environment)
		return owner != nil && owner.APIVersion == platformv1alpha1.GroupVersion.String() && owner.Kind == "Run" && owner.Name == run.Name && owner.UID == run.UID
	default:
		return false
	}
}

func (s *KubernetesResourceService) GetEnvironment(ctx context.Context, namespace, name string) (Environment, error) {
	var environment platformv1alpha1.Environment
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &environment); err != nil {
		return Environment{}, err
	}
	return environmentDTO(&environment), nil
}

func runDTO(run *platformv1alpha1.Run) Run {
	result := Run{
		Name:       run.Name,
		UID:        string(run.UID),
		Generation: run.Generation,
		CreatedAt:  run.CreationTimestamp.Time,
		Intent: RunIntent{
			Selector:          RunSelector{Environment: run.Spec.EnvironmentRef, Project: run.Spec.ProjectRef, Template: run.Spec.TemplateRef},
			Agent:             run.Spec.Agent,
			Prompt:            run.Spec.Prompt,
			CredentialProfile: run.Spec.CredentialProfileRef,
		},
		CancelRequested: run.Spec.Cancel,
		State:           string(run.Status.State),
		Branch:          run.Status.Branch,
		Usage: RunUsage{
			CPUSeconds: run.Status.Usage.CPUSeconds,
			TokensIn:   run.Status.Usage.TokensIn,
			TokensOut:  run.Status.Usage.TokensOut,
		},
	}
	if run.Status.EnvironmentRef != nil {
		result.Environment = &RunEnvironment{
			Name:      run.Status.EnvironmentRef.Name,
			Ownership: string(run.Status.EnvironmentRef.Ownership),
		}
	}
	return result
}

func runSummaryDTO(run *platformv1alpha1.Run) RunSummary {
	full := runDTO(run)
	var environment *RunEnvironment
	if full.Environment != nil {
		environment = &RunEnvironment{
			Name:      boundedRunes(full.Environment.Name, 253),
			Ownership: boundedRunes(full.Environment.Ownership, 32),
		}
	}
	return RunSummary{
		Name: full.Name, UID: full.UID, Generation: full.Generation, CreatedAt: full.CreatedAt,
		Agent: boundedRunes(full.Intent.Agent, runAgentSummaryRunes), PromptPreview: boundedPromptPreview(full.Intent.Prompt),
		CancelRequested: full.CancelRequested, State: boundedRunes(full.State, 64), Environment: environment,
	}
}

func boundedRunes(value string, limit int) string {
	count := 0
	for offset := range value {
		if count == limit {
			return value[:offset] + "…"
		}
		count++
	}
	return value
}

func boundedPromptPreview(prompt string) string {
	var preview strings.Builder
	runeCount := 0
	truncated := false
	for _, r := range prompt {
		if runeCount == runPromptPreviewRunes {
			truncated = true
			break
		}
		preview.WriteRune(r)
		runeCount++
	}
	normalized := strings.Join(strings.Fields(preview.String()), " ")
	if normalized != "" && truncated {
		return normalized + "…"
	}
	return normalized
}

func environmentDTO(environment *platformv1alpha1.Environment) Environment {
	backend := environment.Spec.Backend
	if backend == "" {
		backend = platformv1alpha1.EnvironmentBackendPod
	}
	result := Environment{
		Name:      environment.Name,
		UID:       string(environment.UID),
		CreatedAt: environment.CreationTimestamp.Time,
		Project:   environment.Spec.ProjectRef,
		Template:  environment.Spec.TemplateRef,
		Backend:   string(backend),
		Paused:    environment.Spec.Paused || environment.Status.Lifecycle.Suspended,
		Phase:     string(environment.Status.Phase),
		Ready:     platformv1alpha1.IsEnvironmentReady(environment),
	}
	if environment.Status.ClaimedBy != nil {
		result.Claim = &EnvironmentClaim{
			RunName: environment.Status.ClaimedBy.Name,
			RunUID:  string(environment.Status.ClaimedBy.UID),
		}
	}
	if environment.Status.LastActiveAt != nil {
		lastActive := environment.Status.LastActiveAt.Time
		result.LastActiveAt = &lastActive
	}
	return result
}
