package controlplane

import (
	"context"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResourceService is the Kubernetes-independent resource API used by HTTP
// handlers. Its representations deliberately contain only the frozen API DTOs.
type ResourceService interface {
	ListRuns(ctx context.Context, namespace string, limit int64, continueToken string) (RunList, error)
	CreateRun(ctx context.Context, namespace string, request CreateRunRequest) (Run, error)
	GetRun(ctx context.Context, namespace, name string) (Run, error)
	CancelRun(ctx context.Context, namespace, name string) (Run, error)
	GetEnvironment(ctx context.Context, namespace, name string) (Environment, error)
}

// KubernetesResourceService stores resource intent in swe.dev CRDs.
type KubernetesResourceService struct {
	Client client.Client
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

	result := RunList{Items: make([]Run, 0, len(runs.Items)), Continue: runs.Continue}
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
		Spec: platformv1alpha1.RunSpec{
			EnvironmentRef: request.Selector.Environment,
			ProjectRef:     request.Selector.Project,
			TemplateRef:    request.Selector.Template,
			Agent:          request.Agent,
			Prompt:         request.Prompt,
		},
	}
	if err := s.Client.Create(ctx, run); err != nil {
		// In particular, do not resolve AlreadyExists here: doing so would read an
		// object before the handler has separately authorized that operation.
		return Run{}, err
	}
	return runDTO(run), nil
}

func (s *KubernetesResourceService) GetRun(ctx context.Context, namespace, name string) (Run, error) {
	var run platformv1alpha1.Run
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		return Run{}, err
	}
	return runDTO(&run), nil
}

func (s *KubernetesResourceService) CancelRun(ctx context.Context, namespace, name string) (Run, error) {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	for attempt := 0; attempt < 5; attempt++ {
		var run platformv1alpha1.Run
		if err := s.Client.Get(ctx, key, &run); err != nil {
			return Run{}, err
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

func (s *KubernetesResourceService) GetEnvironment(ctx context.Context, namespace, name string) (Environment, error) {
	var environment platformv1alpha1.Environment
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &environment); err != nil {
		return Environment{}, err
	}
	return environmentDTO(&environment), nil
}

func runDTO(run *platformv1alpha1.Run) Run {
	result := Run{
		Name:      run.Name,
		UID:       string(run.UID),
		CreatedAt: run.CreationTimestamp.Time,
		Intent: RunIntent{
			Selector: RunSelector{Environment: run.Spec.EnvironmentRef, Project: run.Spec.ProjectRef, Template: run.Spec.TemplateRef},
			Agent:    run.Spec.Agent,
			Prompt:   run.Spec.Prompt,
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
		Paused:    environment.Spec.Paused,
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
