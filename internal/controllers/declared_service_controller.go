package controllers

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/controlplaneclient"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	"github.com/Chris-Cullins/swe-platform/internal/sandboxclient"
	"github.com/Chris-Cullins/swe-platform/internal/serviceconfig"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const declaredServiceInterval = 20 * time.Second

type RepositoryServiceConnector interface {
	ReadWorkspaceServices(context.Context, lifecycle.ExecutionFence) (sandboxclient.WorkspaceServicesFile, error)
	ReconcileRepositoryServices(context.Context, lifecycle.ExecutionFence, []platformv1alpha1.EnvironmentServiceDeclaration, []platformv1alpha1.EnvironmentPortalRoute, uint64, uint64, []*sandboxdv1.ManagedServiceSpec) error
}
type PortalRouteResolver interface {
	GetPortalRoute(context.Context, string, string, string) (controlplaneclient.PortalRoute, error)
}

type DeclaredServiceReconciler struct {
	client.Client
	APIReader client.Reader
	Scope     *tenancy.ReconcileScope
	Connector RepositoryServiceConnector
	Routes    PortalRouteResolver
}

// The repository-service controller uses its rotating projected service-account
// token only for the control plane's authenticated, proof-bearing portal discovery.
// +kubebuilder:rbac:groups=swe.dev,resources=environments/portal,verbs=get
// +kubebuilder:rbac:groups=swe.dev,resources=environmentservices/portal,verbs=get

func (r *DeclaredServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var env platformv1alpha1.Environment
	if err := r.reader().Get(ctx, req.NamespacedName, &env); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if r.Scope != nil {
		var err error
		ctx, _, err = r.Scope.Begin(ctx, env.Namespace, tenancy.LifecycleActive)
		if errors.Is(err, tenancy.ErrOutOfScope) {
			return ctrl.Result{}, nil
		}
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if !env.DeletionTimestamp.IsZero() || !serviceEnvironmentActive(&env) {
		return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
	}
	fence := lifecycle.CaptureExecutionFence(&env)
	file, err := r.Connector.ReadWorkspaceServices(ctx, fence)
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "read repository service declarations")
		return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
	}
	parsed, err := serviceconfig.Parse(file.Data)
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "parse repository service declarations")
		return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
	}
	converged, collision, err := convergeRepositoryDeclarations(&env, parsed)
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "converge repository service declarations")
		return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
	}
	if collision != "" {
		ctrl.LoggerFrom(ctx).Info("repository service declarations conflict with durable intent", "collision", collision)
		return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
	}
	if !reflect.DeepEqual(env.Spec.Services, converged) {
		var current platformv1alpha1.Environment
		if err := r.reader().Get(ctx, req.NamespacedName, &current); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if err := fence.Validate(&current); err != nil || !reflect.DeepEqual(current.Spec.Services, env.Spec.Services) {
			return ctrl.Result{Requeue: true}, nil
		}
		base := current.DeepCopy()
		current.Spec.Services = converged
		if err := r.Patch(ctx, &current, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	specs := make([]*sandboxdv1.ManagedServiceSpec, 0)
	routeProofs := make([]platformv1alpha1.EnvironmentPortalRoute, 0)
	routeRevision := uint64(env.Status.NextPortalRouteGeneration)
	for _, declaration := range env.Spec.Services {
		if declaration.Source != platformv1alpha1.EnvironmentServiceSourceRepository {
			continue
		}
		route, err := r.Routes.GetPortalRoute(ctx, env.Namespace, env.Name, declaration.Name)
		if err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "discover repository service portal route", "service", declaration.Name)
			return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
		}
		if route.EnvironmentUID != string(env.UID) || route.Service != declaration.Name || route.DeclarationInstanceID != declaration.InstanceID || route.Revision != declaration.Revision || route.RouteGeneration < 1 {
			return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
		}
		var current platformv1alpha1.Environment
		if err := r.reader().Get(ctx, req.NamespacedName, &current); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		routeProof, routeCurrent := exactRoute(&current, declaration, route.RouteGeneration, !route.Disabled)
		if err := fence.Validate(&current); err != nil || !reflect.DeepEqual(current.Spec.Services, env.Spec.Services) || !routeCurrent {
			return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
		}
		routeProofs = append(routeProofs, routeProof)
		if uint64(routeProof.Generation) > routeRevision {
			routeRevision = uint64(routeProof.Generation)
		}
		if route.Disabled {
			if hasActiveRepositoryRoute(&current) || env.Generation < 1 {
				return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
			}
			if err := r.Connector.ReconcileRepositoryServices(ctx, fence, append([]platformv1alpha1.EnvironmentServiceDeclaration(nil), env.Spec.Services...), routeProofs, uint64(env.Generation), routeRevision, nil); err != nil {
				ctrl.LoggerFrom(ctx).Error(err, "stop repository services for disabled portal authority")
				return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
			}
			return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
		}
		if declaration.Launch == nil || len(declaration.Launch.Argv) == 0 {
			return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
		}
		argv := make([]string, len(declaration.Launch.Argv))
		for i := range argv {
			argv[i] = string(declaration.Launch.Argv[i])
		}
		specs = append(specs, &sandboxdv1.ManagedServiceSpec{Role: declaration.Name, Spec: &sandboxdv1.ProcessSpec{Argv: argv, EnvMode: sandboxdv1.EnvironmentMode_ENVIRONMENT_MODE_INHERIT, Env: map[string]string{"PORT": strconv.Itoa(int(declaration.TargetPort)), "PUBLIC_URL": route.URL}}})
	}
	if env.Generation < 1 {
		return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
	}
	if err := r.Connector.ReconcileRepositoryServices(ctx, fence, append([]platformv1alpha1.EnvironmentServiceDeclaration(nil), env.Spec.Services...), routeProofs, uint64(env.Generation), routeRevision, specs); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "reconcile repository service processes")
		return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
	}
	return ctrl.Result{RequeueAfter: declaredServiceInterval}, nil
}

func (r *DeclaredServiceReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
func (r *DeclaredServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	p := builder.WithPredicates(predicate.Funcs{CreateFunc: func(event.CreateEvent) bool { return true }, DeleteFunc: func(event.DeleteEvent) bool { return true }, GenericFunc: func(event.GenericEvent) bool { return true }, UpdateFunc: func(e event.UpdateEvent) bool {
		old, ok1 := e.ObjectOld.(*platformv1alpha1.Environment)
		new, ok2 := e.ObjectNew.(*platformv1alpha1.Environment)
		return !ok1 || !ok2 || observationRelevantEnvironmentUpdate(old, new)
	}})
	return ctrl.NewControllerManagedBy(mgr).Named("declared-service").For(&platformv1alpha1.Environment{}, p).Complete(r)
}

func serviceEnvironmentActive(env *platformv1alpha1.Environment) bool {
	return platformv1alpha1.IsEnvironmentReady(env) && !env.Spec.Paused && !env.Status.Lifecycle.Suspended && (env.Spec.Lifecycle.Hold == nil || !env.Spec.Lifecycle.Hold.Enabled)
}

func exactRoute(env *platformv1alpha1.Environment, d platformv1alpha1.EnvironmentServiceDeclaration, generation int64, active bool) (platformv1alpha1.EnvironmentPortalRoute, bool) {
	var found platformv1alpha1.EnvironmentPortalRoute
	count := 0
	for _, route := range env.Status.PortalRoutes {
		if route.Name == d.Name && route.Active == active && route.DeclarationInstanceID == d.InstanceID && route.DeclarationRevision == d.Revision && route.Generation == generation {
			found = route
			count++
		}
	}
	return found, count == 1
}

func hasActiveRepositoryRoute(env *platformv1alpha1.Environment) bool {
	repository := make(map[string]struct{})
	for _, declaration := range env.Spec.Services {
		if declaration.Source == platformv1alpha1.EnvironmentServiceSourceRepository {
			repository[declaration.Name] = struct{}{}
		}
	}
	for _, route := range env.Status.PortalRoutes {
		if route.Active {
			if _, ok := repository[route.Name]; ok {
				return true
			}
		}
	}
	return false
}

func convergeRepositoryDeclarations(env *platformv1alpha1.Environment, desired []serviceconfig.Declaration) ([]platformv1alpha1.EnvironmentServiceDeclaration, string, error) {
	api, existing := make(map[string]platformv1alpha1.EnvironmentServiceDeclaration), make(map[string]platformv1alpha1.EnvironmentServiceDeclaration)
	occupied := make(map[int32]bool)
	for _, d := range env.Spec.Services {
		occupied[d.TargetPort] = true
		if d.Source == platformv1alpha1.EnvironmentServiceSourceAPI {
			api[d.Name] = d
		} else {
			existing[d.Name] = d
		}
	}
	explicit := make(map[int32]string)
	for _, d := range desired {
		if _, ok := api[d.Name]; ok {
			return env.Spec.Services, d.Name, nil
		}
		if d.Port != nil {
			if other, ok := explicit[*d.Port]; ok {
				return env.Spec.Services, fmt.Sprintf("%s/%s", other, d.Name), nil
			}
			explicit[*d.Port] = d.Name
			occupied[*d.Port] = true
		}
	}
	result := make([]platformv1alpha1.EnvironmentServiceDeclaration, 0, len(api)+len(desired))
	for _, d := range api {
		result = append(result, d)
	}
	repositoryPorts := make(map[int32]string, len(desired))
	for _, wanted := range desired {
		port := int32(0)
		old, found := existing[wanted.Name]
		if wanted.Port != nil {
			port = *wanted.Port
		} else if found {
			port = old.TargetPort
		} else {
			port = allocateServicePort(env.UID, wanted.Name, occupied)
		}
		if other, duplicate := repositoryPorts[port]; duplicate {
			return env.Spec.Services, fmt.Sprintf("%s/%s", other, wanted.Name), nil
		}
		repositoryPorts[port] = wanted.Name
		occupied[port] = true
		argv := make([]platformv1alpha1.EnvironmentServiceLaunchArgument, len(wanted.Argv))
		for i := range argv {
			argv[i] = platformv1alpha1.EnvironmentServiceLaunchArgument(wanted.Argv[i])
		}
		d := platformv1alpha1.EnvironmentServiceDeclaration{Name: wanted.Name, Source: platformv1alpha1.EnvironmentServiceSourceRepository, Launch: &platformv1alpha1.EnvironmentServiceLaunch{Argv: argv}, Protocol: platformv1alpha1.EnvironmentServiceProtocolHTTP, TargetPort: port, Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject, Readiness: platformv1alpha1.EnvironmentServiceReadinessTCPConnect}
		if found {
			d.InstanceID, d.Revision = old.InstanceID, old.Revision
			if !reflect.DeepEqual(old.Launch, d.Launch) || old.TargetPort != port {
				if d.Revision == math.MaxInt64 {
					return nil, "", fmt.Errorf("service %q revision is exhausted", wanted.Name)
				}
				d.Revision++
			}
		} else {
			id, err := randomServiceDeclarationInstanceID()
			if err != nil {
				return nil, "", err
			}
			d.InstanceID, d.Revision = id, 1
		}
		result = append(result, d)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, "", nil
}

func randomServiceDeclarationInstanceID() (string, error) {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate repository service declaration instance ID: %w", err)
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)), nil
}

func allocateServicePort(uid types.UID, name string, occupied map[int32]bool) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(string(uid) + "\x00" + name))
	const size = 65535 - 49152 + 1
	start := int32(h.Sum32()%size) + 49152
	for i := int32(0); i < size; i++ {
		p := 49152 + (start-49152+i)%size
		if !occupied[p] {
			return p
		}
	}
	return 0
}
