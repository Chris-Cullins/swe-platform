package cli

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
)

func newEnvironmentCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "environment", Short: "Manage environments"}
	cmd.AddCommand(newEnvironmentHoldCommand(true), newEnvironmentHoldCommand(false), newEnvironmentServicesCommand())
	return cmd
}

func newEnvironmentServicesCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "services", Short: "Manage durable desired service declarations"}
	cmd.AddCommand(
		newEnvironmentServicesListCommand(),
		newEnvironmentServiceWriteCommand(false),
		newEnvironmentServiceWriteCommand(true),
		newEnvironmentServiceRemoveCommand(),
	)
	return cmd
}

func newEnvironmentServicesListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list ENVIRONMENT",
		Short: "List an environment's desired service declarations",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			clients, err := newKubeClients()
			if err != nil {
				return err
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			return listEnvironmentServices(cmd.Context(), clients.Client, types.NamespacedName{Namespace: namespace, Name: args[0]}, cmd.OutOrStdout())
		},
	}
}

func newEnvironmentServiceWriteCommand(update bool) *cobra.Command {
	var targetPort uint32
	verb := "declare"
	short := "Declare a desired service on an environment"
	if update {
		verb = "update"
		short = "Update an existing desired service declaration"
	}
	cmd := &cobra.Command{
		Use:   verb + " ENVIRONMENT SERVICE",
		Short: short,
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateEnvironmentServiceInput(args[1], targetPort); err != nil {
				return err
			}
			clients, err := newKubeClients()
			if err != nil {
				return err
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			service, err := writeEnvironmentService(cmd.Context(), clients.Client, types.NamespacedName{Namespace: namespace, Name: args[0]}, args[1], int32(targetPort), update)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "service %s %sd on environment %s at revision %d\n", args[1], verb, args[0], service.Revision)
			return nil
		},
	}
	cmd.Flags().Uint32Var(&targetPort, "target-port", 0, "Explicit port on the Environment loopback (1-65535, excluding 50051)")
	_ = cmd.MarkFlagRequired("target-port")
	return cmd
}

func newEnvironmentServiceRemoveCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "remove ENVIRONMENT SERVICE",
		Short: "Remove a durable desired service declaration",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateEnvironmentServiceName(args[1]); err != nil {
				return err
			}
			clients, err := newKubeClients()
			if err != nil {
				return err
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			if err := removeEnvironmentService(cmd.Context(), clients.Client, types.NamespacedName{Namespace: namespace, Name: args[0]}, args[1]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "service %s removed from environment %s\n", args[1], args[0])
			return nil
		},
	}
}

func newEnvironmentHoldCommand(enabled bool) *cobra.Command {
	use := "hold ENVIRONMENT"
	short := "Explicitly hold an environment"
	verb := "held"
	if !enabled {
		use = "release ENVIRONMENT"
		short = "Release an explicit environment hold"
		verb = "released"
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			clients, err := newKubeClients()
			if err != nil {
				return err
			}
			namespace, _ := cmd.Flags().GetString("namespace")
			revision, err := setEnvironmentHold(cmd.Context(), clients.Client, types.NamespacedName{Namespace: namespace, Name: args[0]}, enabled)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "environment %s %s at hold-policy revision %d\n", args[0], verb, revision)
			return nil
		},
	}
}

func setEnvironmentHold(ctx context.Context, kube client.Client, key types.NamespacedName, enabled bool) (int64, error) {
	revision := int64(0)
	var pinnedUID types.UID
	pinned := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var environment platformv1alpha1.Environment
		if err := kube.Get(ctx, key, &environment); err != nil {
			return err
		}
		// Pin the immutable UID on the first successful read so a conflict
		// retry cannot patch a same-name replacement incarnation.
		if !pinned {
			pinnedUID = environment.UID
			pinned = true
		} else if environment.UID != pinnedUID {
			return lifecycle.ErrEnvironmentIncarnationChanged
		}
		if environment.Spec.Paused {
			return fmt.Errorf("environment has a legacy pause awaiting controller migration")
		}
		before := environment.DeepCopy()
		if environment.Spec.Lifecycle.Hold == nil {
			if enabled {
				environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: enabled, Revision: 1}
			}
		} else if environment.Spec.Lifecycle.Hold.Enabled != enabled {
			environment.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: enabled, Revision: environment.Spec.Lifecycle.Hold.Revision + 1}
		}
		if environment.Spec.Lifecycle.Hold != nil {
			revision = environment.Spec.Lifecycle.Hold.Revision
		}
		if holdPoliciesEqual(before.Spec.Lifecycle.Hold, environment.Spec.Lifecycle.Hold) {
			return nil
		}
		return kube.Patch(ctx, &environment, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		return 0, fmt.Errorf("set environment %q hold policy: %w", key.Name, err)
	}
	return revision, nil
}

func holdPoliciesEqual(a, b *platformv1alpha1.EnvironmentHoldPolicy) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func validateEnvironmentServiceName(name string) error {
	if len(validation.IsDNS1123Label(name)) != 0 {
		return fmt.Errorf("service name must be a Kubernetes DNS-1123 label")
	}
	return nil
}

func validateEnvironmentServiceInput(name string, targetPort uint32) error {
	if err := validateEnvironmentServiceName(name); err != nil {
		return err
	}
	if targetPort == 0 || targetPort > 65535 || targetPort == platformv1alpha1.EnvironmentServiceControlPort {
		return fmt.Errorf("--target-port must be between 1 and 65535 and must not be sandboxd control port %d", platformv1alpha1.EnvironmentServiceControlPort)
	}
	return nil
}

func desiredEnvironmentService(name string, targetPort int32, revision int64, instanceID ...string) platformv1alpha1.EnvironmentServiceDeclaration {
	id := ""
	if len(instanceID) != 0 {
		id = instanceID[0]
	}
	return platformv1alpha1.EnvironmentServiceDeclaration{
		Name:       name,
		InstanceID: id,
		Revision:   revision,
		Source:     platformv1alpha1.EnvironmentServiceSourceAPI,
		Protocol:   platformv1alpha1.EnvironmentServiceProtocolHTTP,
		TargetPort: targetPort,
		Visibility: platformv1alpha1.EnvironmentServiceVisibilityProject,
		Readiness:  platformv1alpha1.EnvironmentServiceReadinessTCPConnect,
	}
}

func writeEnvironmentService(ctx context.Context, kube client.Client, key types.NamespacedName, name string, targetPort int32, update bool) (platformv1alpha1.EnvironmentServiceDeclaration, error) {
	var result platformv1alpha1.EnvironmentServiceDeclaration
	var pinnedUID types.UID
	pinned := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var environment platformv1alpha1.Environment
		if err := kube.Get(ctx, key, &environment); err != nil {
			return err
		}
		if !pinned {
			pinnedUID = environment.UID
			pinned = true
		} else if environment.UID != pinnedUID {
			return lifecycle.ErrEnvironmentIncarnationChanged
		}

		before := environment.DeepCopy()
		index := environmentServiceIndex(environment.Spec.Services, name)
		if index < 0 {
			if update {
				return fmt.Errorf("service %q is not declared", name)
			}
			if len(environment.Spec.Services) >= platformv1alpha1.EnvironmentServiceMaxDeclarations {
				return fmt.Errorf("environment already has the maximum of %d service declarations", platformv1alpha1.EnvironmentServiceMaxDeclarations)
			}
			instanceID, err := randomServiceInstanceID()
			if err != nil {
				return err
			}
			result = desiredEnvironmentService(name, targetPort, 1, instanceID)
			environment.Spec.Services = append(environment.Spec.Services, result)
		} else {
			existing := environment.Spec.Services[index]
			if existing.Source == platformv1alpha1.EnvironmentServiceSourceRepository {
				return fmt.Errorf("service %q is Repository-owned and cannot be mutated by the services CLI", name)
			}
			instanceID := existing.InstanceID
			if update && instanceID == "" {
				var err error
				instanceID, err = randomServiceInstanceID()
				if err != nil {
					return err
				}
			}
			desired := desiredEnvironmentService(name, targetPort, existing.Revision, instanceID)
			if existing == desired {
				result = existing
				return nil
			}
			if !update {
				return fmt.Errorf("service %q is already declared with different configuration; use update", name)
			}
			if existing.Revision == math.MaxInt64 {
				return fmt.Errorf("service %q revision cannot be increased", name)
			}
			desired.Revision++
			result = desired
			environment.Spec.Services[index] = desired
		}
		return kube.Patch(ctx, &environment, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		return platformv1alpha1.EnvironmentServiceDeclaration{}, fmt.Errorf("%s service %q on environment %q: %w", map[bool]string{false: "declare", true: "update"}[update], name, key.Name, err)
	}
	return result, nil
}

func randomServiceInstanceID() (string, error) {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate service declaration instance ID: %w", err)
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)), nil
}

func validServiceInstanceID(value string) bool {
	if len(value) < 20 || len(value) > 63 {
		return false
	}
	for _, c := range value {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func removeEnvironmentService(ctx context.Context, kube client.Client, key types.NamespacedName, name string) error {
	var pinnedUID types.UID
	pinned := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var environment platformv1alpha1.Environment
		if err := kube.Get(ctx, key, &environment); err != nil {
			return err
		}
		if !pinned {
			pinnedUID = environment.UID
			pinned = true
		} else if environment.UID != pinnedUID {
			return lifecycle.ErrEnvironmentIncarnationChanged
		}
		index := environmentServiceIndex(environment.Spec.Services, name)
		if index < 0 {
			return nil
		}
		if environment.Spec.Services[index].Source == platformv1alpha1.EnvironmentServiceSourceRepository {
			return fmt.Errorf("service %q is Repository-owned and cannot be mutated by the services CLI", name)
		}
		before := environment.DeepCopy()
		environment.Spec.Services = append(environment.Spec.Services[:index], environment.Spec.Services[index+1:]...)
		return kube.Patch(ctx, &environment, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		return fmt.Errorf("remove service %q from environment %q: %w", name, key.Name, err)
	}
	return nil
}

func environmentServiceIndex(services []platformv1alpha1.EnvironmentServiceDeclaration, name string) int {
	for i := range services {
		if services[i].Name == name {
			return i
		}
	}
	return -1
}

const serviceObservationMaxAge = 15 * time.Second

func listEnvironmentServices(ctx context.Context, kube client.Reader, key types.NamespacedName, out io.Writer) error {
	return listEnvironmentServicesAt(ctx, kube, key, out, time.Now())
}

// listEnvironmentServicesAt keeps the advisory display's wall-clock qualification testable.
func listEnvironmentServicesAt(ctx context.Context, kube client.Reader, key types.NamespacedName, out io.Writer, now time.Time) error {
	var environment platformv1alpha1.Environment
	if err := kube.Get(ctx, key, &environment); err != nil {
		return fmt.Errorf("get environment %q: %w", key.Name, err)
	}
	services := append([]platformv1alpha1.EnvironmentServiceDeclaration(nil), environment.Spec.Services...)
	sort.Slice(services, func(i, j int) bool { return services[i].Name < services[j].Name })
	fmt.Fprintln(out, "NAME\tSOURCE\tREVISION\tPROTOCOL\tTARGET-PORT\tVISIBILITY\tREADINESS\tSTATE\tREASON\tOBSERVED-AT\tFRESHNESS")
	for _, service := range services {
		state, reason, observedAt, freshness := "-", "-", "-", "NO-OBSERVATION"
		observations := environment.Status.ServiceObservations
		if observations != nil {
			for _, observation := range observations.Records {
				if observation.Name == service.Name {
					state, reason, observedAt, freshness = string(observation.State), string(observation.Reason), observations.ObservedAt.UTC().Format("2006-01-02T15:04:05Z"), "STALE"
					suspended := environment.Spec.Paused || environment.Status.Lifecycle.Suspended || environment.Spec.Lifecycle.Hold != nil && environment.Spec.Lifecycle.Hold.Enabled
					ready := platformv1alpha1.IsEnvironmentReady(&environment)
					classificationCurrent := false
					switch observation.State {
					case platformv1alpha1.EnvironmentServiceObservationPending:
						classificationCurrent = observations.ExecutionGeneration == nil && !suspended && !ready
					case platformv1alpha1.EnvironmentServiceObservationUnavailable:
						classificationCurrent = observations.ExecutionGeneration == nil && suspended
					default:
						classificationCurrent = observations.ExecutionGeneration != nil && *observations.ExecutionGeneration == environment.Status.ExecutionGeneration && ready && !suspended
					}
					age := now.Sub(observations.ObservedAt.Time)
					if environment.DeletionTimestamp.IsZero() && observations.ObservedGeneration == environment.Generation && observation.DeclarationRevision == service.Revision && observations.LifecycleEpoch == environment.Status.Lifecycle.Epoch && observations.HoldRevision == lifecycle.HoldPolicyRevision(&environment) && classificationCurrent && age >= 0 && age <= serviceObservationMaxAge {
						freshness = "CURRENT"
					}
					break
				}
			}
		}
		fmt.Fprintf(out, "%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", service.Name, service.Source, service.Revision, service.Protocol, service.TargetPort, service.Visibility, service.Readiness, state, reason, observedAt, freshness)
	}
	return nil
}
