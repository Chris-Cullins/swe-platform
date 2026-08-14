package egressproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/egressidentity"
	"github.com/Chris-Cullins/swe-platform/internal/egresspod"
	"github.com/Chris-Cullins/swe-platform/internal/egresspolicy"
	"github.com/Chris-Cullins/swe-platform/internal/tenancy"
)

const CurrentnessTimeout = 2 * time.Second
const MaxCurrentnessStates = 2048

// IdentityLookup resolves an untrusted presented certificate fingerprint to
// canonical claims. Implementations must not treat the certificate subject as
// authority. The future publisher may provide this lookup without granting the
// proxy Kubernetes Secret-read permission.
type IdentityLookup interface {
	Lookup(context.Context, [sha256.Size]byte) ([]byte, error)
}

// CurrentnessAuthorizer is an inert Kubernetes currentness proof. Reader must
// be an uncached API reader in future production wiring. The shipped command
// deliberately does not construct this type.
type CurrentnessAuthorizer struct {
	Reader            client.Reader
	Identities        IdentityLookup
	Installation      tenancy.InstallationIdentity
	PolicyConfigMap   types.NamespacedName
	currentnessWindow time.Duration

	mu      sync.Mutex
	records map[[sha256.Size]byte]*executionRecord
}

type executionState struct {
	done chan struct{}
	once sync.Once
}

type executionRecord struct {
	mu         sync.Mutex
	state      *executionState
	users      int
	leases     int
	loopCancel context.CancelFunc
	loopDone   chan struct{}
}

func newExecutionState() *executionState { return &executionState{done: make(chan struct{})} }
func (s *executionState) revoke()        { s.once.Do(func() { close(s.done) }) }

// Authorize performs a complete uncached proof for every new target. Any
// failure revokes the fingerprint's previously returned currentness signal.
// Successful leases share one immediate, deadline-bounded proof loop until the
// final lease releases it; no cached authorization survives uncertainty.
func (a *CurrentnessAuthorizer) Authorize(ctx context.Context, hint Identity, target string) (Authorization, error) {
	if a == nil || a.Reader == nil || a.Identities == nil || a.Installation.Key.Namespace == "" || a.Installation.Key.Name == "" || a.Installation.UID == "" || a.PolicyConfigMap.Namespace == "" || a.PolicyConfigMap.Name == "" {
		return Authorization{}, errors.New("currentness authorizer is not configured")
	}
	if hint.Fingerprint == ([sha256.Size]byte{}) {
		return Authorization{}, errors.New("certificate fingerprint is required")
	}
	evaluationCtx, cancel := context.WithTimeout(ctx, CurrentnessTimeout)
	defer cancel()
	record, err := a.record(hint.Fingerprint)
	if err != nil {
		return Authorization{}, err
	}
	defer func() {
		a.release(hint.Fingerprint, record)
	}()
	authorization, err := a.authorize(evaluationCtx, hint, target)
	record.mu.Lock()
	defer record.mu.Unlock()
	if err != nil {
		record.state.revoke()
		if record.loopCancel != nil {
			record.loopCancel()
		}
		return Authorization{}, err
	}
	if isClosed(record.state.done) {
		if record.leases != 0 {
			return Authorization{}, errors.New("revoked currentness leases are still draining")
		}
		record.state = newExecutionState()
	}
	authorization.Currentness = record.state.done
	authorization.ReleaseCurrentness = a.acquireLeaseLocked(hint.Fingerprint, record, record.state, hint, target)
	return authorization, nil
}

func (a *CurrentnessAuthorizer) acquireLeaseLocked(fingerprint [sha256.Size]byte, record *executionRecord, state *executionState, hint Identity, target string) func() {
	record.leases++
	if record.leases == 1 {
		ctx, cancel := context.WithCancel(context.Background())
		record.loopCancel = cancel
		record.loopDone = make(chan struct{})
		go a.runCurrentnessLoop(ctx, record, state, hint, target, record.loopDone)
	}
	return sync.OnceFunc(func() { a.releaseLease(fingerprint, record, state) })
}

func (a *CurrentnessAuthorizer) runCurrentnessLoop(ctx context.Context, record *executionRecord, state *executionState, hint Identity, target string, done chan struct{}) {
	defer close(done)
	window := a.currentnessWindow
	if window <= 0 {
		window = CurrentnessTimeout
	}
	// Polling cannot bound staleness to interval if it waits interval and then
	// grants a proof another interval. Split the complete currentness window
	// equally between time to the next proof and that proof's API deadline.
	cycle := window / 2
	if cycle <= 0 {
		cycle = window
	}
	for {
		cycleDeadline := time.Now().Add(cycle)
		proofCtx, cancel := context.WithDeadline(ctx, cycleDeadline)
		_, err := a.authorize(proofCtx, hint, target)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				record.mu.Lock()
				if record.state == state {
					state.revoke()
				}
				record.mu.Unlock()
			}
			return
		}
		wait := time.NewTimer(time.Until(cycleDeadline))
		select {
		case <-ctx.Done():
			if !wait.Stop() {
				<-wait.C
			}
			return
		case <-wait.C:
		}
	}
}

func (a *CurrentnessAuthorizer) authorize(ctx context.Context, hint Identity, target string) (Authorization, error) {
	hostname, err := egresspolicy.ParseHostname(target)
	if err != nil || string(hostname) != target {
		return Authorization{}, errors.New("target is not a canonical egress hostname")
	}
	canonicalClaims, err := a.Identities.Lookup(ctx, hint.Fingerprint)
	if err != nil {
		return Authorization{}, fmt.Errorf("resolve certificate identity: %w", err)
	}
	claims, err := egressidentity.Parse(canonicalClaims)
	if err != nil {
		return Authorization{}, err
	}
	if claims.InstallationNamespace != a.Installation.Key.Namespace || claims.InstallationName != a.Installation.Key.Name || claims.InstallationUID != a.Installation.UID || a.PolicyConfigMap.Namespace != a.Installation.Key.Namespace {
		return Authorization{}, errors.New("identity or policy ConfigMap does not match the configured Installation")
	}
	if claims.ProjectNamespace == a.Installation.Key.Namespace {
		return Authorization{}, errors.New("Project namespace must be distinct from the Installation system namespace")
	}
	presentedFingerprint := hex.EncodeToString(hint.Fingerprint[:])
	if claims.CertificateFingerprint != presentedFingerprint {
		return Authorization{}, errors.New("presented certificate fingerprint does not match canonical identity")
	}

	installationKey := a.Installation.Key
	var installation platformv1alpha1.Installation
	if err := a.Reader.Get(ctx, installationKey, &installation); err != nil {
		return Authorization{}, fmt.Errorf("get Installation: %w", err)
	}
	if installation.UID != a.Installation.UID || !stable(&installation.ObjectMeta) {
		return Authorization{}, errors.New("Installation identity is not current")
	}
	verifier := tenancy.Verifier{Reader: a.Reader, Installation: a.Installation, Mode: tenancy.ModeScoped}
	claim, err := verifier.VerifyNamespace(ctx, claims.ProjectNamespace)
	if err != nil {
		return Authorization{}, err
	}
	if claim.Lifecycle != tenancy.LifecycleActive || claim.ProjectName != claims.ProjectName || claim.ProjectUID != claims.ProjectUID || claims.EnvironmentNamespace != claims.ProjectNamespace {
		return Authorization{}, errors.New("active exact Namespace claim does not match identity")
	}
	var namespace corev1.Namespace
	if err := a.Reader.Get(ctx, types.NamespacedName{Name: claims.ProjectNamespace}, &namespace); err != nil || namespace.UID != claim.NamespaceUID || !stable(&namespace.ObjectMeta) {
		return Authorization{}, errors.New("Namespace identity is not current")
	}

	var project platformv1alpha1.Project
	projectKey := types.NamespacedName{Namespace: claims.ProjectNamespace, Name: claims.ProjectName}
	if err := a.Reader.Get(ctx, projectKey, &project); err != nil {
		return Authorization{}, fmt.Errorf("get Project: %w", err)
	}
	if project.UID != claims.ProjectUID || !stable(&project.ObjectMeta) {
		return Authorization{}, errors.New("Project identity is not current")
	}

	var configMap corev1.ConfigMap
	if err := a.Reader.Get(ctx, a.PolicyConfigMap, &configMap); err != nil {
		return Authorization{}, fmt.Errorf("get policy ConfigMap: %w", err)
	}
	config, err := egresspolicy.ParseConfigMap(&configMap)
	if err != nil {
		return Authorization{}, err
	}
	if config.Mode != egresspolicy.ModeRestricted || config.RestrictedProfile == nil {
		return Authorization{}, errors.New("currentness authorization requires restricted mode")
	}
	policy, err := egresspolicy.Evaluate(hostnames(config.Ceiling), hostnames(config.Baseline), project.Spec.EgressAllowlist)
	if err != nil {
		return Authorization{}, err
	}
	if !slices.Contains(policy.Effective, hostname) {
		return Authorization{}, errors.New("destination is outside the effective policy")
	}
	revision, err := (egresspolicy.RuntimeRevisionInputs{
		InstallationUID: installation.UID, ConfigMapUID: configMap.UID,
		ConfigMapContentSHA256: config.ContentSHA256, ProjectUID: project.UID,
		Ceiling: hostnames(config.Ceiling), Baseline: hostnames(config.Baseline),
		ProjectSelection: project.Spec.EgressAllowlist,
	}).Revision()
	if err != nil || claims.RuntimePolicyRevision != revision.String() {
		return Authorization{}, errors.New("runtime policy revision is not current")
	}

	var environment platformv1alpha1.Environment
	environmentKey := types.NamespacedName{Namespace: claims.EnvironmentNamespace, Name: claims.EnvironmentName}
	if err := a.Reader.Get(ctx, environmentKey, &environment); err != nil {
		return Authorization{}, fmt.Errorf("get Environment: %w", err)
	}
	if environment.UID != claims.EnvironmentUID || !stable(&environment.ObjectMeta) || environment.Spec.ProjectRef != project.Name ||
		environment.Status.ExecutionGeneration != claims.ExecutionGeneration || environment.Status.PodName != claims.PodName ||
		environment.Status.Provisioning == nil || environment.Status.Provisioning.Project == nil || !environment.Status.Provisioning.ProjectVerified ||
		environment.Status.Provisioning.Project.Name != project.Name || environment.Status.Provisioning.Project.UID != project.UID ||
		!platformv1alpha1.IsEnvironmentReady(&environment) || environment.Spec.Paused || environment.Status.Lifecycle.Suspended ||
		environment.Spec.Lifecycle.Hold != nil && environment.Spec.Lifecycle.Hold.Enabled {
		return Authorization{}, errors.New("Environment execution or frozen Project is not current")
	}

	var pod corev1.Pod
	if err := a.Reader.Get(ctx, types.NamespacedName{Namespace: claims.EnvironmentNamespace, Name: claims.PodName}, &pod); err != nil {
		return Authorization{}, fmt.Errorf("get Pod: %w", err)
	}
	owner := metav1.GetControllerOf(&pod)
	annotations := pod.GetAnnotations()
	if pod.UID != claims.PodUID || !stable(&pod.ObjectMeta) || pod.Spec.RestartPolicy != corev1.RestartPolicyNever || owner == nil || owner.APIVersion != platformv1alpha1.GroupVersion.String() || owner.Kind != "Environment" || owner.Name != environment.Name || owner.UID != environment.UID ||
		annotations[egresspod.ExecutionGenerationAnnotation] != strconv.FormatInt(claims.ExecutionGeneration, 10) ||
		annotations[egresspod.PolicyRevisionAnnotation] != claims.RuntimePolicyRevision ||
		annotations[egresspod.ForwarderRevisionAnnotation] != claims.ForwarderRevision || claims.ForwarderRevision != egresspod.ForwarderRevision ||
		annotations[egresspod.CertificateFingerprintAnnotation] != claims.CertificateFingerprint {
		return Authorization{}, errors.New("Pod execution identity is not current")
	}

	// Start a complete second uncached pass. UID/resourceVersion drift or
	// canonical authority changes invalidate the entire evaluation.
	var finalConfigMap corev1.ConfigMap
	if err := a.Reader.Get(ctx, a.PolicyConfigMap, &finalConfigMap); err != nil {
		return Authorization{}, fmt.Errorf("recheck policy ConfigMap: %w", err)
	}
	finalConfig, err := egresspolicy.ParseConfigMap(&finalConfigMap)
	if err != nil || finalConfigMap.UID != configMap.UID || finalConfigMap.ResourceVersion != configMap.ResourceVersion || finalConfig.ContentSHA256 != config.ContentSHA256 {
		return Authorization{}, errors.New("policy ConfigMap changed during authorization")
	}
	finalClaims, err := a.Identities.Lookup(ctx, hint.Fingerprint)
	if err != nil || !slices.Equal(finalClaims, canonicalClaims) {
		return Authorization{}, errors.New("certificate identity changed during authorization")
	}
	finalClaim, err := verifier.VerifyNamespace(ctx, claims.ProjectNamespace)
	if err != nil || finalClaim != claim || finalClaim.Lifecycle != tenancy.LifecycleActive {
		return Authorization{}, errors.New("Namespace claim changed during authorization")
	}
	var finalNamespace corev1.Namespace
	var finalProject platformv1alpha1.Project
	var finalEnvironment platformv1alpha1.Environment
	var finalPod corev1.Pod
	if err := a.Reader.Get(ctx, types.NamespacedName{Name: claims.ProjectNamespace}, &finalNamespace); err != nil || finalNamespace.UID != namespace.UID || finalNamespace.ResourceVersion != namespace.ResourceVersion || !stable(&finalNamespace.ObjectMeta) {
		return Authorization{}, errors.New("Namespace changed during authorization")
	}
	if err := a.Reader.Get(ctx, projectKey, &finalProject); err != nil || finalProject.UID != project.UID || finalProject.ResourceVersion != project.ResourceVersion || !stable(&finalProject.ObjectMeta) {
		return Authorization{}, errors.New("Project changed during authorization")
	}
	if err := a.Reader.Get(ctx, environmentKey, &finalEnvironment); err != nil || finalEnvironment.UID != environment.UID || finalEnvironment.ResourceVersion != environment.ResourceVersion || !stable(&finalEnvironment.ObjectMeta) {
		return Authorization{}, errors.New("Environment changed during authorization")
	}
	if err := a.Reader.Get(ctx, types.NamespacedName{Namespace: claims.EnvironmentNamespace, Name: claims.PodName}, &finalPod); err != nil || finalPod.UID != pod.UID || finalPod.ResourceVersion != pod.ResourceVersion || !stable(&finalPod.ObjectMeta) {
		return Authorization{}, errors.New("Pod changed during authorization")
	}
	var finalInstallation platformv1alpha1.Installation
	if err := a.Reader.Get(ctx, installationKey, &finalInstallation); err != nil || finalInstallation.UID != installation.UID || finalInstallation.ResourceVersion != installation.ResourceVersion || !stable(&finalInstallation.ObjectMeta) {
		return Authorization{}, errors.New("Installation changed during authorization")
	}
	if err := ctx.Err(); err != nil {
		return Authorization{}, fmt.Errorf("currentness evaluation expired: %w", err)
	}
	return Authorization{
		ExecutionKey: string(environment.UID) + "/" + strconv.FormatInt(claims.ExecutionGeneration, 10) + "/" + string(pod.UID),
		ProjectKey:   string(project.UID), DeniedPrefixes: deniedPrefixes(*config.RestrictedProfile),
	}, nil
}

func stable(meta *metav1.ObjectMeta) bool {
	return meta != nil && meta.UID != "" && meta.DeletionTimestamp.IsZero()
}

func hostnames(values []egresspolicy.Hostname) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = string(value)
	}
	return result
}

func deniedPrefixes(profile egresspolicy.RestrictedProfile) []netip.Prefix {
	values := make([]string, 0, len(profile.APIServerCIDRs)+len(profile.PodCIDRs)+len(profile.ServiceCIDRs)+len(profile.NodeCIDRs)+len(profile.ControlPlaneCIDRs)+len(profile.AdditionalDeniedCIDRs)+len(profile.ResolverIPs))
	values = append(values, profile.APIServerCIDRs...)
	values = append(values, profile.PodCIDRs...)
	values = append(values, profile.ServiceCIDRs...)
	values = append(values, profile.NodeCIDRs...)
	values = append(values, profile.ControlPlaneCIDRs...)
	values = append(values, profile.AdditionalDeniedCIDRs...)
	result := make([]netip.Prefix, 0, len(values)+len(profile.ResolverIPs))
	for _, value := range values {
		result = append(result, netip.MustParsePrefix(value))
	}
	for _, value := range profile.ResolverIPs {
		address := netip.MustParseAddr(value)
		result = append(result, netip.PrefixFrom(address, address.BitLen()))
	}
	return result
}

func (a *CurrentnessAuthorizer) record(fingerprint [sha256.Size]byte) (*executionRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.records == nil {
		a.records = make(map[[sha256.Size]byte]*executionRecord)
	}
	record := a.records[fingerprint]
	if record == nil {
		if len(a.records) >= MaxCurrentnessStates {
			return nil, errors.New("currentness state capacity exceeded")
		}
		record = &executionRecord{state: newExecutionState()}
		a.records[fingerprint] = record
	}
	record.users++
	return record, nil
}

func (a *CurrentnessAuthorizer) release(fingerprint [sha256.Size]byte, record *executionRecord) {
	record.mu.Lock()
	defer record.mu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	record.users--
	if record.users == 0 && record.leases == 0 && a.records[fingerprint] == record {
		delete(a.records, fingerprint)
	}
}

func (a *CurrentnessAuthorizer) releaseLease(fingerprint [sha256.Size]byte, record *executionRecord, state *executionState) {
	record.mu.Lock()
	if record.state != state || record.leases == 0 {
		record.mu.Unlock()
		return
	}
	record.leases--
	if record.leases != 0 {
		record.mu.Unlock()
		return
	}
	state.revoke()
	cancel, done := record.loopCancel, record.loopDone
	record.loopCancel, record.loopDone = nil, nil
	record.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if record.state == state && record.users == 0 && record.leases == 0 && a.records[fingerprint] == record {
		delete(a.records, fingerprint)
	}
}

func isClosed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}
