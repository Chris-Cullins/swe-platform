package egresspod

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Chris-Cullins/swe-platform/internal/egressidentity"
)

// PublicationError reports a fixed, credential-free stage. Uncertain means the
// create or release write may have succeeded despite its response. It is never
// safe to blindly retry publication, including after a definite later failure:
// the credential may already exist. No error implies rollback or cleanup.
type PublicationError struct {
	Stage     string
	Uncertain bool
}

func (e *PublicationError) Error() string {
	if e.Uncertain {
		return fmt.Sprintf("egress publication %s outcome is uncertain", e.Stage)
	}
	return fmt.Sprintf("egress publication %s failed", e.Stage)
}

// PublishCredential is an inert, single-attempt transaction with no runtime
// caller. reader must be uncached; pod is the persisted gated Pod, not a desired
// spec. The caller must supply authoritative claims and preparation inputs.
// This only binds a credential and releases a scheduling gate. It does not prove
// current policy, proxy readiness, path forcing or permission to run workloads.
//
// There is no cross-object atomicity: Secret/authority can change after the last
// read. Future activation must provide live authority checks and ordered fencing.
// A lost issuance binding cannot be recovered by adopting an existing Secret.
func PublishCredential(ctx context.Context, writer client.Client, reader client.Reader, now time.Time, pod *corev1.Pod, in RestrictedEgress, claims egressidentity.Claims) error {
	failed := func(stage string) error { return &PublicationError{Stage: stage} }
	if writer == nil || reader == nil || pod == nil || pod.UID == "" || pod.ResourceVersion == "" || !pod.DeletionTimestamp.IsZero() {
		return failed("input")
	}
	// Revalidate before creating any credential, then require the same observed
	// Pod again immediately before release. Even unrelated concurrent changes
	// fail closed; the caller must not retry with a newly adopted binding.
	var current corev1.Pod
	if err := reader.Get(ctx, client.ObjectKeyFromObject(pod), &current); err != nil {
		return failed("read Pod")
	}
	if !samePublicationPod(pod, &current) {
		return failed("validate Pod")
	}
	secret, binding, err := IssueCredentialSecret(now, &current, in, claims)
	if err != nil {
		return failed("issue")
	}
	if err := writer.Create(ctx, secret); err != nil {
		return &PublicationError{Stage: "create", Uncertain: !definiteWriteRejection(err)}
	}
	if err := binding.SealCreatedSecret(secret); err != nil || !secret.DeletionTimestamp.IsZero() {
		return &PublicationError{Stage: "create", Uncertain: true}
	}
	var persisted corev1.Secret
	if err := reader.Get(ctx, client.ObjectKeyFromObject(secret), &persisted); err != nil {
		return failed("read Secret")
	}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(pod), &current); err != nil {
		return failed("read Pod")
	}
	if !samePublicationPod(pod, &current) || !persisted.DeletionTimestamp.IsZero() || ValidateBoundPod(&current, in, &persisted, binding) != nil {
		return failed("validate binding")
	}
	issued, err := egressidentity.Parse(persisted.Data[egressidentity.CanonicalClaimsKey])
	if err != nil {
		return failed("validate claims")
	}
	annotations := current.DeepCopy().Annotations
	annotations[PolicyRevisionAnnotation] = issued.RuntimePolicyRevision
	annotations[ForwarderRevisionAnnotation] = issued.ForwarderRevision
	annotations[CertificateFingerprintAnnotation] = issued.CertificateFingerprint
	// UID and resourceVersion tests protect against replacement and concurrent
	// deletion/mutation. Annotation publication and gate removal are one Pod
	// write, preserving every unrelated annotation from the guarded version.
	patch, err := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": current.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": current.ResourceVersion},
		{"op": "test", "path": "/spec/schedulingGates/0/name", "value": SchedulingGateName},
		{"op": "add", "path": "/metadata/annotations", "value": annotations},
		{"op": "remove", "path": "/spec/schedulingGates/0"},
	})
	if err != nil {
		return failed("encode release")
	}
	if err := writer.Patch(ctx, &current, client.RawPatch(types.JSONPatchType, patch)); err != nil {
		return &PublicationError{Stage: "release", Uncertain: !definiteWriteRejection(err)}
	}
	expected := pod.DeepCopy()
	expected.Spec.SchedulingGates = nil
	if current.UID != pod.UID || current.Namespace != pod.Namespace || current.Name != pod.Name || current.ResourceVersion == "" || current.ResourceVersion == pod.ResourceVersion || !current.DeletionTimestamp.IsZero() ||
		!apiequality.Semantic.DeepEqual(current.Spec, expected.Spec) || !apiequality.Semantic.DeepEqual(current.OwnerReferences, pod.OwnerReferences) || !apiequality.Semantic.DeepEqual(current.Annotations, annotations) {
		return &PublicationError{Stage: "release", Uncertain: true}
	}
	return nil
}

func samePublicationPod(expected, current *corev1.Pod) bool {
	return current.UID == expected.UID && current.ResourceVersion == expected.ResourceVersion && current.DeletionTimestamp.IsZero() &&
		current.Namespace == expected.Namespace && current.Name == expected.Name &&
		apiequality.Semantic.DeepEqual(current.Spec, expected.Spec) &&
		apiequality.Semantic.DeepEqual(current.OwnerReferences, expected.OwnerReferences) &&
		apiequality.Semantic.DeepEqual(current.Annotations, expected.Annotations)
}

func definiteWriteRejection(err error) bool {
	return apierrors.IsAlreadyExists(err) || apierrors.IsConflict(err) || apierrors.IsForbidden(err) ||
		apierrors.IsUnauthorized(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsNotFound(err)
}
