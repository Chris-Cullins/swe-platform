package repositorycredential

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	SecretType                    corev1.SecretType = "swe.dev/repository-credential"
	TokenKey                                        = "token"
	AnnotationRunName                               = "swe.dev/repository-credential-run-name"
	AnnotationRunUID                                = "swe.dev/repository-credential-run-uid"
	AnnotationProvider                              = "swe.dev/repository-credential-provider"
	AnnotationRepository                            = "swe.dev/repository-credential-repository"
	AnnotationSourceRepository                      = "swe.dev/repository-credential-source-repository"
	AnnotationInstallationID                        = "swe.dev/repository-credential-installation-id"
	AnnotationExpiry                                = "swe.dev/repository-credential-expiry"
	AnnotationEnvironmentUID                        = "swe.dev/repository-credential-environment-uid"
	AnnotationExecutionGeneration                   = "swe.dev/repository-credential-execution-generation"
	AnnotationTokenGeneration                       = "swe.dev/repository-credential-token-generation"
)

func SecretName(runUID types.UID) string { return "run-repository-credential-" + string(runUID) }

func PendingRevocationSecretName(runUID types.UID) string {
	return SecretName(runUID) + "-pending-revocation"
}

// ExactManagedIdentity validates deletion authority without requiring usable token data.
func ExactManagedIdentity(secret *corev1.Secret, runName string, runUID types.UID, provider, source, canonical string) bool {
	if secret == nil || secret.Name != SecretName(runUID) || secret.Type != SecretType || len(secret.OwnerReferences) != 0 {
		return false
	}
	a := secret.Annotations
	return a[AnnotationRunName] == runName && a[AnnotationRunUID] == string(runUID) && a[AnnotationProvider] == provider && a[AnnotationSourceRepository] == source && a[AnnotationRepository] == canonical
}

type Lease struct {
	Credential
	SecretUID           types.UID
	ResourceVersion     string
	RunName             string
	RunUID              types.UID
	Provider            string
	SourceRepository    string
	EnvironmentUID      types.UID
	ExecutionGeneration int64
	TokenGeneration     int64
}

func Parse(secret *corev1.Secret, runName string, runUID types.UID) (*Lease, error) {
	if secret == nil || !secret.DeletionTimestamp.IsZero() || secret.Name != SecretName(runUID) || secret.Type != SecretType || len(secret.OwnerReferences) != 0 {
		return nil, errors.New("foreign repository credential secret")
	}
	a := secret.Annotations
	if a[AnnotationRunName] != runName || a[AnnotationRunUID] != string(runUID) || a[AnnotationProvider] == "" || a[AnnotationRepository] == "" || a[AnnotationSourceRepository] == "" {
		return nil, errors.New("foreign repository credential secret")
	}
	installation, e1 := strconv.ParseInt(a[AnnotationInstallationID], 10, 64)
	expiry, e2 := time.Parse(time.RFC3339Nano, a[AnnotationExpiry])
	execution, e3 := strconv.ParseInt(a[AnnotationExecutionGeneration], 10, 64)
	generation, e4 := strconv.ParseInt(a[AnnotationTokenGeneration], 10, 64)
	token, ok := secret.Data[TokenKey]
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || installation < 1 || execution < 0 || generation < 1 || !ok || len(secret.Data) != 1 || len(token) == 0 || len(token) > MaxTokenBytes || bytes.IndexByte(token, 0) >= 0 {
		return nil, errors.New("malformed repository credential secret")
	}
	return &Lease{Credential: Credential{Token: append([]byte(nil), token...), Repository: a[AnnotationRepository], InstallationID: installation, ExpiresAt: expiry}, SecretUID: secret.UID, ResourceVersion: secret.ResourceVersion, RunName: runName, RunUID: runUID, Provider: a[AnnotationProvider], SourceRepository: a[AnnotationSourceRepository], EnvironmentUID: types.UID(a[AnnotationEnvironmentUID]), ExecutionGeneration: execution, TokenGeneration: generation}, nil
}

func ParsePendingRevocation(secret *corev1.Secret, runName string, runUID types.UID) (*Lease, error) {
	if secret == nil || secret.Name != PendingRevocationSecretName(runUID) {
		return nil, errors.New("foreign pending repository credential revocation")
	}
	copy := secret.DeepCopy()
	defer func() {
		for _, value := range copy.Data {
			clear(value)
		}
	}()
	copy.Name = SecretName(runUID)
	return Parse(copy, runName, runUID)
}

func NewSecret(namespace, runName string, runUID types.UID, provider, sourceRepository string, c *Credential, generation int64, envUID types.UID, execution int64, now time.Time) (*corev1.Secret, error) {
	if sourceRepository == "" || c == nil || c.Repository == "" || c.InstallationID < 1 || !c.ExpiresAt.After(now.Add(MinimumValidity)) || len(c.Token) == 0 || len(c.Token) > MaxTokenBytes || bytes.IndexByte(c.Token, 0) >= 0 {
		return nil, fmt.Errorf("invalid issued repository credential")
	}
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: SecretName(runUID), Annotations: map[string]string{AnnotationRunName: runName, AnnotationRunUID: string(runUID), AnnotationProvider: provider, AnnotationRepository: c.Repository, AnnotationSourceRepository: sourceRepository, AnnotationInstallationID: strconv.FormatInt(c.InstallationID, 10), AnnotationExpiry: c.ExpiresAt.Format(time.RFC3339Nano), AnnotationEnvironmentUID: string(envUID), AnnotationExecutionGeneration: strconv.FormatInt(execution, 10), AnnotationTokenGeneration: strconv.FormatInt(generation, 10)}}, Type: SecretType, Data: map[string][]byte{TokenKey: append([]byte(nil), c.Token...)}}, nil
}

func NewPendingRevocationSecret(namespace, runName string, runUID types.UID, provider, sourceRepository string, c *Credential, generation int64, now time.Time) (*corev1.Secret, error) {
	secret, err := NewSecret(namespace, runName, runUID, provider, sourceRepository, c, generation, "", 0, now)
	if err != nil {
		return nil, err
	}
	secret.Name = PendingRevocationSecretName(runUID)
	return secret, nil
}

func ClearLease(l *Lease) {
	if l != nil {
		clear(l.Token)
	}
}
