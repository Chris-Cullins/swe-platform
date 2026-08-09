// Package egresspod contains an inert staged Pod-spec foundation for future
// restricted egress. No controller calls this package today.
package egresspod

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/Chris-Cullins/swe-platform/internal/egressidentity"
)

const (
	ForwarderName                 = "egress-forwarder"
	CredentialVolume              = "egress-client-credentials"
	ProxyCAVolume                 = "egress-proxy-ca"
	CredentialMount               = "/var/run/swe-platform/egress/client"
	ProxyCAMount                  = "/var/run/swe-platform/egress/proxy"
	ProxyServerCAKey              = "ca.crt"
	SchedulingGateName            = "egress.swe.dev/identity-ready"
	ExecutionGenerationAnnotation = "swe.dev/execution-generation"
	LoopbackPort                  = int32(15001)
	ForwarderRevision             = "1"
)

type RestrictedEgress struct {
	Backend           string
	OperatingSystem   string
	ForwarderImage    string
	ProxyAddress      string
	ProxyServerName   string
	CredentialSecret  string
	ProxyCASecret     string
	CredentialFSGroup int64
}

// Binding is retained across Secret creation and uncached reread. Its private
// expected identity and hashes prevent validation from adopting an unrelated,
// internally consistent same-name Secret.
type Binding struct {
	data                   map[string][sha256.Size]byte
	secretNamespace        string
	secretName             string
	podName                string
	podUID                 types.UID
	createdSecretUID       types.UID
	createdResourceVersion string
	sealed                 bool
}

// PrepareRestrictedEgress atomically adds an unscheduled native restartable
// init-sidecar. The referenced credential Secret must not exist yet. A future
// publisher may issue it only after CREATE returns a UID and the persisted Pod
// passes ValidateBoundPod; only then may that publisher remove the gate.
func PrepareRestrictedEgress(pod *corev1.Pod, in RestrictedEgress) error {
	if err := validatePreparation(pod, in); err != nil {
		return err
	}
	copy := pod.DeepCopy()
	copy.Spec.AutomountServiceAccountToken = ptr.To(false)
	copy.Spec.EnableServiceLinks = ptr.To(false)
	copy.Spec.OS = &corev1.PodOS{Name: corev1.Linux}
	copy.Spec.SchedulingGates = append(copy.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: SchedulingGateName})
	proxyURL := "http://127.0.0.1:" + strconv.Itoa(int(LoopbackPort))
	for i := range copy.Spec.Containers {
		copy.Spec.Containers[i].Env = proxyEnvironment(copy.Spec.Containers[i].Env, proxyURL)
	}
	for i := range copy.Spec.InitContainers {
		copy.Spec.InitContainers[i].Env = proxyEnvironment(copy.Spec.InitContainers[i].Env, proxyURL)
	}
	forwarder := forwarderContainer(in)
	copy.Spec.InitContainers = append([]corev1.Container{forwarder}, copy.Spec.InitContainers...)
	clientVolume, caVolume := credentialVolumes(in)
	copy.Spec.Volumes = append(copy.Spec.Volumes, clientVolume, caVolume)
	*pod = *copy
	return nil
}

func forwarderContainer(in RestrictedEgress) corev1.Container {
	restart := corev1.ContainerRestartPolicyAlways
	return corev1.Container{
		Name: ForwarderName, Image: in.ForwarderImage, ImagePullPolicy: corev1.PullIfNotPresent,
		TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		Command: []string{"egress-proxy", "forward"},
		Args: []string{"--address=127.0.0.1:" + strconv.Itoa(int(LoopbackPort)), "--server=" + in.ProxyAddress,
			"--server-name=" + in.ProxyServerName, "--cert=" + CredentialMount + "/" + egressidentity.ClientCertificateKey,
			"--key=" + CredentialMount + "/" + egressidentity.ClientPrivateKeyKey, "--ca=" + ProxyCAMount + "/" + ProxyServerCAKey},
		RestartPolicy: &restart,
		StartupProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
			Port: intstr.FromInt32(LoopbackPort), Host: "127.0.0.1"}}, PeriodSeconds: 1, TimeoutSeconds: 1, FailureThreshold: 30, SuccessThreshold: 1},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
		},
		SecurityContext: &corev1.SecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](65532), RunAsGroup: ptr.To[int64](65532),
			ReadOnlyRootFilesystem: ptr.To(true), AllowPrivilegeEscalation: ptr.To(false), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
		VolumeMounts: []corev1.VolumeMount{{Name: CredentialVolume, MountPath: CredentialMount, ReadOnly: true}, {Name: ProxyCAVolume, MountPath: ProxyCAMount, ReadOnly: true}},
	}
}

func credentialVolumes(in RestrictedEgress) (corev1.Volume, corev1.Volume) {
	mode := int32(0o440)
	return corev1.Volume{Name: CredentialVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: in.CredentialSecret, DefaultMode: &mode, Optional: ptr.To(false), Items: []corev1.KeyToPath{
				{Key: egressidentity.ClientCertificateKey, Path: egressidentity.ClientCertificateKey},
				{Key: egressidentity.ClientPrivateKeyKey, Path: egressidentity.ClientPrivateKeyKey},
			}}}}, corev1.Volume{Name: ProxyCAVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: in.ProxyCASecret, DefaultMode: &mode, Optional: ptr.To(false), Items: []corev1.KeyToPath{{Key: ProxyServerCAKey, Path: ProxyServerCAKey}},
		}}}
}

// IssueCredentialSecret validates the persisted gated Pod, binds claims to its
// exact UID/execution, and returns one immutable exact-Pod-owned Secret.
func IssueCredentialSecret(now time.Time, pod *corev1.Pod, in RestrictedEgress, claims egressidentity.Claims) (*corev1.Secret, *Binding, error) {
	if err := validatePersistedPod(pod, in); err != nil {
		return nil, nil, err
	}
	owner := metav1.GetControllerOf(pod)
	generation, err := strconv.ParseInt(pod.Annotations[ExecutionGenerationAnnotation], 10, 64)
	if pod.UID == "" || owner == nil || owner.APIVersion != "swe.dev/v1alpha1" || owner.Kind != "Environment" || owner.UID == "" || generation < 1 || err != nil {
		return nil, nil, errors.New("persisted Pod has no exact Environment execution")
	}
	if claims.EnvironmentNamespace != pod.Namespace || claims.EnvironmentName != owner.Name || claims.EnvironmentUID != owner.UID ||
		claims.ProjectNamespace != pod.Namespace || claims.PodName != pod.Name || claims.PodUID != pod.UID ||
		claims.ExecutionGeneration != generation || claims.ForwarderRevision != ForwarderRevision {
		return nil, nil, errors.New("authoritative identity does not match persisted Pod execution")
	}
	issued, material, err := egressidentity.IssueForClaims(now, claims)
	if err != nil {
		return nil, nil, err
	}
	defer material.Clear()
	canonical, err := issued.CanonicalBytes()
	if err != nil {
		return nil, nil, err
	}
	immutable := true
	controller := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: in.CredentialSecret, Namespace: pod.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID, Controller: &controller}}}, Immutable: &immutable,
		Data: map[string][]byte{egressidentity.ClientCertificateKey: append([]byte(nil), material.Certificate...), egressidentity.ClientPrivateKeyKey: append([]byte(nil), material.PrivateKey...), egressidentity.ClientTrustKey: append([]byte(nil), material.CA...), egressidentity.CanonicalClaimsKey: canonical}}
	binding := &Binding{
		data:            make(map[string][sha256.Size]byte, len(secret.Data)),
		secretNamespace: secret.Namespace,
		secretName:      secret.Name,
		podName:         pod.Name,
		podUID:          pod.UID,
	}
	for key, value := range secret.Data {
		binding.data[key] = sha256.Sum256(value)
	}
	return secret, binding, nil
}

// SealCreatedSecret records the identity returned by the one successful Secret
// CREATE. Future wiring must not call this after AlreadyExists, GET, or adoption.
func (binding *Binding) SealCreatedSecret(secret *corev1.Secret) error {
	if binding == nil || binding.sealed {
		return errors.New("issuance binding is absent or already sealed")
	}
	if secret == nil || secret.UID == "" || secret.ResourceVersion == "" {
		return errors.New("successful Secret CREATE response with identity is required")
	}
	if err := binding.validateIssuedSecret(secret); err != nil {
		return fmt.Errorf("Secret CREATE response does not match issuance: %w", err)
	}
	binding.createdSecretUID = secret.UID
	binding.createdResourceVersion = secret.ResourceVersion
	binding.sealed = true
	return nil
}

// ValidateBoundPod validates the exact persisted Pod and exact persisted Secret
// immediately before a future caller may remove the scheduling gate.
func ValidateBoundPod(pod *corev1.Pod, in RestrictedEgress, secret *corev1.Secret, binding *Binding) error {
	if secret == nil || binding == nil || !binding.sealed || binding.createdSecretUID == "" || binding.createdResourceVersion == "" {
		return errors.New("persisted credential Secret and issuance binding are required")
	}
	if secret.UID != binding.createdSecretUID || secret.ResourceVersion != binding.createdResourceVersion {
		return errors.New("persisted credential Secret identity does not match successful creation")
	}
	if err := binding.validateIssuedSecret(secret); err != nil {
		return err
	}
	claims, err := egressidentity.Parse(secret.Data[egressidentity.CanonicalClaimsKey])
	if err != nil {
		return err
	}
	if pod == nil || pod.UID == "" || pod.Name != claims.PodName || pod.UID != claims.PodUID || pod.Namespace != claims.EnvironmentNamespace {
		return errors.New("identity does not bind the exact API-assigned Pod")
	}
	if claims.ForwarderRevision != ForwarderRevision {
		return errors.New("forwarder security revision is stale")
	}
	fingerprint, err := egressidentity.FingerprintCertificate(secret.Data[egressidentity.ClientCertificateKey])
	if err != nil || claims.CertificateFingerprint != hex.EncodeToString(fingerprint[:]) {
		return errors.New("client certificate fingerprint does not match identity")
	}
	if _, err := tls.X509KeyPair(secret.Data[egressidentity.ClientCertificateKey], secret.Data[egressidentity.ClientPrivateKeyKey]); err != nil || !reflect.DeepEqual(secret.Data[egressidentity.ClientTrustKey], secret.Data[egressidentity.ClientCertificateKey]) {
		return errors.New("client certificate, private key, and self trust do not match")
	}
	generation, err := strconv.ParseInt(pod.Annotations[ExecutionGenerationAnnotation], 10, 64)
	owner := metav1.GetControllerOf(pod)
	if err != nil || generation != claims.ExecutionGeneration || owner == nil || owner.APIVersion != "swe.dev/v1alpha1" || owner.Kind != "Environment" || owner.Name != claims.EnvironmentName || owner.UID != claims.EnvironmentUID {
		return errors.New("persisted Pod execution or Environment owner does not match identity")
	}
	secretOwner := metav1.GetControllerOf(secret)
	if secret.Name != in.CredentialSecret || secret.Namespace != pod.Namespace || secret.Immutable == nil || !*secret.Immutable || secretOwner == nil || secretOwner.APIVersion != "v1" || secretOwner.Kind != "Pod" || secretOwner.Name != pod.Name || secretOwner.UID != pod.UID || len(secret.Data[egressidentity.ClientPrivateKeyKey]) == 0 || len(secret.Data[egressidentity.ClientTrustKey]) == 0 {
		return errors.New("credential Secret is not immutable and exact-Pod-owned")
	}
	if err := validatePersistedPod(pod, in); err != nil {
		return err
	}
	return nil
}

func (binding *Binding) validateIssuedSecret(secret *corev1.Secret) error {
	if len(binding.data) != 4 || len(secret.Data) != len(binding.data) {
		return errors.New("persisted credential Secret data changed")
	}
	for key, want := range binding.data {
		if sha256.Sum256(secret.Data[key]) != want {
			return errors.New("persisted credential Secret does not match issuance")
		}
	}
	owner := metav1.GetControllerOf(secret)
	if secret.Name != binding.secretName || secret.Namespace != binding.secretNamespace || secret.Immutable == nil || !*secret.Immutable ||
		owner == nil || owner.APIVersion != "v1" || owner.Kind != "Pod" || owner.Name != binding.podName || owner.UID != binding.podUID {
		return errors.New("credential Secret is not the exact immutable issued object")
	}
	return nil
}

func validatePreparation(pod *corev1.Pod, in RestrictedEgress) error {
	if pod == nil || pod.Name == "" || pod.UID != "" {
		return errors.New("pre-create Pod with a fixed name and no UID is required")
	}
	if in.Backend != "pod" || in.OperatingSystem != "linux" {
		return fmt.Errorf("restricted egress backend/OS %q/%q is unsupported", in.Backend, in.OperatingSystem)
	}
	if in.ForwarderImage == "" || in.CredentialSecret == "" || in.ProxyCASecret == "" || in.ProxyServerName == "" || in.CredentialSecret == in.ProxyCASecret {
		return errors.New("forwarder image, credential Secret, proxy CA Secret, and server name are required")
	}
	host, port, err := net.SplitHostPort(in.ProxyAddress)
	if err != nil || host == "" || port == "" {
		return errors.New("proxy address must be an explicit host and port")
	}
	if parsed, err := strconv.ParseUint(port, 10, 16); err != nil || parsed == 0 {
		return errors.New("proxy address port is invalid")
	}
	if pod.Spec.OS != nil && pod.Spec.OS.Name != corev1.Linux {
		return errors.New("Windows Pods are unsupported")
	}
	if pod.Spec.HostPID || pod.Spec.HostIPC || pod.Spec.HostNetwork || pod.Spec.ShareProcessNamespace != nil && *pod.Spec.ShareProcessNamespace {
		return errors.New("restricted egress Pod must not use host or shared process namespaces")
	}
	if pod.Spec.NodeName != "" {
		return errors.New("restricted egress Pod must be held by the scheduler")
	}
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.FSGroup == nil || in.CredentialFSGroup <= 0 || *pod.Spec.SecurityContext.FSGroup != in.CredentialFSGroup {
		return errors.New("restricted egress Pod requires an fsGroup for read-only credentials")
	}
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Name != "environment" {
		return errors.New("restricted egress requires exactly the environment container")
	}
	if len(pod.Spec.InitContainers) != 0 && (len(pod.Spec.InitContainers) != 2 || pod.Spec.InitContainers[0].Name != "repository-clone" || pod.Spec.InitContainers[1].Name != "project-hooks") {
		return errors.New("restricted egress requires clone then project-hooks init ordering")
	}
	if len(pod.Spec.SchedulingGates) != 0 {
		return errors.New("restricted egress preparation requires no existing scheduling gates")
	}
	return rejectAmbientCredentialReferences(pod, in)
}

func validatePersistedPod(pod *corev1.Pod, in RestrictedEgress) error {
	if pod == nil {
		return errors.New("persisted Pod is required")
	}
	if pod.Spec.OS == nil || pod.Spec.OS.Name != corev1.Linux || len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != SchedulingGateName {
		return errors.New("persisted Pod is not Linux and identity-gated")
	}
	if pod.Spec.HostPID || pod.Spec.HostIPC || pod.Spec.HostNetwork || pod.Spec.ShareProcessNamespace != nil && *pod.Spec.ShareProcessNamespace {
		return errors.New("persisted Pod uses a forbidden namespace")
	}
	if pod.Spec.NodeName != "" {
		return errors.New("persisted Pod bypasses the scheduling gate")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || pod.Spec.EnableServiceLinks == nil || *pod.Spec.EnableServiceLinks || pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.FSGroup == nil || *pod.Spec.SecurityContext.FSGroup != in.CredentialFSGroup {
		return errors.New("persisted Pod ambient identity settings changed")
	}
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Name != "environment" || len(pod.Spec.EphemeralContainers) != 0 ||
		len(pod.Spec.InitContainers) != 1 && len(pod.Spec.InitContainers) != 3 || len(pod.Spec.InitContainers) == 3 && (pod.Spec.InitContainers[1].Name != "repository-clone" || pod.Spec.InitContainers[2].Name != "project-hooks") {
		return errors.New("persisted Pod has no native restartable egress init-sidecar")
	}
	forwarder := pod.Spec.InitContainers[0]
	if !apiequality.Semantic.DeepEqual(forwarder, forwarderContainer(in)) {
		return errors.New("persisted forwarder isolation changed")
	}
	wantClient, wantCA := credentialVolumes(in)
	clientVolume, caVolume := false, false
	for _, volume := range pod.Spec.Volumes {
		clientVolume = clientVolume || apiequality.Semantic.DeepEqual(volume, wantClient)
		caVolume = caVolume || apiequality.Semantic.DeepEqual(volume, wantCA)
		if volume.Name != CredentialVolume && volume.Name != ProxyCAVolume && volumeSourceReferencesEgressSecret(volume.VolumeSource, in) {
			return errors.New("persisted Pod gained an aliased egress Secret volume")
		}
		if volume.Name != CredentialVolume && volume.Name != ProxyCAVolume && volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.ServiceAccountToken != nil {
					return errors.New("persisted Pod gained an ambient credential projection")
				}
			}
		}
	}
	if !clientVolume || !caVolume {
		return errors.New("persisted egress Secret sources changed")
	}
	all := append(append([]corev1.Container(nil), pod.Spec.InitContainers[1:]...), pod.Spec.Containers...)
	for _, container := range all {
		for _, mount := range container.VolumeMounts {
			if mount.Name == CredentialVolume || mount.Name == ProxyCAVolume {
				return errors.New("persisted workload gained an egress credential mount")
			}
		}
		for _, from := range container.EnvFrom {
			if from.SecretRef != nil && (from.SecretRef.Name == in.CredentialSecret || from.SecretRef.Name == in.ProxyCASecret) {
				return errors.New("persisted workload gained egress Secret envFrom")
			}
		}
		for _, variable := range container.Env {
			if variable.ValueFrom != nil && variable.ValueFrom.SecretKeyRef != nil && (variable.ValueFrom.SecretKeyRef.Name == in.CredentialSecret || variable.ValueFrom.SecretKeyRef.Name == in.ProxyCASecret) {
				return errors.New("persisted workload gained egress Secret env")
			}
		}
	}
	for _, container := range pod.Spec.EphemeralContainers {
		for _, mount := range container.VolumeMounts {
			if mount.Name == CredentialVolume || mount.Name == ProxyCAVolume {
				return errors.New("persisted ephemeral container gained an egress credential mount")
			}
		}
		for _, from := range container.EnvFrom {
			if from.SecretRef != nil && (from.SecretRef.Name == in.CredentialSecret || from.SecretRef.Name == in.ProxyCASecret) {
				return errors.New("persisted ephemeral container gained egress Secret envFrom")
			}
		}
		for _, variable := range container.Env {
			if variable.ValueFrom != nil && variable.ValueFrom.SecretKeyRef != nil && (variable.ValueFrom.SecretKeyRef.Name == in.CredentialSecret || variable.ValueFrom.SecretKeyRef.Name == in.ProxyCASecret) {
				return errors.New("persisted ephemeral container gained egress Secret env")
			}
		}
	}
	return nil
}

func rejectAmbientCredentialReferences(pod *corev1.Pod, in RestrictedEgress) error {
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == CredentialVolume || volume.Name == ProxyCAVolume || volumeSourceReferencesEgressSecret(volume.VolumeSource, in) {
			return errors.New("egress credential or proxy CA volume already exists")
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.ServiceAccountToken != nil {
					return errors.New("ambient projected credential or service-account token is forbidden")
				}
			}
		}
	}
	for _, container := range append(append([]corev1.Container(nil), pod.Spec.InitContainers...), pod.Spec.Containers...) {
		for _, from := range container.EnvFrom {
			if from.SecretRef != nil && (from.SecretRef.Name == in.CredentialSecret || from.SecretRef.Name == in.ProxyCASecret) {
				return errors.New("ambient egress Secret envFrom is forbidden")
			}
		}
		for _, variable := range container.Env {
			if variable.ValueFrom != nil && variable.ValueFrom.SecretKeyRef != nil && (variable.ValueFrom.SecretKeyRef.Name == in.CredentialSecret || variable.ValueFrom.SecretKeyRef.Name == in.ProxyCASecret) {
				return errors.New("ambient egress Secret env is forbidden")
			}
		}
	}
	return nil
}

// volumeSourceReferencesEgressSecret exhaustively covers Secret-name fields in
// the pinned corev1.VolumeSource API, including deprecated inline providers.
func volumeSourceReferencesEgressSecret(source corev1.VolumeSource, in RestrictedEgress) bool {
	reserved := func(name string) bool {
		return name == in.CredentialSecret || name == in.ProxyCASecret
	}
	if source.Secret != nil && reserved(source.Secret.SecretName) ||
		source.ISCSI != nil && source.ISCSI.SecretRef != nil && reserved(source.ISCSI.SecretRef.Name) ||
		source.RBD != nil && source.RBD.SecretRef != nil && reserved(source.RBD.SecretRef.Name) ||
		source.FlexVolume != nil && source.FlexVolume.SecretRef != nil && reserved(source.FlexVolume.SecretRef.Name) ||
		source.Cinder != nil && source.Cinder.SecretRef != nil && reserved(source.Cinder.SecretRef.Name) ||
		source.CephFS != nil && source.CephFS.SecretRef != nil && reserved(source.CephFS.SecretRef.Name) ||
		source.AzureFile != nil && reserved(source.AzureFile.SecretName) ||
		source.ScaleIO != nil && source.ScaleIO.SecretRef != nil && reserved(source.ScaleIO.SecretRef.Name) ||
		source.StorageOS != nil && source.StorageOS.SecretRef != nil && reserved(source.StorageOS.SecretRef.Name) ||
		source.CSI != nil && source.CSI.NodePublishSecretRef != nil && reserved(source.CSI.NodePublishSecretRef.Name) {
		return true
	}
	if source.Projected != nil {
		for _, projection := range source.Projected.Sources {
			if projection.Secret != nil && reserved(projection.Secret.Name) {
				return true
			}
		}
	}
	return false
}

func proxyEnvironment(existing []corev1.EnvVar, proxyURL string) []corev1.EnvVar {
	names := map[string]struct{}{"HTTP_PROXY": {}, "HTTPS_PROXY": {}, "http_proxy": {}, "https_proxy": {}, "NO_PROXY": {}, "no_proxy": {}}
	result := make([]corev1.EnvVar, 0, len(existing)+6)
	for _, variable := range existing {
		if _, replace := names[variable.Name]; !replace {
			result = append(result, variable)
		}
	}
	return append(result, corev1.EnvVar{Name: "HTTP_PROXY", Value: proxyURL}, corev1.EnvVar{Name: "HTTPS_PROXY", Value: proxyURL},
		corev1.EnvVar{Name: "http_proxy", Value: proxyURL}, corev1.EnvVar{Name: "https_proxy", Value: proxyURL},
		corev1.EnvVar{Name: "NO_PROXY", Value: ""}, corev1.EnvVar{Name: "no_proxy", Value: ""})
}
