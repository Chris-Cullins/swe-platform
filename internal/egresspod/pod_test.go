package egresspod

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Chris-Cullins/swe-platform/internal/egressidentity"
)

func fixture() (*corev1.Pod, RestrictedEgress) {
	group := int64(10001)
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "env-one", Namespace: "project", Annotations: map[string]string{ExecutionGenerationAnnotation: "1"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "swe.dev/v1alpha1", Kind: "Environment", Name: "one", UID: "e", Controller: &controller}}}, Spec: corev1.PodSpec{
		SecurityContext: &corev1.PodSecurityContext{FSGroup: &group}, Containers: []corev1.Container{{Name: "environment", Env: []corev1.EnvVar{{Name: "NO_PROXY", Value: "metadata"}}}},
		InitContainers: []corev1.Container{{Name: "repository-clone"}, {Name: "project-hooks"}}, Volumes: []corev1.Volume{{Name: "workspace"}},
	}}
	return pod, RestrictedEgress{Backend: "pod", OperatingSystem: "linux", ForwarderImage: "proxy@sha256:abc", ProxyAddress: "proxy.system.svc:8443", ProxyServerName: "proxy.system.svc", CredentialSecret: "egress-one", ProxyCASecret: "proxy-ca", CredentialFSGroup: group}
}

func TestPrepareRestrictedEgressOrderingIsolationAndEnvironment(t *testing.T) {
	pod, input := fixture()
	if err := PrepareRestrictedEgress(pod, input); err != nil {
		t.Fatal(err)
	}
	if got := []string{pod.Spec.InitContainers[0].Name, pod.Spec.InitContainers[1].Name, pod.Spec.InitContainers[2].Name}; !reflect.DeepEqual(got, []string{ForwarderName, "repository-clone", "project-hooks"}) {
		t.Fatalf("init order = %v", got)
	}
	forwarder := pod.Spec.InitContainers[0]
	if forwarder.RestartPolicy == nil || *forwarder.RestartPolicy != corev1.ContainerRestartPolicyAlways || forwarder.StartupProbe == nil || len(forwarder.Resources.Requests) != 2 || len(forwarder.Resources.Limits) != 2 {
		t.Fatalf("forwarder lifecycle/resources = %#v", forwarder)
	}
	if forwarder.SecurityContext == nil || forwarder.SecurityContext.RunAsNonRoot == nil || !*forwarder.SecurityContext.RunAsNonRoot || forwarder.SecurityContext.ReadOnlyRootFilesystem == nil || !*forwarder.SecurityContext.ReadOnlyRootFilesystem || len(forwarder.SecurityContext.Capabilities.Drop) != 1 {
		t.Fatalf("forwarder security = %#v", forwarder.SecurityContext)
	}
	if len(forwarder.VolumeMounts) != 2 || forwarder.VolumeMounts[0].Name != CredentialVolume || forwarder.VolumeMounts[1].Name != ProxyCAVolume {
		t.Fatalf("forwarder mounts = %#v", forwarder.VolumeMounts)
	}
	for _, container := range append(pod.Spec.InitContainers[1:], pod.Spec.Containers...) {
		for _, mount := range container.VolumeMounts {
			if mount.Name == CredentialVolume || mount.Name == ProxyCAVolume {
				t.Fatalf("%s received egress credential mount", container.Name)
			}
		}
		values := map[string]string{}
		for _, variable := range container.Env {
			values[variable.Name] = variable.Value
		}
		if values["HTTP_PROXY"] != "http://127.0.0.1:15001" || values["https_proxy"] != "http://127.0.0.1:15001" || values["NO_PROXY"] != "" || values["no_proxy"] != "" {
			t.Fatalf("%s proxy env = %#v", container.Name, values)
		}
	}
	if pod.Spec.OS == nil || pod.Spec.OS.Name != corev1.Linux || len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != SchedulingGateName {
		t.Fatalf("Pod was not staged: %#v", pod.Spec)
	}
	if pod.Spec.EnableServiceLinks == nil || *pod.Spec.EnableServiceLinks || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || pod.Spec.ShareProcessNamespace != nil {
		t.Fatalf("Pod ambient settings = %#v", pod.Spec)
	}
	if pod.Spec.Volumes[len(pod.Spec.Volumes)-1].Secret.SecretName != input.ProxyCASecret {
		t.Fatal("administrator proxy CA is not separate")
	}
}

func TestValidateBoundPodRequiresExactUIDAndFingerprint(t *testing.T) {
	pod, input := fixture()
	if err := PrepareRestrictedEgress(pod, input); err != nil {
		t.Fatal(err)
	}
	pod.UID = "pod-uid"
	claims := egressidentity.Claims{InstallationNamespace: "system", InstallationName: "main", InstallationUID: "i", ProjectNamespace: "project", ProjectName: "project", ProjectUID: "p", EnvironmentNamespace: pod.Namespace, EnvironmentName: "one", EnvironmentUID: "e", PodName: pod.Name, PodUID: pod.UID, ExecutionGeneration: 1, RuntimePolicyRevision: "policy-revision", ForwarderRevision: ForwarderRevision}
	secret, binding, err := IssueCredentialSecret(time.Unix(1000, 0), pod, input, claims)
	if err != nil {
		t.Fatal(err)
	}
	secret.UID, secret.ResourceVersion = "secret-uid", "1"
	if err := binding.SealCreatedSecret(secret); err != nil {
		t.Fatal(err)
	}
	if err := ValidateBoundPod(pod, input, secret, binding); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*corev1.Pod, *corev1.Secret){
		"same-name replacement Pod":         func(p *corev1.Pod, _ *corev1.Secret) { p.UID = "replacement" },
		"same-name replacement Environment": func(p *corev1.Pod, _ *corev1.Secret) { p.OwnerReferences[0].UID = "replacement" },
		"replacement Secret UID":            func(_ *corev1.Pod, s *corev1.Secret) { s.UID = "replacement" },
		"changed Secret resourceVersion":    func(_ *corev1.Pod, s *corev1.Secret) { s.ResourceVersion = "2" },
		"fingerprint":                       func(_ *corev1.Pod, s *corev1.Secret) { s.Data[egressidentity.ClientCertificateKey] = []byte("bad") },
		"private key":                       func(_ *corev1.Pod, s *corev1.Secret) { s.Data[egressidentity.ClientPrivateKeyKey] = []byte("bad") },
		"gate":                              func(p *corev1.Pod, _ *corev1.Secret) { p.Spec.SchedulingGates = nil },
		"admission nodeName":                func(p *corev1.Pod, _ *corev1.Secret) { p.Spec.NodeName = "worker" },
		"admission mount": func(p *corev1.Pod, _ *corev1.Secret) {
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: CredentialVolume})
		},
		"admission Secret alias": func(p *corev1.Pod, _ *corev1.Secret) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "alias", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: input.CredentialSecret}}})
		},
		"admission sidecar": func(p *corev1.Pod, _ *corev1.Secret) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "injected"})
		},
		"ephemeral target": func(p *corev1.Pod, _ *corev1.Secret) {
			p.Spec.EphemeralContainers = append(p.Spec.EphemeralContainers, corev1.EphemeralContainer{TargetContainerName: ForwarderName})
		},
	} {
		t.Run(name, func(t *testing.T) {
			changedPod := pod.DeepCopy()
			changedSecret := secret.DeepCopy()
			mutate(changedPod, changedSecret)
			if err := ValidateBoundPod(changedPod, input, changedSecret, binding); err == nil {
				t.Fatal("stale binding accepted")
			}
		})
	}
	stalePod, staleInput := fixture()
	if err := PrepareRestrictedEgress(stalePod, staleInput); err != nil {
		t.Fatal(err)
	}
	stalePod.UID = "old-pod"
	if _, _, err := IssueCredentialSecret(time.Unix(1000, 0), stalePod, staleInput, claims); err == nil {
		t.Fatal("stale same-name Pod/Environment was adopted during issuance")
	}
}

func TestBindingSealsOnlyExactSuccessfulCreateOnce(t *testing.T) {
	pod, input := fixture()
	if err := PrepareRestrictedEgress(pod, input); err != nil {
		t.Fatal(err)
	}
	pod.UID = "pod-uid"
	claims := egressidentity.Claims{InstallationNamespace: "system", InstallationName: "main", InstallationUID: "i", ProjectNamespace: "project", ProjectName: "project", ProjectUID: "p", EnvironmentNamespace: pod.Namespace, EnvironmentName: "one", EnvironmentUID: "e", PodName: pod.Name, PodUID: pod.UID, ExecutionGeneration: 1, RuntimePolicyRevision: "policy-revision", ForwarderRevision: ForwarderRevision}
	secret, binding, err := IssueCredentialSecret(time.Unix(1000, 0), pod, input, claims)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBoundPod(pod, input, secret, binding); err == nil {
		t.Fatal("unsealed binding accepted")
	}
	for name, mutate := range map[string]func(*corev1.Secret){
		"empty UID":             func(s *corev1.Secret) { s.ResourceVersion = "1" },
		"empty resourceVersion": func(s *corev1.Secret) { s.UID = "secret-uid" },
		"tampered data": func(s *corev1.Secret) {
			s.UID, s.ResourceVersion = "secret-uid", "1"
			s.Data[egressidentity.ClientCertificateKey] = []byte("tampered")
		},
		"tampered owner": func(s *corev1.Secret) {
			s.UID, s.ResourceVersion = "secret-uid", "1"
			s.OwnerReferences[0].UID = "other-pod"
		},
		"tampered name": func(s *corev1.Secret) {
			s.UID, s.ResourceVersion = "secret-uid", "1"
			s.Name = "other-secret"
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := secret.DeepCopy()
			mutate(changed)
			if err := binding.SealCreatedSecret(changed); err == nil {
				t.Fatal("tampered create response sealed")
			}
		})
	}
	secret.UID, secret.ResourceVersion = "secret-uid", "1"
	if err := binding.SealCreatedSecret(secret); err != nil {
		t.Fatal(err)
	}
	if err := binding.SealCreatedSecret(secret); err == nil {
		t.Fatal("binding resealed")
	}
}

func TestPrepareRejectsEveryVolumeSourceSecretAlias(t *testing.T) {
	ref := func(name string) *corev1.LocalObjectReference {
		return &corev1.LocalObjectReference{Name: name}
	}
	sources := map[string]func(string) corev1.VolumeSource{
		"Secret": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: name}}
		},
		"Projected": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: name}}}}}}
		},
		"CSI": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{NodePublishSecretRef: ref(name)}}
		},
		"ISCSI": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{ISCSI: &corev1.ISCSIVolumeSource{SecretRef: ref(name)}}
		},
		"RBD": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{RBD: &corev1.RBDVolumeSource{SecretRef: ref(name)}}
		},
		"FlexVolume": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{FlexVolume: &corev1.FlexVolumeSource{SecretRef: ref(name)}}
		},
		"Cinder": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{Cinder: &corev1.CinderVolumeSource{SecretRef: ref(name)}}
		},
		"CephFS": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{CephFS: &corev1.CephFSVolumeSource{SecretRef: ref(name)}}
		},
		"AzureFile": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{AzureFile: &corev1.AzureFileVolumeSource{SecretName: name}}
		},
		"ScaleIO": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{ScaleIO: &corev1.ScaleIOVolumeSource{SecretRef: ref(name)}}
		},
		"StorageOS": func(name string) corev1.VolumeSource {
			return corev1.VolumeSource{StorageOS: &corev1.StorageOSVolumeSource{SecretRef: ref(name)}}
		},
	}
	for sourceName, source := range sources {
		for secretName, selectName := range map[string]func(RestrictedEgress) string{
			"credential": func(in RestrictedEgress) string { return in.CredentialSecret },
			"proxy CA":   func(in RestrictedEgress) string { return in.ProxyCASecret },
		} {
			t.Run(sourceName+"/"+secretName, func(t *testing.T) {
				pod, input := fixture()
				pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "alias", VolumeSource: source(selectName(input))})
				if err := PrepareRestrictedEgress(pod, input); err == nil {
					t.Fatal("reserved Secret alias accepted")
				}
			})
		}
	}
}

func TestValidateBoundPodRejectsAdmissionInjectedCSIAlias(t *testing.T) {
	pod, input := fixture()
	if err := PrepareRestrictedEgress(pod, input); err != nil {
		t.Fatal(err)
	}
	pod.UID = "pod-uid"
	claims := egressidentity.Claims{InstallationNamespace: "system", InstallationName: "main", InstallationUID: "i", ProjectNamespace: "project", ProjectName: "project", ProjectUID: "p", EnvironmentNamespace: pod.Namespace, EnvironmentName: "one", EnvironmentUID: "e", PodName: pod.Name, PodUID: pod.UID, ExecutionGeneration: 1, RuntimePolicyRevision: "policy-revision", ForwarderRevision: ForwarderRevision}
	secret, binding, err := IssueCredentialSecret(time.Unix(1000, 0), pod, input, claims)
	if err != nil {
		t.Fatal(err)
	}
	secret.UID, secret.ResourceVersion = "secret-uid", "1"
	if err := binding.SealCreatedSecret(secret); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "alias", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{NodePublishSecretRef: &corev1.LocalObjectReference{Name: input.ProxyCASecret}}}})
	if err := ValidateBoundPod(pod, input, secret, binding); err == nil {
		t.Fatal("admission-injected CSI Secret alias accepted")
	}
}

func TestPrepareRejectsUnsupportedAndAmbientCredentialExposureAtomically(t *testing.T) {
	for name, mutate := range map[string]func(*corev1.Pod, *RestrictedEgress){
		"unsupported backend": func(_ *corev1.Pod, in *RestrictedEgress) { in.Backend = "kubevirt" }, "Windows": func(_ *corev1.Pod, in *RestrictedEgress) { in.OperatingSystem = "windows" },
		"host PID": func(p *corev1.Pod, _ *RestrictedEgress) { p.Spec.HostPID = true }, "no fsGroup": func(p *corev1.Pod, _ *RestrictedEgress) { p.Spec.SecurityContext.FSGroup = nil },
		"preset nodeName": func(p *corev1.Pod, _ *RestrictedEgress) { p.Spec.NodeName = "worker" },
		"aliased Secret volume": func(p *corev1.Pod, in *RestrictedEgress) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "alias", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: in.CredentialSecret}}})
		},
		"projected Secret": func(p *corev1.Pod, in *RestrictedEgress) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "alias", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: in.CredentialSecret}}}}}}})
		},
		"service account projection": func(p *corev1.Pod, _ *RestrictedEgress) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}}}}}})
		},
		"Secret env": func(p *corev1.Pod, in *RestrictedEgress) {
			p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: in.CredentialSecret}, Key: "key"}}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			pod, input := fixture()
			mutate(pod, &input)
			before := pod.DeepCopy()
			if err := PrepareRestrictedEgress(pod, input); err == nil {
				t.Fatal("invalid input accepted")
			}
			if !reflect.DeepEqual(pod, before) {
				t.Fatal("failed preparation mutated Pod")
			}
		})
	}
}
