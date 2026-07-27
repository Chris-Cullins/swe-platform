package sandboxclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
)

func TestDialTerminalBindsExactExecutionAfterFinalRead(t *testing.T) {
	const identity = "pod-a.sandboxd.swe.dev"
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	environment := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid"},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "default"},
		Status: platformv1alpha1.EnvironmentStatus{
			ExecutionGeneration: 3,
			Lifecycle:           platformv1alpha1.EnvironmentLifecycleStatus{Epoch: 7},
			Phase:               platformv1alpha1.EnvironmentPhaseReady,
			PodName:             "env-environment",
			Endpoints:           platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
			Conditions: []metav1.Condition{{
				Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue,
				Reason: "SandboxdReady", Message: "sandboxd is ready",
			}},
		},
	}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: environment.Namespace, UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
	environment.Status.Provisioning = processTestProvisioning(environment, template, nil)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: environment.Status.PodName, Namespace: environment.Namespace, UID: "pod-uid-1",
			Annotations: map[string]string{
				executionGenerationAnnotation:   "3",
				sandboxdauth.IdentityAnnotation: identity,
				sandboxdauth.TrustAnnotation:    string(processTestCertificate(t, identity)),
				sandboxdauth.TokenAnnotation:    "terminal-token",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: environment.Name,
				UID: environment.UID, Controller: processTestPtr(true),
			}},
		},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever},
		Status: corev1.PodStatus{
			PodIP:      "10.0.0.1",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	newClient := func() client.Client {
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(environment.DeepCopy(), template.DeepCopy(), pod.DeepCopy()).Build()
	}

	terminal, health, execution, closeConnection, err := (Connector{Reader: newClient()}).DialTerminal(context.Background(), lifecycle.CaptureExecutionFence(environment))
	if err != nil || terminal == nil || health == nil || closeConnection == nil {
		t.Fatalf("valid terminal dial: terminal nil=%t, health nil=%t, close nil=%t, error=%v", terminal == nil, health == nil, closeConnection == nil, err)
	}
	if execution.environmentUID != environment.UID || execution.executionGeneration != 3 || execution.lifecycleEpoch != 7 || execution.podName != pod.Name || execution.podUID != pod.UID {
		t.Fatalf("terminal execution = %#v", execution)
	}
	if err := closeConnection(); err != nil {
		t.Fatal(err)
	}

	epochRace := &environmentChangingReader{Reader: newClient(), mutate: func(current *platformv1alpha1.Environment) {
		current.Status.Lifecycle.Epoch++
		current.Status.Lifecycle.Suspended = true
	}}
	terminal, health, execution, closeConnection, err = (Connector{Reader: epochRace}).DialTerminal(context.Background(), lifecycle.CaptureExecutionFence(environment))
	if err == nil || !strings.Contains(err.Error(), "execution changed while resolving terminal endpoint") || epochRace.environmentGets != 2 || terminal != nil || health != nil || closeConnection != nil || execution != (TerminalExecution{}) {
		t.Fatalf("post-dial lifecycle race: terminal nil=%t, health nil=%t, execution=%#v, close nil=%t, error=%v, Environment reads=%d", terminal == nil, health == nil, execution, closeConnection == nil, err, epochRace.environmentGets)
	}
	executionRace := &environmentChangingReader{Reader: newClient(), mutate: func(current *platformv1alpha1.Environment) {
		current.Status.ExecutionGeneration++
	}}
	terminal, health, execution, closeConnection, err = (Connector{Reader: executionRace}).DialTerminal(context.Background(), lifecycle.CaptureExecutionFence(environment))
	if err == nil || !strings.Contains(err.Error(), "execution changed while resolving terminal endpoint") || executionRace.environmentGets != 2 || terminal != nil || health != nil || closeConnection != nil || execution != (TerminalExecution{}) {
		t.Fatalf("post-dial execution-generation race: terminal nil=%t, health nil=%t, execution=%#v, close nil=%t, error=%v, Environment reads=%d", terminal == nil, health == nil, execution, closeConnection == nil, err, executionRace.environmentGets)
	}

	podRace := &podChangingReader{Reader: newClient(), mutate: func(current *corev1.Pod) {
		current.UID = "pod-uid-2"
	}}
	terminal, health, execution, closeConnection, err = (Connector{Reader: podRace}).DialTerminal(context.Background(), lifecycle.CaptureExecutionFence(environment))
	if err == nil || !strings.Contains(err.Error(), "execution changed while resolving terminal endpoint") || podRace.podGets != 2 || terminal != nil || health != nil || closeConnection != nil || execution != (TerminalExecution{}) {
		t.Fatalf("post-dial same-name Pod race: terminal nil=%t, health nil=%t, execution=%#v, close nil=%t, error=%v, Pod reads=%d", terminal == nil, health == nil, execution, closeConnection == nil, err, podRace.podGets)
	}
}

func TestDialProcessValidatesCurrentEnvironmentPodAndCredentialIncarnation(t *testing.T) {
	const identity = "pod-a.sandboxd.swe.dev"
	certificate := processTestCertificate(t, identity)
	env := &platformv1alpha1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid"}, Status: platformv1alpha1.EnvironmentStatus{
		ExecutionGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-environment", Endpoints: platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
		Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, Reason: "SandboxdReady", Message: "sandboxd is ready"}},
	}}
	env.Spec.TemplateRef = "default"
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: env.Spec.TemplateRef, Namespace: env.Namespace, UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "image", Size: "small"}}
	env.Status.Provisioning = processTestProvisioning(env, template, nil)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: env.Status.PodName, Namespace: env.Namespace, UID: "pod-uid", Annotations: map[string]string{
		sandboxdauth.IdentityAnnotation: identity, sandboxdauth.SecretUIDAnnotation: "secret-uid", sandboxdauth.SecretNameAnnotation: "env-environment-sandboxd", executionGenerationAnnotation: "1",
	}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: processTestPtr(true)}}}, Status: corev1.PodStatus{
		PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
	}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever}}
	capabilities, err := json.Marshal(sandboxdauth.Config{Grants: []sandboxdauth.Grant{{TokenHash: sandboxdauth.TokenVerifier("process-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityProcess}}}})
	if err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "env-environment-sandboxd", Namespace: env.Namespace, UID: "secret-uid", Annotations: map[string]string{
		sandboxdauth.IdentityAnnotation: identity, sandboxdauth.PodUIDAnnotation: string(pod.UID),
	}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: processTestPtr(true)}}}, Data: map[string][]byte{
		sandboxdauth.TLSCertKey: certificate, sandboxdauth.CapabilitiesKey: capabilities, sandboxdauth.ProcessTokenKey: []byte("process-token"),
	}}

	newClient := func(objects ...client.Object) client.Client {
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		if err := platformv1alpha1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objects, template.DeepCopy())...).Build()
	}

	process, closeConnection, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), secret.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env))
	if err != nil || process == nil || closeConnection == nil {
		t.Fatalf("valid process dial handle: process nil=%t, close nil=%t, error=%v", process == nil, closeConnection == nil, err)
	}
	if err := closeConnection(); err != nil {
		t.Fatal(err)
	}
	for name, grants := range map[string][]sandboxdauth.Grant{
		"broadened": {{TokenHash: sandboxdauth.TokenVerifier("process-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityProcess, sandboxdauth.CapabilityFilesystem}}},
		"duplicate": {
			{TokenHash: sandboxdauth.TokenVerifier("process-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityProcess}},
			{TokenHash: sandboxdauth.TokenVerifier("process-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityProcess}},
		},
		"wrong": {{TokenHash: sandboxdauth.TokenVerifier("process-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityFilesystem}}},
	} {
		t.Run("process capability "+name, func(t *testing.T) {
			malformed := secret.DeepCopy()
			malformed.Data[sandboxdauth.CapabilitiesKey], _ = json.Marshal(sandboxdauth.Config{Grants: grants})
			if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), malformed)}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "exact process capability") {
				t.Fatalf("malformed process grant error = %v", err)
			}
		})
	}
	freshEpoch := env.DeepCopy()
	freshEpoch.Status.Lifecycle.Epoch = 1
	if _, _, err := (Connector{Reader: newClient(freshEpoch, pod.DeepCopy(), secret.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "lifecycle epoch changed") {
		t.Fatalf("stale lifecycle epoch error = %v", err)
	}
	staleGeneration := env.DeepCopy()
	staleGeneration.Status.ExecutionGeneration = 2
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), secret.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(staleGeneration)); err == nil || !strings.Contains(err.Error(), "execution generation changed") {
		t.Fatalf("stale execution generation error = %v", err)
	}
	for _, policy := range []corev1.RestartPolicy{corev1.RestartPolicyAlways, corev1.RestartPolicyOnFailure, ""} {
		restartable := pod.DeepCopy()
		restartable.Spec.RestartPolicy = policy
		if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), restartable, secret.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "restart policy") {
			t.Fatalf("restart policy %q error = %v", policy, err)
		}
	}
	for _, annotation := range []string{"", "0", "01", "malformed", "2"} {
		malformed := pod.DeepCopy()
		malformed.Annotations[executionGenerationAnnotation] = annotation
		if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), malformed, secret.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "execution generation") {
			t.Fatalf("execution annotation %q error = %v", annotation, err)
		}
	}
	legacy := env.DeepCopy()
	legacy.Status.ExecutionGeneration = 0
	if _, _, err := (Connector{Reader: newClient(legacy, pod.DeepCopy(), secret.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(legacy)); err == nil || !strings.Contains(err.Error(), "execution generation changed") {
		t.Fatalf("legacy generation zero error = %v", err)
	}
	activityReader := &environmentChangingReader{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), secret.DeepCopy()), mutate: func(environment *platformv1alpha1.Environment) {
		environment.Annotations = map[string]string{"lifecycle.swe.dev/activity-terminal": `{"id":"activity"}`}
	}}
	_, closeActivity, err := (Connector{Reader: activityReader}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env))
	if err != nil || closeActivity == nil || activityReader.environmentGets != 2 {
		t.Fatalf("metadata-only activity dial: close nil=%t, error=%v, Environment reads=%d", closeActivity == nil, err, activityReader.environmentGets)
	}
	if err := closeActivity(); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"template", "project"} {
		t.Run("final resolve rejects replaced "+source, func(t *testing.T) {
			currentEnvironment := env.DeepCopy()
			objects := []client.Object{currentEnvironment, pod.DeepCopy(), secret.DeepCopy()}
			var project *platformv1alpha1.Project
			if source == "project" {
				currentEnvironment.Spec.ProjectRef = "project"
				project = &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: env.Namespace, UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/repo"}}}
				currentEnvironment.Status.Provisioning = processTestProvisioning(currentEnvironment, template, project)
				objects = append(objects, project)
			}
			base := newClient(objects...)
			reads := 0
			reader := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
				Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
					isSource := source == "template" && key == client.ObjectKeyFromObject(template)
					if project != nil {
						isSource = key == client.ObjectKeyFromObject(project)
					}
					if !isSource {
						return underlying.Get(ctx, key, object, options...)
					}
					reads++
					if err := underlying.Get(ctx, key, object, options...); err != nil {
						return err
					}
					if reads == 2 {
						object.SetUID("replacement-uid")
					}
					return nil
				},
			})
			process, closeConnection, err := (Connector{Reader: reader}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(currentEnvironment))
			if err == nil || !strings.Contains(err.Error(), "execution changed while resolving process endpoint") || process != nil || closeConnection != nil || reads != 2 {
				t.Fatalf("source replacement: process nil=%t, close nil=%t, error=%v, source reads=%d", process == nil, closeConnection == nil, err, reads)
			}
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*platformv1alpha1.Environment)
	}{
		{name: "epoch changes", mutate: func(environment *platformv1alpha1.Environment) {
			environment.Status.Lifecycle.Epoch++
		}},
		{name: "readiness is withdrawn", mutate: func(environment *platformv1alpha1.Environment) {
			environment.Status.Phase = platformv1alpha1.EnvironmentPhaseSetup
			environment.Status.PodName = ""
			environment.Status.Endpoints.Sandboxd = ""
		}},
		{name: "pod and endpoint are replaced", mutate: func(environment *platformv1alpha1.Environment) {
			environment.Status.PodName = "env-environment-replacement"
			environment.Status.Endpoints.Sandboxd = "10.0.0.2:50051"
		}},
	} {
		t.Run("final read "+test.name, func(t *testing.T) {
			reader := &environmentChangingReader{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), secret.DeepCopy()), mutate: test.mutate}
			if _, _, err := (Connector{Reader: reader}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "execution changed while resolving") || reader.environmentGets != 2 {
				t.Fatalf("racing execution error = %v, Environment reads = %d", err, reader.environmentGets)
			}
		})
	}
	suspended := env.DeepCopy()
	suspended.Status.Lifecycle.Suspended = true
	suspended.Status.Lifecycle.SuspensionReason = platformv1alpha1.EnvironmentSuspensionReasonIdle
	if _, _, err := (Connector{Reader: newClient(suspended, pod.DeepCopy(), secret.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(suspended)); err == nil || !strings.Contains(err.Error(), "current reachable incarnation") {
		t.Fatalf("suspended environment error = %v", err)
	}
	longNameEnv := env.DeepCopy()
	longNameEnv.Name = strings.Repeat("long-environment-", 5)
	longNamePod := pod.DeepCopy()
	longNamePod.OwnerReferences[0].Name = longNameEnv.Name
	longNamePod.Annotations[sandboxdauth.SecretNameAnnotation] = "bounded-credential-name"
	longNameSecret := secret.DeepCopy()
	longNameSecret.Name = "bounded-credential-name"
	longNameSecret.OwnerReferences[0].Name = longNameEnv.Name
	_, closeLongName, err := (Connector{Reader: newClient(longNameEnv, longNamePod, longNameSecret)}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(longNameEnv))
	if err != nil {
		t.Fatalf("long-name Environment credential lookup: %v", err)
	}
	if err := closeLongName(); err != nil {
		t.Fatal(err)
	}

	wrongOwner := pod.DeepCopy()
	wrongOwner.OwnerReferences[0].Name = "other-environment"
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), wrongOwner, secret.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("wrong pod owner error = %v", err)
	}

	staleCredential := secret.DeepCopy()
	staleCredential.Annotations[sandboxdauth.PodUIDAnnotation] = "replaced-pod"
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), staleCredential)}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "current environment pod") {
		t.Fatalf("stale credential error = %v", err)
	}
	replacementSecret := secret.DeepCopy()
	replacementSecret.UID = "replacement-secret-uid"
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), replacementSecret)}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "current environment pod") {
		t.Fatalf("replacement Secret error = %v", err)
	}
	wrongSecretOwner := secret.DeepCopy()
	wrongSecretOwner.OwnerReferences[0].Kind = "Run"
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy(), pod.DeepCopy(), wrongSecretOwner)}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || !strings.Contains(err.Error(), "current environment pod") {
		t.Fatalf("wrong Secret owner kind error = %v", err)
	}

	replacedEnvironment := env.DeepCopy()
	replacedEnvironment.UID = types.UID("replaced-environment")
	if _, _, err := (Connector{Reader: newClient(env.DeepCopy())}).DialProcess(context.Background(), lifecycle.CaptureExecutionFence(replacedEnvironment)); err == nil || !strings.Contains(err.Error(), "incarnation changed") {
		t.Fatalf("stale environment UID error = %v", err)
	}
}

func TestResolveExecutionProvisioningSources(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid", Generation: 1},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "template", ProjectRef: "project"},
		Status: platformv1alpha1.EnvironmentStatus{ObservedGeneration: 1, ExecutionGeneration: 1, Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "env-environment",
			Endpoints:  platformv1alpha1.EnvironmentEndpoints{Sandboxd: "10.0.0.1:50051"},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}},
		},
	}
	template := &platformv1alpha1.EnvironmentTemplate{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: env.Namespace, UID: "template-uid", Generation: 1}, Spec: platformv1alpha1.EnvironmentTemplateSpec{Image: "frozen", Size: "small"}}
	project := &platformv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "project", Namespace: env.Namespace, UID: "project-uid", Generation: 1}, Spec: platformv1alpha1.ProjectSpec{Repositories: []string{"https://example.test/repo"}}}
	env.Status.Provisioning = processTestProvisioning(env, template, project)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: env.Status.PodName, Namespace: env.Namespace, UID: "pod-uid", Annotations: map[string]string{executionGenerationAnnotation: "1"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: processTestPtr(true)}}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever}, Status: corev1.PodStatus{PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}

	resolve := func(snapshot *platformv1alpha1.EnvironmentProvisioningSnapshot, liveTemplate *platformv1alpha1.EnvironmentTemplate, liveProject *platformv1alpha1.Project) error {
		current := env.DeepCopy()
		current.Status.Provisioning = snapshot
		objects := []client.Object{current, pod.DeepCopy(), liveTemplate}
		if liveProject != nil {
			objects = append(objects, liveProject)
		}
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		_, err := (Connector{Reader: reader}).ResolveExecution(context.Background(), lifecycle.CaptureExecutionFence(current))
		return err
	}
	if err := resolve(env.Status.Provisioning.DeepCopy(), template.DeepCopy(), project.DeepCopy()); err != nil {
		t.Fatalf("complete verified exact sources: %v", err)
	}
	for _, source := range []string{"template", "project"} {
		snapshot := env.Status.Provisioning.DeepCopy()
		if source == "template" {
			snapshot.TemplateVerified = false
		} else {
			snapshot.ProjectVerified = false
		}
		if err := resolve(snapshot, template.DeepCopy(), project.DeepCopy()); err == nil {
			t.Fatalf("unverified %s source was accepted", source)
		}
	}
	for _, test := range []struct {
		name           string
		mutateTemplate func(*platformv1alpha1.EnvironmentTemplate)
		mutateProject  func(*platformv1alpha1.Project)
	}{
		{name: "replacement Template", mutateTemplate: func(v *platformv1alpha1.EnvironmentTemplate) { v.UID = "replacement" }},
		{name: "deleting Template", mutateTemplate: func(v *platformv1alpha1.EnvironmentTemplate) {
			now := metav1.Now()
			v.DeletionTimestamp = &now
			v.Finalizers = []string{"test"}
		}},
		{name: "replacement Project", mutateProject: func(v *platformv1alpha1.Project) { v.UID = "replacement" }},
		{name: "deleting Project", mutateProject: func(v *platformv1alpha1.Project) {
			now := metav1.Now()
			v.DeletionTimestamp = &now
			v.Finalizers = []string{"test"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			liveTemplate, liveProject := template.DeepCopy(), project.DeepCopy()
			if test.mutateTemplate != nil {
				test.mutateTemplate(liveTemplate)
			}
			if test.mutateProject != nil {
				test.mutateProject(liveProject)
			}
			if err := resolve(env.Status.Provisioning.DeepCopy(), liveTemplate, liveProject); err == nil {
				t.Fatal("stale/deleting source was accepted")
			}
		})
	}
	edited := template.DeepCopy()
	edited.Generation++
	edited.Spec.Image = "live-edit"
	edited.Spec.Backend = platformv1alpha1.EnvironmentBackendKubeVirt
	if err := resolve(env.Status.Provisioning.DeepCopy(), edited, project.DeepCopy()); err != nil {
		t.Fatalf("same-UID live provisioning edit changed authoritative snapshot backend: %v", err)
	}
}

type environmentChangingReader struct {
	client.Reader
	environmentGets int
	mutate          func(*platformv1alpha1.Environment)
}

type podChangingReader struct {
	client.Reader
	podGets int
	mutate  func(*corev1.Pod)
}

func (r *podChangingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, object, options...); err != nil {
		return err
	}
	if pod, ok := object.(*corev1.Pod); ok {
		r.podGets++
		if r.podGets > 1 {
			r.mutate(pod)
		}
	}
	return nil
}

func (r *environmentChangingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, object, options...); err != nil {
		return err
	}
	if environment, ok := object.(*platformv1alpha1.Environment); ok {
		r.environmentGets++
		if r.environmentGets > 1 {
			r.mutate(environment)
		}
	}
	return nil
}

func processTestCertificate(t *testing.T, serverName string) []byte {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: serverName}, DNSNames: []string{serverName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func processTestPtr[T any](value T) *T { return &value }

func processTestProvisioning(env *platformv1alpha1.Environment, template *platformv1alpha1.EnvironmentTemplate, project *platformv1alpha1.Project) *platformv1alpha1.EnvironmentProvisioningSnapshot {
	snapshot := platformv1alpha1.ResolveEnvironmentProvisioning(env, template, project)
	snapshot.TemplateVerified = true
	snapshot.ProjectVerified = project != nil
	return snapshot
}
