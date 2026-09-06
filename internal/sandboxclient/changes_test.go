package sandboxclient

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestChangesPublicationProofRejectsEveryExecutionAndCredentialDrift(t *testing.T) {
	mutations := map[string]func(client.Object){
		"environment UID": func(o client.Object) {
			if v, ok := o.(*platformv1alpha1.Environment); ok {
				v.UID = "replacement"
			}
		},
		"generation": func(o client.Object) {
			if v, ok := o.(*platformv1alpha1.Environment); ok {
				v.Status.ExecutionGeneration++
			}
		},
		"epoch": func(o client.Object) {
			if v, ok := o.(*platformv1alpha1.Environment); ok {
				v.Status.Lifecycle.Epoch++
			}
		},
		"hold": func(o client.Object) {
			if v, ok := o.(*platformv1alpha1.Environment); ok {
				v.Spec.Paused = true
			}
		},
		"pod UID": func(o client.Object) {
			if v, ok := o.(*corev1.Pod); ok {
				v.UID = "replacement"
			}
		},
		"pod endpoint": func(o client.Object) {
			if v, ok := o.(*corev1.Pod); ok {
				v.Status.PodIP = "10.0.0.2"
			}
		},
		"secret UID": func(o client.Object) {
			if v, ok := o.(*corev1.Secret); ok {
				v.UID = "replacement"
			}
		},
		"token": func(o client.Object) {
			if v, ok := o.(*corev1.Secret); ok {
				v.Data[sandboxdauth.ChangesTokenKey] = []byte("replacement")
			}
		},
		"capabilities": func(o client.Object) {
			if v, ok := o.(*corev1.Secret); ok {
				v.Data[sandboxdauth.CapabilitiesKey] = []byte(`{}`)
			}
		},
		"TLS": func(o client.Object) {
			if v, ok := o.(*corev1.Secret); ok {
				v.Data[sandboxdauth.TLSCertKey] = []byte("replacement")
			}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			env, reader := newProcessPoolFixture(t)
			c := Connector{Reader: reader}
			fence := lifecycle.CaptureExecutionFence(env)
			var secret corev1.Secret
			kube := reader.Reader.(client.Client)
			if err := kube.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "credential"}, &secret); err != nil {
				t.Fatal(err)
			}
			var config sandboxdauth.Config
			if err := json.Unmarshal(secret.Data[sandboxdauth.CapabilitiesKey], &config); err != nil {
				t.Fatal(err)
			}
			config.Grants = append(config.Grants, sandboxdauth.Grant{TokenHash: sandboxdauth.TokenVerifier("changes-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityChanges}})
			secret.Data[sandboxdauth.CapabilitiesKey], _ = json.Marshal(config)
			secret.Data[sandboxdauth.ChangesTokenKey] = []byte("changes-token")
			if err := kube.Update(context.Background(), &secret); err != nil {
				t.Fatal(err)
			}
			_, secretCopy, proof, err := c.resolveProcessTarget(context.Background(), fence)
			if err != nil {
				t.Fatal(err)
			}
			observation := ChangesObservation{proof: proof, capabilitiesHash: sha256.Sum256(secretCopy.Data[sandboxdauth.CapabilitiesKey]), tokenHash: sha256.Sum256(secretCopy.Data[sandboxdauth.ChangesTokenKey])}
			if err := c.ChangesCurrent(context.Background(), fence, observation); err != nil {
				t.Fatal(err)
			}
			reader.setMutation(mutate)
			if err := c.ChangesCurrent(context.Background(), fence, observation); err == nil {
				t.Fatal("stale proof accepted")
			}
		})
	}
}
