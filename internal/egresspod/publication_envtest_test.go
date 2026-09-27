package egresspod

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestPublicationAPIServer(t *testing.T) {
	if os.Getenv("SWE_TEST_EGRESS_APISERVER") != "1" {
		t.Skip("set SWE_TEST_EGRESS_APISERVER=1 to run disposable Kubernetes v1.35.0 API-server validation")
	}
	// Explicit false prevents USE_EXISTING_CLUSTER from selecting a real cluster.
	// No scheduler, kubelet, networking or live conformance runner is involved.
	env := &envtest.Environment{UseExistingCluster: ptr.To(false), DownloadBinaryAssets: true, DownloadBinaryAssetsVersion: "1.35.0", BinaryAssetsDirectory: t.TempDir()}
	env.ControlPlane.GetAPIServer().Configure().Set("advertise-address", "127.0.0.1")
	config, err := env.Start()
	if err != nil {
		t.Fatalf("start disposable API server: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop disposable API server: %v", err)
		}
	})
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, mode := range []string{"success", "resource-version", "uid", "deleting"} {
		t.Run(mode, func(t *testing.T) {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "publication-"}}
			if err := c.Create(ctx, ns); err != nil {
				t.Fatal(err)
			}
			if err := c.Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns.Name}}); err != nil {
				t.Fatal(err)
			}
			pod, in := fixture()
			pod.Namespace = ns.Name
			pod.Annotations["other.example/note"] = "preserve-me"
			pod.Spec.Containers[0].Image = "example.invalid/workload:fixture"
			for i := range pod.Spec.InitContainers {
				pod.Spec.InitContainers[i].Image = "example.invalid/workload:fixture"
			}
			pod.Spec.Volumes[0].EmptyDir = &corev1.EmptyDirVolumeSource{}
			in.ForwarderImage = "example.invalid/proxy@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			if err := PrepareRestrictedEgress(pod, in); err != nil {
				t.Fatal(err)
			}
			if err := c.Create(ctx, pod); err != nil {
				t.Fatal(err)
			}
			if pod.UID == "" || pod.ResourceVersion == "" {
				t.Fatal("server did not assign identity")
			}
			writer := &publicationClient{Client: c}
			if mode != "success" {
				writer.patch = func(ctx context.Context, obj client.Object, patch client.Patch) error {
					var changed corev1.Pod
					if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &changed); err != nil {
						return err
					}
					switch mode {
					case "resource-version":
						changed.Annotations["concurrent.example/value"] = "must-survive"
						if err := c.Update(ctx, &changed); err != nil {
							return err
						}
					case "uid":
						if err := c.Delete(ctx, &changed, client.GracePeriodSeconds(0)); err != nil {
							return err
						}
						replacement := pod.DeepCopy()
						replacement.UID, replacement.ResourceVersion = "", ""
						replacement.CreationTimestamp = metav1.Time{}
						if err := c.Create(ctx, replacement); err != nil {
							return err
						}
						// Isolate the UID guard: make the RV test match the new
						// object while retaining the original UID test.
						data, err := patch.Data(obj)
						if err != nil {
							return err
						}
						var ops []map[string]any
						if err := json.Unmarshal(data, &ops); err != nil {
							return err
						}
						ops[1]["value"] = replacement.ResourceVersion
						data, err = json.Marshal(ops)
						if err != nil {
							return err
						}
						patch = client.RawPatch(types.JSONPatchType, data)
					case "deleting":
						changed.Finalizers = []string{"test.example/retain"}
						if err := c.Update(ctx, &changed); err != nil {
							return err
						}
						if err := c.Delete(ctx, &changed); err != nil {
							return err
						}
					}
					return c.Patch(ctx, obj, patch)
				}
			}
			err := PublishCredential(ctx, writer, c, time.Now(), pod, in, publicationClaims(pod))
			if mode == "success" {
				if err != nil {
					t.Fatal(err)
				}
				assertPublished(t, c, pod, in)
				// Kubernetes disallows re-adding gates after release.
				var released corev1.Pod
				if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &released); err != nil {
					t.Fatal(err)
				}
				released.Spec.SchedulingGates = pod.Spec.SchedulingGates
				if err := c.Update(ctx, &released); err == nil {
					t.Fatal("API accepted re-adding scheduling gate")
				}
				return
			}
			var failure *PublicationError
			if !errors.As(err, &failure) || failure.Stage != "release" || failure.Uncertain || writer.patches != 1 {
				t.Fatalf("expected guarded release failure: %v", err)
			}
			var retained corev1.Pod
			if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &retained); err != nil {
				t.Fatal(err)
			}
			if len(retained.Spec.SchedulingGates) != 1 || retained.Annotations[CertificateFingerprintAnnotation] != "" {
				t.Fatal("guarded API write changed Pod")
			}
			if mode == "resource-version" && retained.Annotations["concurrent.example/value"] != "must-survive" {
				t.Fatal("concurrent annotation lost")
			}
			if mode == "uid" && retained.UID == pod.UID {
				t.Fatal("replacement fixture did not change UID")
			}
			if mode == "deleting" && retained.DeletionTimestamp.IsZero() {
				t.Fatal("deletion fixture did not start deletion")
			}
		})
	}
}
