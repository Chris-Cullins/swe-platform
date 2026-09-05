package cli

import (
	"context"
	"errors"
	"testing"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestDeleteRunPinsUIDAcrossReplacement(t *testing.T) {
	for _, replace := range []bool{false, true} {
		s := runtime.NewScheme()
		if err := platformv1alpha1.AddToScheme(s); err != nil {
			t.Fatal(err)
		}
		run := &platformv1alpha1.Run{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "original"}}
		calls := 0
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(run).WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				calls++
				options := &client.DeleteOptions{}
				options.ApplyOptions(opts)
				if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "original" {
					t.Fatal("missing original UID precondition")
				}
				if replace {
					if err := c.Delete(ctx, run); err != nil {
						t.Fatal(err)
					}
					replacement := run.DeepCopy()
					replacement.UID, replacement.ResourceVersion = "replacement", ""
					if err := c.Create(ctx, replacement); err != nil {
						t.Fatal(err)
					}
					// controller-runtime's fake does not enforce UID delete
					// preconditions. Emulate the API server's conflict response.
					return apierrors.NewConflict(schema.GroupResource{Group: "swe.dev", Resource: "runs"}, obj.GetName(), errors.New("UID precondition failed"))
				}
				return c.Delete(ctx, obj, opts...)
			},
		}).Build()
		err := deleteRun(context.Background(), c, "ns", "run")
		if replace {
			if !apierrors.IsConflict(err) {
				t.Fatalf("replacement delete = %v", err)
			}
			var kept platformv1alpha1.Run
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), &kept); err != nil || kept.UID != "replacement" {
				t.Fatal("replacement was not preserved")
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("delete attempts = %d", calls)
		}
	}
}

func TestDeleteRunRequiresNamespace(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"delete-run", "run"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("accepted implicit namespace")
	}
}
