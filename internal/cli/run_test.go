package cli

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
)

type uncertainCreateClient struct {
	client.Client
	lostResponse bool
}

func (c *uncertainCreateClient) Create(ctx context.Context, object client.Object, opts ...client.CreateOption) error {
	if err := c.Client.Create(ctx, object, opts...); err != nil {
		return err
	}
	if !c.lostResponse {
		c.lostResponse = true
		return errors.New("API response lost after persistence")
	}
	return nil
}

func TestCreateRunIsDeclarativeAndIdempotent(t *testing.T) {
	s := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).Build()
	clients := &kubeClients{Client: c}
	call := func(prompt string) error {
		return createRun(context.Background(), clients, "ns", "stable", "small", "", "", "test", prompt, false, 0)
	}
	if err := call("do it"); err != nil {
		t.Fatal(err)
	}
	if err := call("do it"); err != nil {
		t.Fatalf("same intent: %v", err)
	}
	if err := call("different"); err == nil {
		t.Fatal("mismatched intent succeeded")
	}
	var runs platformv1alpha1.RunList
	if err := c.List(context.Background(), &runs); err != nil {
		t.Fatal(err)
	}
	var envs platformv1alpha1.EnvironmentList
	if err := c.List(context.Background(), &envs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || len(envs.Items) != 0 {
		t.Fatalf("runs=%d environments=%d", len(runs.Items), len(envs.Items))
	}
}

func TestCreateRunRecoversUncertainCreateResponse(t *testing.T) {
	s := runtime.NewScheme()
	if err := platformv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	base := fake.NewClientBuilder().WithScheme(s).Build()
	clients := &kubeClients{Client: &uncertainCreateClient{Client: base}}
	if err := createRun(context.Background(), clients, "ns", "stable-timeout", "small", "", "", "test", "do it", false, 0); err != nil {
		t.Fatalf("uncertain create was not recovered: %v", err)
	}
	var runs platformv1alpha1.RunList
	if err := base.List(context.Background(), &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Name != "stable-timeout" {
		t.Fatalf("runs = %#v", runs.Items)
	}
}
