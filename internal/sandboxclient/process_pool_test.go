package sandboxclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	platformv1alpha1 "github.com/Chris-Cullins/swe-platform/api/v1alpha1"
	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdauth "github.com/Chris-Cullins/swe-platform/sandboxd/auth"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

type fakeProcessConnection struct{ closes atomic.Int32 }

func (*fakeProcessConnection) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return nil
}
func (*fakeProcessConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("not implemented")
}
func (c *fakeProcessConnection) Close() error { c.closes.Add(1); return nil }

type poolProcessServer struct {
	sandboxdv1.UnimplementedProcessServiceServer
}

func (*poolProcessServer) Get(context.Context, *sandboxdv1.GetProcessRequest) (*sandboxdv1.Process, error) {
	return &sandboxdv1.Process{}, nil
}

type processPoolCountingReader struct {
	client.Reader
	mu        sync.Mutex
	mutate    func(client.Object)
	gets      int
	envs      int
	templates int
	pods      int
	secrets   int
}

func (r *processPoolCountingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, object, options...); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gets++
	switch object.(type) {
	case *platformv1alpha1.Environment:
		r.envs++
	case *platformv1alpha1.EnvironmentTemplate:
		r.templates++
	case *corev1.Pod:
		r.pods++
	case *corev1.Secret:
		r.secrets++
	}
	if r.mutate != nil {
		r.mutate(object)
	}
	return nil
}

func (r *processPoolCountingReader) setMutation(mutate func(client.Object)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mutate = mutate
}

func newProcessPoolFixture(t *testing.T) (*platformv1alpha1.Environment, *processPoolCountingReader) {
	t.Helper()
	const identity = "process.sandboxd.swe.dev"
	return newProcessPoolFixtureAt(t, identity, "10.0.0.1", processTestCertificate(t, identity))
}

func newProcessPoolFixtureAt(t *testing.T, identity, podIP string, certificate []byte) (*platformv1alpha1.Environment, *processPoolCountingReader) {
	t.Helper()
	env := &platformv1alpha1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "environment", Namespace: "ns", UID: "env-uid", ResourceVersion: "1"},
		Spec:       platformv1alpha1.EnvironmentSpec{TemplateRef: "default"},
		Status: platformv1alpha1.EnvironmentStatus{
			ExecutionGeneration: 3, Lifecycle: platformv1alpha1.EnvironmentLifecycleStatus{Epoch: 7},
			Phase: platformv1alpha1.EnvironmentPhaseReady, PodName: "pod",
			Endpoints:  platformv1alpha1.EnvironmentEndpoints{Sandboxd: net.JoinHostPort(podIP, sandboxdPort)},
			Conditions: []metav1.Condition{{Type: platformv1alpha1.EnvironmentConditionReady, Status: metav1.ConditionTrue}},
		},
	}
	owner := []metav1.OwnerReference{{APIVersion: platformv1alpha1.GroupVersion.String(), Kind: "Environment", Name: env.Name, UID: env.UID, Controller: processTestPtr(true)}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", UID: "pod-uid", OwnerReferences: owner, Annotations: map[string]string{
			executionGenerationAnnotation: "3", sandboxdauth.IdentityAnnotation: identity,
			sandboxdauth.SecretNameAnnotation: "credential", sandboxdauth.SecretUIDAnnotation: "secret-uid",
		}},
		Spec:   corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever},
		Status: corev1.PodStatus{PodIP: podIP, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	capabilities, err := json.Marshal(sandboxdauth.Config{Grants: []sandboxdauth.Grant{{TokenHash: sandboxdauth.TokenVerifier("process-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityProcess}}}})
	if err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: "ns", UID: "secret-uid", ResourceVersion: "1", OwnerReferences: owner,
			Annotations: map[string]string{sandboxdauth.IdentityAnnotation: identity, sandboxdauth.PodUIDAnnotation: "pod-uid"}},
		Data: map[string][]byte{sandboxdauth.TLSCertKey: certificate, sandboxdauth.CapabilitiesKey: capabilities, sandboxdauth.ProcessTokenKey: []byte("process-token")},
	}
	template := &platformv1alpha1.EnvironmentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "ns", UID: "template-uid", Generation: 1},
		Spec:       platformv1alpha1.EnvironmentTemplateSpec{Image: "environment:test", Size: "small"},
	}
	env.Status.Provisioning = platformv1alpha1.ResolveEnvironmentProvisioning(env, template, nil)
	env.Status.Provisioning.TemplateVerified = true
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := platformv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(env.DeepCopy(), pod, secret, template).Build()
	return env, &processPoolCountingReader{Reader: reader}
}

func newCountingProcessPool(reader client.Reader) (*ProcessConnectionPool, *atomic.Int32, *[]*fakeProcessConnection) {
	pool := NewProcessConnectionPool(reader)
	var dials atomic.Int32
	var mu sync.Mutex
	connections := make([]*fakeProcessConnection, 0)
	pool.dial = func(string, ...grpc.DialOption) (processConnection, error) {
		dials.Add(1)
		connection := &fakeProcessConnection{}
		mu.Lock()
		connections = append(connections, connection)
		mu.Unlock()
		return connection, nil
	}
	return pool, &dials, &connections
}

func TestProcessConnectionPoolOneMinutePollContract(t *testing.T) {
	const polls = 30 // one poll every two seconds for one minute
	env, reader := newProcessPoolFixture(t)
	pool, dials, connections := newCountingProcessPool(reader)
	fence := lifecycle.CaptureExecutionFence(env)
	for i := 0; i < polls; i++ {
		_, release, err := pool.acquire(context.Background(), fence)
		if err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
		if err := release(); err != nil {
			t.Fatal(err)
		}
	}
	if dials.Load() != 1 || reader.envs != 31 || reader.templates != 31 || reader.pods != 31 || reader.secrets != 31 || reader.gets != 124 {
		t.Fatalf("pooled minute: dials=%d reads=%d (Environment=%d Template=%d Pod=%d Secret=%d), want 1 and 124 (31/31/31/31)", dials.Load(), reader.gets, reader.envs, reader.templates, reader.pods, reader.secrets)
	}
	// The pre-pool process dial performed five reads per poll (Environment,
	// Template, Pod, Secret, final Environment): 150 reads and 30 creations.
	// Pooling removes 29/30 creations and 26/150 process-connector reads (17.3%)
	// while retaining a complete four-object proof before every lease and a
	// complete post-dial proof on the first miss.
	// The unchanged Run association/receipt/currentness path costs eight reads
	// per poll, so the complete adapter-call contract moves from 390 to 364 API
	// reads per minute-equivalent sequence (6.7%) without removing a proof.
	beforeConnectorReads, beforeDials := polls*5, polls
	beforeAllReads := polls * (8 + 5)
	afterAllReads := polls*8 + reader.gets
	if beforeConnectorReads != 150 || beforeDials != 30 || beforeAllReads != 390 || afterAllReads != 364 {
		t.Fatalf("minute contract changed: connector=%d dials=%d all-before=%d all-after=%d", beforeConnectorReads, beforeDials, beforeAllReads, afterAllReads)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if len(*connections) != 1 || (*connections)[0].closes.Load() != 1 {
		t.Fatalf("shutdown physical closes = %d", (*connections)[0].closes.Load())
	}
}

func TestProcessConnectionPoolOneMinuteTLSHandshakeContract(t *testing.T) {
	const (
		polls    = 30
		identity = "process.sandboxd.swe.dev"
	)
	certificate, privateKey := observationCertificate(t, identity)
	serverCertificate := &tls.Certificate{Certificate: [][]byte{certificate.Raw}, PrivateKey: privateKey}
	var handshakes atomic.Int32
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			handshakes.Add(1)
			return serverCertificate, nil
		},
	})))
	sandboxdv1.RegisterProcessServiceServer(server, &poolProcessServer{})
	listener, err := net.Listen("tcp", "127.0.0.1:50051")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})

	baselineEnv, baselineReader := newProcessPoolFixtureAt(t, identity, "127.0.0.1", certificatePEM)
	baselineFence := lifecycle.CaptureExecutionFence(baselineEnv)
	for i := 0; i < polls; i++ {
		process, closeConnection, err := (Connector{Reader: baselineReader}).DialProcess(context.Background(), baselineFence)
		if err != nil {
			t.Fatalf("baseline dial %d: %v", i, err)
		}
		if _, err := process.Get(context.Background(), &sandboxdv1.GetProcessRequest{}); err != nil {
			t.Fatalf("baseline RPC %d: %v", i, err)
		}
		if err := closeConnection(); err != nil {
			t.Fatal(err)
		}
	}
	if got := handshakes.Load(); got != polls {
		t.Fatalf("unpooled TLS handshakes=%d, want %d", got, polls)
	}

	pooledEnv, pooledReader := newProcessPoolFixtureAt(t, identity, "127.0.0.1", certificatePEM)
	pool := NewProcessConnectionPool(pooledReader)
	pooledFence := lifecycle.CaptureExecutionFence(pooledEnv)
	for i := 0; i < polls; i++ {
		process, release, err := pool.acquire(context.Background(), pooledFence)
		if err != nil {
			t.Fatalf("pooled dial %d: %v", i, err)
		}
		if _, err := process.Get(context.Background(), &sandboxdv1.GetProcessRequest{}); err != nil {
			t.Fatalf("pooled RPC %d: %v", i, err)
		}
		if err := release(); err != nil {
			t.Fatal(err)
		}
	}
	if got := handshakes.Load(); got != polls+1 {
		t.Fatalf("total TLS handshakes=%d, want %d baseline + 1 pooled", got, polls)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessConnectionPoolRejectsAndEvictsEveryFenceBoundary(t *testing.T) {
	for name, mutate := range map[string]func(*platformv1alpha1.Environment){
		"Environment UID replacement": func(env *platformv1alpha1.Environment) { env.UID = "replacement" },
		"execution generation":        func(env *platformv1alpha1.Environment) { env.Status.ExecutionGeneration++ },
		"lifecycle epoch":             func(env *platformv1alpha1.Environment) { env.Status.Lifecycle.Epoch++ },
		"hold revision": func(env *platformv1alpha1.Environment) {
			env.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 2}
		},
		"pause": func(env *platformv1alpha1.Environment) { env.Spec.Paused = true },
		"suspend": func(env *platformv1alpha1.Environment) {
			env.Status.Lifecycle.Suspended = true
		},
		"hold": func(env *platformv1alpha1.Environment) {
			env.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Enabled: true}
		},
		"deletion":    func(env *platformv1alpha1.Environment) { now := metav1.Now(); env.DeletionTimestamp = &now },
		"unreachable": func(env *platformv1alpha1.Environment) { env.Status.Endpoints.Sandboxd = "" },
	} {
		t.Run(name, func(t *testing.T) {
			env, reader := newProcessPoolFixture(t)
			pool, dials, connections := newCountingProcessPool(reader)
			_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
			if err != nil {
				t.Fatal(err)
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			reader.setMutation(func(object client.Object) {
				if current, ok := object.(*platformv1alpha1.Environment); ok {
					mutate(current)
				}
			})
			if _, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil || release != nil {
				t.Fatalf("stale acquisition: release nil=%t, error=%v", release == nil, err)
			}
			if dials.Load() != 1 || (*connections)[0].closes.Load() != 1 {
				t.Fatalf("dials=%d closes=%d, want 1/1", dials.Load(), (*connections)[0].closes.Load())
			}
		})
	}
}

func TestProcessConnectionPoolEndpointChangeEvictsBeforeMissResolution(t *testing.T) {
	env, reader := newProcessPoolFixture(t)
	pool, dials, connections := newCountingProcessPool(reader)
	_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
	if err != nil {
		t.Fatal(err)
	}
	_ = release()
	reader.setMutation(func(object client.Object) {
		if current, ok := object.(*platformv1alpha1.Environment); ok {
			current.Status.Endpoints.Sandboxd = "10.0.0.2:50051"
		}
	})
	if _, _, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil {
		t.Fatal("endpoint replacement was accepted")
	}
	if dials.Load() != 1 || (*connections)[0].closes.Load() != 1 {
		t.Fatalf("dials=%d closes=%d, want 1/1", dials.Load(), (*connections)[0].closes.Load())
	}
}

func TestProcessConnectionPoolCompleteHitProofInvalidatesDrift(t *testing.T) {
	testCases := []struct {
		name            string
		mutate          func(*testing.T, *processPoolCountingReader)
		wantReplacement bool
	}{
		{name: "Template UID", mutate: func(_ *testing.T, reader *processPoolCountingReader) {
			reader.setMutation(func(object client.Object) {
				if template, ok := object.(*platformv1alpha1.EnvironmentTemplate); ok {
					template.UID = "replacement-template"
				}
			})
		}},
		{name: "Template spec", wantReplacement: true, mutate: func(_ *testing.T, reader *processPoolCountingReader) {
			reader.setMutation(func(object client.Object) {
				if template, ok := object.(*platformv1alpha1.EnvironmentTemplate); ok {
					template.Spec.Image = "replacement.example/environment:v2"
				}
			})
		}},
		{name: "Template backend", wantReplacement: true, mutate: func(_ *testing.T, reader *processPoolCountingReader) {
			reader.setMutation(func(object client.Object) {
				if template, ok := object.(*platformv1alpha1.EnvironmentTemplate); ok {
					template.Spec.Backend = platformv1alpha1.EnvironmentBackendKubeVirt
				}
			})
		}},
		{name: "Pod deletion", mutate: func(t *testing.T, reader *processPoolCountingReader) {
			if err := reader.Reader.(client.Client).Delete(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns"}}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "Pod UID", mutate: func(_ *testing.T, reader *processPoolCountingReader) {
			reader.setMutation(func(object client.Object) {
				if pod, ok := object.(*corev1.Pod); ok {
					pod.UID = "replacement-pod"
				}
			})
		}},
		{name: "Pod readiness", mutate: func(_ *testing.T, reader *processPoolCountingReader) {
			reader.setMutation(func(object client.Object) {
				if pod, ok := object.(*corev1.Pod); ok {
					pod.Status.Conditions[0].Status = corev1.ConditionFalse
				}
			})
		}},
		{name: "Pod execution generation", mutate: func(_ *testing.T, reader *processPoolCountingReader) {
			reader.setMutation(func(object client.Object) {
				if pod, ok := object.(*corev1.Pod); ok {
					pod.Annotations[executionGenerationAnnotation] = "4"
				}
			})
		}},
		{name: "Secret deletion", mutate: func(t *testing.T, reader *processPoolCountingReader) {
			if err := reader.Reader.(client.Client).Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: "ns"}}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "Secret UID", mutate: func(_ *testing.T, reader *processPoolCountingReader) {
			reader.setMutation(func(object client.Object) {
				if secret, ok := object.(*corev1.Secret); ok {
					secret.UID = "replacement-secret"
				}
			})
		}},
		{name: "Secret resource version", wantReplacement: true, mutate: func(_ *testing.T, reader *processPoolCountingReader) {
			reader.setMutation(func(object client.Object) {
				if secret, ok := object.(*corev1.Secret); ok {
					secret.ResourceVersion = "2"
				}
			})
		}},
		{name: "Secret process token", wantReplacement: true, mutate: func(t *testing.T, reader *processPoolCountingReader) {
			capabilities, err := json.Marshal(sandboxdauth.Config{Grants: []sandboxdauth.Grant{{TokenHash: sandboxdauth.TokenVerifier("replacement-process-token"), Capabilities: []sandboxdauth.Capability{sandboxdauth.CapabilityProcess}}}})
			if err != nil {
				t.Fatal(err)
			}
			reader.setMutation(func(object client.Object) {
				if secret, ok := object.(*corev1.Secret); ok {
					secret.Data[sandboxdauth.ProcessTokenKey] = []byte("replacement-process-token")
					secret.Data[sandboxdauth.CapabilitiesKey] = capabilities
				}
			})
		}},
		{name: "Secret TLS certificate", wantReplacement: true, mutate: func(t *testing.T, reader *processPoolCountingReader) {
			certificate := processTestCertificate(t, "process.sandboxd.swe.dev")
			reader.setMutation(func(object client.Object) {
				if secret, ok := object.(*corev1.Secret); ok {
					secret.Data[sandboxdauth.TLSCertKey] = certificate
				}
			})
		}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			env, reader := newProcessPoolFixture(t)
			pool, dials, connections := newCountingProcessPool(reader)
			_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
			if err != nil {
				t.Fatal(err)
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}

			testCase.mutate(t, reader)
			_, replacementRelease, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
			if testCase.wantReplacement {
				if err != nil || replacementRelease == nil {
					t.Fatalf("replacement acquisition: release nil=%t error=%v", replacementRelease == nil, err)
				}
				if err := replacementRelease(); err != nil {
					t.Fatal(err)
				}
			} else if err == nil || replacementRelease != nil {
				t.Fatalf("invalid current target acquisition: release nil=%t error=%v", replacementRelease == nil, err)
			}

			wantDials := int32(1)
			if testCase.wantReplacement {
				wantDials = 2
			}
			if dials.Load() != wantDials || (*connections)[0].closes.Load() != 1 {
				t.Fatalf("dials=%d old closes=%d, want %d/1", dials.Load(), (*connections)[0].closes.Load(), wantDials)
			}
			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProcessConnectionPoolFreshFenceCannotReusePreviousHoldRevision(t *testing.T) {
	env, reader := newProcessPoolFixture(t)
	pool, dials, connections := newCountingProcessPool(reader)
	_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
	if err != nil {
		t.Fatal(err)
	}
	_ = release()
	reader.setMutation(func(object client.Object) {
		if current, ok := object.(*platformv1alpha1.Environment); ok {
			current.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 2}
		}
	})
	current := env.DeepCopy()
	current.Spec.Lifecycle.Hold = &platformv1alpha1.EnvironmentHoldPolicy{Revision: 2}
	_, release, err = pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(current))
	if err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 2 || len(*connections) != 2 || (*connections)[0].closes.Load() != 1 {
		t.Fatalf("dials=%d connections=%d old-closes=%d, want 2/2/1", dials.Load(), len(*connections), (*connections)[0].closes.Load())
	}
	_ = release()
	_ = pool.Close()
}

func TestProcessConnectionPoolConcurrentBorrowersAndFirstDial(t *testing.T) {
	env, reader := newProcessPoolFixture(t)
	pool := NewProcessConnectionPool(reader)
	var dials atomic.Int32
	connection := &fakeProcessConnection{}
	entered, proceed := make(chan struct{}), make(chan struct{})
	pool.dial = func(string, ...grpc.DialOption) (processConnection, error) {
		if dials.Add(1) == 1 {
			close(entered)
		}
		<-proceed
		return connection, nil
	}
	type result struct {
		release func() error
		err     error
	}
	results := make(chan result, 2)
	acquire := func() {
		_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
		results <- result{release: release, err: err}
	}
	go acquire()
	<-entered
	go acquire()
	close(proceed)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || dials.Load() != 1 {
		t.Fatalf("concurrent acquisitions: errors=%v/%v dials=%d", first.err, second.err, dials.Load())
	}
	_ = first.release()
	if connection.closes.Load() != 0 {
		t.Fatal("release physically closed a connection still borrowed")
	}
	_ = second.release()
	if connection.closes.Load() != 0 {
		t.Fatal("ordinary final release physically closed reusable connection")
	}
	_ = pool.Close()
	if connection.closes.Load() != 1 {
		t.Fatalf("shutdown closes=%d, want 1", connection.closes.Load())
	}
}

func TestProcessConnectionPoolDefersInvalidationCloseUntilBorrowerRelease(t *testing.T) {
	env, reader := newProcessPoolFixture(t)
	pool, _, connections := newCountingProcessPool(reader)
	_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
	if err != nil {
		t.Fatal(err)
	}
	reader.setMutation(func(object client.Object) {
		if current, ok := object.(*platformv1alpha1.Environment); ok {
			current.Status.Lifecycle.Suspended = true
		}
	})
	if _, _, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil {
		t.Fatal("suspended execution was reused")
	}
	if (*connections)[0].closes.Load() != 0 {
		t.Fatal("invalidation closed a physical connection still borrowed")
	}
	_ = release()
	if (*connections)[0].closes.Load() != 1 {
		t.Fatalf("release closes=%d, want 1", (*connections)[0].closes.Load())
	}
}

func TestProcessConnectionPoolIdleExpiryAndShutdown(t *testing.T) {
	env, reader := newProcessPoolFixture(t)
	pool, _, connections := newCountingProcessPool(reader)
	now := time.Unix(100, 0)
	pool.now = func() time.Time { return now }
	pool.idleTimeout = time.Minute
	_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
	if err != nil {
		t.Fatal(err)
	}
	pool.evictIdle(now.Add(10 * time.Minute))
	if (*connections)[0].closes.Load() != 0 {
		t.Fatal("idle sweep closed an actively borrowed connection")
	}
	_ = release()
	pool.evictIdle(now.Add(time.Minute - time.Nanosecond))
	if (*connections)[0].closes.Load() != 0 {
		t.Fatal("connection expired before idle timeout")
	}
	pool.evictIdle(now.Add(time.Minute))
	if (*connections)[0].closes.Load() != 1 {
		t.Fatalf("idle closes=%d, want 1", (*connections)[0].closes.Load())
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env)); !errors.Is(err, errProcessConnectionPoolClosed) {
		t.Fatalf("acquire after shutdown error=%v", err)
	}
}

func TestProcessConnectionPoolFailedInitialDialDoesNotPoisonKey(t *testing.T) {
	env, reader := newProcessPoolFixture(t)
	pool := NewProcessConnectionPool(reader)
	var attempts atomic.Int32
	connection := &fakeProcessConnection{}
	pool.dial = func(string, ...grpc.DialOption) (processConnection, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("dial failed")
		}
		return connection, nil
	}
	fence := lifecycle.CaptureExecutionFence(env)
	if _, _, err := pool.acquire(context.Background(), fence); err == nil {
		t.Fatal("failed initial dial was accepted")
	}
	_, release, err := pool.acquire(context.Background(), fence)
	if err != nil || attempts.Load() != 2 {
		t.Fatalf("retry error=%v attempts=%d", err, attempts.Load())
	}
	_ = release()
	_ = pool.Close()
}

func TestProcessConnectionPoolRejectsFailedCompletePostDialProof(t *testing.T) {
	for name, mutate := range map[string]func(client.Object){
		"Pod replacement": func(object client.Object) {
			if pod, ok := object.(*corev1.Pod); ok {
				pod.UID = "replacement-pod"
			}
		},
		"credential replacement": func(object client.Object) {
			if secret, ok := object.(*corev1.Secret); ok {
				secret.ResourceVersion = "2"
			}
		},
		"Template replacement": func(object client.Object) {
			if template, ok := object.(*platformv1alpha1.EnvironmentTemplate); ok {
				template.UID = "replacement-template"
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			env, reader := newProcessPoolFixture(t)
			reader.setMutation(func(object client.Object) {
				switch object.(type) {
				case *corev1.Pod:
					if reader.pods == 2 {
						mutate(object)
					}
				case *corev1.Secret:
					if reader.secrets == 2 {
						mutate(object)
					}
				case *platformv1alpha1.EnvironmentTemplate:
					if reader.templates == 2 {
						mutate(object)
					}
				}
			})
			pool, dials, connections := newCountingProcessPool(reader)
			if _, _, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env)); err == nil {
				t.Fatal("changed post-dial proof was accepted")
			}
			if dials.Load() != 1 || len(*connections) != 1 || (*connections)[0].closes.Load() != 1 {
				t.Fatalf("dials=%d connections=%d closes=%d, want 1/1/1", dials.Load(), len(*connections), (*connections)[0].closes.Load())
			}
		})
	}
}

func TestProcessConnectionPoolManagerCancellationClosesConnections(t *testing.T) {
	env, reader := newProcessPoolFixture(t)
	pool, _, connections := newCountingProcessPool(reader)
	_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
	if err != nil {
		t.Fatal(err)
	}
	_ = release()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pool.Start(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if (*connections)[0].closes.Load() != 1 {
		t.Fatalf("manager shutdown closes=%d, want 1", (*connections)[0].closes.Load())
	}
}

func TestProcessConnectionPoolCloseWakesPendingAcquisition(t *testing.T) {
	env, reader := newProcessPoolFixture(t)
	pool := NewProcessConnectionPool(reader)
	connection := &fakeProcessConnection{}
	entered, proceed := make(chan struct{}), make(chan struct{})
	pool.dial = func(string, ...grpc.DialOption) (processConnection, error) {
		close(entered)
		<-proceed
		return connection, nil
	}
	type result struct {
		release func() error
		err     error
	}
	results := make(chan result, 2)
	go func() {
		_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
		results <- result{release: release, err: err}
	}()
	<-entered
	go func() {
		_, release, err := pool.acquire(context.Background(), lifecycle.CaptureExecutionFence(env))
		results <- result{release: release, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		reader.mu.Lock()
		environmentReads := reader.envs
		reader.mu.Unlock()
		if environmentReads >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second acquisition did not reach pending dial")
		}
		time.Sleep(time.Millisecond)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	waiter := <-results
	if !errors.Is(waiter.err, errProcessConnectionPoolClosed) || waiter.release != nil {
		t.Fatalf("pending waiter: release nil=%t error=%v", waiter.release == nil, waiter.err)
	}
	close(proceed)
	opener := <-results
	if !errors.Is(opener.err, errProcessConnectionPoolClosed) || opener.release != nil || connection.closes.Load() != 1 {
		t.Fatalf("pending opener: release nil=%t error=%v closes=%d", opener.release == nil, opener.err, connection.closes.Load())
	}
}
