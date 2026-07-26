package server

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

func TestServiceObservationReportsConnectedAndNotConnectedInRequestOrder(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	connectedPort := uint32(listener.Addr().(*net.TCPAddr).Port)

	closedListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedPort := uint32(closedListener.Addr().(*net.TCPAddr).Port)
	if err := closedListener.Close(); err != nil {
		t.Fatal(err)
	}

	conn := newConn(t, t.TempDir())
	response, err := sandboxdv1.NewServiceObservationServiceClient(conn).Observe(context.Background(), &sandboxdv1.ObserveServicesRequest{Probes: []*sandboxdv1.ServiceProbe{
		tcpServiceProbe("connected", connectedPort),
		tcpServiceProbe("not-connected", closedPort),
	}})
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if len(response.Observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(response.Observations))
	}
	if got := response.Observations[0]; got.Id != "connected" || got.Outcome != sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_CONNECTED || got.Reason != sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_ACCEPTED {
		t.Fatalf("connected observation = %#v", got)
	}
	if got := response.Observations[1]; got.Id != "not-connected" || got.Outcome != sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_NOT_CONNECTED || got.Reason != sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_FAILED {
		t.Fatalf("not-connected observation = %#v", got)
	}
}

func TestServiceObservationReportsIPv6LoopbackListener(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	port := uint32(listener.Addr().(*net.TCPAddr).Port)

	conn := newConn(t, t.TempDir())
	response, err := sandboxdv1.NewServiceObservationServiceClient(conn).Observe(context.Background(), &sandboxdv1.ObserveServicesRequest{
		Probes: []*sandboxdv1.ServiceProbe{tcpServiceProbe("ipv6", port)},
	})
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if len(response.Observations) != 1 || response.Observations[0].Outcome != sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_CONNECTED {
		t.Fatalf("IPv6 observation = %#v", response.Observations)
	}
}

func TestServiceObservationRejectsInvalidRequestsBeforeDialing(t *testing.T) {
	valid := tcpServiceProbe("valid", 8080)
	tests := map[string][]*sandboxdv1.ServiceProbe{
		"nil probe":          {nil},
		"empty id":           {tcpServiceProbe("", 8080)},
		"oversized id":       {tcpServiceProbe(strings.Repeat("a", maxServiceProbeIDBytes+1), 8080)},
		"non-portable id":    {tcpServiceProbe("not valid", 8080)},
		"duplicate id":       {valid, tcpServiceProbe("valid", 8081)},
		"zero port":          {tcpServiceProbe("zero", 0)},
		"out-of-range port":  {tcpServiceProbe("large", 65536)},
		"reserved port":      {tcpServiceProbe("sandboxd", 50051)},
		"missing probe kind": {{Id: "missing", TargetPort: 8080}},
	}
	tooMany := make([]*sandboxdv1.ServiceProbe, maxServiceProbes+1)
	for i := range tooMany {
		tooMany[i] = tcpServiceProbe(string(rune('a'+i%26))+strings.Repeat("x", i/26), uint32(1000+i))
	}
	tests["too many probes"] = tooMany

	for name, probes := range tests {
		t.Run(name, func(t *testing.T) {
			var dials atomic.Int32
			observer := NewServiceObservationServer(50051)
			observer.dialContext = func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("unexpected dial")
			}
			_, err := observer.Observe(context.Background(), &sandboxdv1.ObserveServicesRequest{Probes: probes})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("status = %v, want InvalidArgument", err)
			}
			if dials.Load() != 0 {
				t.Fatalf("invalid request performed %d dials", dials.Load())
			}
		})
	}
}

func TestServiceObservationRejectsConfiguredControlPort(t *testing.T) {
	observer := NewServiceObservationServer(50052)
	for _, port := range []uint32{50051, 50052} {
		_, err := observer.Observe(context.Background(), &sandboxdv1.ObserveServicesRequest{
			Probes: []*sandboxdv1.ServiceProbe{tcpServiceProbe("control", port)},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("port %d status = %v, want InvalidArgument", port, err)
		}
	}
}

func TestServiceObservationBoundsErrorsAndPreservesCorrelation(t *testing.T) {
	observer := NewServiceObservationServer(50051)
	observer.dialContext = func(_ context.Context, _, address string) (net.Conn, error) {
		if strings.HasSuffix(address, ":8001") {
			time.Sleep(20 * time.Millisecond)
		}
		return nil, errors.New("private operating-system detail")
	}
	response, err := observer.Observe(context.Background(), &sandboxdv1.ObserveServicesRequest{Probes: []*sandboxdv1.ServiceProbe{
		tcpServiceProbe("first", 8001),
		tcpServiceProbe("second", 8002),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Observations) != 2 || response.Observations[0].Id != "first" || response.Observations[1].Id != "second" {
		t.Fatalf("response correlation = %#v", response.Observations)
	}
	for _, observation := range response.Observations {
		if observation.Outcome != sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_NOT_CONNECTED || observation.Reason != sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_FAILED {
			t.Fatalf("bounded observation = %#v", observation)
		}
		if strings.Contains(observation.String(), "operating-system") {
			t.Fatalf("observation disclosed dial error: %s", observation)
		}
	}
}

func TestServiceObservationEnforcesWholeCallAndGlobalConcurrencyBounds(t *testing.T) {
	observer := NewServiceObservationServer(50051)
	observer.callTimeout = 40 * time.Millisecond
	observer.probeTimeout = time.Second
	observer.probeSlots = make(chan struct{}, 2)
	var current atomic.Int32
	var maximum atomic.Int32
	observer.dialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		now := current.Add(1)
		defer current.Add(-1)
		for {
			seen := maximum.Load()
			if now <= seen || maximum.CompareAndSwap(seen, now) {
				break
			}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	request := &sandboxdv1.ObserveServicesRequest{Probes: []*sandboxdv1.ServiceProbe{
		tcpServiceProbe("one", 8001), tcpServiceProbe("two", 8002),
		tcpServiceProbe("three", 8003), tcpServiceProbe("four", 8004),
	}}
	start := time.Now()
	var wait sync.WaitGroup
	wait.Add(2)
	for range 2 {
		go func() {
			defer wait.Done()
			response, err := observer.Observe(context.Background(), request)
			if err != nil {
				t.Errorf("observe: %v", err)
				return
			}
			for _, observation := range response.Observations {
				if observation.Outcome != sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_TIMED_OUT {
					t.Errorf("outcome = %s, want TIMED_OUT", observation.Outcome)
				}
			}
		}()
	}
	wait.Wait()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("whole-call bound took %s", elapsed)
	}
	if maximum.Load() != 2 {
		t.Fatalf("maximum concurrent dials = %d, want 2", maximum.Load())
	}
	if len(observer.probeSlots) != 0 {
		t.Fatalf("probe slots leaked: %d", len(observer.probeSlots))
	}
}

func TestServiceObservationCancellationReleasesGlobalSlot(t *testing.T) {
	observer := NewServiceObservationServer(50051)
	observer.callTimeout = time.Second
	observer.probeTimeout = time.Second
	observer.probeSlots = make(chan struct{}, 1)
	started := make(chan struct{})
	var once sync.Once
	observer.dialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := observer.Observe(ctx, &sandboxdv1.ObserveServicesRequest{Probes: []*sandboxdv1.ServiceProbe{tcpServiceProbe("cancel", 8080)}})
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("status = %v, want Canceled", err)
	}
	if len(observer.probeSlots) != 0 {
		t.Fatalf("probe slot leaked after cancellation: %d", len(observer.probeSlots))
	}
}

func tcpServiceProbe(id string, port uint32) *sandboxdv1.ServiceProbe {
	return &sandboxdv1.ServiceProbe{
		Id: id, TargetPort: port,
		Probe: &sandboxdv1.ServiceProbe_TcpConnect{TcpConnect: &sandboxdv1.TCPConnectProbe{}},
	}
}
