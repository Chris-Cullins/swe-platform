package server

import (
	"context"
	"errors"
	"net"
	"regexp"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const (
	maxServiceProbes             = 32
	maxServiceProbeIDBytes       = 64
	defaultServiceProbeTimeout   = time.Second
	defaultServiceObserveTimeout = 2 * time.Second
	defaultServiceProbeLimit     = 8
)

var serviceProbeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

var serviceObservationLoopbacks = []string{"127.0.0.1", "::1"}

var reservedControlPorts = map[uint32]struct{}{
	50051: {},
}

type dialContextFunc func(context.Context, string, string) (net.Conn, error)

// ServiceObservationServer implements a stateless, bounded loopback observer.
type ServiceObservationServer struct {
	sandboxdv1.UnimplementedServiceObservationServiceServer

	dialContext  dialContextFunc
	probeTimeout time.Duration
	callTimeout  time.Duration
	probeSlots   chan struct{}
	controlPort  uint32
}

// NewServiceObservationServer creates an observer whose concurrency limit is
// shared by every RPC handled by this server instance.
func NewServiceObservationServer(controlPort uint32) *ServiceObservationServer {
	dialer := &net.Dialer{}
	return &ServiceObservationServer{
		dialContext:  dialer.DialContext,
		probeTimeout: defaultServiceProbeTimeout,
		callTimeout:  defaultServiceObserveTimeout,
		probeSlots:   make(chan struct{}, defaultServiceProbeLimit),
		controlPort:  controlPort,
	}
}

// Observe validates the complete request before performing any observation.
// Results preserve request order and contain no operating-system error text.
func (s *ServiceObservationServer) Observe(ctx context.Context, req *sandboxdv1.ObserveServicesRequest) (*sandboxdv1.ObserveServicesResponse, error) {
	if err := validateServiceProbes(req.GetProbes(), s.controlPort); err != nil {
		return nil, err
	}
	if len(req.GetProbes()) == 0 {
		return &sandboxdv1.ObserveServicesResponse{}, nil
	}

	callCtx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()
	observations := make([]*sandboxdv1.ServiceProbeObservation, len(req.Probes))
	var wait sync.WaitGroup
	wait.Add(len(req.Probes))
	for i, probe := range req.Probes {
		go func() {
			defer wait.Done()
			observations[i] = s.observeOne(callCtx, probe)
		}()
	}
	wait.Wait()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &sandboxdv1.ObserveServicesResponse{Observations: observations}, nil
}

func validateServiceProbes(probes []*sandboxdv1.ServiceProbe, controlPort uint32) error {
	if len(probes) > maxServiceProbes {
		return status.Errorf(codes.InvalidArgument, "probes exceeds maximum of %d", maxServiceProbes)
	}
	ids := make(map[string]struct{}, len(probes))
	for i, probe := range probes {
		if probe == nil {
			return status.Errorf(codes.InvalidArgument, "probe %d is required", i)
		}
		if len(probe.Id) == 0 || len(probe.Id) > maxServiceProbeIDBytes || !serviceProbeIDPattern.MatchString(probe.Id) {
			return status.Errorf(codes.InvalidArgument, "probe %d id must be 1-%d bytes of portable correlation characters", i, maxServiceProbeIDBytes)
		}
		if _, duplicate := ids[probe.Id]; duplicate {
			return status.Errorf(codes.InvalidArgument, "probe %d id is duplicated", i)
		}
		ids[probe.Id] = struct{}{}
		if probe.TargetPort == 0 || probe.TargetPort > 65535 {
			return status.Errorf(codes.InvalidArgument, "probe %d target_port must be between 1 and 65535", i)
		}
		_, staticallyReserved := reservedControlPorts[probe.TargetPort]
		if staticallyReserved || probe.TargetPort == controlPort {
			return status.Errorf(codes.InvalidArgument, "probe %d target_port is reserved for sandboxd control", i)
		}
		if probe.GetTcpConnect() == nil {
			return status.Errorf(codes.InvalidArgument, "probe %d must select tcp_connect", i)
		}
	}
	return nil
}

func (s *ServiceObservationServer) observeOne(ctx context.Context, probe *sandboxdv1.ServiceProbe) *sandboxdv1.ServiceProbeObservation {
	result := &sandboxdv1.ServiceProbeObservation{Id: probe.Id}
	probeCtx, cancel := context.WithTimeout(ctx, s.probeTimeout)
	defer cancel()
	type dialResult struct {
		connected bool
		err       error
	}
	dialResults := make(chan dialResult, len(serviceObservationLoopbacks))
	for _, loopback := range serviceObservationLoopbacks {
		go func() {
			connected, err := s.dialLoopback(probeCtx, loopback, probe.TargetPort)
			dialResults <- dialResult{connected: connected, err: err}
		}()
	}
	connected := false
	timedOut := false
	for range serviceObservationLoopbacks {
		dial := <-dialResults
		if dial.connected {
			connected = true
			cancel()
		} else if errors.Is(dial.err, context.DeadlineExceeded) {
			timedOut = true
		}
	}
	if connected {
		result.Outcome = sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_CONNECTED
		result.Reason = sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_ACCEPTED
		return result
	}
	if probeCtx.Err() != nil {
		setServiceProbeContextResult(result, probeCtx.Err())
		return result
	}
	if timedOut {
		setServiceProbeContextResult(result, context.DeadlineExceeded)
		return result
	}
	result.Outcome = sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_NOT_CONNECTED
	result.Reason = sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CONNECTION_FAILED
	return result
}

func (s *ServiceObservationServer) dialLoopback(ctx context.Context, loopback string, port uint32) (bool, error) {
	select {
	case s.probeSlots <- struct{}{}:
		defer func() { <-s.probeSlots }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	connection, err := s.dialContext(ctx, "tcp", net.JoinHostPort(loopback, strconv.FormatUint(uint64(port), 10)))
	if err != nil {
		return false, err
	}
	_ = connection.Close()
	return true, nil
}

func setServiceProbeContextResult(result *sandboxdv1.ServiceProbeObservation, err error) {
	result.Outcome = sandboxdv1.ServiceProbeOutcome_SERVICE_PROBE_OUTCOME_TIMED_OUT
	if errors.Is(err, context.Canceled) {
		result.Reason = sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_CANCELED
		return
	}
	result.Reason = sandboxdv1.ServiceProbeReason_SERVICE_PROBE_REASON_DEADLINE_EXCEEDED
}
