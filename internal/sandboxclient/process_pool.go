package sandboxclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Chris-Cullins/swe-platform/internal/lifecycle"
	sandboxdv1 "github.com/Chris-Cullins/swe-platform/sandboxd/gen/proto/sandboxd/v1"
)

const defaultProcessConnectionIdleTimeout = 30 * time.Second

var errProcessConnectionPoolClosed = errors.New("sandboxd process connection pool is closed")

type processConnection interface {
	grpc.ClientConnInterface
	Close() error
}

type processConnectionKey struct {
	environmentUID      types.UID
	executionGeneration int64
}

type processConnectionEntry struct {
	connection processConnection
	proof      processConnectionProof
	borrowers  int
	idleSince  time.Time
	evicted    bool
	closed     bool
}

type pendingProcessConnection struct {
	ready chan struct{}
}

// ProcessConnectionPool owns reusable, process-capability-only sandboxd
// connections for one operator process. Physical connections are keyed by the
// immutable Environment UID and backend-neutral execution generation. The
// pool never serves terminal clients and does not broaden a connection's
// per-execution capability.
type ProcessConnectionPool struct {
	reader      client.Reader
	idleTimeout time.Duration
	now         func() time.Time
	dial        func(string, ...grpc.DialOption) (processConnection, error)

	mu      sync.Mutex
	entries map[processConnectionKey]*processConnectionEntry
	pending map[processConnectionKey]*pendingProcessConnection
	closed  bool
	close   sync.Once
	done    chan struct{}
}

func NewProcessConnectionPool(reader client.Reader) *ProcessConnectionPool {
	return &ProcessConnectionPool{
		reader: reader, idleTimeout: defaultProcessConnectionIdleTimeout, now: time.Now,
		dial: func(target string, options ...grpc.DialOption) (processConnection, error) {
			return grpc.NewClient(target, options...)
		},
		entries: make(map[processConnectionKey]*processConnectionEntry),
		pending: make(map[processConnectionKey]*pendingProcessConnection),
		done:    make(chan struct{}),
	}
}

// Start makes the pool a controller-runtime manager Runnable. Manager
// cancellation deterministically closes all process-owned physical
// connections; idle entries are also evicted while the manager is running.
func (p *ProcessConnectionPool) Start(ctx context.Context) error {
	interval := p.idleTimeout / 2
	if interval <= 0 || interval > 10*time.Second {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer p.Close()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			p.evictIdle(now)
		}
	}
}

// Close prevents new leases and physically closes every pooled connection.
// It is safe to call concurrently and remains deterministic with outstanding
// borrowers; their later release callbacks become no-ops.
func (p *ProcessConnectionPool) Close() error {
	var closeErr error
	p.close.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.done)
		entries := make([]*processConnectionEntry, 0, len(p.entries))
		for key, entry := range p.entries {
			delete(p.entries, key)
			entry.evicted = true
			if !entry.closed {
				entry.closed = true
				entries = append(entries, entry)
			}
		}
		p.mu.Unlock()
		for _, entry := range entries {
			if err := entry.connection.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		}
	})
	return closeErr
}

func (p *ProcessConnectionPool) acquire(ctx context.Context, fence lifecycle.ExecutionFence) (sandboxdv1.ProcessServiceClient, func() error, error) {
	key := processConnectionKey{environmentUID: fence.EnvironmentUID(), executionGeneration: fence.ExecutionGeneration()}
	for {
		select {
		case <-p.done:
			return nil, nil, errProcessConnectionPoolClosed
		default:
		}
		// Mandatory uncached proof on every attempted reuse: the full opaque fence,
		// Template/backend, exact Pod, and credential Secret must still identify
		// the same reachable execution and process capability.
		env, err := fence.Revalidate(ctx, p.reader)
		if err != nil {
			p.invalidate(key)
			return nil, nil, err
		}
		if !processEnvironmentReachable(env) {
			p.invalidate(key)
			return nil, nil, fmt.Errorf("environment is not the current reachable incarnation")
		}
		_, secret, proof, err := (Connector{Reader: p.reader}).resolveProcessTargetForEnvironment(ctx, env)
		if err != nil {
			p.invalidate(key)
			return nil, nil, err
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, nil, errProcessConnectionPoolClosed
		}
		if entry := p.entries[key]; entry != nil && !entry.evicted {
			if entry.proof.matches(proof) {
				entry.borrowers++
				entry.idleSince = time.Time{}
				p.mu.Unlock()
				return sandboxdv1.NewProcessServiceClient(entry.connection), p.releaseFunc(key, entry), nil
			}
			p.evictLocked(key, entry)
		}
		if pending := p.pending[key]; pending != nil {
			ready := pending.ready
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-p.done:
				return nil, nil, errProcessConnectionPoolClosed
			case <-ready:
				continue
			}
		}
		pending := &pendingProcessConnection{ready: make(chan struct{})}
		p.pending[key] = pending
		p.mu.Unlock()

		connection, proof, err := p.open(ctx, fence, secret, proof)
		p.mu.Lock()
		delete(p.pending, key)
		close(pending.ready)
		if err != nil {
			p.mu.Unlock()
			return nil, nil, err
		}
		if p.closed {
			p.mu.Unlock()
			_ = connection.Close()
			return nil, nil, errProcessConnectionPoolClosed
		}
		entry := &processConnectionEntry{connection: connection, proof: proof, borrowers: 1}
		p.entries[key] = entry
		p.mu.Unlock()
		return sandboxdv1.NewProcessServiceClient(connection), p.releaseFunc(key, entry), nil
	}
}

func (p *ProcessConnectionPool) open(ctx context.Context, fence lifecycle.ExecutionFence, secret *corev1.Secret, proof processConnectionProof) (processConnection, processConnectionProof, error) {
	options, err := processDialOptions(secret, proof.identity)
	if err != nil {
		return nil, processConnectionProof{}, err
	}
	connection, err := p.dial(proof.execution.endpoint, options...)
	if err != nil {
		return nil, processConnectionProof{}, err
	}
	_, _, current, err := (Connector{Reader: p.reader}).resolveProcessTarget(ctx, fence)
	if err != nil || !proof.matches(current) {
		_ = connection.Close()
		if err != nil {
			return nil, processConnectionProof{}, fmt.Errorf("environment execution changed while resolving process endpoint: %w", err)
		}
		return nil, processConnectionProof{}, fmt.Errorf("environment execution changed while resolving process endpoint")
	}
	return connection, current, nil
}

func (p *ProcessConnectionPool) releaseFunc(key processConnectionKey, entry *processConnectionEntry) func() error {
	var once sync.Once
	return func() error {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			if entry.borrowers > 0 {
				entry.borrowers--
			}
			if entry.borrowers == 0 {
				if entry.evicted || p.closed {
					p.closeEntryLocked(entry)
				} else if p.entries[key] == entry {
					entry.idleSince = p.now()
				}
			}
		})
		return nil
	}
}

func (p *ProcessConnectionPool) invalidate(key processConnectionKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry := p.entries[key]; entry != nil {
		p.evictLocked(key, entry)
	}
}

func (p *ProcessConnectionPool) evictIdle(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, entry := range p.entries {
		if entry.borrowers == 0 && !entry.idleSince.IsZero() && now.Sub(entry.idleSince) >= p.idleTimeout {
			p.evictLocked(key, entry)
		}
	}
}

func (p *ProcessConnectionPool) evictLocked(key processConnectionKey, entry *processConnectionEntry) {
	if p.entries[key] == entry {
		delete(p.entries, key)
	}
	entry.evicted = true
	if entry.borrowers == 0 {
		p.closeEntryLocked(entry)
	}
}

func (p *ProcessConnectionPool) closeEntryLocked(entry *processConnectionEntry) {
	if entry.closed {
		return
	}
	entry.closed = true
	_ = entry.connection.Close()
}
