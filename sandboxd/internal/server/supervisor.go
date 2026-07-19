package server

import (
	"context"
	"sync"
)

// Supervisor owns every execution domain in one sandboxd daemon epoch.
type Supervisor struct {
	mu      sync.Mutex
	closed  bool
	domains map[*processDomain]func()
	wg      sync.WaitGroup
}

func NewSupervisor() *Supervisor { return &Supervisor{domains: make(map[*processDomain]func())} }

// start admits and launches a domain while holding the shutdown admission lock.
// Thus Close can never take a kill snapshot between admission and launch.
func (s *Supervisor) start(d *processDomain, closing func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = d.close()
		return context.Canceled
	}
	if err := d.start(); err != nil {
		return err
	}
	s.domains[d] = closing
	s.wg.Add(1)
	return nil
}

func (s *Supervisor) done(d *processDomain) {
	s.mu.Lock()
	if _, ok := s.domains[d]; ok {
		delete(s.domains, d)
		s.wg.Done()
	}
	s.mu.Unlock()
}

// Close fences admissions, force-stops all domains, and waits for their waiters
// and pipe drains to finish (or for ctx to expire).
func (s *Supervisor) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	domains := make([]*processDomain, 0, len(s.domains))
	callbacks := make([]func(), 0, len(s.domains))
	for d, closing := range s.domains {
		domains = append(domains, d)
		callbacks = append(callbacks, closing)
	}
	s.mu.Unlock()
	for _, closing := range callbacks {
		if closing != nil {
			closing()
		}
	}
	for _, d := range domains {
		_ = d.force()
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
