package egressproxy

import (
	"errors"
	"sync"
	"time"
)

type quotaEntry struct {
	active, pending int
	tokens          float64
	last            time.Time
}
type QuotaManager struct {
	mu       sync.Mutex
	global   int
	projects map[string]int
	exec     map[string]*quotaEntry
	now      func() time.Time
}

type Reservation struct {
	q           *QuotaManager
	ex, project string
	state       reservationState
}

type reservationState uint8

const (
	reservationPending reservationState = iota
	reservationActive
	reservationReleased
)

func NewQuotaManager() *QuotaManager {
	return &QuotaManager{projects: map[string]int{}, exec: map[string]*quotaEntry{}, now: time.Now}
}
func (q *QuotaManager) Acquire(ex, project string) (func(), error) {
	r, err := q.Reserve(ex, project)
	if err != nil {
		return nil, err
	}
	if err := r.Activate(); err != nil {
		r.Release()
		return nil, err
	}
	return r.Release, nil
}

func (q *QuotaManager) Reserve(ex, project string) (*Reservation, error) {
	if ex == "" || project == "" {
		return nil, errors.New("identity keys required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	e := q.exec[ex]
	if e == nil {
		e = &quotaEntry{tokens: 20, last: q.now()}
		q.exec[ex] = e
	}
	now := q.now()
	e.tokens += now.Sub(e.last).Seconds() * 10
	if e.tokens > 20 {
		e.tokens = 20
	}
	e.last = now
	if e.tokens < 1 || e.pending >= 16 {
		return nil, errors.New("quota exceeded")
	}
	e.tokens--
	e.pending++
	return &Reservation{q: q, ex: ex, project: project}, nil
}

func (r *Reservation) Activate() error {
	r.q.mu.Lock()
	defer r.q.mu.Unlock()
	e := r.q.exec[r.ex]
	if r.state == reservationReleased {
		return errors.New("reservation released")
	}
	if r.state == reservationActive {
		return nil
	}
	if e.active >= 32 || r.q.projects[r.project] >= 256 || r.q.global >= 2048 {
		return errors.New("quota exceeded")
	}
	e.pending--
	e.active++
	r.q.projects[r.project]++
	r.q.global++
	r.state = reservationActive
	return nil
}

func (r *Reservation) Release() {
	r.q.mu.Lock()
	defer r.q.mu.Unlock()
	if r.state == reservationReleased {
		return
	}
	e := r.q.exec[r.ex]
	if r.state == reservationActive {
		e.active--
		r.q.projects[r.project]--
		if r.q.projects[r.project] == 0 {
			delete(r.q.projects, r.project)
		}
		r.q.global--
	} else {
		e.pending--
	}
	r.state = reservationReleased
}
