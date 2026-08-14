package controlplane

import (
	"context"
	"sync"
)

const maxTranscriptGateEntries = 4096

// transcriptGate provides the process-local half of transcript deletion. It is
// intentionally bounded: authenticated and authorized callers cannot allocate
// more than maxEntries exact-identity entries. Deleting entries survive failed
// cleanup so reads and appends remain cut off for a later retry.
type transcriptGate struct {
	mu         sync.Mutex
	maxEntries int
	entries    map[RunIdentity]*transcriptGateEntry
}

type transcriptGateEntry struct {
	cutoff    bool
	completed bool
	cleaning  bool
	appends   int
	streams   map[*transcriptAdmission]context.CancelFunc
	changed   chan struct{}
}

type transcriptAdmission struct {
	gate   *transcriptGate
	run    RunIdentity
	entry  *transcriptGateEntry
	stream bool
	once   sync.Once
}

type transcriptCutoff struct {
	gate  *transcriptGate
	run   RunIdentity
	entry *transcriptGateEntry
}

func newTranscriptGate(maxEntries int) *transcriptGate {
	if maxEntries <= 0 {
		maxEntries = maxTranscriptGateEntries
	}
	return &transcriptGate{maxEntries: maxEntries, entries: make(map[RunIdentity]*transcriptGateEntry)}
}

func (g *transcriptGate) admit(ctx context.Context, run RunIdentity, stream bool) (context.Context, *transcriptAdmission, error) {
	g.mu.Lock()
	entry := g.entries[run]
	if entry != nil && entry.cutoff {
		g.mu.Unlock()
		return ctx, nil, ErrTranscriptCutoff
	}
	if entry == nil {
		if len(g.entries) >= g.maxEntries {
			g.mu.Unlock()
			return ctx, nil, ErrTranscriptGateLimit
		}
		entry = &transcriptGateEntry{streams: make(map[*transcriptAdmission]context.CancelFunc), changed: make(chan struct{})}
		g.entries[run] = entry
	}
	admission := &transcriptAdmission{gate: g, run: run, entry: entry, stream: stream}
	if stream {
		streamContext, cancel := context.WithCancel(ctx)
		entry.streams[admission] = cancel
		g.mu.Unlock()
		return streamContext, admission, nil
	}
	entry.appends++
	g.mu.Unlock()
	return ctx, admission, nil
}

func (a *transcriptAdmission) release() {
	if a == nil {
		return
	}
	a.once.Do(func() {
		a.gate.mu.Lock()
		if a.stream {
			if cancel, ok := a.entry.streams[a]; ok {
				delete(a.entry.streams, a)
				cancel()
				a.gate.signal(a.entry)
			}
		} else {
			a.entry.appends--
			a.gate.signal(a.entry)
		}
		a.gate.removeIdle(a.run, a.entry)
		a.gate.mu.Unlock()
	})
}

func (g *transcriptGate) cutoff(run RunIdentity) (*transcriptCutoff, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	entry := g.entries[run]
	if entry == nil {
		if len(g.entries) >= g.maxEntries {
			return nil, ErrTranscriptGateLimit
		}
		entry = &transcriptGateEntry{streams: make(map[*transcriptAdmission]context.CancelFunc), changed: make(chan struct{})}
		g.entries[run] = entry
	}
	entry.cutoff = true
	for _, cancel := range entry.streams {
		cancel()
	}
	return &transcriptCutoff{gate: g, run: run, entry: entry}, nil
}

// beginCleanup waits for every request admitted before cutoff, including
// canceled streams, and serializes cleanup retries. A completed process-local
// cleanup is an idempotent success.
func (c *transcriptCutoff) beginCleanup(ctx context.Context) (bool, error) {
	for {
		c.gate.mu.Lock()
		if c.entry.completed {
			c.gate.mu.Unlock()
			return true, nil
		}
		if c.entry.appends == 0 && len(c.entry.streams) == 0 && !c.entry.cleaning {
			c.entry.cleaning = true
			c.gate.mu.Unlock()
			return false, nil
		}
		changed := c.entry.changed
		c.gate.mu.Unlock()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-changed:
		}
	}
}

func (c *transcriptCutoff) finish(success bool) {
	c.gate.mu.Lock()
	c.entry.cleaning = false
	if success {
		c.entry.completed = true
		c.entry.cutoff = false
	}
	c.gate.signal(c.entry)
	if success {
		c.gate.removeIdle(c.run, c.entry)
	}
	c.gate.mu.Unlock()
}

func (g *transcriptGate) signal(entry *transcriptGateEntry) {
	close(entry.changed)
	entry.changed = make(chan struct{})
}

func (g *transcriptGate) removeIdle(run RunIdentity, entry *transcriptGateEntry) {
	if !entry.cutoff && entry.appends == 0 && len(entry.streams) == 0 && g.entries[run] == entry {
		delete(g.entries, run)
	}
}
