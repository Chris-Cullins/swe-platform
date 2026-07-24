package controlplane

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var transcriptMigrations embed.FS

const maxPostgresTranscriptSequence = uint64(math.MaxInt64)

// PostgresTranscriptStoreOptions controls durable retention and local polling resources.
// Retention and idempotency are intentionally scoped to each immutable Run UID.
type PostgresTranscriptStoreOptions struct {
	MaxEventsPerRun  int
	MaxBytesPerRun   int
	MaxReplayEvents  int
	MaxSubscribers   int
	SubscriberBuffer int
	PollInterval     time.Duration
}

// DefaultPostgresTranscriptStoreOptions returns the production defaults.
func DefaultPostgresTranscriptStoreOptions() PostgresTranscriptStoreOptions {
	return PostgresTranscriptStoreOptions{
		MaxEventsPerRun:  10_000,
		MaxBytesPerRun:   64 << 20,
		MaxReplayEvents:  1_000,
		MaxSubscribers:   1_024,
		SubscriberBuffer: 64,
		PollInterval:     500 * time.Millisecond,
	}
}

// PostgresTranscriptStore persists events and idempotency records in PostgreSQL. Polling
// database replay is the correctness path; a future notification path may only wake polls.
type PostgresTranscriptStore struct {
	pool       *pgxpool.Pool
	options    PostgresTranscriptStoreOptions
	generation string
	cursorKey  []byte

	subscriberMu sync.Mutex
	subscribers  int
}

// NewPostgresTranscriptStore connects, applies embedded migrations, and loads the durable
// cursor identity. It returns only after the database can satisfy the store contract.
func NewPostgresTranscriptStore(ctx context.Context, databaseURL string, options PostgresTranscriptStoreOptions) (*PostgresTranscriptStore, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, errors.New("PostgreSQL transcript database URL is required")
	}
	options = normalizePostgresTranscriptOptions(options)
	if options.MaxReplayEvents == math.MaxInt {
		return nil, errors.New("PostgreSQL transcript replay limit is too large")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("configure PostgreSQL transcript store: %w", err)
	}
	store := &PostgresTranscriptStore{pool: pool, options: options}
	if err := store.initialize(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func normalizePostgresTranscriptOptions(options PostgresTranscriptStoreOptions) PostgresTranscriptStoreOptions {
	defaults := DefaultPostgresTranscriptStoreOptions()
	if options.MaxEventsPerRun <= 0 {
		options.MaxEventsPerRun = defaults.MaxEventsPerRun
	}
	if options.MaxBytesPerRun <= 0 {
		options.MaxBytesPerRun = defaults.MaxBytesPerRun
	}
	if options.MaxReplayEvents <= 0 {
		options.MaxReplayEvents = defaults.MaxReplayEvents
	}
	if options.MaxSubscribers <= 0 {
		options.MaxSubscribers = defaults.MaxSubscribers
	}
	if options.SubscriberBuffer <= 0 {
		options.SubscriberBuffer = defaults.SubscriberBuffer
	}
	if options.PollInterval <= 0 {
		options.PollInterval = defaults.PollInterval
	}
	return options
}

func (s *PostgresTranscriptStore) initialize(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transcript migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(739675093427891376)`); err != nil {
		return fmt.Errorf("lock transcript migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS transcript_schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create transcript migration ledger: %w", err)
	}
	entries, err := fs.ReadDir(transcriptMigrations, "migrations")
	if err != nil {
		return fmt.Errorf("read transcript migrations: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		var applied bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM transcript_schema_migrations WHERE version = $1)`, entry.Name()).Scan(&applied); err != nil {
			return fmt.Errorf("check transcript migration %s: %w", entry.Name(), err)
		}
		if applied {
			continue
		}
		migration, err := transcriptMigrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read transcript migration %s: %w", entry.Name(), err)
		}
		if _, err := tx.Exec(ctx, string(migration)); err != nil {
			return fmt.Errorf("apply transcript migration %s: %w", entry.Name(), err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO transcript_schema_migrations(version) VALUES ($1)`, entry.Name()); err != nil {
			return fmt.Errorf("record transcript migration %s: %w", entry.Name(), err)
		}
	}
	generationBytes := make([]byte, 16)
	cursorKey := make([]byte, 32)
	if _, err := rand.Read(generationBytes); err != nil {
		return fmt.Errorf("generate transcript store identity: %w", err)
	}
	if _, err := rand.Read(cursorKey); err != nil {
		return fmt.Errorf("generate transcript cursor key: %w", err)
	}
	generation := base64.RawURLEncoding.EncodeToString(generationBytes)
	if _, err := tx.Exec(ctx, `INSERT INTO transcript_store_metadata(singleton, generation, cursor_key) VALUES (true, $1, $2) ON CONFLICT (singleton) DO NOTHING`, generation, cursorKey); err != nil {
		return fmt.Errorf("initialize transcript cursor identity: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT generation, cursor_key FROM transcript_store_metadata WHERE singleton`).Scan(&s.generation, &s.cursorKey); err != nil {
		return fmt.Errorf("load transcript cursor identity: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transcript migrations: %w", err)
	}
	return nil
}

// Close releases database connections. Existing subscriptions should be unsubscribed first.
func (s *PostgresTranscriptStore) Close() { s.pool.Close() }

func (s *PostgresTranscriptStore) Append(ctx context.Context, run RunIdentity, input AppendTranscriptInput) (AppendTranscriptResult, error) {
	size := transcriptEventSize(input)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return AppendTranscriptResult{}, fmt.Errorf("begin transcript append: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO transcript_runs(namespace, run_uid) VALUES ($1, $2) ON CONFLICT DO NOTHING`, run.Namespace, string(run.UID)); err != nil {
		return AppendTranscriptResult{}, fmt.Errorf("initialize transcript run: %w", err)
	}
	var databaseHighWater int64
	var retainedEvents int
	var retainedBytes int64
	if err := tx.QueryRow(ctx, `SELECT high_water, retained_events, retained_bytes FROM transcript_runs WHERE namespace = $1 AND run_uid = $2 FOR UPDATE`, run.Namespace, string(run.UID)).Scan(&databaseHighWater, &retainedEvents, &retainedBytes); err != nil {
		return AppendTranscriptResult{}, fmt.Errorf("lock transcript run: %w", err)
	}
	highWater := uint64(databaseHighWater)
	prior, found, err := queryIdempotentEvent(ctx, tx, run, input.Source, input.IdempotencyKey, s.cursor)
	if err != nil {
		return AppendTranscriptResult{}, err
	}
	if found {
		if equalAppendInput(prior, input) {
			if err := tx.Commit(ctx); err != nil {
				return AppendTranscriptResult{}, fmt.Errorf("commit transcript idempotency replay: %w", err)
			}
			return AppendTranscriptResult{Event: prior, Replayed: true}, nil
		}
		return AppendTranscriptResult{}, ErrIdempotencyConflict
	}
	if size > s.options.MaxBytesPerRun {
		return AppendTranscriptResult{}, ErrTranscriptCapacity
	}

	evictThrough := int64(0)
	freedEvents, freedBytes := 0, int64(0)
	if retainedEvents+1 > s.options.MaxEventsPerRun || retainedBytes+int64(size) > int64(s.options.MaxBytesPerRun) {
		rows, err := tx.Query(ctx, `SELECT sequence, event_size FROM transcript_events WHERE namespace = $1 AND run_uid = $2 ORDER BY sequence`, run.Namespace, string(run.UID))
		if err != nil {
			return AppendTranscriptResult{}, fmt.Errorf("select transcript retention candidates: %w", err)
		}
		for rows.Next() && (retainedEvents-freedEvents+1 > s.options.MaxEventsPerRun || retainedBytes-freedBytes+int64(size) > int64(s.options.MaxBytesPerRun)) {
			var eventSize int64
			if err := rows.Scan(&evictThrough, &eventSize); err != nil {
				rows.Close()
				return AppendTranscriptResult{}, fmt.Errorf("scan transcript retention candidate: %w", err)
			}
			freedEvents++
			freedBytes += eventSize
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return AppendTranscriptResult{}, fmt.Errorf("read transcript retention candidates: %w", err)
		}
		rows.Close()
	}
	if evictThrough > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM transcript_events WHERE namespace = $1 AND run_uid = $2 AND sequence <= $3`, run.Namespace, string(run.UID), evictThrough); err != nil {
			return AppendTranscriptResult{}, fmt.Errorf("evict transcript events: %w", err)
		}
	}
	if highWater == maxPostgresTranscriptSequence {
		return AppendTranscriptResult{}, ErrTranscriptCapacity
	}
	sequence := highWater + 1
	createdAt := time.Now().UTC()
	var sourceSequence any
	if input.SourceSequence != nil {
		sourceSequence = strconv.FormatUint(*input.SourceSequence, 10)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO transcript_events(namespace, run_uid, sequence, source, source_sequence, idempotency_key, event_type, data, created_at, event_size) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, run.Namespace, string(run.UID), int64(sequence), []byte(input.Source), sourceSequence, []byte(input.IdempotencyKey), []byte(input.Type), []byte(input.Data), createdAt, size); err != nil {
		return AppendTranscriptResult{}, fmt.Errorf("insert transcript event: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE transcript_runs SET high_water = $3, retained_events = retained_events - $4 + 1, retained_bytes = retained_bytes - $5 + $6 WHERE namespace = $1 AND run_uid = $2`, run.Namespace, string(run.UID), int64(sequence), freedEvents, freedBytes, size); err != nil {
		return AppendTranscriptResult{}, fmt.Errorf("advance transcript sequence: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AppendTranscriptResult{}, fmt.Errorf("commit transcript event: %w", err)
	}
	event := TranscriptEvent{ID: s.cursor(run, sequence), Sequence: sequence, Source: input.Source, SourceSequence: cloneUint64(input.SourceSequence), Type: input.Type, Data: append(json.RawMessage(nil), input.Data...), CreatedAt: createdAt}
	return AppendTranscriptResult{Event: event}, nil
}

type postgresQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func queryIdempotentEvent(ctx context.Context, query postgresQuery, run RunIdentity, source, key string, cursor func(RunIdentity, uint64) string) (TranscriptEvent, bool, error) {
	var event TranscriptEvent
	var sourceSequence *string
	var databaseSequence int64
	var sourceBytes, typeBytes, data []byte
	err := query.QueryRow(ctx, `SELECT sequence, source, source_sequence, event_type, data, created_at FROM transcript_events WHERE namespace = $1 AND run_uid = $2 AND source = $3 AND idempotency_key = $4`, run.Namespace, string(run.UID), []byte(source), []byte(key)).Scan(&databaseSequence, &sourceBytes, &sourceSequence, &typeBytes, &data, &event.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TranscriptEvent{}, false, nil
	}
	if err != nil {
		return TranscriptEvent{}, false, fmt.Errorf("read transcript idempotency record: %w", err)
	}
	event.Sequence = uint64(databaseSequence)
	event.Source = string(sourceBytes)
	event.Type = string(typeBytes)
	if sourceSequence != nil {
		parsed, err := strconv.ParseUint(*sourceSequence, 10, 64)
		if err != nil {
			return TranscriptEvent{}, false, fmt.Errorf("decode transcript source sequence: %w", err)
		}
		event.SourceSequence = &parsed
	}
	event.Data = json.RawMessage(data)
	event.ID = cursor(run, event.Sequence)
	return event, true, nil
}

func (s *PostgresTranscriptStore) Subscribe(ctx context.Context, run RunIdentity, cursor string) (TranscriptSubscription, error) {
	sequence := uint64(0)
	if cursor != "" {
		parsed, err := s.parseCursor(run, cursor)
		if err != nil {
			return TranscriptSubscription{}, err
		}
		sequence = parsed
	}
	if !s.reserveSubscriber() {
		return TranscriptSubscription{}, ErrSubscriberCapacity
	}
	history, gap, highWater, err := s.replay(ctx, run, sequence, cursor != "")
	if err != nil {
		s.releaseSubscriber()
		return TranscriptSubscription{}, err
	}
	subscriptionContext, cancel := context.WithCancel(ctx)
	events := make(chan TranscriptEvent, s.options.SubscriberBuffer)
	dropped := make(chan struct{})
	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			cancel()
			s.releaseSubscriber()
		})
	}
	go s.poll(subscriptionContext, run, highWater, events, dropped)
	return TranscriptSubscription{History: history, Events: events, Dropped: dropped, Gap: gap, Unsubscribe: unsubscribe}, nil
}

func (s *PostgresTranscriptStore) replay(ctx context.Context, run RunIdentity, sequence uint64, hasCursor bool) ([]TranscriptEvent, *TranscriptGap, uint64, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nil, 0, fmt.Errorf("begin transcript replay: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var databaseHighWater int64
	err = tx.QueryRow(ctx, `SELECT high_water FROM transcript_runs WHERE namespace = $1 AND run_uid = $2`, run.Namespace, string(run.UID)).Scan(&databaseHighWater)
	if errors.Is(err, pgx.ErrNoRows) {
		databaseHighWater = 0
	} else if err != nil {
		return nil, nil, 0, fmt.Errorf("read transcript high-water mark: %w", err)
	}
	highWater := uint64(databaseHighWater)
	if sequence > highWater {
		return nil, nil, 0, ErrInvalidCursor
	}
	var databaseEarliest *int64
	if err := tx.QueryRow(ctx, `SELECT MIN(sequence) FROM transcript_events WHERE namespace = $1 AND run_uid = $2`, run.Namespace, string(run.UID)).Scan(&databaseEarliest); err != nil {
		return nil, nil, 0, fmt.Errorf("read earliest transcript sequence: %w", err)
	}
	earliest := highWater + 1
	if databaseEarliest != nil {
		earliest = uint64(*databaseEarliest)
	}
	if hasCursor && sequence+1 < earliest {
		return nil, nil, 0, &TranscriptCursorError{Err: ErrExpiredCursor, Gap: s.gap(run, earliest, highWater)}
	}
	history, err := queryTranscriptEvents(ctx, tx, run, sequence, s.options.MaxReplayEvents+1, s.cursor)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(history) > s.options.MaxReplayEvents {
		resumeSequence := highWater - uint64(s.options.MaxReplayEvents)
		return nil, nil, 0, &TranscriptCursorError{Err: ErrReplayLimit, Gap: &TranscriptGap{ResumeAfter: s.cursor(run, resumeSequence), EarliestSequence: earliest, LatestSequence: highWater}}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, 0, fmt.Errorf("commit transcript replay snapshot: %w", err)
	}
	var gap *TranscriptGap
	if !hasCursor && earliest > 1 {
		gap = s.gap(run, earliest, highWater)
	}
	return history, gap, highWater, nil
}

type postgresRows interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func queryTranscriptEvents(ctx context.Context, query postgresRows, run RunIdentity, after uint64, limit int, cursor func(RunIdentity, uint64) string) ([]TranscriptEvent, error) {
	if after > maxPostgresTranscriptSequence {
		return nil, ErrInvalidCursor
	}
	rows, err := query.Query(ctx, `SELECT sequence, source, source_sequence, event_type, data, created_at FROM transcript_events WHERE namespace = $1 AND run_uid = $2 AND sequence > $3 ORDER BY sequence LIMIT $4`, run.Namespace, string(run.UID), int64(after), limit)
	if err != nil {
		return nil, fmt.Errorf("query transcript events: %w", err)
	}
	defer rows.Close()
	events := make([]TranscriptEvent, 0)
	for rows.Next() {
		var event TranscriptEvent
		var sourceSequence *string
		var databaseSequence int64
		var sourceBytes, typeBytes, data []byte
		if err := rows.Scan(&databaseSequence, &sourceBytes, &sourceSequence, &typeBytes, &data, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan transcript event: %w", err)
		}
		event.Sequence = uint64(databaseSequence)
		event.Source = string(sourceBytes)
		event.Type = string(typeBytes)
		if sourceSequence != nil {
			parsed, err := strconv.ParseUint(*sourceSequence, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("decode transcript source sequence: %w", err)
			}
			event.SourceSequence = &parsed
		}
		event.Data = json.RawMessage(data)
		event.ID = cursor(run, event.Sequence)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read transcript events: %w", err)
	}
	return events, nil
}

func (s *PostgresTranscriptStore) poll(ctx context.Context, run RunIdentity, after uint64, events chan<- TranscriptEvent, dropped chan<- struct{}) {
	ticker := time.NewTicker(s.options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		batch, earliest, highWater, err := s.pollBatch(ctx, run, after)
		if err != nil {
			if ctx.Err() == nil {
				close(dropped)
			}
			return
		}
		if highWater > after && earliest > after+1 {
			close(dropped)
			return
		}
		for _, event := range batch {
			if event.Sequence != after+1 {
				close(dropped)
				return
			}
			select {
			case events <- event:
				after = event.Sequence
			default:
				close(dropped)
				return
			}
		}
	}
}

func (s *PostgresTranscriptStore) pollBatch(ctx context.Context, run RunIdentity, after uint64) ([]TranscriptEvent, uint64, uint64, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var databaseHighWater int64
	err = tx.QueryRow(ctx, `SELECT high_water FROM transcript_runs WHERE namespace = $1 AND run_uid = $2`, run.Namespace, string(run.UID)).Scan(&databaseHighWater)
	if errors.Is(err, pgx.ErrNoRows) {
		databaseHighWater = 0
	} else if err != nil {
		return nil, 0, 0, err
	}
	highWater := uint64(databaseHighWater)
	earliest := highWater + 1
	var databaseEarliest *int64
	if err := tx.QueryRow(ctx, `SELECT MIN(sequence) FROM transcript_events WHERE namespace = $1 AND run_uid = $2`, run.Namespace, string(run.UID)).Scan(&databaseEarliest); err != nil {
		return nil, 0, 0, err
	}
	if databaseEarliest != nil {
		earliest = uint64(*databaseEarliest)
	}
	batch, err := queryTranscriptEvents(ctx, tx, run, after, s.options.SubscriberBuffer, s.cursor)
	if err != nil {
		return nil, 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, 0, 0, err
	}
	return batch, earliest, highWater, nil
}

func (s *PostgresTranscriptStore) reserveSubscriber() bool {
	s.subscriberMu.Lock()
	defer s.subscriberMu.Unlock()
	if s.subscribers >= s.options.MaxSubscribers {
		return false
	}
	s.subscribers++
	return true
}

func (s *PostgresTranscriptStore) releaseSubscriber() {
	s.subscriberMu.Lock()
	s.subscribers--
	s.subscriberMu.Unlock()
}

func (s *PostgresTranscriptStore) cursor(run RunIdentity, sequence uint64) string {
	runHash := sha256.Sum256([]byte(run.Namespace + "\x00" + string(run.UID)))
	payload := "v1." + s.generation + "." + base64.RawURLEncoding.EncodeToString(runHash[:16]) + "." + strconv.FormatUint(sequence, 10)
	signature := hmac.New(sha256.New, s.cursorKey)
	_, _ = signature.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(signature.Sum(nil)[:16])
}

func (s *PostgresTranscriptStore) parseCursor(run RunIdentity, cursor string) (uint64, error) {
	parts := strings.Split(cursor, ".")
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != s.generation {
		return 0, ErrInvalidCursor
	}
	sequence, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil || !hmac.Equal([]byte(s.cursor(run, sequence)), []byte(cursor)) {
		return 0, ErrInvalidCursor
	}
	return sequence, nil
}

func (s *PostgresTranscriptStore) gap(run RunIdentity, earliest, latest uint64) *TranscriptGap {
	resume := uint64(0)
	if earliest > 0 {
		resume = earliest - 1
	}
	return &TranscriptGap{ResumeAfter: s.cursor(run, resume), EarliestSequence: earliest, LatestSequence: latest}
}

var _ TranscriptStore = (*PostgresTranscriptStore)(nil)
