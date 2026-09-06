package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/Chris-Cullins/swe-platform/sandboxd/changes"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrChangesConflict = errors.New("changes observation revision changed")

type ChangesRecord struct {
	Revision       int64            `json:"revision"`
	EnvironmentUID string           `json:"environmentUID"`
	Baseline       changes.Snapshot `json:"baseline"`
	Current        changes.Snapshot `json:"current"`
	CapturedAt     time.Time        `json:"capturedAt"`
	Final          bool             `json:"final"`
	Unavailable    bool             `json:"unavailable"`
}

type ChangesStore interface {
	Load(context.Context, RunIdentity) (ChangesRecord, error)
	Save(context.Context, RunIdentity, int64, ChangesRecord) error
	Delete(context.Context, RunIdentity) error
}

// NewChangesStore shares the process-owned PostgreSQL pool when transcripts are
// durable. The development fallback has a separate explicit 128 MiB total cap.
func NewChangesStore(transcripts TranscriptStore) ChangesStore {
	if durable, ok := transcripts.(*PostgresTranscriptStore); ok {
		return &postgresChangesStore{pool: durable.pool}
	}
	return &memoryChangesStore{records: make(map[RunIdentity][]byte)}
}

func encodeChanges(r ChangesRecord) ([]byte, error) {
	if err := r.Baseline.Validate(); err != nil {
		return nil, err
	}
	if err := r.Current.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(r)
	if len(data) > 2*changes.MaxEncodedBytes {
		return nil, ErrTranscriptCapacity
	}
	return data, err
}

type memoryChangesStore struct {
	mu      sync.Mutex
	records map[RunIdentity][]byte
	bytes   int
}

func (s *memoryChangesStore) Load(_ context.Context, id RunIdentity) (ChangesRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var r ChangesRecord
	if data := s.records[id]; data != nil {
		if err := json.Unmarshal(data, &r); err != nil {
			return r, err
		}
	}
	return r, nil
}
func (s *memoryChangesStore) Save(_ context.Context, id RunIdentity, expected int64, r ChangesRecord) error {
	data, err := encodeChanges(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var old ChangesRecord
	prior := s.records[id]
	if prior != nil {
		if err := json.Unmarshal(prior, &old); err != nil {
			return err
		}
	}
	if old.Revision != expected || old.Final || r.Revision != expected+1 {
		return ErrChangesConflict
	}
	if len(s.records) >= 1024 && prior == nil || s.bytes-len(prior)+len(data) > 128<<20 {
		return ErrTranscriptCapacity
	}
	s.records[id] = data
	s.bytes += len(data) - len(prior)
	return nil
}
func (s *memoryChangesStore) Delete(_ context.Context, id RunIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bytes -= len(s.records[id])
	delete(s.records, id)
	return nil
}

type postgresChangesStore struct{ pool *pgxpool.Pool }

func (s *postgresChangesStore) Load(ctx context.Context, id RunIdentity) (ChangesRecord, error) {
	var data []byte
	var r ChangesRecord
	err := s.pool.QueryRow(ctx, `SELECT data FROM run_changes WHERE namespace=$1 AND namespace_uid=$2 AND run_uid=$3`, id.Namespace, string(id.NamespaceUID), string(id.UID)).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(data, &r)
	return r, err
}
func (s *postgresChangesStore) Save(ctx context.Context, id RunIdentity, expected int64, r ChangesRecord) error {
	if r.Revision != expected+1 {
		return ErrChangesConflict
	}
	data, err := encodeChanges(r)
	if err != nil {
		return err
	}
	var count int64
	if expected == 0 {
		tag, err := s.pool.Exec(ctx, `INSERT INTO run_changes(namespace,namespace_uid,run_uid,revision,final,data) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, id.Namespace, string(id.NamespaceUID), string(id.UID), r.Revision, r.Final, data)
		if err != nil {
			return err
		}
		count = tag.RowsAffected()
	} else {
		tag, err := s.pool.Exec(ctx, `UPDATE run_changes SET revision=$4,final=$5,data=$6 WHERE namespace=$1 AND namespace_uid=$2 AND run_uid=$3 AND revision=$7 AND NOT final`, id.Namespace, string(id.NamespaceUID), string(id.UID), r.Revision, r.Final, data, expected)
		if err != nil {
			return err
		}
		count = tag.RowsAffected()
	}
	if count != 1 {
		return ErrChangesConflict
	}
	return nil
}
func (s *postgresChangesStore) Delete(ctx context.Context, id RunIdentity) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM run_changes WHERE namespace=$1 AND namespace_uid=$2 AND run_uid=$3`, id.Namespace, string(id.NamespaceUID), string(id.UID))
	return err
}
