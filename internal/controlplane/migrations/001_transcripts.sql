CREATE TABLE transcript_store_metadata (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    generation text NOT NULL,
    cursor_key bytea NOT NULL CHECK (octet_length(cursor_key) = 32)
);

CREATE TABLE transcript_runs (
    namespace text NOT NULL,
    run_uid text NOT NULL,
    high_water bigint NOT NULL DEFAULT 0 CHECK (high_water >= 0),
    retained_events integer NOT NULL DEFAULT 0 CHECK (retained_events >= 0),
    retained_bytes bigint NOT NULL DEFAULT 0 CHECK (retained_bytes >= 0),
    PRIMARY KEY (namespace, run_uid)
);

CREATE TABLE transcript_events (
    namespace text NOT NULL,
    run_uid text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    source bytea NOT NULL,
    source_sequence text,
    idempotency_key bytea NOT NULL,
    event_type bytea NOT NULL,
    data bytea NOT NULL,
    created_at timestamptz NOT NULL,
    event_size integer NOT NULL CHECK (event_size > 0),
    PRIMARY KEY (namespace, run_uid, sequence),
    UNIQUE (namespace, run_uid, source, idempotency_key),
    FOREIGN KEY (namespace, run_uid) REFERENCES transcript_runs(namespace, run_uid) ON DELETE CASCADE
);

CREATE INDEX transcript_events_replay_idx
    ON transcript_events (namespace, run_uid, sequence);
