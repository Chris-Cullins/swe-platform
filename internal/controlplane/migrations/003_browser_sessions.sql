CREATE TABLE browser_sessions (
    selector bytea PRIMARY KEY CHECK (octet_length(selector) = 32),
    key_id text NOT NULL CHECK (key_id <> '' AND key_id NOT LIKE '%.%'),
    nonce bytea NOT NULL CHECK (octet_length(nonce) = 12),
    ciphertext bytea NOT NULL,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at)
);

CREATE INDEX browser_sessions_expires_at_idx ON browser_sessions (expires_at);
CREATE INDEX browser_sessions_key_id_expires_at_idx ON browser_sessions (key_id, expires_at);
