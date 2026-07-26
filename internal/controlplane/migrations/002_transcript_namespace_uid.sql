ALTER TABLE transcript_runs
    ADD COLUMN namespace_uid text;

CREATE INDEX transcript_runs_namespace_identity_idx
    ON transcript_runs (namespace, namespace_uid, run_uid);
