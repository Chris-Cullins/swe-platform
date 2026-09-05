CREATE TABLE run_changes (
    namespace text NOT NULL,
    namespace_uid text NOT NULL,
    run_uid text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    final boolean NOT NULL,
    data bytea NOT NULL CHECK (octet_length(data) <= 50331648),
    PRIMARY KEY(namespace, namespace_uid, run_uid)
);
