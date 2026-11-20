-- +goose Up
CREATE TABLE IF NOT EXISTS feed_event_quarantine (
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL CHECK (partition >= 0),
    record_offset BIGINT NOT NULL CHECK (record_offset >= 0),
    source TEXT NOT NULL CHECK (source IN ('catalog', 'task')),
    reason TEXT NOT NULL CHECK (reason IN (
        'too_large', 'invalid_json', 'unknown_type',
        'invalid_catalog', 'invalid_task', 'wrong_source'
    )),
    payload_size INTEGER NOT NULL CHECK (payload_size >= 0),
    identity_kind TEXT,
    identity_hash BYTEA,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ,
    replayed_by TEXT,
    CHECK ((replayed_at IS NULL) = (replayed_by IS NULL)),
    CHECK ((identity_kind IS NULL AND identity_hash IS NULL) OR (
        identity_kind IS NOT NULL AND identity_kind IN ('event', 'entity')
        AND identity_hash IS NOT NULL AND octet_length(identity_hash) = 32
    )),
    PRIMARY KEY (topic, partition, record_offset)
);

CREATE INDEX IF NOT EXISTS feed_event_quarantine_pending
    ON feed_event_quarantine (rejected_at) WHERE replayed_at IS NULL;

-- +goose Down
-- История отказов остается доступной после отката приложения.
SELECT 1;
