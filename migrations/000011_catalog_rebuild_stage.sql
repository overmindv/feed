-- +goose Up
CREATE TABLE IF NOT EXISTS feed_rebuild_catalog_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    watermark bigint NOT NULL CHECK (watermark >= 0),
    through bigint NOT NULL DEFAULT 0 CHECK (through >= 0 AND through <= watermark)
);

CREATE TABLE IF NOT EXISTS feed_rebuild_catalog_changes (
    run_id text NOT NULL REFERENCES feed_rebuild_catalog_runs (run_id),
    revision bigint NOT NULL CHECK (revision > 0),
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    aggregate_type text NOT NULL CHECK (aggregate_type IN ('university', 'program', 'course', 'topic')),
    aggregate_id uuid NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    actor_user_id uuid,
    title text CHECK (char_length(title) BETWEEN 1 AND 256),
    ancestors text[] NOT NULL CHECK (cardinality(ancestors) <= 127),
    scope_reset boolean NOT NULL,
    PRIMARY KEY (run_id, revision),
    UNIQUE (run_id, event_id),
    CHECK (
        (event_type IN ('catalog.' || aggregate_type || '.created', 'catalog.' || aggregate_type || '.updated')
            AND title IS NOT NULL)
        OR
        (event_type IN ('catalog.' || aggregate_type || '.deleted', 'catalog.' || aggregate_type || '.unpublished')
            AND title IS NULL AND actor_user_id IS NULL AND cardinality(ancestors) = 0 AND NOT scope_reset)
    )
);

CREATE INDEX IF NOT EXISTS feed_rebuild_catalog_changes_aggregate_idx
    ON feed_rebuild_catalog_changes (run_id, aggregate_type, aggregate_id, revision DESC);

-- +goose Down
-- Keep inactive staging data available for a resumed import.
SELECT 1;
