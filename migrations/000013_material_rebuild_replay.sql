-- +goose Up
CREATE TABLE IF NOT EXISTS feed_rebuild_material_replay (
    run_id text PRIMARY KEY REFERENCES feed_rebuild_material_runs (run_id),
    position jsonb NOT NULL CHECK (jsonb_typeof(position) = 'object'),
    target jsonb NOT NULL CHECK (jsonb_typeof(target) = 'object')
);

CREATE TABLE IF NOT EXISTS feed_rebuild_material_events (
    run_id text NOT NULL REFERENCES feed_rebuild_material_replay (run_id),
    partition integer NOT NULL CHECK (partition >= 0),
    kafka_offset bigint NOT NULL CHECK (kafka_offset >= 0),
    event_id uuid NOT NULL,
    material_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    occurred_at timestamptz NOT NULL,
    revision_id uuid,
    title text,
    PRIMARY KEY (run_id, partition, kafka_offset),
    CHECK ((revision_id IS NULL) = (title IS NULL))
);

CREATE INDEX IF NOT EXISTS feed_rebuild_material_events_version_idx
    ON feed_rebuild_material_events (run_id, material_id, version DESC);

-- +goose Down
-- Keep replayed events and their checkpoints for inspection.
SELECT 1;
