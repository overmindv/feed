-- +goose Up
CREATE TABLE IF NOT EXISTS feed_rebuild_material_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    kafka_position jsonb NOT NULL CHECK (jsonb_typeof(kafka_position) = 'object'),
    watermark bigint CHECK (watermark >= 0),
    after_id uuid,
    complete boolean NOT NULL DEFAULT false,
    unresolved bigint NOT NULL DEFAULT 0 CHECK (unresolved >= 0),
    CHECK (watermark IS NOT NULL OR (after_id IS NULL AND NOT complete AND unresolved = 0))
);

CREATE TABLE IF NOT EXISTS feed_rebuild_material_rows (
    run_id text NOT NULL REFERENCES feed_rebuild_material_runs (run_id),
    material_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    tombstone boolean NOT NULL,
    title text CHECK (char_length(title) BETWEEN 1 AND 256),
    occurred_at timestamptz,
    PRIMARY KEY (run_id, material_id),
    CHECK (tombstone = (title IS NULL))
);

-- +goose Down
-- Keep the staged snapshot and its Kafka position available for inspection.
SELECT 1;
