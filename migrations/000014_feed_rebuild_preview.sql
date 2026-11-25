-- +goose Up
CREATE TABLE IF NOT EXISTS feed_rebuild_preview_runs (
    run_id text PRIMARY KEY REFERENCES feed_rebuild_catalog_runs (run_id),
    catalog_watermark bigint NOT NULL CHECK (catalog_watermark >= 0),
    material_watermark bigint NOT NULL CHECK (material_watermark >= 0),
    FOREIGN KEY (run_id) REFERENCES feed_rebuild_material_replay (run_id)
);

CREATE TABLE IF NOT EXISTS feed_rebuild_preview_catalog_state (
    run_id text NOT NULL REFERENCES feed_rebuild_preview_runs (run_id),
    resource_key text NOT NULL,
    catalog_revision bigint NOT NULL CHECK (catalog_revision > 0),
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    visible boolean NOT NULL,
    fence_revision bigint NOT NULL CHECK (fence_revision BETWEEN 0 AND catalog_revision),
    PRIMARY KEY (run_id, resource_key)
);

CREATE TABLE IF NOT EXISTS feed_rebuild_preview_catalog_cards (
    run_id text NOT NULL REFERENCES feed_rebuild_preview_runs (run_id),
    event_key text NOT NULL,
    kind text NOT NULL,
    id uuid NOT NULL,
    actor_user_id uuid,
    occurred_at timestamptz NOT NULL,
    title text NOT NULL,
    text text NOT NULL,
    href text NOT NULL,
    catalog_revision bigint NOT NULL CHECK (catalog_revision > 0),
    visibility_keys text[] NOT NULL,
    PRIMARY KEY (run_id, event_key, kind),
    UNIQUE (run_id, id)
);

CREATE INDEX IF NOT EXISTS feed_rebuild_preview_fences_idx
    ON feed_rebuild_preview_catalog_state (run_id, resource_key)
    INCLUDE (fence_revision) WHERE fence_revision > 0;

CREATE TABLE IF NOT EXISTS feed_rebuild_preview_material_state (
    run_id text NOT NULL REFERENCES feed_rebuild_preview_runs (run_id),
    material_id uuid NOT NULL,
    id uuid NOT NULL,
    version bigint NOT NULL CHECK (version >= 0),
    revision_id uuid,
    title text,
    occurred_at timestamptz,
    PRIMARY KEY (run_id, material_id),
    UNIQUE (run_id, id),
    CHECK ((revision_id IS NULL) = (title IS NULL))
);

-- +goose Down
-- Keep rebuilt projections for comparison.
SELECT 1;
