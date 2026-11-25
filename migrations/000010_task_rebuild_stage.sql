-- +goose Up
CREATE TABLE IF NOT EXISTS feed_rebuild_task_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'),
    watermark bigint NOT NULL CHECK (watermark >= 0),
    through bigint NOT NULL DEFAULT 0 CHECK (through >= 0 AND through <= watermark)
);

CREATE TABLE IF NOT EXISTS feed_rebuild_task_changes (
    run_id text NOT NULL REFERENCES feed_rebuild_task_runs (run_id),
    revision bigint NOT NULL CHECK (revision > 0),
    event_id uuid NOT NULL,
    event_type text NOT NULL CHECK (event_type IN ('task.published', 'task.unpublished')),
    occurred_at timestamptz NOT NULL,
    task_id uuid NOT NULL,
    task_version_id uuid,
    title text,
    topic_id uuid,
    task_type text,
    difficulty text,
    PRIMARY KEY (run_id, revision),
    UNIQUE (run_id, event_id),
    CHECK (event_type <> 'task.published' OR (task_version_id IS NOT NULL AND title IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS feed_rebuild_task_changes_task_idx
    ON feed_rebuild_task_changes (run_id, task_id, revision DESC);

-- +goose Down
-- The previous Feed does not use these tables; keep staged runs for inspection.
SELECT 1;
