-- +goose Up
ALTER TABLE feed_event_inbox
    DROP CONSTRAINT feed_event_inbox_source_check,
    ADD CONSTRAINT feed_event_inbox_source_check CHECK (source IN ('catalog', 'task', 'material')) NOT VALID;

ALTER TABLE feed_event_quarantine
    DROP CONSTRAINT feed_event_quarantine_source_check,
    DROP CONSTRAINT feed_event_quarantine_reason_check,
    ADD CONSTRAINT feed_event_quarantine_source_check CHECK (source IN ('catalog', 'task', 'material')) NOT VALID,
    ADD CONSTRAINT feed_event_quarantine_reason_check CHECK (reason IN (
        'too_large', 'invalid_json', 'unknown_type', 'invalid_catalog',
        'invalid_task', 'invalid_material', 'wrong_source'
    )) NOT VALID;

CREATE TABLE IF NOT EXISTS feed_material_state (
    material_id UUID PRIMARY KEY,
    id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    revision_id UUID,
    title TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    CHECK ((revision_id IS NULL) = (title IS NULL))
);

CREATE INDEX IF NOT EXISTS feed_material_public_window_idx
    ON feed_material_state (occurred_at DESC, id DESC) WHERE revision_id IS NOT NULL;

-- +goose Down
-- Старый Feed не читает таблицу материалов. Версии и журнал сохраняются.
SELECT 1;
