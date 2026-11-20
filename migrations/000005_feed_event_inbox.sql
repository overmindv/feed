-- +goose Up
CREATE TABLE IF NOT EXISTS feed_event_inbox (
    source TEXT NOT NULL CHECK (source IN ('catalog', 'task')),
    event_id UUID NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, event_id)
);

-- +goose Down
-- Откат приложения не требует удалять историю принятых событий.
SELECT 1;
