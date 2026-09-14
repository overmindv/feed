-- +goose Up
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- feed_items — нормализованная лента событий. Каждая строка = одно событие
-- жизни каталога или задач, отрендеренное в заголовок/ссылку на этапе consume
-- (raw_payload сохраняется как источник истины для отладки/перерендера).
CREATE TABLE feed_items (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source         TEXT NOT NULL,                 -- 'catalog' | 'task'
    event_key      TEXT NOT NULL,                 -- idempotency: 'catalog'→aggregate_id, 'task'→task_id
    kind           TEXT NOT NULL,                 -- 'university.created'|'course.activated'|'task.published'...
    actor_user_id  UUID NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    title          TEXT NOT NULL,
    text           TEXT NOT NULL DEFAULT '',
    href           TEXT NOT NULL DEFAULT '',
    raw_payload    JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Уникальный ключ по источнику+событию делает приём идемпотентным:
    -- при at-least-once-доставке Kafka повторная строка не создаётся.
    CONSTRAINT feed_items_source_key UNIQUE (source, event_key)
);
CREATE INDEX feed_items_occurred_idx ON feed_items (occurred_at DESC, id DESC);

-- +goose Down
DROP TABLE IF EXISTS feed_items;
