-- +goose Up
-- Блок Б аудита ленты:
-- 1) События задач (task.published) не несут доверенного actor — у них actor_user_id
--    пустой, что ломало вставку в NOT NULL колонку. Actor становится nullable.
-- 2) Идемпотентность по (source, event_key) теряла разные события одного агрегата
--    (например catalog.course.created и последующий course.activated). Ключ
--    дополняется kind: redelivery того же события дедуплицируется, а разные типы
--    событий одного агрегата сохраняются.

ALTER TABLE feed_items ALTER COLUMN actor_user_id DROP NOT NULL;

ALTER TABLE feed_items DROP CONSTRAINT feed_items_source_key;
ALTER TABLE feed_items ADD CONSTRAINT feed_items_source_event_key UNIQUE (source, event_key, kind);

-- +goose Down
ALTER TABLE feed_items DROP CONSTRAINT feed_items_source_event_key;
ALTER TABLE feed_items ADD CONSTRAINT feed_items_source_key UNIQUE (source, event_key);
ALTER TABLE feed_items ALTER COLUMN actor_user_id SET NOT NULL;
