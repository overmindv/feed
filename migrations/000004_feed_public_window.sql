-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS feed_items_public_window_idx
    ON feed_items (occurred_at DESC, id DESC)
    WHERE source <> 'catalog' OR catalog_revision IS NOT NULL;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_index WHERE indexrelid='feed_items_public_window_idx'::regclass AND NOT indisvalid) THEN
        RAISE EXCEPTION 'Rebuild the invalid feed index before retrying';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS feed_items_public_window_idx;
