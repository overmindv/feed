-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS feed_material_quarantine_open_idx
    ON feed_event_quarantine (source)
    WHERE source = 'material' AND replayed_at IS NULL
        AND reason IN ('invalid_json', 'too_large', 'invalid_material');

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_index WHERE indexrelid='feed_material_quarantine_open_idx'::regclass AND NOT indisvalid) THEN
        RAISE EXCEPTION 'Rebuild the invalid material quarantine index before retrying';
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
SELECT 1;
