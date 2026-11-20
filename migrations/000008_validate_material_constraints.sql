-- +goose Up
ALTER TABLE feed_event_inbox VALIDATE CONSTRAINT feed_event_inbox_source_check;
ALTER TABLE feed_event_quarantine VALIDATE CONSTRAINT feed_event_quarantine_source_check;
ALTER TABLE feed_event_quarantine VALIDATE CONSTRAINT feed_event_quarantine_reason_check;

-- +goose Down
SELECT 1;
