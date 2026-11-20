-- +goose Up
CREATE TABLE feed_catalog_visibility (
    resource_key TEXT PRIMARY KEY,
    catalog_revision BIGINT NOT NULL CHECK (catalog_revision > 0),
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    visible BOOLEAN NOT NULL,
    fence_revision BIGINT NOT NULL CHECK (fence_revision BETWEEN 0 AND catalog_revision)
);
CREATE INDEX feed_catalog_fences_idx ON feed_catalog_visibility (resource_key)
    INCLUDE (fence_revision) WHERE fence_revision > 0;

ALTER TABLE feed_items
    ADD COLUMN catalog_revision BIGINT,
    ADD COLUMN visibility_keys TEXT[],
    ADD CONSTRAINT feed_items_catalog_visibility_check CHECK (
        (catalog_revision IS NULL AND visibility_keys IS NULL)
        OR (source = 'catalog' AND catalog_revision IS NOT NULL AND catalog_revision > 0
            AND visibility_keys IS NOT NULL AND cardinality(visibility_keys) BETWEEN 1 AND 128)
    ) NOT VALID;

-- +goose Down
ALTER TABLE feed_items
    DROP CONSTRAINT feed_items_catalog_visibility_check,
    DROP COLUMN visibility_keys,
    DROP COLUMN catalog_revision;
DROP TABLE feed_catalog_visibility;
