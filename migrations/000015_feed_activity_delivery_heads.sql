-- +goose Up
CREATE TABLE IF NOT EXISTS feed_activity_delivery_heads (
    consumer_group text NOT NULL CHECK (consumer_group <> ''),
    topic text NOT NULL CHECK (topic <> ''),
    partition integer NOT NULL CHECK (partition >= 0),
    first_offset bigint NOT NULL CHECK (first_offset >= 0),
    highest_offset bigint NOT NULL CHECK (highest_offset >= first_offset),
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    continuous boolean NOT NULL DEFAULT true,
    PRIMARY KEY (consumer_group, topic, partition)
);

-- +goose Down
-- Delivery observations remain available after rollback.
SELECT 1;
