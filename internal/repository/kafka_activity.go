package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/feed/internal/domain"
)

func (r *Postgres) RecordKafkaActivity(ctx context.Context, delivery domain.KafkaActivity) (bool, error) {
	p := delivery.Position
	if delivery.Group == "" || p.Topic == "" || p.Partition < 0 || p.Offset < 0 || delivery.Fingerprint == ([32]byte{}) {
		return false, errors.New("invalid Kafka activity position")
	}
	write, err := kafkaActivityStatement(delivery)
	if err != nil {
		return false, err
	}
	var batch pgx.Batch
	if write.query != "" {
		batch.Queue(write.query, write.args...)
	}
	batch.Queue(recordActivityHead, delivery.Group, p.Topic, p.Partition, p.Offset, delivery.Fingerprint[:])

	// SendBatch executes both writes in one implicit transaction.
	results := r.pool.SendBatch(ctx, &batch)
	defer func() { _ = results.Close() }()
	var quarantined bool
	if write.query != "" {
		tag, writeErr := results.Exec()
		if writeErr != nil {
			return false, fmt.Errorf("%s: %w", write.name, writeErr)
		}
		quarantined = delivery.Rejection != nil && tag.RowsAffected() != 0
	}
	if _, progressErr := results.Exec(); progressErr != nil {
		return false, fmt.Errorf("record Kafka activity position: %w", progressErr)
	}
	if closeErr := results.Close(); closeErr != nil {
		return false, fmt.Errorf("commit Kafka activity: %w", closeErr)
	}

	return quarantined, nil
}

func kafkaActivityStatement(delivery domain.KafkaActivity) (activityStatement, error) {
	var write activityStatement
	var count int
	if delivery.Catalog != nil {
		write = catalogChangeStatement(*delivery.Catalog)
		count++
	}
	if delivery.Task != nil {
		write = taskPublicationStatement(*delivery.Task)
		count++
	}
	if delivery.Material != nil {
		write = materialChangeStatement(*delivery.Material)
		count++
	}
	if delivery.Rejection != nil {
		if delivery.Rejection.Position != delivery.Position {
			return activityStatement{}, errors.New("rejection and Kafka activity positions differ")
		}
		write = rejectionStatement(*delivery.Rejection)
		count++
	}
	if count > 1 {
		return activityStatement{}, errors.New("expected at most one change per Kafka activity")
	}

	return write, nil
}

const recordActivityHead = `
	INSERT INTO feed_activity_delivery_heads AS observed
		(consumer_group, topic, partition, first_offset, highest_offset, fingerprint)
	VALUES ($1, $2, $3, $4, $4, $5)
	ON CONFLICT (consumer_group, topic, partition) DO UPDATE SET
		continuous = observed.continuous AND EXCLUDED.highest_offset - observed.highest_offset = 1,
		highest_offset = GREATEST(observed.highest_offset, EXCLUDED.highest_offset),
		fingerprint = CASE WHEN EXCLUDED.highest_offset > observed.highest_offset
		                   THEN EXCLUDED.fingerprint ELSE observed.fingerprint END
	WHERE EXCLUDED.highest_offset <> observed.highest_offset OR EXCLUDED.fingerprint <> observed.fingerprint`
