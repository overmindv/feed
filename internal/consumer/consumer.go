package consumer

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"

	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
)

type activityStore interface {
	RecordKafkaActivity(context.Context, domain.KafkaActivity) (bool, error)
}

type Handler func(context.Context, domain.EventPosition, string, []byte) error

func NewHandler(store activityStore, log *slog.Logger, catalogTopic, taskTopic, materialTopic, group string) Handler {
	return func(ctx context.Context, position domain.EventPosition, key string, value []byte) error {
		var source string
		switch position.Topic {
		case catalogTopic:
			source = "catalog"
		case taskTopic:
			source = sourceTask
		case materialTopic:
			if materialTopic == "" {
				return errors.New("material activity is disabled")
			}
			source = sourceMaterial
		default:
			return errors.New("unexpected kafka topic")
		}

		return handleActivity(ctx, store, log, position, source, key, value, group)
	}
}

func handleActivity(
	ctx context.Context, store activityStore, log *slog.Logger,
	position domain.EventPosition, source, key string, value []byte, group string,
) error {
	message, err := decodeForSource(source, key, value)
	delivery := domain.KafkaActivity{Group: group, Position: position, Fingerprint: activityFingerprint(key, value)}
	if err == nil {
		delivery.Catalog, delivery.Task, delivery.Material = message.catalog, message.task, message.material
		_, err = store.RecordKafkaActivity(ctx, delivery)

		return err
	}
	var reason domain.RejectionReason
	if !errors.As(err, &reason) {
		return err
	}
	delivery.Rejection = &domain.EventRejection{
		Position: position, Source: source, Reason: reason, PayloadSize: len(value),
		Identity: rejectionIdentity(source, key, value),
	}
	fresh, err := store.RecordKafkaActivity(ctx, delivery)
	if err != nil {
		return err
	}
	if fresh {
		log.Warn("feed event quarantined", "topic", position.Topic, "partition", position.Partition,
			"offset", position.Offset, "reason", reason, "payload_size", len(value))
	}

	return nil
}

func activityFingerprint(key string, value []byte) [sha256.Size]byte {
	keyHash := sha256.Sum256([]byte(key))
	valueHash := sha256.Sum256(value)
	var hashes [2 * sha256.Size]byte
	copy(hashes[:sha256.Size], keyHash[:])
	copy(hashes[sha256.Size:], valueHash[:])

	return sha256.Sum256(hashes[:])
}

func decodeForSource(source, key string, value []byte) (event, error) {
	message, err := decodeEvent(key, value)
	if message.source != "" && message.source != source {
		return event{}, domain.RejectWrongSource
	}

	return message, err
}

func (e event) apply(ctx context.Context, writer repository.ActivityWriter) error {
	if e.catalog != nil {
		return writer.ApplyCatalogChange(ctx, *e.catalog)
	}
	if e.task != nil {
		return writer.ApplyTaskPublication(ctx, *e.task)
	}
	if e.material != nil {
		return writer.ApplyMaterialChange(ctx, *e.material)
	}

	return nil
}
