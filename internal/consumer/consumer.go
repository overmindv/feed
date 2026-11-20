package consumer

import (
	"context"
	"errors"
	"log/slog"

	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
)

type activityStore interface {
	repository.ActivityWriter
	SaveRejection(context.Context, domain.EventRejection) (bool, error)
}

type Handler func(context.Context, domain.EventPosition, string, []byte) error

func NewHandler(store activityStore, log *slog.Logger, catalogTopic, taskTopic string) Handler {
	return func(ctx context.Context, position domain.EventPosition, key string, value []byte) error {
		var source string
		switch position.Topic {
		case catalogTopic:
			source = "catalog"
		case taskTopic:
			source = sourceTask
		default:
			return errors.New("unexpected kafka topic")
		}

		return handleActivity(ctx, store, log, position, source, key, value)
	}
}

func handleActivity(
	ctx context.Context, store activityStore, log *slog.Logger,
	position domain.EventPosition, source, key string, value []byte,
) error {
	message, err := decodeForSource(source, key, value)
	if err == nil {
		return message.apply(ctx, store)
	}
	var reason domain.RejectionReason
	if !errors.As(err, &reason) {
		return err
	}
	fresh, err := store.SaveRejection(ctx, domain.EventRejection{
		Position: position, Source: source, Reason: reason, PayloadSize: len(value),
		Identity: rejectionIdentity(source, key, value),
	})
	if err != nil {
		return err
	}
	if fresh {
		log.Warn("feed event quarantined", "topic", position.Topic, "partition", position.Partition,
			"offset", position.Offset, "reason", reason, "payload_size", len(value))
	}

	return nil
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

	return nil
}
