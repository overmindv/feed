package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/parker"
)

func NewHandler(store repository.Repository, log *slog.Logger) parker.Handler {
	return func(ctx context.Context, key string, value []byte) error {
		message, err := decodeEvent(key, value)
		if err != nil {
			log.Error("feed: parse event", "error", err)
			return nil
		}
		if message.catalog != nil {
			return store.ApplyCatalogChange(ctx, *message.catalog)
		}
		if message.task != nil {
			if _, err := store.InsertItem(ctx, *message.task); err != nil {
				return fmt.Errorf("feed: insert item: %w", err)
			}
		}

		return nil
	}
}
