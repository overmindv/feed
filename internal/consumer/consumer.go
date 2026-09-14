package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/parker"
)

// NewHandler создаёт parker.Handler потребления событий активности.
// Каждое сообщение парсится, фильтруется и отображается в запись ленты,
// которая идемпотентно сохраняется (at-most-once при at-least-once доставке).
func NewHandler(store repository.Repository, log *slog.Logger) parker.Handler {
	return func(ctx context.Context, key string, value []byte) error {
		item, ok, err := Render(key, value)
		if err != nil {
			// Битый конверт (ваlied-формат) — не ретраим вечно; логируем и пропускаем.
			log.Error("feed: parse event", "key", key, "error", err)
			return nil
		}
		if !ok {
			// Отфильтрованное событие (deleted, updated-черновик и т.п.) — ничего не делаем.
			return nil
		}
		if _, err := store.InsertItem(ctx, item); err != nil {
			return fmt.Errorf("feed: insert item: %w", err)
		}
		return nil
	}
}
