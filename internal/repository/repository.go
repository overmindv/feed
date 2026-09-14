package repository

import (
	"context"

	"github.com/overmindv/feed/internal/domain"
)

// Repository — storage contract ленты.
type Repository interface {
	// Ping проверяет доступность хранилища (для /ready).
	Ping(ctx context.Context) error
	// InsertItem вставляет запись ленты; возвращает false, если такая запись
	// уже существует (идемпотентность при at-least-once-доставке).
	InsertItem(ctx context.Context, item domain.FeedItem) (bool, error)
	// ListItems возвращает записи ленты по убыванию времени события.
	ListItems(ctx context.Context, limit, offset int) ([]domain.FeedItem, error)
}
