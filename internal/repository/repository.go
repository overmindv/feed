package repository

import (
	"context"

	"github.com/overmindv/feed/internal/domain"
)

// Repository — storage contract ленты.
type Repository interface {
	// Ping проверяет доступность хранилища (для /ready).
	Ping(ctx context.Context) error
	ActivityWriter
	// ListItems возвращает записи ленты по убыванию времени события.
	ListItems(ctx context.Context, limit, offset int) ([]domain.FeedItem, error)
}

type ActivityWriter interface {
	ApplyTaskPublication(ctx context.Context, publication domain.TaskPublication) error
	ApplyCatalogChange(ctx context.Context, change domain.CatalogChange) error
}
