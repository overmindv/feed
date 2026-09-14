package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/domain"
)

// Postgres — потоковая реализация Repository на pgx/v5.
type Postgres struct {
	pool *pgxpool.Pool
}

// New создаёт Postgres-хранилище ленты на переданном пуле соединений.
func New(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

// Ping проверяет доступность хранилища.
func (r *Postgres) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// InsertItem вставляет запись ленты; false если такая (source, event_key) уже
// существует — на конфликте ничего не делаем и не считаем это ошибкой.
func (r *Postgres) InsertItem(ctx context.Context, item domain.FeedItem) (bool, error) {
	payload, err := json.Marshal(item.RawPayload)
	if err != nil {
		return false, fmt.Errorf("сериализовать raw payload: %w", err)
	}
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO feed_items
			(source, event_key, kind, actor_user_id, aggregate_type, aggregate_id,
			 occurred_at, title, text, href, raw_payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (source, event_key) DO NOTHING`,
		item.Source, item.EventKey, item.Kind, item.ActorUserID,
		item.AggregateType, item.AggregateID, item.OccurredAt,
		item.Title, item.Text, item.Href, payload)
	if err != nil {
		return false, fmt.Errorf("insert feed item: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

// ListItems возвращает записи ленты по убыванию времени события.
func (r *Postgres) ListItems(ctx context.Context, limit, offset int) ([]domain.FeedItem, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id::text, source, event_key, kind, actor_user_id::text,
		       aggregate_type, aggregate_id, occurred_at, title, text, href, raw_payload
		FROM feed_items
		ORDER BY occurred_at DESC, id DESC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list feed items: %w", err)
	}
	defer rows.Close()

	items := make([]domain.FeedItem, 0)
	for rows.Next() {
		var item domain.FeedItem
		if err := rows.Scan(&item.ID, &item.Source, &item.EventKey, &item.Kind,
			&item.ActorUserID, &item.AggregateType, &item.AggregateID, &item.OccurredAt,
			&item.Title, &item.Text, &item.Href, &item.RawPayload); err != nil {
			return nil, fmt.Errorf("scan feed item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return nil, fmt.Errorf("iterate feed items: %w", err)
	}

	return items, nil
}
