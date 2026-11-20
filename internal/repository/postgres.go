package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/domain"
)

// Postgres — потоковая реализация Repository на pgx/v5.
type Postgres struct {
	pool *pgxpool.Pool
	activityWriter
}

type sqlExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type activityWriter struct{ db sqlExecutor }

// New создаёт Postgres-хранилище ленты на переданном пуле соединений.
func New(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool, activityWriter: activityWriter{db: pool}}
}

// Ping проверяет доступность хранилища.
func (r *Postgres) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r activityWriter) ApplyTaskPublication(ctx context.Context, publication domain.TaskPublication) error {
	item := publication.Item
	eventKey := item.AggregateID
	if publication.EventID != "" {
		eventKey = "event:" + publication.EventID
	}
	_, err := r.db.Exec(ctx, `
		WITH received AS (
			INSERT INTO feed_event_inbox (source,event_id)
			SELECT 'task', $1::uuid WHERE $1::uuid IS NOT NULL
			ON CONFLICT DO NOTHING
			RETURNING 1
		)
		INSERT INTO feed_items
			(source, event_key, kind, actor_user_id, aggregate_type, aggregate_id,
			 occurred_at, title, text, href, raw_payload)
		SELECT 'task', $2, 'task.published', NULL, 'task', $3, $4, $5, '', $6, $7::jsonb
		WHERE $1::uuid IS NULL OR EXISTS (SELECT 1 FROM received)
		ON CONFLICT (source, event_key, kind) DO NOTHING`,
		nullableUUID(publication.EventID), eventKey, item.AggregateID, item.OccurredAt,
		item.Title, item.Href, item.RawPayload)
	if err != nil {
		return fmt.Errorf("apply task publication: %w", err)
	}

	return nil
}

// nullableUUID сохраняет отсутствие необязательного UUID.
func nullableUUID(value string) any {
	if value == "" {
		return nil
	}

	return value
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
		SELECT id::text, source, event_key, kind, COALESCE(actor_user_id::text, ''),
		       aggregate_type, aggregate_id, occurred_at, title, text, href, raw_payload
		FROM feed_items f
		WHERE (f.source <> 'catalog' OR f.catalog_revision IS NOT NULL)
		  AND NOT EXISTS (
		    SELECT 1 FROM feed_catalog_visibility AS visibility
		    WHERE visibility.resource_key = ANY(f.visibility_keys)
		      AND visibility.fence_revision > 0
		      AND visibility.fence_revision > f.catalog_revision
		  )
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
