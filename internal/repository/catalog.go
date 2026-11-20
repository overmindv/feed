package repository

import (
	"context"
	"fmt"

	"github.com/overmindv/feed/internal/domain"
)

func (r activityWriter) ApplyCatalogChange(ctx context.Context, change domain.CatalogChange) error {
	visible := change.Item != nil
	var fence int64
	if !visible || change.ScopeReset {
		fence = change.CatalogRevision
	}
	var item domain.FeedItem
	var keys []string
	if visible {
		item = *change.Item
		keys = make([]string, 1, len(change.Ancestors)+1)
		keys[0] = change.ResourceKey
		keys = append(keys, change.Ancestors...)
	}

	// Старая блокировка не меняет текущий статус, но скрывает карточки, выпущенные до нее.
	_, err := r.db.Exec(ctx, `
		WITH received AS (
			INSERT INTO feed_event_inbox (source,event_id)
			SELECT 'catalog', $15::uuid WHERE $15::uuid IS NOT NULL
			ON CONFLICT DO NOTHING
			RETURNING 1
		), accepted AS (
			INSERT INTO feed_catalog_visibility AS current
				(resource_key, catalog_revision, aggregate_version, visible, fence_revision)
			SELECT $1, $2, $3, $4, $5
			WHERE $15::uuid IS NULL OR EXISTS (SELECT 1 FROM received)
			ON CONFLICT (resource_key) DO UPDATE SET
				catalog_revision = GREATEST(current.catalog_revision, EXCLUDED.catalog_revision),
				aggregate_version = GREATEST(current.aggregate_version, EXCLUDED.aggregate_version),
				visible = CASE WHEN EXCLUDED.catalog_revision > current.catalog_revision
				               THEN EXCLUDED.visible ELSE current.visible END,
				fence_revision = GREATEST(current.fence_revision, EXCLUDED.fence_revision)
			WHERE (EXCLUDED.catalog_revision > current.catalog_revision
				AND EXCLUDED.aggregate_version > current.aggregate_version)
			   OR (EXCLUDED.catalog_revision < current.catalog_revision
				AND EXCLUDED.aggregate_version < current.aggregate_version
				AND EXCLUDED.fence_revision > current.fence_revision)
			RETURNING catalog_revision
		)
		INSERT INTO feed_items (source, event_key, kind, actor_user_id, aggregate_type, aggregate_id,
		                        occurred_at, title, text, href, raw_payload, catalog_revision, visibility_keys)
		SELECT 'catalog', $1, $6, $7, $8, $9, $10, $11, $12, $13, '{}'::jsonb, accepted.catalog_revision, $14
		FROM accepted WHERE $4 AND accepted.catalog_revision = $2
		ON CONFLICT (source, event_key, kind) DO UPDATE SET
			actor_user_id=EXCLUDED.actor_user_id, occurred_at=EXCLUDED.occurred_at,
			title=EXCLUDED.title, text=EXCLUDED.text, href=EXCLUDED.href,
			raw_payload=EXCLUDED.raw_payload, catalog_revision=EXCLUDED.catalog_revision,
			visibility_keys=EXCLUDED.visibility_keys`, change.ResourceKey, change.CatalogRevision,
		change.AggregateVersion, visible, fence, item.Kind, nullableUUID(item.ActorUserID),
		item.AggregateType, item.AggregateID, item.OccurredAt, item.Title, item.Text, item.Href, keys, nullableUUID(change.EventID))
	if err != nil {
		return fmt.Errorf("apply catalog visibility: %w", err)
	}

	return nil
}
