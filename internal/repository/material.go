package repository

import (
	"context"

	"github.com/overmindv/feed/internal/domain"
)

func (r activityWriter) ApplyMaterialChange(ctx context.Context, change domain.MaterialChange) error {
	_, err := materialChangeStatement(change).exec(ctx, r.db)

	return err
}

func materialChangeStatement(change domain.MaterialChange) activityStatement {
	return activityStatement{name: "apply material change", query: `
		WITH received AS (
			INSERT INTO feed_event_inbox (source, event_id)
			VALUES ('material', $2::uuid)
			ON CONFLICT DO NOTHING
			RETURNING 1
		)
		INSERT INTO feed_material_state AS current (material_id, aggregate_version, revision_id, title, occurred_at)
		SELECT $1::uuid, $3::bigint, $4::uuid, NULLIF($5::text, ''), $6::timestamptz FROM received
		ON CONFLICT (material_id) DO UPDATE SET
			aggregate_version = EXCLUDED.aggregate_version,
			revision_id = EXCLUDED.revision_id,
			title = EXCLUDED.title,
			occurred_at = EXCLUDED.occurred_at
		WHERE current.aggregate_version < EXCLUDED.aggregate_version
			OR (current.aggregate_version = EXCLUDED.aggregate_version
				AND current.revision_id IS NOT NULL AND EXCLUDED.revision_id IS NULL)`, args: []any{
		change.MaterialID, change.EventID, change.AggregateVersion, nullableUUID(change.RevisionID), change.Title, change.OccurredAt,
	},
	}
}

const materialQuarantineHold = `EXISTS (
		SELECT 1 FROM feed_event_quarantine
		WHERE source = 'material' AND replayed_at IS NULL
			AND reason IN ('invalid_json', 'too_large', 'invalid_material')
	)`

const publicMaterialFilter = `revision_id IS NOT NULL AND NOT ` + materialQuarantineHold

const materialFeedRows = `
	SELECT id, 'material', material_id::text, 'material.published', '',
		'material', material_id::text, occurred_at, title, '',
		'/materials/' || material_id::text, '{}'::jsonb
	FROM feed_material_state WHERE ` + publicMaterialFilter
