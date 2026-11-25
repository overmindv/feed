package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/feed/internal/rebuild"
)

var ErrCatalogRebuildAdvanced = errors.New("entities rebuild checkpoint changed; resume with the same run")

func (r *Postgres) LoadCatalogCheckpoint(ctx context.Context, id string) (rebuild.Checkpoint, bool, error) {
	var checkpoint rebuild.Checkpoint
	err := r.pool.QueryRow(ctx,
		"SELECT watermark, through FROM feed_rebuild_catalog_runs WHERE run_id = $1", id,
	).Scan(&checkpoint.Watermark, &checkpoint.Through)
	if errors.Is(err, pgx.ErrNoRows) {
		return checkpoint, false, nil
	}
	if err != nil {
		return checkpoint, false, fmt.Errorf("load Entities rebuild checkpoint: %w", err)
	}

	return checkpoint, true, nil
}

func (r *Postgres) SaveCatalogPage(ctx context.Context, id string, after int64, page rebuild.CatalogPage) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO feed_rebuild_catalog_runs (run_id, watermark) VALUES ($1, $2)
			ON CONFLICT (run_id) DO NOTHING`, id, page.Watermark); err != nil {
			return fmt.Errorf("create Entities rebuild checkpoint: %w", err)
		}
		var checkpoint rebuild.Checkpoint
		if err := tx.QueryRow(ctx, `
			SELECT watermark, through FROM feed_rebuild_catalog_runs WHERE run_id = $1 FOR UPDATE`, id,
		).Scan(&checkpoint.Watermark, &checkpoint.Through); err != nil {
			return fmt.Errorf("lock Entities rebuild checkpoint: %w", err)
		}
		if checkpoint.Watermark != page.Watermark || checkpoint.Through != after {
			return ErrCatalogRebuildAdvanced
		}
		if len(page.Changes) != 0 {
			payload, err := json.Marshal(page.Changes)
			if err != nil {
				return fmt.Errorf("encode Entities rebuild page: %w", err)
			}
			if _, writeErr := tx.Exec(ctx, stageCatalogChanges, id, payload); writeErr != nil {
				return fmt.Errorf("write Entities rebuild page: %w", writeErr)
			}
		}
		if _, err := tx.Exec(ctx, "UPDATE feed_rebuild_catalog_runs SET through = $2 WHERE run_id = $1",
			id, page.Through); err != nil {
			return fmt.Errorf("advance Entities rebuild checkpoint: %w", err)
		}

		return nil
	})
}

const stageCatalogChanges = `
	INSERT INTO feed_rebuild_catalog_changes
		(run_id, revision, event_id, event_type, occurred_at, aggregate_type, aggregate_id,
		 aggregate_version, actor_user_id, title, ancestors, scope_reset)
	SELECT $1, revision, event_id, event_type, occurred_at, aggregate_type, aggregate_id,
	       aggregate_version, actor_user_id, title, COALESCE(ancestors, ARRAY[]::text[]), scope_reset
	FROM jsonb_to_recordset($2::jsonb) AS entry (
		revision bigint, event_id uuid, event_type text, occurred_at timestamptz,
		aggregate_type text, aggregate_id uuid, aggregate_version bigint,
		actor_user_id uuid, title text, ancestors text[], scope_reset boolean
	)`
