package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/feed/internal/rebuild"
)

var ErrMaterialRebuildAdvanced = errors.New("content rebuild checkpoint changed; resume with the same run")

func (r *Postgres) LoadMaterialCheckpoint(ctx context.Context, id string) (rebuild.MaterialCheckpoint, bool, error) {
	var checkpoint rebuild.MaterialCheckpoint
	err := r.pool.QueryRow(ctx, `
		SELECT watermark, COALESCE(after_id::text, ''), complete, unresolved
		FROM feed_rebuild_material_runs WHERE run_id = $1`, id,
	).Scan(&checkpoint.Watermark, &checkpoint.AfterID, &checkpoint.Complete, &checkpoint.Unresolved)
	if errors.Is(err, pgx.ErrNoRows) {
		return checkpoint, false, nil
	}
	if err != nil {
		return checkpoint, false, fmt.Errorf("load Content rebuild checkpoint: %w", err)
	}

	return checkpoint, true, nil
}

func (r *Postgres) StartMaterialCheckpoint(ctx context.Context, id string, position rebuild.MaterialPosition) (rebuild.MaterialCheckpoint, error) {
	payload, err := json.Marshal(position)
	if err != nil {
		return rebuild.MaterialCheckpoint{}, fmt.Errorf("encode material Kafka position: %w", err)
	}
	if _, writeErr := r.pool.Exec(ctx, `
		INSERT INTO feed_rebuild_material_runs (run_id, kafka_position) VALUES ($1, $2::jsonb)
		ON CONFLICT (run_id) DO NOTHING`, id, payload); writeErr != nil {
		return rebuild.MaterialCheckpoint{}, fmt.Errorf("save material Kafka position: %w", writeErr)
	}
	checkpoint, found, err := r.LoadMaterialCheckpoint(ctx, id)
	if err == nil && !found {
		err = ErrMaterialRebuildAdvanced
	}

	return checkpoint, err
}

func (r *Postgres) SaveMaterialPage(ctx context.Context, id string, before rebuild.MaterialCheckpoint, page rebuild.MaterialPage) (rebuild.MaterialCheckpoint, error) {
	next := rebuild.MaterialCheckpoint{Watermark: &page.Watermark, AfterID: before.AfterID, Complete: page.NextAfterID == ""}
	var missing int64
	for i := range page.Rows {
		if !page.Rows[i].Tombstone && page.Rows[i].OccurredAt == nil {
			missing++
		}
		next.AfterID = page.Rows[i].MaterialID
	}
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE feed_rebuild_material_runs
			SET watermark = $4, after_id = NULLIF($5, '')::uuid, complete = $6, unresolved = unresolved + $7
			WHERE run_id = $1 AND watermark IS NOT DISTINCT FROM $2::bigint
				AND after_id IS NOT DISTINCT FROM NULLIF($3, '')::uuid AND NOT complete
			RETURNING unresolved`, id, before.Watermark, before.AfterID, page.Watermark, next.AfterID, next.Complete, missing,
		).Scan(&next.Unresolved)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrMaterialRebuildAdvanced
		}
		if err != nil {
			return fmt.Errorf("advance Content rebuild checkpoint: %w", err)
		}
		if len(page.Rows) == 0 {
			return nil
		}
		payload, err := json.Marshal(page.Rows)
		if err != nil {
			return fmt.Errorf("encode Content snapshot page: %w", err)
		}
		if _, writeErr := tx.Exec(ctx, stageMaterialRows, id, payload); writeErr != nil {
			return fmt.Errorf("write Content snapshot page: %w", writeErr)
		}

		return nil
	})
	if err != nil {
		return before, err
	}

	return next, nil
}

const stageMaterialRows = `
	INSERT INTO feed_rebuild_material_rows (run_id, material_id, revision_id, version, tombstone, title, occurred_at)
	SELECT $1, material_id, revision_id, version, tombstone, title, occurred_at
	FROM jsonb_to_recordset($2::jsonb) AS entry (
		material_id uuid, revision_id uuid, version bigint, tombstone boolean, title text, occurred_at timestamptz
	)`
