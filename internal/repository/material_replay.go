package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/feed/internal/rebuild"
)

func (r *Postgres) LoadMaterialReplay(ctx context.Context, id string) (rebuild.MaterialReplayState, error) {
	var state rebuild.MaterialReplayState
	var position, target []byte
	err := r.pool.QueryRow(ctx, `
		SELECT snapshot.watermark, COALESCE(replay.position, snapshot.kafka_position), replay.target
		FROM feed_rebuild_material_runs AS snapshot
		LEFT JOIN feed_rebuild_material_replay AS replay USING (run_id)
		WHERE snapshot.run_id = $1 AND snapshot.complete`, id,
	).Scan(&state.Watermark, &position, &target)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, rebuild.ErrMaterialReplaySnapshot
	}
	if err != nil {
		return state, fmt.Errorf("load Content replay checkpoint: %w", err)
	}
	if json.Unmarshal(position, &state.Position) != nil || (target != nil && json.Unmarshal(target, &state.Target) != nil) {
		return rebuild.MaterialReplayState{}, rebuild.ErrMaterialReplayRange
	}

	return state, nil
}

func (r *Postgres) StartMaterialReplay(ctx context.Context, id string, target rebuild.MaterialPosition) (rebuild.MaterialReplayState, error) {
	payload, err := json.Marshal(target)
	if err != nil {
		return rebuild.MaterialReplayState{}, fmt.Errorf("encode Content replay target: %w", err)
	}
	if _, err = r.pool.Exec(ctx, `
		INSERT INTO feed_rebuild_material_replay (run_id, position, target)
		SELECT run_id, kafka_position, $2::jsonb FROM feed_rebuild_material_runs
		WHERE run_id = $1 AND complete
		ON CONFLICT (run_id) DO NOTHING`, id, payload); err != nil {
		return rebuild.MaterialReplayState{}, fmt.Errorf("start Content replay: %w", err)
	}

	return r.LoadMaterialReplay(ctx, id)
}

func (r *Postgres) SaveMaterialReplay(ctx context.Context, id string, before rebuild.MaterialReplayState, batch rebuild.MaterialReplayBatch) error {
	old, err := json.Marshal(before.Position)
	if err != nil {
		return fmt.Errorf("encode previous Content replay offsets: %w", err)
	}
	target, err := json.Marshal(before.Target)
	if err != nil {
		return fmt.Errorf("encode Content replay target: %w", err)
	}
	next, err := json.Marshal(batch.Position)
	if err != nil {
		return fmt.Errorf("encode next Content replay offsets: %w", err)
	}

	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, updateErr := tx.Exec(ctx, `
			UPDATE feed_rebuild_material_replay SET position = $4::jsonb
			WHERE run_id = $1 AND position = $2::jsonb AND target = $3::jsonb`, id, old, target, next)
		if updateErr != nil {
			return fmt.Errorf("advance Content replay offsets: %w", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return ErrMaterialRebuildAdvanced
		}
		if len(batch.Events) == 0 {
			return nil
		}
		payload, encodeErr := json.Marshal(batch.Events)
		if encodeErr != nil {
			return fmt.Errorf("encode Content replay batch: %w", encodeErr)
		}
		if _, writeErr := tx.Exec(ctx, stageMaterialEvents, id, payload); writeErr != nil {
			return fmt.Errorf("stage Content replay events: %w", writeErr)
		}

		return nil
	})
}

const stageMaterialEvents = `
	INSERT INTO feed_rebuild_material_events
		(run_id, partition, kafka_offset, event_id, material_id, version, occurred_at, revision_id, title)
	SELECT $1, partition, "offset", event_id, material_id, version, occurred_at, revision_id, title
	FROM jsonb_to_recordset($2::jsonb) AS entry (
		partition integer, "offset" bigint, event_id uuid, material_id uuid,
		version bigint, occurred_at timestamptz, revision_id uuid, title text
	)`
