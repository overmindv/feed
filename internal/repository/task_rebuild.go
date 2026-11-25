package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/feed/internal/rebuild"
)

var ErrTaskRebuildAdvanced = errors.New("tasks rebuild checkpoint changed; resume with the same run")

func (r *Postgres) LoadTaskCheckpoint(ctx context.Context, id string) (rebuild.TaskCheckpoint, bool, error) {
	var checkpoint rebuild.TaskCheckpoint
	err := r.pool.QueryRow(ctx,
		"SELECT watermark, through FROM feed_rebuild_task_runs WHERE run_id = $1", id,
	).Scan(&checkpoint.Watermark, &checkpoint.Through)
	if errors.Is(err, pgx.ErrNoRows) {
		return checkpoint, false, nil
	}
	if err != nil {
		return checkpoint, false, fmt.Errorf("load Tasks rebuild checkpoint: %w", err)
	}

	return checkpoint, true, nil
}

func (r *Postgres) SaveTaskPage(ctx context.Context, id string, after int64, page rebuild.TaskPage) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO feed_rebuild_task_runs (run_id, watermark) VALUES ($1, $2)
			ON CONFLICT (run_id) DO NOTHING`, id, page.Watermark); err != nil {
			return fmt.Errorf("create Tasks rebuild checkpoint: %w", err)
		}

		var checkpoint rebuild.TaskCheckpoint
		if err := tx.QueryRow(ctx, `
			SELECT watermark, through FROM feed_rebuild_task_runs WHERE run_id = $1 FOR UPDATE`, id,
		).Scan(&checkpoint.Watermark, &checkpoint.Through); err != nil {
			return fmt.Errorf("lock Tasks rebuild checkpoint: %w", err)
		}
		if checkpoint.Watermark != page.Watermark || checkpoint.Through != after {
			return ErrTaskRebuildAdvanced
		}

		if len(page.Changes) != 0 {
			payload, err := json.Marshal(page.Changes)
			if err != nil {
				return fmt.Errorf("encode Tasks rebuild page: %w", err)
			}
			if _, writeErr := tx.Exec(ctx, stageTaskChanges, id, payload); writeErr != nil {
				return fmt.Errorf("write Tasks rebuild page: %w", writeErr)
			}
		}
		if _, err := tx.Exec(ctx, "UPDATE feed_rebuild_task_runs SET through = $2 WHERE run_id = $1",
			id, page.Through); err != nil {
			return fmt.Errorf("advance Tasks rebuild checkpoint: %w", err)
		}

		return nil
	})
}

const stageTaskChanges = `
	INSERT INTO feed_rebuild_task_changes
		(run_id, revision, event_id, event_type, occurred_at, task_id,
		 task_version_id, title, topic_id, task_type, difficulty)
	SELECT $1, revision, event_id, event_type, occurred_at, task_id,
	       task_version_id, title, topic_id, task_type, difficulty
	FROM jsonb_to_recordset($2::jsonb) AS entry (
		revision bigint, event_id uuid, event_type text, occurred_at timestamptz, task_id uuid,
		task_version_id uuid, title text, topic_id uuid, task_type text, difficulty text
	)`
