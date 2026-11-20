package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/feed/internal/domain"
)

var (
	ErrRejectionNotFound = errors.New("rejection not found")
	ErrAlreadyReplayed   = errors.New("rejection already replayed")
)

func (r *Postgres) SaveRejection(ctx context.Context, rejection domain.EventRejection) (bool, error) {
	p := rejection.Position
	var kind any
	var hash []byte
	if rejection.Identity.Kind != "" {
		kind, hash = rejection.Identity.Kind, rejection.Identity.Hash[:]
	}
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO feed_event_quarantine (topic, partition, record_offset, source, reason, payload_size, identity_kind, identity_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (topic, partition, record_offset) DO NOTHING`,
		p.Topic, p.Partition, p.Offset, rejection.Source, rejection.Reason, rejection.PayloadSize, kind, hash)
	if err != nil {
		return false, fmt.Errorf("save rejection: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

func (r *Postgres) OldestRejectionAge(ctx context.Context) (float64, error) {
	var age float64
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(GREATEST(EXTRACT(EPOCH FROM (now() - (
			SELECT rejected_at FROM feed_event_quarantine
			WHERE replayed_at IS NULL ORDER BY rejected_at LIMIT 1
		))), 0), 0)::float8`).Scan(&age)

	return age, err
}

func (r *Postgres) ReplayRejection(
	ctx context.Context, p domain.EventPosition,
	apply func(context.Context, string, domain.RejectionIdentity, ActivityWriter) error,
) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		var source string
		var replayed bool
		var operator string
		var identity domain.RejectionIdentity
		var hash []byte
		err := tx.QueryRow(ctx, `
			SELECT source, replayed_at IS NOT NULL, session_user, COALESCE(identity_kind, ''), identity_hash
			FROM feed_event_quarantine
			WHERE topic = $1 AND partition = $2 AND record_offset = $3
			FOR UPDATE`, p.Topic, p.Partition, p.Offset).Scan(&source, &replayed, &operator, &identity.Kind, &hash)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrRejectionNotFound
		}
		if err != nil {
			return fmt.Errorf("lock rejection: %w", err)
		}
		if replayed {
			return ErrAlreadyReplayed
		}
		copy(identity.Hash[:], hash)
		if applyErr := apply(ctx, source, identity, activityWriter{db: tx}); applyErr != nil {
			return applyErr
		}
		_, err = tx.Exec(ctx, `
			UPDATE feed_event_quarantine SET replayed_at = clock_timestamp(), replayed_by = $4
			WHERE topic = $1 AND partition = $2 AND record_offset = $3`,
			p.Topic, p.Partition, p.Offset, operator)
		if err != nil {
			return fmt.Errorf("mark rejection replayed: %w", err)
		}

		return nil
	})
}
