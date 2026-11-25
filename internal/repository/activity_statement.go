package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

type activityStatement struct {
	name  string
	query string
	args  []any
}

func (s activityStatement) exec(ctx context.Context, db sqlExecutor) (pgconn.CommandTag, error) {
	tag, err := db.Exec(ctx, s.query, s.args...)
	if err != nil {
		return tag, fmt.Errorf("%s: %w", s.name, err)
	}

	return tag, nil
}
