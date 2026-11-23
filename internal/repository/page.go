package repository

import (
	"context"
	"fmt"

	"github.com/overmindv/feed/internal/domain"
)

const (
	feedCardRows = `
		SELECT f.id, f.kind, f.title, f.text, f.href, COALESCE(f.actor_user_id::text, '') AS actor_user_id,
		       f.occurred_at, false AS material
		FROM feed_items f WHERE ` + publicFeedFilter
	materialCardRows = `
		SELECT m.id, 'material.published', m.title, '', '/materials/' || m.material_id::text, '',
		       m.occurred_at, true
		FROM feed_material_state m WHERE ` + publicMaterialFilter
	feedPageSelect = `SELECT id::text, kind, title, text, href, actor_user_id, occurred_at, material FROM (`
	feedPageOrder  = ` ORDER BY occurred_at DESC, id DESC`
	feedPageLimit  = ` LIMIT $1`
	strictBefore   = ` AND (occurred_at, id) < ($2::timestamptz, $3::uuid)`
	throughCursor  = ` AND (occurred_at, id) <= ($2::timestamptz, $3::uuid)`
)

func feedPageQuery(materialActivity bool, after *domain.FeedCursor) string {
	feedBoundary, materialBoundary := "", ""
	if after != nil {
		feedBoundary, materialBoundary = strictBefore, strictBefore
		// При одинаковых ключах карточка материала идет первой.
		if after.Material {
			feedBoundary = throughCursor
		}
	}
	feed := feedCardRows + feedBoundary + feedPageOrder + feedPageLimit
	if !materialActivity {
		return feedPageSelect + feed + `) cards` + feedPageOrder
	}

	materials := materialCardRows + materialBoundary + feedPageOrder + feedPageLimit

	return feedPageSelect + "(" + feed + ") UNION ALL (" + materials + ")" +
		`) cards` + feedPageOrder + `, material DESC` + feedPageLimit
}

func (r *Postgres) ListPage(ctx context.Context, limit int, after *domain.FeedCursor) (domain.FeedPage, error) {
	if limit < 1 || limit > domain.MaxPageSize {
		limit = domain.DefaultPageSize
	}
	args := []any{limit + 1}
	if after != nil {
		args = append(args, after.OccurredAt, after.ID)
	}
	rows, err := r.pool.Query(ctx, feedPageQuery(r.materialActivity, after), args...)
	if err != nil {
		return domain.FeedPage{}, fmt.Errorf("list feed page: %w", err)
	}
	defer rows.Close()

	page := domain.FeedPage{Items: make([]domain.FeedItem, 0, limit)}
	var last domain.FeedCursor
	for rows.Next() {
		if len(page.Items) == limit {
			page.Next = &last

			break
		}
		var card domain.FeedItem
		var material bool
		if scanErr := rows.Scan(&card.ID, &card.Kind, &card.Title, &card.Text, &card.Href, &card.ActorUserID,
			&card.OccurredAt, &material); scanErr != nil {
			return domain.FeedPage{}, fmt.Errorf("scan feed page: %w", scanErr)
		}
		page.Items = append(page.Items, card)
		last = domain.FeedCursor{OccurredAt: card.OccurredAt, ID: card.ID, Material: material}
	}
	if err := rows.Err(); err != nil {
		return domain.FeedPage{}, fmt.Errorf("iterate feed page: %w", err)
	}

	return page, nil
}
