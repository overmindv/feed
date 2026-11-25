package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/overmindv/feed/internal/domain"
)

var (
	ErrFeedPreviewSources  = errors.New("complete the Entities journal and Content catch-up for this run before comparing projections")
	ErrFeedPreviewConflict = errors.New("source history contains conflicting versions or event identities")
)

type FeedPreviewReport struct {
	CatalogWatermark        int64 `json:"catalog_watermark"`
	MaterialWatermark       int64 `json:"material_watermark"`
	CatalogCards            int64 `json:"catalog_cards"`
	MaterialCards           int64 `json:"material_cards"`
	OnlyLive                int64 `json:"only_live"`
	OnlyPreview             int64 `json:"only_preview"`
	DifferentSortKeys       int64 `json:"different_sort_keys"`
	DifferentFields         int64 `json:"different_fields"`
	DifferentCatalogStates  int64 `json:"different_catalog_states"`
	DifferentMaterialStates int64 `json:"different_material_states"`
	UnresolvedMaterialDates int64 `json:"unresolved_material_dates"`
	MaterialQuarantined     bool  `json:"material_quarantined"`
	Matches                 bool  `json:"entities_and_content_match"`
}

func (r *Postgres) CompareFeedPreview(ctx context.Context, id string) (FeedPreviewReport, error) {
	var report FeedPreviewReport
	err := pgx.BeginTxFunc(ctx, r.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		if _, execErr := tx.Exec(ctx, "SET LOCAL work_mem = '16MB'"); execErr != nil {
			return fmt.Errorf("bound projection comparison memory: %w", execErr)
		}
		tag, insertErr := tx.Exec(ctx, startFeedPreview, id)
		if insertErr != nil {
			return fmt.Errorf("start Feed preview: %w", insertErr)
		}
		if tag.RowsAffected() != 0 {
			if buildErr := buildFeedPreview(ctx, tx, id); buildErr != nil {
				return buildErr
			}
		}
		readErr := tx.QueryRow(ctx, compareFeedPreview, id).Scan(
			&report.CatalogWatermark, &report.MaterialWatermark, &report.CatalogCards, &report.MaterialCards,
			&report.OnlyLive, &report.OnlyPreview, &report.DifferentSortKeys, &report.DifferentFields,
			&report.DifferentCatalogStates, &report.DifferentMaterialStates, &report.UnresolvedMaterialDates,
			&report.MaterialQuarantined,
		)
		if errors.Is(readErr, pgx.ErrNoRows) {
			return ErrFeedPreviewSources
		}
		if readErr != nil {
			return fmt.Errorf("compare Feed preview: %w", readErr)
		}

		return nil
	})
	if err != nil {
		return FeedPreviewReport{}, err
	}
	report.Matches = report.OnlyLive == 0 && report.OnlyPreview == 0 && report.DifferentSortKeys == 0 &&
		report.DifferentFields == 0 && report.DifferentCatalogStates == 0 && report.DifferentMaterialStates == 0 &&
		report.UnresolvedMaterialDates == 0 && !report.MaterialQuarantined

	return report, nil
}

func buildFeedPreview(ctx context.Context, tx pgx.Tx, id string) error {
	var conflict bool
	if err := tx.QueryRow(ctx, feedPreviewInputConflict, id).Scan(&conflict); err != nil {
		return fmt.Errorf("validate Feed preview sources: %w", err)
	}
	if conflict {
		return ErrFeedPreviewConflict
	}
	rendering, err := previewCatalogRendering()
	if err != nil {
		return err
	}
	if _, writeErr := tx.Exec(ctx, buildPreviewCatalogState, id); writeErr != nil {
		return fmt.Errorf("build catalog preview state: %w", writeErr)
	}
	if _, writeErr := tx.Exec(ctx, buildPreviewCatalogCards, id, rendering); writeErr != nil {
		return fmt.Errorf("build catalog preview cards: %w", writeErr)
	}
	if _, writeErr := tx.Exec(ctx, buildPreviewMaterial, id, domain.MaterialCardTitlePrefix); writeErr != nil {
		return fmt.Errorf("build Content preview state: %w", writeErr)
	}

	return nil
}

func previewCatalogRendering() ([]byte, error) {
	type rendering struct {
		EventType   string `json:"event_type"`
		Kind        string `json:"kind"`
		TitlePrefix string `json:"title_prefix"`
		HrefPrefix  string `json:"href_prefix"`
	}
	items := make([]rendering, 0, 8)
	for _, aggregate := range []string{"university", "program", "course", "topic"} {
		for _, action := range []string{"created", "updated"} {
			p := domain.CatalogPresentation(aggregate, action == "updated")
			items = append(items, rendering{
				EventType: "catalog." + aggregate + "." + action,
				Kind:      p.Kind, TitlePrefix: p.TitlePrefix, HrefPrefix: p.HrefPrefix,
			})
		}
	}
	data, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("encode catalog card rendering: %w", err)
	}

	return data, nil
}
