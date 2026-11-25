package rebuild

import (
	"context"
	"errors"
	"time"
)

type MaterialRow struct {
	MaterialID string     `json:"material_id"`
	RevisionID string     `json:"revision_id"`
	Version    int64      `json:"version"`
	Tombstone  bool       `json:"tombstone"`
	Title      string     `json:"title,omitempty"`
	OccurredAt *time.Time `json:"occurred_at,omitempty"`
}

type MaterialPage struct {
	Watermark   int64
	NextAfterID string
	Rows        []MaterialRow
}

type MaterialCheckpoint struct {
	Watermark  *int64
	AfterID    string
	Complete   bool
	Unresolved int64
}

type MaterialSource interface {
	ReadMaterialPage(context.Context, *int64, string) (MaterialPage, error)
}

type MaterialPositioner interface {
	CaptureMaterialPosition(context.Context) (MaterialPosition, error)
}

type MaterialStager interface {
	LoadMaterialCheckpoint(context.Context, string) (MaterialCheckpoint, bool, error)
	StartMaterialCheckpoint(context.Context, string, MaterialPosition) (MaterialCheckpoint, error)
	SaveMaterialPage(context.Context, string, MaterialCheckpoint, MaterialPage) (MaterialCheckpoint, error)
}

func StageMaterials(ctx context.Context, source MaterialSource, positioner MaterialPositioner, stager MaterialStager,
	id string, maxPages int) (MaterialCheckpoint, error) {
	if !ValidRunID(id) || maxPages < 1 || maxPages > MaxJournalPages {
		return MaterialCheckpoint{}, errors.New("invalid Content rebuild identifier or page budget")
	}
	checkpoint, found, err := stager.LoadMaterialCheckpoint(ctx, id)
	if err != nil {
		return checkpoint, err
	}
	if !found {
		position, captureErr := positioner.CaptureMaterialPosition(ctx)
		if captureErr != nil {
			return checkpoint, captureErr
		}
		checkpoint, err = stager.StartMaterialCheckpoint(ctx, id, position)
		if err != nil {
			return checkpoint, err
		}
	}
	for range maxPages {
		if checkpoint.Complete {
			break
		}
		page, readErr := source.ReadMaterialPage(ctx, checkpoint.Watermark, checkpoint.AfterID)
		if readErr != nil {
			return checkpoint, readErr
		}
		if !validMaterialPage(checkpoint, page) {
			return checkpoint, ErrMaterialSnapshot
		}
		next, saveErr := stager.SaveMaterialPage(ctx, id, checkpoint, page)
		if saveErr != nil {
			return checkpoint, saveErr
		}
		checkpoint = next
	}

	return checkpoint, nil
}

func validMaterialPage(before MaterialCheckpoint, page MaterialPage) bool {
	if page.Watermark < 0 || (before.Watermark != nil && page.Watermark != *before.Watermark) ||
		len(page.Rows) > materialSnapshotSize || (before.AfterID != "" && len(page.Rows) == 0) {
		return false
	}
	last := before.AfterID
	for i := range page.Rows {
		row := &page.Rows[i]
		if row.MaterialID <= last || row.Version < 0 || row.Version > page.Watermark {
			return false
		}
		last = row.MaterialID
	}

	return page.NextAfterID == "" || (len(page.Rows) == materialSnapshotSize && page.NextAfterID == last)
}
