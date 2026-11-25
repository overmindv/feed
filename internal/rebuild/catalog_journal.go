package rebuild

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	catalogJournalPath  = "/v1/internal/catalog/feed-changes"
	catalogPageSize     = 100
	maxCatalogPage      = 1 << 20
	catalogActiveStatus = "active"
)

var ErrCatalogJournalGap = errors.New("invalid or incomplete Entities journal page")

type CatalogChange struct {
	Revision         int64     `json:"revision"`
	EventID          string    `json:"event_id"`
	EventType        string    `json:"event_type"`
	OccurredAt       time.Time `json:"occurred_at"`
	AggregateType    string    `json:"aggregate_type"`
	AggregateID      string    `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	ActorUserID      string    `json:"actor_user_id,omitempty"`
	Title            string    `json:"title,omitempty"`
	Ancestors        []string  `json:"ancestors,omitempty"`
	ScopeReset       bool      `json:"scope_reset"`
}

type CatalogPage struct {
	Checkpoint
	More    bool
	Changes []CatalogChange
}

type CatalogSource interface {
	ReadCatalogPage(context.Context, *int64, int64) (CatalogPage, error)
}

type CatalogStager interface {
	LoadCatalogCheckpoint(context.Context, string) (Checkpoint, bool, error)
	SaveCatalogPage(context.Context, string, int64, CatalogPage) error
}

type CatalogJournal struct {
	*journalHTTP
}

func NewCatalogJournal(address, token string, allowHTTP bool) (*CatalogJournal, error) {
	transport, err := newJournalHTTP(address, token, catalogJournalPath, "FEED_ENTITIES_URL", "ENTITIES_FEED_REPLAY_TOKEN", allowHTTP)
	if err != nil {
		return nil, err
	}

	return &CatalogJournal{journalHTTP: transport}, nil
}

type catalogEntry struct {
	Revision         int64     `json:"catalog_revision"`
	EventID          string    `json:"event_id"`
	EventType        string    `json:"event_type"`
	OccurredAt       time.Time `json:"occurred_at"`
	AggregateType    string    `json:"aggregate_type"`
	AggregateID      string    `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	ActorUserID      string    `json:"actor_user_id"`
	Payload          struct {
		Name   string `json:"name"`
		Title  string `json:"title"`
		Status string `json:"status"`
	} `json:"payload"`
	Visibility struct {
		Visible    *bool    `json:"visible"`
		Ancestors  []string `json:"ancestors"`
		ScopeReset *bool    `json:"scope_reset"`
	} `json:"feed_visibility"`
}

func (j *CatalogJournal) ReadCatalogPage(ctx context.Context, watermark *int64, after int64) (CatalogPage, error) {
	payload, err := j.readPage(ctx, watermark, after, catalogPageSize, maxCatalogPage)
	if err == journalStatusError(http.StatusConflict) {
		return CatalogPage{}, ErrCatalogJournalGap
	}
	if err != nil {
		return CatalogPage{}, err
	}
	var wire struct {
		Watermark *int64         `json:"watermark"`
		Through   *int64         `json:"through"`
		More      *bool          `json:"more"`
		Events    []catalogEntry `json:"events"`
	}
	if err = json.Unmarshal(payload, &wire); err != nil || wire.Watermark == nil || wire.Through == nil ||
		wire.More == nil || wire.Events == nil || len(wire.Events) > catalogPageSize {
		return CatalogPage{}, ErrCatalogJournalGap
	}
	page := CatalogPage{
		Checkpoint: Checkpoint{Watermark: *wire.Watermark, Through: *wire.Through},
		More:       *wire.More,
		Changes:    make([]CatalogChange, 0, len(wire.Events)),
	}
	for i := range wire.Events {
		change, ok := wire.Events[i].change()
		if !ok {
			return CatalogPage{}, ErrCatalogJournalGap
		}
		page.Changes = append(page.Changes, change)
	}

	return page, nil
}

func (e *catalogEntry) change() (CatalogChange, bool) {
	if !validCatalogKind(e.AggregateType) || !validCatalogID(e.AggregateID) || !validCatalogID(e.EventID) ||
		e.AggregateVersion < 1 || e.OccurredAt.IsZero() || e.Visibility.Visible == nil || e.Visibility.ScopeReset == nil {
		return CatalogChange{}, false
	}
	change := CatalogChange{
		Revision: e.Revision, EventID: e.EventID, EventType: e.EventType, OccurredAt: e.OccurredAt,
		AggregateType: e.AggregateType, AggregateID: e.AggregateID, AggregateVersion: e.AggregateVersion,
	}
	prefix := "catalog." + e.AggregateType + "."
	switch e.EventType {
	case prefix + "deleted", prefix + "unpublished":
		return change, !*e.Visibility.Visible && !*e.Visibility.ScopeReset
	case prefix + "created", prefix + "updated":
	default:
		return CatalogChange{}, false
	}
	if !*e.Visibility.Visible || e.Payload.Status != catalogActiveStatus ||
		(e.ActorUserID != "" && !validCatalogID(e.ActorUserID)) ||
		!validCatalogAncestors(e.AggregateType+":"+e.AggregateID, e.Visibility.Ancestors) {
		return CatalogChange{}, false
	}
	title := strings.TrimSpace(e.Payload.Name)
	if title == "" {
		title = strings.TrimSpace(e.Payload.Title)
	}
	if title == "" || utf8.RuneCountInString(title) > 256 || strings.ContainsRune(title, 0) {
		return CatalogChange{}, false
	}
	change.ActorUserID, change.Title = e.ActorUserID, title
	change.Ancestors, change.ScopeReset = e.Visibility.Ancestors, *e.Visibility.ScopeReset

	return change, true
}

func validCatalogKind(kind string) bool {
	switch kind {
	case "university", "program", "course", "topic":
		return true
	default:
		return false
	}
}

func validCatalogID(id string) bool {
	return validID(id) && id == strings.ToLower(id)
}

func validCatalogAncestors(self string, keys []string) bool {
	if keys == nil || len(keys) > 127 {
		return false
	}
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		kind, id, ok := strings.Cut(key, ":")
		if !ok || key == self || !validCatalogKind(kind) || !validCatalogID(id) {
			return false
		}
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}

	return true
}

func validCatalogPage(page CatalogPage, watermark *int64, after int64) bool {
	if after < 0 || (watermark != nil && page.Watermark != *watermark) ||
		page.Through < after || page.Through-after > catalogPageSize || page.Through > page.Watermark ||
		page.More != (page.Through < page.Watermark) || (page.More && page.Through == after) {
		return false
	}
	// Through also covers prerequisite changes that Entities does not return.
	revision := after
	for i := range page.Changes {
		change := &page.Changes[i]
		if change.Revision <= revision || change.Revision > page.Through {
			return false
		}
		revision = change.Revision
	}

	return true
}

func StageCatalog(ctx context.Context, source CatalogSource, stager CatalogStager, id string, maxPages int) (Checkpoint, error) {
	if !ValidRunID(id) || maxPages < 1 || maxPages > MaxJournalPages {
		return Checkpoint{}, errors.New("invalid Entities rebuild identifier or page budget")
	}
	checkpoint, found, err := stager.LoadCatalogCheckpoint(ctx, id)
	if err != nil {
		return checkpoint, err
	}
	for range maxPages {
		var watermark *int64
		if found {
			if checkpoint.Through == checkpoint.Watermark {
				break
			}
			watermark = &checkpoint.Watermark
		}
		page, readErr := source.ReadCatalogPage(ctx, watermark, checkpoint.Through)
		if readErr != nil {
			return checkpoint, readErr
		}
		if !validCatalogPage(page, watermark, checkpoint.Through) {
			return checkpoint, ErrCatalogJournalGap
		}
		if saveErr := stager.SaveCatalogPage(ctx, id, checkpoint.Through, page); saveErr != nil {
			return checkpoint, saveErr
		}
		checkpoint, found = page.Checkpoint, true
	}

	return checkpoint, nil
}
