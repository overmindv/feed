package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/rebuild"
	"github.com/overmindv/feed/internal/repository"
)

const (
	catalogRebuildID    = "fixture-catalog-rebuild"
	rebuildCourseID     = "de310000-0000-0000-0000-000000000001"
	rebuildUniversity   = "university:de310000-0000-0000-0000-000000000002"
	rebuildCatalogTitle = "Fixture course"
	courseCreated       = "catalog.course.created"
	courseUnpublished   = "catalog.course.unpublished"
)

func catalogRebuildChange(revision int64, eventType string) rebuild.CatalogChange {
	return rebuild.CatalogChange{
		Revision: revision, EventID: fmt.Sprintf("de320000-0000-0000-0000-%012d", revision),
		EventType: eventType, AggregateType: "course", AggregateID: rebuildCourseID, AggregateVersion: revision,
		OccurredAt: time.Date(2026, 11, 25, 11, 0, 0, 0, time.UTC).Add(time.Duration(revision) * time.Second),
		Title:      rebuildCatalogTitle, Ancestors: []string{rebuildUniversity}, ScopeReset: true,
	}
}

func catalogJournalFixture(change rebuild.CatalogChange) map[string]any {
	public := change.EventType != courseUnpublished
	// Send private fields even on a hidden record to check that the client drops them.
	return map[string]any{
		"catalog_revision": change.Revision, "event_id": change.EventID, "event_type": change.EventType,
		"aggregate_type": change.AggregateType, "aggregate_id": change.AggregateID, "aggregate_version": change.AggregateVersion,
		"occurred_at": change.OccurredAt, "actor_user_id": rebuildCourseID,
		"payload": map[string]string{"name": change.Title, "status": "active", "description": "not for Feed"},
		"feed_visibility": map[string]any{
			"visible": public, "ancestors": change.Ancestors, "scope_reset": public && change.ScopeReset,
		},
	}
}

func catalogRebuildServer(t *testing.T, token string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.Method != http.MethodGet || r.URL.Path != "/v1/internal/catalog/feed-changes" ||
			r.Header.Get("Authorization") != "Bearer "+token || query.Get("limit") != "100" {
			t.Error("unexpected Entities journal request")
		}
		call := calls.Add(1)
		through := int64(5)
		events := make([]map[string]any, 0, 2)
		after := "3"
		wantWatermark := "5"
		switch call {
		case 1:
			after, wantWatermark, through = "0", "", 2
			events = append(events, catalogJournalFixture(catalogRebuildChange(2, courseCreated)))
		case 2:
			after, through = "2", 3
		}
		if query.Get("after") != after || query.Get("watermark") != wantWatermark {
			t.Error("unexpected journal cursor", query)
		}
		if call == 3 {
			w.WriteHeader(http.StatusConflict)

			return
		}
		if call > 3 {
			hide := catalogRebuildChange(4, courseUnpublished)
			hide.OccurredAt = hide.OccurredAt.Add(-time.Hour)
			publish := catalogRebuildChange(5, courseCreated)
			if call == 4 {
				publish.EventID = catalogRebuildChange(2, courseCreated).EventID
			}
			events = append(events, catalogJournalFixture(hide), catalogJournalFixture(publish))
		}
		if err := json.NewEncoder(w).Encode(map[string]any{
			"watermark": 5, "through": through, "more": through < 5, "events": events,
		}); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)

	return server, calls
}

//nolint:paralleltest // Uses migrations and a dedicated PostgreSQL schema.
func TestCatalogRebuildResumesWithoutChangingActiveFeed(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool, false)
	initial := catalogRebuildChange(2, courseCreated)
	if err := store.ApplyCatalogChange(ctx, domain.CatalogChange{
		EventID: initial.EventID, ResourceKey: "course:" + rebuildCourseID, CatalogRevision: 2,
		AggregateVersion: 2, Ancestors: initial.Ancestors, ScopeReset: initial.ScopeReset,
		Item: &domain.FeedItem{AggregateID: rebuildCourseID, OccurredAt: initial.OccurredAt, Title: initial.Title,
			Href: "/courses/" + rebuildCourseID, RawPayload: []byte("{}")},
	}); err != nil {
		t.Fatal(err)
	}
	before, err := store.ListPage(ctx, 20, nil)
	if err != nil || len(before.Items) != 1 {
		t.Fatal("failed to seed the active feed", err)
	}

	token := strings.Repeat("y", 32)
	server, calls := catalogRebuildServer(t, token)
	source, err := rebuild.NewCatalogJournal(server.URL, token, true)
	if err != nil {
		t.Fatal(err)
	}
	defer source.CloseIdleConnections()
	stage := func(ctx context.Context) (rebuild.Checkpoint, error) {
		return rebuild.StageCatalog(ctx, source, store, catalogRebuildID, 1)
	}
	for _, want := range []int64{2, 3} {
		cp, stageErr := stage(ctx)
		if stageErr != nil || cp.Through != want || cp.Watermark != 5 {
			t.Fatal("did not stage initial and filtered page", cp, stageErr)
		}
	}
	if _, gap := stage(ctx); !errors.Is(gap, rebuild.ErrCatalogJournalGap) {
		t.Fatal("history gap was not reported", gap)
	}
	if _, duplicate := stage(ctx); duplicate == nil {
		t.Fatal("stored duplicate event ID")
	}
	failed, found, err := store.LoadCatalogCheckpoint(ctx, catalogRebuildID)
	if err != nil || !found || failed.Through != 3 || failed.Watermark != 5 {
		t.Fatal("failed page advanced the checkpoint", failed, err)
	}
	var count int
	if readErr := pool.QueryRow(ctx, "SELECT count(*) FROM feed_rebuild_catalog_changes WHERE run_id = $1",
		catalogRebuildID).Scan(&count); readErr != nil || count != 1 {
		t.Fatal("failed page left partial rows", count, readErr)
	}

	for range 2 {
		done, stageErr := stage(ctx)
		if stageErr != nil || done.Through != 5 || done.Watermark != 5 {
			t.Fatal("did not complete after retry", done, stageErr)
		}
	}
	if calls.Load() != 5 {
		t.Error("completed import fetched another page")
	}
	var hides, resets int
	if readErr := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE event_type = $2 AND title IS NULL AND actor_user_id IS NULL
			AND cardinality(ancestors) = 0 AND NOT scope_reset AND occurred_at < $3),
			count(*) FILTER (WHERE title = $4 AND actor_user_id = $5 AND ancestors = $6 AND scope_reset)
		FROM feed_rebuild_catalog_changes WHERE run_id = $1`,
		catalogRebuildID, courseUnpublished, initial.OccurredAt, initial.Title, rebuildCourseID, initial.Ancestors,
	).Scan(&hides, &resets); readErr != nil || hides != 1 || resets != 2 {
		t.Error("lost a visibility barrier or kept private fields", hides, resets, readErr)
	}
	if stale := store.SaveCatalogPage(ctx, catalogRebuildID, 0, rebuild.CatalogPage{Checkpoint: failed}); !errors.Is(stale, repository.ErrCatalogRebuildAdvanced) {
		t.Error("stale writer did not stop", stale)
	}
	after, err := store.ListPage(ctx, 20, nil)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Error("the import changed the active feed", err)
	}
}

func BenchmarkCatalogRebuildPage(b *testing.B) {
	store := repository.New(feedTestPool(b), false)
	const size = 100
	page := rebuild.CatalogPage{
		Checkpoint: rebuild.Checkpoint{Watermark: size, Through: size},
		Changes:    make([]rebuild.CatalogChange, size),
	}
	for i := range page.Changes {
		page.Changes[i] = catalogRebuildChange(int64(i+1), courseCreated)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if err := store.SaveCatalogPage(b.Context(), fmt.Sprintf("catalog-bench-%d", i), 0, page); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
