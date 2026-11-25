package repository_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/rebuild"
	"github.com/overmindv/feed/internal/repository"
)

const previewRunID = "fixture-feed-preview"

func previewCatalogFixture(t testing.TB, store *repository.Postgres, id string, changes []rebuild.CatalogChange) {
	t.Helper()
	size := int64(len(changes))
	if err := store.SaveCatalogPage(t.Context(), id, 0, rebuild.CatalogPage{
		Checkpoint: rebuild.Checkpoint{Watermark: size, Through: size}, Changes: changes,
	}); err != nil {
		t.Fatal(err)
	}
}

func previewMaterialFixture(t testing.TB, store *repository.Postgres, id string, rows []rebuild.MaterialRow, events []rebuild.MaterialReplayEvent) {
	t.Helper()
	ctx := t.Context()
	from, err := (&positionFixture{}).CaptureMaterialPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := store.StartMaterialCheckpoint(ctx, id, from)
	if err != nil {
		t.Fatal(err)
	}
	if _, saveErr := store.SaveMaterialPage(ctx, id, snap, rebuild.MaterialPage{Watermark: 5, Rows: rows}); saveErr != nil {
		t.Fatal(saveErr)
	}
	target := replayOffset(from, from.Partitions[0].NextOffset+int64(len(events)))
	replay, err := store.StartMaterialReplay(ctx, id, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		for i := range events {
			events[i].Offset = from.Partitions[0].NextOffset + int64(i)
		}
		if saveErr := store.SaveMaterialReplay(ctx, id, replay, rebuild.MaterialReplayBatch{Position: target, Events: events}); saveErr != nil {
			t.Fatal(saveErr)
		}
	}
}

func previewCatalogChanges() []rebuild.CatalogChange {
	before := catalogRebuildChange(1, courseCreated)
	parentKind, parentID, _ := strings.Cut(rebuildUniversity, ":")
	hide := catalogRebuildChange(2, "catalog.university.unpublished")
	hide.AggregateID, hide.AggregateType = parentID, parentKind
	hide.ActorUserID, hide.Title, hide.Ancestors, hide.ScopeReset = "", "", nil, false
	restore := catalogRebuildChange(3, "catalog.university.updated")
	restore.AggregateID, restore.AggregateType, restore.Ancestors, restore.ScopeReset = parentID, parentKind, nil, false
	update := catalogRebuildChange(4, "catalog.course.updated")
	update.OccurredAt = materialFixtureTime()

	return []rebuild.CatalogChange{before, hide, restore, update}
}

func applyPreviewCatalogFixture(t *testing.T, store *repository.Postgres, entry rebuild.CatalogChange) {
	t.Helper()
	change := domain.CatalogChange{
		EventID: entry.EventID, ResourceKey: entry.AggregateType + ":" + entry.AggregateID, CatalogRevision: entry.Revision,
		AggregateVersion: entry.AggregateVersion, ScopeReset: entry.ScopeReset, Ancestors: entry.Ancestors,
	}
	if entry.Title != "" {
		p := domain.CatalogPresentation(entry.AggregateType, strings.HasSuffix(entry.EventType, ".updated"))
		change.Item = &domain.FeedItem{
			Kind: p.Kind, ActorUserID: entry.ActorUserID, AggregateType: entry.AggregateType, AggregateID: entry.AggregateID,
			OccurredAt: entry.OccurredAt, Title: p.TitlePrefix + entry.Title, Text: entry.Title, Href: p.HrefPrefix + entry.AggregateID,
		}
	}
	if err := store.ApplyCatalogChange(t.Context(), change); err != nil {
		t.Fatal(err)
	}
}

func previewMaterialEvents() []rebuild.MaterialReplayEvent {
	at := materialFixtureTime()
	published := rebuild.MaterialReplayEvent{
		EventID: fixtureMaterialID(8), MaterialID: fixtureMaterialID(1), RevisionID: fixtureMaterialRevision, Version: 6,
		OccurredAt: at, Title: domain.MaterialCardTitlePrefix + materialStampTitle,
	}
	withdrawn := rebuild.MaterialReplayEvent{
		EventID: fixtureMaterialID(9), MaterialID: fixtureMaterialID(2), Version: 7, OccurredAt: at.Add(-time.Hour),
	}

	return []rebuild.MaterialReplayEvent{published, withdrawn, published}
}

func applyPreviewMaterialFixture(t *testing.T, store *repository.Postgres, e rebuild.MaterialReplayEvent) {
	t.Helper()
	if err := store.ApplyMaterialChange(t.Context(), domain.MaterialChange{
		EventID: e.EventID, MaterialID: e.MaterialID, RevisionID: e.RevisionID,
		AggregateVersion: e.Version, OccurredAt: e.OccurredAt, Title: e.Title,
	}); err != nil {
		t.Fatal(err)
	}
}

//nolint:paralleltest // Uses migrations and a dedicated PostgreSQL schema.
func TestFeedPreviewMatchesVisibilityStableIDsAndOrder(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool, true)
	catalog := previewCatalogChanges()
	previewCatalogFixture(t, store, previewRunID, catalog)
	if _, err := store.CompareFeedPreview(ctx, previewRunID); !errors.Is(err, repository.ErrFeedPreviewSources) {
		t.Fatal("accepted incomplete preview sources", err)
	}
	for _, i := range []int{0, 2, 3, 1} {
		applyPreviewCatalogFixture(t, store, catalog[i])
	}
	at := materialFixtureTime()
	events := previewMaterialEvents()
	previewMaterialFixture(t, store, previewRunID, []rebuild.MaterialRow{
		{MaterialID: fixtureMaterialID(1), RevisionID: fixtureMaterialRevision, Title: materialStampTitle},
		{MaterialID: fixtureMaterialID(2), RevisionID: fixtureMaterialRevision, Title: materialStampTitle, Version: 5, OccurredAt: &at},
	}, events)
	for _, event := range events {
		applyPreviewMaterialFixture(t, store, event)
	}
	taskID := fixtureMaterialID(20)
	if err := store.ApplyTaskPublication(ctx, domain.TaskPublication{
		EventID: fixtureMaterialID(21),
		Item:    domain.FeedItem{AggregateID: taskID, Title: "Fixture task", Href: "/tasks/" + taskID, OccurredAt: at, RawPayload: []byte("{}")},
	}); err != nil {
		t.Fatal(err)
	}
	before, err := store.ListPage(ctx, 20, nil)
	if err != nil || len(before.Items) != 4 {
		t.Fatal("unexpected fixture visibility", before, err)
	}
	report, err := store.CompareFeedPreview(ctx, previewRunID)
	if err != nil || !report.Matches || report.CatalogCards != 2 || report.MaterialCards != 1 ||
		report.CatalogWatermark != 4 || report.MaterialWatermark != 5 {
		t.Fatal("preview did not preserve identities, order and visibility", report, err)
	}
	after, err := store.ListPage(ctx, 20, nil)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("preview modified the active feed", after, err)
	}
	updated := events[0]
	updated.EventID, updated.Version, updated.Title, updated.OccurredAt = fixtureMaterialID(10), 8, "Updated fixture", at.Add(time.Second)
	applyPreviewMaterialFixture(t, store, updated)
	report, err = store.CompareFeedPreview(ctx, previewRunID)
	if err != nil || report.Matches || report.DifferentSortKeys != 1 || report.DifferentFields != 1 || report.DifferentMaterialStates != 1 {
		t.Error("did not detect live changes against the frozen preview", report, err)
	}
	if _, saveErr := store.SaveRejection(ctx, domain.EventRejection{
		Position: domain.EventPosition{Topic: materialFixtureTopic}, Source: "material", Reason: domain.RejectInvalidMaterial,
	}); saveErr != nil {
		t.Fatal(saveErr)
	}
	report, err = store.CompareFeedPreview(ctx, previewRunID)
	if err != nil || report.Matches || !report.MaterialQuarantined || report.OnlyPreview != 1 {
		t.Error("did not report the active quarantine hold", report, err)
	}
}

func BenchmarkBuildFeedPreview(b *testing.B) {
	ctx := b.Context()
	pool := feedTestPool(b)
	store := repository.New(pool, true)
	const rows = 10000
	at := materialFixtureTime()
	rendering := domain.CatalogPresentation("course", false)
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO feed_catalog_visibility (resource_key, catalog_revision, aggregate_version, visible, fence_revision)
			SELECT 'course:' || md5(n::text)::uuid::text, n, 1, true, 0 FROM generate_series(1,$1::integer) n`, []any{rows}},
		{`INSERT INTO feed_items (source,event_key,kind,aggregate_type,aggregate_id,occurred_at,title,text,href,raw_payload,catalog_revision,visibility_keys)
			SELECT 'catalog','course:' || md5(n::text)::uuid::text,$4,'course',md5(n::text)::uuid::text,$2,$3,
				'fixture','/courses/' || md5(n::text)::uuid::text,'{}',n,ARRAY['course:' || md5(n::text)::uuid::text]
			FROM generate_series(1,$1::integer) n`, []any{rows, at, rendering.TitlePrefix + "fixture", rendering.Kind}},
		{`INSERT INTO feed_material_state (material_id, aggregate_version, revision_id, title, occurred_at)
			SELECT md5(n::text)::uuid, 5, md5('revision' || n::text)::uuid, $3, $2 FROM generate_series(1,$1::integer) n`,
			[]any{rows, at, domain.MaterialCardTitlePrefix + "fixture"}},
	} {
		if _, err := pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		b.StopTimer()
		id := fmt.Sprintf("preview-bench-%d", i)
		previewMaterialFixture(b, store, id, nil, nil)
		for _, stmt := range []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO feed_rebuild_catalog_runs (run_id,watermark,through) VALUES ($1,$2,$2)`, []any{id, rows}},
			{`INSERT INTO feed_rebuild_catalog_changes
				(run_id,revision,event_id,event_type,occurred_at,aggregate_type,aggregate_id,aggregate_version,title,ancestors,scope_reset)
				SELECT $1,n,md5('event' || n::text)::uuid,'catalog.course.created',$3,'course',md5(n::text)::uuid,
					1,'fixture',ARRAY[]::text[],false FROM generate_series(1,$2::integer) n`, []any{id, rows, at}},
			{`INSERT INTO feed_rebuild_material_rows (run_id,material_id,revision_id,version,tombstone,title,occurred_at)
				SELECT $1,md5(n::text)::uuid,md5('revision' || n::text)::uuid,5,false,'fixture',$3
				FROM generate_series(1,$2::integer) n`, []any{id, rows, at}},
		} {
			if _, err := pool.Exec(ctx, stmt.sql, stmt.args...); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		report, err := store.CompareFeedPreview(ctx, id)
		if err != nil || !report.Matches || report.CatalogCards != rows || report.MaterialCards != rows {
			b.Fatal("could not compare the full fixture", report, err)
		}
		i++
	}
}

//nolint:paralleltest // Uses migrations and a dedicated PostgreSQL schema.
func TestFeedPreviewRetainsUnknownDateAndRollsBackConflict(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool, true)
	previewCatalogFixture(t, store, previewRunID, nil)
	previewMaterialFixture(t, store, previewRunID, []rebuild.MaterialRow{{
		MaterialID: fixtureMaterialID(1), RevisionID: fixtureMaterialRevision, Title: materialStampTitle,
	}}, nil)
	report, err := store.CompareFeedPreview(ctx, previewRunID)
	if err != nil || report.Matches || report.UnresolvedMaterialDates != 1 || report.MaterialCards != 0 {
		t.Fatal("treated a material without a date as ready", report, err)
	}

	const conflictRun = "fixture-preview-conflict"
	previewCatalogFixture(t, store, conflictRun, nil)
	events := previewMaterialEvents()
	events[2].Version++
	previewMaterialFixture(t, store, conflictRun, nil, events)
	if _, err = store.CompareFeedPreview(ctx, conflictRun); !errors.Is(err, repository.ErrFeedPreviewConflict) {
		t.Fatal("accepted divergent events with the same ID", err)
	}
	var saved bool
	if err = pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM feed_rebuild_preview_runs WHERE run_id = $1)", conflictRun).
		Scan(&saved); err != nil || saved {
		t.Error("a failed preview left a saved run", saved, err)
	}
}
