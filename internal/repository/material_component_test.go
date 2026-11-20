package repository_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/httpapi"
	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/feed/internal/service"
)

const (
	materialTopic  = "content.material.v1"
	bookID         = "e1100000-0000-4000-8000-000000000001"
	revisionID     = "e1100000-0000-4000-8000-000000000002"
	fixtureHeading = "Динамическое программирование"
	privateMarker  = "fixture-private-user-detail"
	publishedKind  = "material.published"
)

func materialWire(t *testing.T, seq, version int, archive bool) []byte {
	t.Helper()

	kind := publishedKind
	payload := map[string]any{"tombstone": archive}
	if archive {
		kind = "material.archived"
	} else {
		payload["metadata"] = map[string]string{"title": fixtureHeading, "description": privateMarker}
	}
	value, err := json.Marshal(map[string]any{
		"event_id":          fmt.Sprintf("e1100000-0000-4000-8000-%012x", seq+100),
		"event_type":        kind,
		"schema_version":    1,
		"occurred_at":       time.Date(2026, 11, 20, 12, seq, 0, 0, time.UTC),
		"aggregate_id":      bookID,
		"aggregate_version": version,
		"revision_id":       revisionID,
		"payload":           payload,
	})
	if err != nil {
		t.Fatal(err)
	}

	return value
}

//nolint:paralleltest // component-тест применяет миграции к PostgreSQL
func TestMaterialFeedLifecycle(t *testing.T) {
	pool := feedTestPool(t)
	store := repository.New(pool, true)
	log := slog.New(slog.DiscardHandler)
	handle := consumer.NewHandler(store, log, catalogTopic, taskTopic, materialTopic)
	api := http.NewServeMux()
	gatewayToken := strings.Repeat("f", 32)
	httpapi.Register(api, service.New(store), gatewayToken, log)

	deliver := func(offset int64, body []byte) {
		t.Helper()
		pos := domain.EventPosition{Topic: materialTopic, Offset: offset}
		if err := handle(t.Context(), pos, bookID, body); err != nil {
			t.Fatal(err)
		}
	}
	read := func() []struct{ ID, Href, Kind, Title, ActorUserID string } {
		t.Helper()
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/v1/feed?limit=1", nil)
		r.Header.Set("X-Feed-Service-Token", gatewayToken)
		api.ServeHTTP(w, r)
		if w.Code != http.StatusOK || strings.Contains(w.Body.String(), privateMarker) {
			t.Fatal("unexpected feed response", w.Code)
		}
		var result struct {
			Items []struct{ ID, Href, Kind, Title, ActorUserID string } `json:"items"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}

		return result.Items
	}

	original := materialWire(t, 1, 10, false)
	deliver(1, original)
	deliver(2, original)
	cards := read()
	if len(cards) != 1 || cards[0].Kind != publishedKind || cards[0].Title != "Опубликован материал: "+fixtureHeading ||
		cards[0].Href != "/materials/"+bookID || cards[0].ActorUserID != "" {
		t.Fatal("invalid public card", cards)
	}
	withoutMaterials, err := repository.New(pool, false).ListItems(t.Context(), 10, 0)
	if err != nil || len(withoutMaterials) != 0 {
		t.Fatal("disabled activity must not expose saved cards", err)
	}
	unknown := `{"event_type":"material.read_confirmed","payload":{"user_id":"fixture-private-user-detail"}}`
	deliver(3, []byte(unknown))
	var reason string
	if scanErr := pool.QueryRow(t.Context(), "SELECT reason FROM feed_event_quarantine").Scan(&reason); scanErr != nil || reason != "unknown_type" {
		t.Fatal(reason, scanErr)
	}

	deliver(4, materialWire(t, 2, 11, true))
	deliver(5, materialWire(t, 3, 9, false))
	deliver(6, materialWire(t, 4, 11, false))
	if archived := read(); len(archived) != 0 {
		t.Fatal("a late publication undid the archive", archived)
	}
	deliver(7, materialWire(t, 5, 12, false))
	found := read()
	if len(found) != 1 || found[0].ID != cards[0].ID || found[0].Href != cards[0].Href {
		t.Fatal("new publication must replace the previous card", found)
	}
	badArchive := bytes.Replace(materialWire(t, 6, 13, true), []byte(`"schema_version":1`), []byte(`"schema_version":2`), 1)
	deliver(8, badArchive)
	const taskID = "e1100000-0000-4000-8000-000000000003"
	task := []byte(`{"event_type":"task.published","task_id":"e1100000-0000-4000-8000-000000000003","occurred_at":"2026-11-20T11:00:00Z"}`)
	if handleErr := handle(t.Context(), domain.EventPosition{Topic: taskTopic}, taskID, task); handleErr != nil {
		t.Fatal(handleErr)
	}
	if visible := read(); len(visible) != 1 || visible[0].Href != "/tasks/"+taskID {
		t.Fatal("a quarantined lifecycle change must hide only materials", visible)
	}
}

//nolint:paralleltest // component-тест применяет миграции к PostgreSQL
func TestMaterialReplayAndConcurrentArchive(t *testing.T) {
	pool := feedTestPool(t)
	store := repository.New(pool, true)
	handle := consumer.NewHandler(store, slog.New(slog.DiscardHandler), catalogTopic, taskTopic, materialTopic)
	valid := materialWire(t, 6, 10, false)
	invalid := []byte(strings.Replace(string(valid), revisionID, "invalid-revision", 1))
	p := domain.EventPosition{Topic: materialTopic, Offset: 1}
	if err := handle(t.Context(), p, bookID, invalid); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Replay(t.Context(), store, p, valid, consumer.ReplayOptions{}); !errors.Is(err, consumer.ErrMaterialUnconfirmed) {
		t.Fatal("publication replay must be verified with Content", err)
	}
	verified := consumer.ReplayOptions{ConfirmMaterialPublication: true}
	wrong := materialWire(t, 9, 10, false)
	_, execErr := pool.Exec(t.Context(), `
		ALTER TABLE feed_material_state ADD CONSTRAINT fixture_deny_write CHECK (aggregate_version < 10) NOT VALID`)
	if execErr != nil {
		t.Fatal(execErr)
	}
	if err := consumer.Replay(t.Context(), store, p, valid, verified); err == nil {
		t.Fatal("replay did not report storage failure")
	}
	var inbox int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM feed_event_inbox").Scan(&inbox); err != nil || inbox != 0 {
		t.Fatal("failed update must roll back the inbox", inbox, err)
	}
	if _, err := pool.Exec(t.Context(), "ALTER TABLE feed_material_state DROP CONSTRAINT fixture_deny_write"); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Replay(t.Context(), store, p, wrong, verified); !errors.Is(err, consumer.ErrIdentityMismatch) {
		t.Fatal("replay changed the event identity", err)
	}
	if err := consumer.Replay(t.Context(), store, p, valid, verified); err != nil {
		t.Fatal(err)
	}

	replayed, err := store.ListItems(t.Context(), 50, 0)
	if err != nil || len(replayed) != 1 {
		t.Fatal("verified replay must clear the quarantine hold", replayed, err)
	}

	ctx := t.Context()
	done := make(chan error, 2)
	for i, body := range [][]byte{materialWire(t, 7, 11, true), materialWire(t, 8, 11, false)} {
		go func() {
			done <- handle(ctx, domain.EventPosition{Topic: materialTopic, Offset: int64(i + 2)}, bookID, body)
		}()
	}
	for range cap(done) {
		if handleErr := <-done; handleErr != nil {
			t.Fatal(handleErr)
		}
	}
	cards, err := store.ListItems(ctx, 50, 0)
	if err != nil || len(cards) != 0 {
		t.Fatal("the archive must win when versions tie", cards, err)
	}
}

func BenchmarkFeedMaterialWindow(b *testing.B) {
	pool := feedTestPool(b)
	ctx := b.Context()
	seed := []string{
		`INSERT INTO feed_items (source,event_key,kind,aggregate_type,aggregate_id,occurred_at,title,href,raw_payload,catalog_revision,visibility_keys)
		SELECT CASE WHEN n % 2 = 0 THEN 'task' ELSE 'catalog' END, n::text,
			CASE WHEN n % 2 = 0 THEN 'task.published' ELSE 'topic.created' END,
			CASE WHEN n % 2 = 0 THEN 'task' ELSE 'topic' END, n::text, now() - n * interval '1 second',
			'fixture', '/'||n::text, '{}'::jsonb,
			CASE WHEN n % 2 = 1 THEN n END, CASE WHEN n % 2 = 1 THEN ARRAY['topic:'||n::text] END
		FROM generate_series(1,50000) n`,
		`INSERT INTO feed_catalog_visibility (resource_key, catalog_revision, aggregate_version, visible, fence_revision)
		SELECT 'topic:'||n::text, n + 1, 2, false, n + 1 FROM generate_series(1,50000,14) n`,
		`INSERT INTO feed_material_state (material_id, aggregate_version, revision_id, title, occurred_at)
		SELECT gen_random_uuid(), n, CASE WHEN n <= 200 THEN gen_random_uuid() END,
			CASE WHEN n <= 200 THEN 'fixture' END, now() - n * interval '1 second'
		FROM generate_series(1,50000) n`,
		"ANALYZE feed_items",
		"ANALYZE feed_material_state",
		"ANALYZE feed_catalog_visibility",
	}
	for _, statement := range seed {
		if _, err := pool.Exec(ctx, statement); err != nil {
			b.Fatal(err)
		}
	}
	for _, offset := range []int{0, 500} {
		b.Run(fmt.Sprintf("offset_%d", offset), func(b *testing.B) {
			b.Run("without_material", func(b *testing.B) {
				measureFeedWindow(b, repository.New(pool, false), offset)
			})
			b.Run("with_material", func(b *testing.B) {
				measureFeedWindow(b, repository.New(pool, true), offset)
			})
		})
	}
}

func measureFeedWindow(b *testing.B, store *repository.Postgres, offset int) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		rows, err := store.ListItems(b.Context(), 50, offset)
		if err != nil || len(rows) != 50 {
			b.Fatal("invalid feed window", err)
		}
	}
}
