package repository_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/httpapi"
	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/feed/internal/service"
	"github.com/overmindv/parker"
)

const (
	catalogTopic      = "catalog.activity.v1"
	taskTopic         = "task.activity.v1"
	activityTestGroup = "feed-fixture-v1"
)

func feedTestPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool, migrate := feedTestSchema(t)
	migrate("../../migrations")

	return pool
}

func feedTestSchema(t testing.TB) (*pgxpool.Pool, func(string)) {
	t.Helper()
	dsn := os.Getenv("FEED_TEST_DSN")
	if dsn == "" {
		t.Skip("FEED_TEST_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("FEED_TEST_DSN must point to a dedicated database ending in _test")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var schema string
	if scanErr := admin.QueryRow(t.Context(), "SELECT 'feed_test_' || replace(gen_random_uuid()::text, '-', '')").Scan(&schema); scanErr != nil {
		t.Fatal(scanErr)
	}
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, createErr := admin.Exec(t.Context(), "CREATE SCHEMA "+quotedSchema); createErr != nil {
		t.Fatal(createErr)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, dropErr := admin.Exec(ctx, "DROP SCHEMA "+quotedSchema+" CASCADE"); dropErr != nil {
			t.Error(dropErr)
		}
	})
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	migrationConn := stdlib.RegisterConnConfig(cfg.ConnConfig)
	t.Cleanup(func() { stdlib.UnregisterConnConfig(migrationConn) })
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool, func(dir string) {
		t.Helper()
		if err := parker.RunMigrate(dir, migrationConn, "up"); err != nil {
			t.Fatal(err)
		}
	}
}

func feedMigrationsBeforeInbox(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"000001_feed_items.sql", "000002_feed_items_actor_and_idempotency.sql",
		"000003_catalog_visibility.sql", "000004_feed_public_window.sql"} {
		source, err := os.ReadFile(filepath.Join("../../migrations", name)) //nolint:gosec // name задан локальным allowlist
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), source, 0600); err != nil { //nolint:gosec // name задан локальным allowlist
			t.Fatal(err)
		}
	}

	return dir
}

//nolint:paralleltest // component-тест использует PostgreSQL и миграции
func TestFeedDoesNotUnhideOldCatalogCards(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool, false)
	if _, err := pool.Exec(ctx, `
		INSERT INTO feed_items (source,event_key,kind,aggregate_type,aggregate_id,occurred_at,title,href,raw_payload)
		VALUES ('catalog','old','course.created','course','old',now(),'old legacy','/courses/old','{}')`); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	handle := consumer.NewHandler(store, log, catalogTopic, taskTopic, "", activityTestGroup)
	token := strings.Repeat("t", 32)
	api := http.NewServeMux()
	httpapi.Register(api, service.New(store), token, log)
	deliver := func(key string, message map[string]any) {
		t.Helper()
		data, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		topic := catalogTopic
		if message["event_type"] == "task.published" {
			topic = taskTopic
		}
		if err := handle(ctx, domain.EventPosition{Topic: topic}, key, data); err != nil {
			t.Fatal(err)
		}
	}
	const task = "aaaa1111-1111-1111-1111-111111111111"
	deliver(task, map[string]any{
		"event_type":  "task.published",
		"task_id":     task,
		"occurred_at": time.Now().UTC(),
		"title":       "test task",
	})
	const course = "11111111-1111-1111-1111-111111111111"
	const topic = "22222222-2222-2222-2222-222222222222"
	const child = "33333333-3333-3333-3333-333333333333"
	coursePath, topicPath, childPath := "/courses/"+course, "/topics/"+topic, "/topics/"+child
	taskPath := "/tasks/" + task
	change := func(kind, id, action string, rev, version int, public, reset bool, ancestors ...string) map[string]any {
		if ancestors == nil {
			ancestors = []string{}
		}

		return map[string]any{
			"event_type":        "catalog." + kind + "." + action,
			"event_id":          fmt.Sprintf("53333333-3333-3333-3333-%012x", 1000-rev),
			"occurred_at":       time.Now().UTC(),
			"aggregate_type":    kind,
			"aggregate_id":      id,
			"catalog_revision":  rev,
			"aggregate_version": version,
			"feed_visibility": map[string]any{
				"visible":     public,
				"scope_reset": reset,
				"ancestors":   ancestors,
			},
			"payload": map[string]any{"status": "active", "name": kind + " fixture"},
		}
	}
	list := func(tokens []string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/v1/feed", nil)
		for _, value := range tokens {
			req.Header.Add("X-Feed-Service-Token", value)
		}
		res := httptest.NewRecorder()
		api.ServeHTTP(res, req)

		return res
	}
	for _, tokens := range [][]string{nil, {"wrong"}, {token, token}} {
		if got := list(tokens).Code; got != http.StatusForbidden {
			t.Error("feed accepted an unauthorized request", got)
		}
	}
	assertCards := func(want ...string) {
		t.Helper()
		res := list([]string{token})
		if res.Code != http.StatusOK || res.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("feed status or cache policy", res.Code)
		}
		var result struct {
			Items []struct {
				Href string `json:"href"`
			} `json:"items"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		seen := make(map[string]bool)
		count := 0
		for _, row := range result.Items {
			switch row.Href {
			case taskPath, coursePath, topicPath, childPath, "/courses/old":
				seen[row.Href] = true
				count++
			}
		}
		if count != len(want)+1 {
			t.Error("unexpected fixture card count", count, "want", len(want)+1, seen)
		}
		for _, href := range append(want, taskPath) {
			if !seen[href] {
				t.Error("missing card", href)
			}
		}
	}
	originalCourse := change("course", course, "created", 1, 1, true, false)
	originalTopic := change("topic", topic, "created", 2, 1, true, false, "course:"+course)
	deliver(course, originalCourse)
	deliver(topic, originalTopic)
	assertCards(coursePath, topicPath)

	hide := change("course", course, "unpublished", 4, 2, false, false)
	deliver(course, hide)
	deliver(course, originalCourse)
	deliver(topic, originalTopic)
	deliver(child, change("topic", child, "created", 3, 1, true, false, "topic:"+topic, "course:"+course))
	assertCards()

	deliver(course, change("course", course, "updated", 5, 3, true, false))
	deliver(course, hide)
	assertCards(coursePath)
	deliver(topic, change("topic", topic, "updated", 6, 2, true, false, "course:"+course))
	freshChild := change("topic", child, "updated", 7, 2, true, false, "topic:"+topic, "course:"+course)
	deliver(child, freshChild)
	deliver(child, freshChild)
	assertCards(coursePath, topicPath, childPath)

	deliver(topic, change("topic", topic, "updated", 8, 3, true, true, "course:"+course))
	deliver(child, freshChild)
	assertCards(coursePath, topicPath)

	deliver(course, change("course", course, "updated", 10, 5, true, false))
	deliver(child, change("topic", child, "updated", 11, 3, true, false, "topic:"+topic, "course:"+course))
	deliver(course, change("course", course, "unpublished", 9, 4, false, false))
	assertCards(coursePath, childPath)

	deliver(course, change("course", course, "unpublished", 12, 6, false, false))
	deliver(topic, change("topic", topic, "unpublished", 13, 4, false, false))
	deliver(course, change("course", course, "updated", 14, 7, true, false))
	deliver(child, change("topic", child, "updated", 15, 4, true, false, "topic:"+topic, "course:"+course))
	assertCards(coursePath, childPath)
}
