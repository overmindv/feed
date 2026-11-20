package repository_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/httpapi"
	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/feed/internal/service"
)

func feedTestPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("FEED_TEST_DSN")
	if dsn == "" {
		t.Skip("FEED_TEST_DSN is not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("FEED_TEST_DSN must point to a dedicated database ending in _test")
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func TestFeedDoesNotUnhideOldCatalogCards(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool)
	if _, err := pool.Exec(ctx, `
		INSERT INTO feed_items (source,event_key,kind,aggregate_type,aggregate_id,occurred_at,title,href,raw_payload)
		VALUES ('catalog','old','course.created','course','old',now(),'old legacy','/courses/old','{}')`); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	handle := consumer.NewHandler(store, log)
	token := strings.Repeat("t", 32)
	api := http.NewServeMux()
	httpapi.Register(api, service.New(store), token, log)
	deliver := func(key string, message map[string]any) {
		t.Helper()
		data, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := handle(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}
	deliver("task-fixture", map[string]any{
		"event_type":  "task.published",
		"task_id":     "task-fixture",
		"occurred_at": time.Now().UTC(),
		"title":       "test task",
	})
	const course = "11111111-1111-1111-1111-111111111111"
	const topic = "22222222-2222-2222-2222-222222222222"
	const child = "33333333-3333-3333-3333-333333333333"
	change := func(kind, id, action string, rev, version int, public, reset bool, ancestors ...string) map[string]any {
		if ancestors == nil {
			ancestors = []string{}
		}

		return map[string]any{
			"event_type":        "catalog." + kind + "." + action,
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
		if len(result.Items) != len(want)+1 {
			t.Error("unexpected card count", len(result.Items), "want", len(want)+1, res.Body.String())
		}
		seen := make(map[string]bool)
		for _, row := range result.Items {
			seen[row.Href] = true
		}
		for _, href := range append(want, "/tasks/task-fixture") {
			if !seen[href] {
				t.Error("missing card", href)
			}
		}
	}
	coursePath, topicPath, childPath := "/courses/"+course, "/topics/"+topic, "/topics/"+child
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
