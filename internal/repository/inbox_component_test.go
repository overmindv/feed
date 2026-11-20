package repository_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/repository"
)

func TestInboxSeparatesPublicationFromDelivery(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool)
	handle := consumer.NewHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	const taskID = "ded10000-0000-0000-0000-000000000001"
	const firstID = "ded10000-0000-0000-0000-000000000002"
	const secondID = "ded10000-0000-0000-0000-000000000003"
	const courseID = "ded10000-0000-0000-0000-000000000004"
	const retryTask = "ded10000-0000-0000-0000-000000000005"
	const retryID = "ded10000-0000-0000-0000-000000000006"
	now := time.Now().UTC()

	wire := func(values map[string]any) []byte {
		t.Helper()
		data, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}

		return data
	}
	published := func(task, eventID, title string) []byte {
		t.Helper()
		values := map[string]any{
			"event_type":  "task.published",
			"task_id":     task,
			"occurred_at": now,
			"title":       title,
		}
		if eventID != "" {
			values["event_id"] = eventID
		}

		return wire(values)
	}
	deliver := func(key string, data []byte) {
		t.Helper()
		if err := handle(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}

	legacy := published(taskID, "", "legacy")
	deliver(taskID, legacy)
	deliver(taskID, legacy)
	first := published(taskID, firstID, "first publication")
	results := make(chan error, 8)
	for range cap(results) {
		go func() { results <- handle(ctx, taskID, first) }()
	}
	for range cap(results) {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	deliver(taskID, published(taskID, secondID, "second publication"))

	// UUID совпадает между источниками, но повтор каталога не меняет состояние.
	catalog := map[string]any{
		"event_type":        "catalog.course.created",
		"event_id":          secondID,
		"aggregate_type":    "course",
		"aggregate_id":      courseID,
		"aggregate_version": 1,
		"catalog_revision":  1001,
		"occurred_at":       now,
		"payload":           map[string]any{"status": "active", "name": "Public course"},
		"feed_visibility":   map[string]any{"visible": true, "ancestors": []string{}},
	}
	deliver(courseID, wire(catalog))
	catalog["event_type"] = "catalog.course.unpublished"
	catalog["aggregate_version"], catalog["catalog_revision"] = 2, 1002
	deliver(courseID, wire(catalog))

	retry := published(retryTask, retryID, "retry publication")
	if _, err := pool.Exec(ctx, `ALTER TABLE feed_items ADD CONSTRAINT fixture_reject_publication
		CHECK (aggregate_id <> 'ded10000-0000-0000-0000-000000000005') NOT VALID`); err != nil {
		t.Fatal(err)
	}
	if err := handle(ctx, retryTask, retry); err == nil {
		t.Fatal("expected fixture to reject the card write")
	}
	var recorded bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM feed_event_inbox WHERE source='task' AND event_id=$1)", retryID).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded {
		t.Error("inbox recorded a publication that did not commit")
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE feed_items DROP CONSTRAINT fixture_reject_publication"); err != nil {
		t.Fatal(err)
	}
	deliver(retryTask, retry)
	deliver(retryTask, retry)

	rows, err := store.ListItems(ctx, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		taskID:               "Новая задача: legacy",
		"event:" + firstID:   "Новая задача: first publication",
		"event:" + secondID:  "Новая задача: second publication",
		"event:" + retryID:   "Новая задача: retry publication",
		"course:" + courseID: "Новый курс: Public course",
	}
	count := 0
	for _, row := range rows {
		if row.AggregateID != taskID && row.AggregateID != retryTask && row.AggregateID != courseID {
			continue
		}
		count++
		if expected[row.EventKey] != row.Title {
			t.Error("unexpected card in inbox fixture", row.EventKey, row.Title)
		}
		delete(expected, row.EventKey)
	}
	if len(expected) > 0 || count != 5 {
		t.Error("wrong publication count or missing history", count, expected)
	}
	var inboxCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM feed_event_inbox WHERE event_id IN ($1,$2,$3)", firstID, secondID, retryID).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if inboxCount != 4 {
		t.Error("inbox lost a source namespace or counted a duplicate", inboxCount)
	}
}

func TestInboxMigrationKeepsOldPublicationIdentity(t *testing.T) {
	ctx := t.Context()
	pool, migrate := feedTestSchema(t)
	migrate(feedMigrationsBeforeInbox(t))

	const taskID = "ded10000-0000-0000-0000-000000000007"
	var cardID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO feed_items (source,event_key,kind,aggregate_type,aggregate_id,occurred_at,title,href,raw_payload)
		VALUES ('task',$1,'task.published','task',$1,now(),'Old card','/tasks/'||$1,'{}')
		RETURNING id::text`, taskID).Scan(&cardID); err != nil {
		t.Fatal(err)
	}
	migrate("../../migrations")

	store := repository.New(pool)
	handle := consumer.NewHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	value, err := json.Marshal(map[string]any{
		"event_type":  "task.published",
		"task_id":     taskID,
		"occurred_at": time.Now().UTC(),
		"title":       "Old event",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle(ctx, taskID, value); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListItems(ctx, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != cardID || rows[0].Title != "Old card" {
		t.Error("migration or replay changed the old card", rows)
	}
}
