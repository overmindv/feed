package consumer

import (
	"encoding/json"
	"testing"
	"time"
)

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func TestRenderCatalogCreated(t *testing.T) {
	ts := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	value := mustJSON(t, map[string]any{
		"event_type":     "catalog.course.created",
		"occurred_at":    ts,
		"actor_user_id":  "11111111-1111-1111-1111-111111111111",
		"aggregate_type": "course",
		"aggregate_id":   "22222222-2222-2222-2222-222222222222",
		"payload": map[string]any{
			"name": "Алгоритмы", "status": "draft",
		},
	})

	item, ok, err := Render("", value)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !ok {
		t.Fatal("created должен попадать в ленту")
	}
	if item.Kind != "course.created" {
		t.Errorf("kind = %q, want course.created", item.Kind)
	}
	if item.Title != "Новый курс: Алгоритмы" {
		t.Errorf("title = %q", item.Title)
	}
	if item.Href != "/courses/22222222-2222-2222-2222-222222222222" {
		t.Errorf("href = %q", item.Href)
	}
	if item.Source != "catalog" || item.EventKey != "22222222-2222-2222-2222-222222222222" {
		t.Errorf("source/event_key = %q/%q", item.Source, item.EventKey)
	}
}

func TestRenderCatalogActivated(t *testing.T) {
	value := mustJSON(t, map[string]any{
		"event_type":     "catalog.university.updated",
		"occurred_at":    time.Now().UTC(),
		"actor_user_id":  "11111111-1111-1111-1111-111111111111",
		"aggregate_type": "university",
		"aggregate_id":   "33333333-3333-3333-3333-333333333333",
		"payload": map[string]any{
			"name": "МГУ", "status": "active",
		},
	})

	item, ok, err := Render("", value)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !ok {
		t.Fatal("activated должен попадать в ленту")
	}
	if item.Kind != "university.activated" {
		t.Errorf("kind = %q, want university.activated", item.Kind)
	}
	if item.Title != "Активирован университет: МГУ" {
		t.Errorf("title = %q", item.Title)
	}
}

func TestRenderCatalogFiltered(t *testing.T) {
	cases := []map[string]any{
		{ // deleted — не в ленту
			"event_type": "catalog.topic.deleted", "occurred_at": time.Now().UTC(),
			"actor_user_id":  "11111111-1111-1111-1111-111111111111",
			"aggregate_type": "topic", "aggregate_id": "44444444-4444-4444-4444-444444444444",
			"payload": map[string]any{"id": "x"},
		},
		{ // updated, но не active (черновик) — не в ленту
			"event_type": "catalog.course.updated", "occurred_at": time.Now().UTC(),
			"actor_user_id":  "11111111-1111-1111-1111-111111111111",
			"aggregate_type": "course", "aggregate_id": "55555555-5555-5555-5555-555555555555",
			"payload": map[string]any{"name": "X", "status": "draft"},
		},
		{ // prerequisite — не в ленту
			"event_type": "catalog.topic.prerequisite.added", "occurred_at": time.Now().UTC(),
			"actor_user_id":  "11111111-1111-1111-1111-111111111111",
			"aggregate_type": "topic", "aggregate_id": "66666666-6666-6666-6666-666666666666",
			"payload": map[string]any{},
		},
	}
	for i, tc := range cases {
		value := mustJSON(t, tc)
		_, ok, err := Render("", value)
		if err != nil {
			t.Fatalf("case %d: render: %v", i, err)
		}
		if ok {
			t.Errorf("case %d: ожидали фильтрацию (ok=false), получено ok=true", i)
		}
	}
}

func TestRenderTaskPublished(t *testing.T) {
	value := mustJSON(t, map[string]any{
		"event_type":  "task.published",
		"occurred_at": time.Now().UTC(),
		"task_id":     "77777777-7777-7777-7777-777777777777",
		"title":       "Двоичный поиск",
		"topic_id":    nil,
		"task_type":   "programming",
		"difficulty":  "easy",
	})

	item, ok, err := Render("", value)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !ok {
		t.Fatal("task.published должен попадать в ленту")
	}
	if item.Kind != "task.published" {
		t.Errorf("kind = %q, want task.published", item.Kind)
	}
	if item.Title != "Новая задача: Двоичный поиск" {
		t.Errorf("title = %q", item.Title)
	}
	if item.Href != "/tasks/77777777-7777-7777-7777-777777777777" {
		t.Errorf("href = %q", item.Href)
	}
	if item.Source != "task" || item.EventKey != "77777777-7777-7777-7777-777777777777" {
		t.Errorf("source/event_key = %q/%q", item.Source, item.EventKey)
	}
}

func TestRenderUnknownEventType(t *testing.T) {
	value := mustJSON(t, map[string]any{"event_type": "other.thing", "occurred_at": time.Now().UTC()})
	if _, _, err := Render("", value); err == nil {
		t.Fatal("ожидали ошибку для неизвестного event_type")
	}
}
