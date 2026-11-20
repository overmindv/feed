package consumer

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const courseID = "22222222-2222-2222-2222-222222222222"

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}

	return data
}

func catalogFixture() map[string]any {
	return map[string]any{
		"event_type":        "catalog.course.created",
		"occurred_at":       time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC),
		"actor_user_id":     "11111111-1111-1111-1111-111111111111",
		"aggregate_type":    "course",
		"aggregate_id":      courseID,
		"catalog_revision":  3,
		"aggregate_version": 2,
		"payload":           map[string]any{"name": "Алгоритмы", "status": "active"},
		"feed_visibility": map[string]any{
			"visible":   true,
			"ancestors": []string{"program:33333333-3333-3333-3333-333333333333"},
		},
	}
}

func TestCatalogShowsOnlyConfirmedPublicEvents(t *testing.T) {
	t.Parallel()

	for _, action := range []string{"created", "updated"} {
		fixture := catalogFixture()
		fixture["event_type"] = "catalog.course." + action
		message, err := decodeEvent(courseID, mustJSON(t, fixture))
		if err != nil || message.catalog == nil || message.catalog.Item == nil {
			t.Fatal("public event was not rendered", err)
		}
		item := message.catalog.Item
		if item.Href != "/courses/"+courseID || item.EventKey != "course:"+courseID || len(item.RawPayload) != 0 ||
			item.Text != "Алгоритмы" || message.catalog.CatalogRevision != 3 || message.catalog.AggregateVersion != 2 ||
			len(message.catalog.Ancestors) != 1 {
			t.Error("unexpected public card", item)
		}
		kind := "course.created"
		if action == "updated" {
			kind = "course.activated"
		}
		if item.Kind != kind {
			t.Error("unexpected card kind", item.Kind)
		}
	}
}

func TestCatalogHidesDraftArchiveDeleteAndInheritedVisibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action string
		status string
		public bool
	}{
		{action: "created", status: "draft", public: true},
		{action: "updated", status: "archived", public: true},
		{action: "updated", status: "active", public: false},
		{action: "unpublished", status: "hidden", public: false},
		{action: "deleted", status: "", public: false},
	}
	for _, tc := range tests {
		fixture := catalogFixture()
		fixture["event_type"] = "catalog.course." + tc.action
		fixture["payload"] = map[string]any{"name": "не для ленты", "status": tc.status}
		fixture["feed_visibility"] = map[string]any{"visible": tc.public, "ancestors": []string{}}
		message, err := decodeEvent(courseID, mustJSON(t, fixture))
		if err != nil || message.catalog == nil || message.catalog.Item != nil {
			t.Error("expected visibility fence without card", tc, err)
		}
	}
}

func TestCatalogIgnoresLegacyPublicationAndChecksResourceIdentity(t *testing.T) {
	t.Parallel()

	fixture := catalogFixture()
	delete(fixture, "feed_visibility")
	message, err := decodeEvent(courseID, mustJSON(t, fixture))
	if err != nil || message.catalog == nil || message.catalog.Item != nil {
		t.Error("legacy publication did not fail closed", err)
	}
	delete(fixture, "catalog_revision")
	delete(fixture, "aggregate_version")
	message, err = decodeEvent(courseID, mustJSON(t, fixture))
	if err != nil || message.catalog != nil {
		t.Error("unversioned event did not fail closed", err)
	}
	fixture = catalogFixture()
	if _, err := decodeEvent("other", mustJSON(t, fixture)); err == nil {
		t.Error("mismatched Kafka key accepted")
	}
	fixture["feed_visibility"] = map[string]any{"visible": true, "ancestors": []string{"course:" + courseID}}
	if _, err := decodeEvent(courseID, mustJSON(t, fixture)); err == nil {
		t.Error("resource is its own ancestor")
	}
	fixture["event_type"] = "catalog.topic.created"
	if _, err := decodeEvent(courseID, mustJSON(t, fixture)); err == nil {
		t.Error("mismatched entity type accepted")
	}
}

func TestCatalogDoesNotRenderPrerequisite(t *testing.T) {
	t.Parallel()

	fixture := catalogFixture()
	fixture["aggregate_type"] = "topic"
	fixture["event_type"] = "catalog.topic.prerequisite.added"
	message, err := decodeEvent(courseID, mustJSON(t, fixture))
	if err != nil || message.catalog != nil {
		t.Error("prerequisite affected visibility", err)
	}
}

func TestTaskPublishedRemainsVisible(t *testing.T) {
	t.Parallel()

	id := "77777777-7777-7777-7777-777777777777"
	message, err := decodeEvent(id, mustJSON(t, map[string]any{
		"event_type":  "task.published",
		"task_id":     id,
		"occurred_at": time.Now().UTC(),
		"title":       "Двоичный поиск",
	}))
	if err != nil || message.task == nil {
		t.Fatal("task publication was not rendered", err)
	}
	item := message.task.Item
	if item.Title != "Новая задача: Двоичный поиск" || item.Source != "task" ||
		item.EventKey != id || item.Href != "/tasks/"+id || message.task.EventID != "" {
		t.Error("unexpected task card", item)
	}
}

func TestEventIdentityFromBothSources(t *testing.T) {
	t.Parallel()

	const id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
	catalog := catalogFixture()
	catalog["event_id"] = id
	message, err := decodeEvent(courseID, mustJSON(t, catalog))
	if err != nil || message.catalog == nil || message.catalog.EventID != id {
		t.Fatal("catalog event lost its identity", err)
	}

	task := map[string]any{
		"event_type":  "task.published",
		"event_id":    id,
		"task_id":     courseID,
		"occurred_at": time.Now().UTC(),
		"title":       "Задача",
	}
	message, err = decodeEvent(courseID, mustJSON(t, task))
	if err != nil || message.task == nil || message.task.EventID != id {
		t.Fatal("task event lost its identity", err)
	}
	if _, err := decodeEvent(id, mustJSON(t, task)); err == nil {
		t.Error("event ID was accepted as the Kafka key instead of task ID")
	}
	for _, bad := range []string{"", "invalid", "00000000-0000-0000-0000-000000000000"} {
		task["event_id"] = bad
		catalog["event_id"] = bad
		for _, fixture := range []map[string]any{task, catalog} {
			if _, err := decodeEvent(courseID, mustJSON(t, fixture)); err == nil {
				t.Error("invalid event identity accepted", bad)
			}
		}
	}
	task["event_id"] = id
	task["event_type"] = "task.unpublished"
	if message, err := decodeEvent(courseID, mustJSON(t, task)); err != nil || message.task != nil {
		t.Error("unpublished event was rendered", err)
	}
}

func TestUnknownAndOversizedEvents(t *testing.T) {
	t.Parallel()

	for _, value := range [][]byte{[]byte("{\"event_type\":\"other.thing\"}"), []byte(strings.Repeat(" ", (1<<20)+1))} {
		if _, err := decodeEvent("", value); err == nil {
			t.Error("invalid event was accepted")
		}
	}
}
