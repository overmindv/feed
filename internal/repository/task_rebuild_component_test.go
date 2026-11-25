package repository_test

import (
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
	rebuildTaskID = "de210000-0000-0000-0000-000000000001"
	rebuildRunID  = "fixture-task-rebuild"
)

func rebuildChange(revision int64, eventType string) rebuild.TaskChange {
	change := rebuild.TaskChange{
		Revision: revision, EventID: fmt.Sprintf("de220000-0000-0000-0000-%012d", revision),
		TaskID: rebuildTaskID, EventType: eventType,
		OccurredAt:    time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC).Add(time.Duration(revision) * time.Second),
		TaskVersionID: "de210000-0000-0000-0000-000000000002",
	}
	if eventType == rebuild.TaskPublished {
		change.Title = "Fixture task"
	}

	return change
}

//nolint:paralleltest // Uses migrations and a dedicated PostgreSQL schema.
func TestTaskRebuildResumesAndLeavesActiveFeedIntact(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool, false)
	initial := rebuildChange(1, rebuild.TaskPublished)
	if err := store.ApplyTaskPublication(ctx, domain.TaskPublication{EventID: initial.EventID, Item: domain.FeedItem{
		AggregateID: rebuildTaskID, OccurredAt: initial.OccurredAt, Title: initial.Title,
		Href: "/tasks/" + rebuildTaskID, RawPayload: []byte("{}"),
	}}); err != nil {
		t.Fatal(err)
	}
	activeBefore, err := store.ListPage(ctx, 20, nil)
	if err != nil || len(activeBefore.Items) != 1 {
		t.Fatal("failed to seed the active feed", err)
	}

	var useDuplicate atomic.Bool
	useDuplicate.Store(true)
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		after := r.URL.Query().Get("after")
		through := 1
		changes := []rebuild.TaskChange{initial}
		if after == "1" {
			through = 3
			hide := rebuildChange(2, rebuild.TaskUnpublished)
			hide.OccurredAt = initial.OccurredAt.Add(-time.Hour)
			publish := rebuildChange(3, rebuild.TaskPublished)
			if useDuplicate.Load() {
				publish.EventID = initial.EventID
			}
			changes = []rebuild.TaskChange{hide, publish}
		}
		if encodeErr := json.NewEncoder(w).Encode(map[string]any{
			"watermark": 3, "through": through, "more": through != 3, "changes": changes,
		}); encodeErr != nil {
			t.Error(encodeErr)
		}
	}))
	defer sourceServer.Close()
	source, err := rebuild.NewTaskJournal(sourceServer.URL, strings.Repeat("x", 32), true)
	if err != nil {
		t.Fatal(err)
	}
	defer source.CloseIdleConnections()

	first, err := rebuild.StageTasks(ctx, source, store, rebuildRunID, 1)
	if err != nil || first.Through != 1 || first.Watermark != 3 {
		t.Fatal("first page was not staged", first, err)
	}
	if _, stageErr := rebuild.StageTasks(ctx, source, store, rebuildRunID, 1); stageErr == nil {
		t.Fatal("stored a second revision with the same event ID")
	}
	failed, found, err := store.LoadTaskCheckpoint(ctx, rebuildRunID)
	if err != nil || !found || failed != first {
		t.Fatal("failed batch advanced the checkpoint", failed, err)
	}
	var count int
	if countErr := pool.QueryRow(ctx, "SELECT count(*) FROM feed_rebuild_task_changes WHERE run_id = $1",
		rebuildRunID).Scan(&count); countErr != nil || count != 1 {
		t.Fatal("failed batch left partial rows", count, countErr)
	}

	useDuplicate.Store(false)
	done, err := rebuild.StageTasks(ctx, source, store, rebuildRunID, 1)
	if err != nil || done.Through != 3 {
		t.Fatal("did not complete after retry", done, err)
	}
	var kind string
	if kindErr := pool.QueryRow(ctx, "SELECT event_type FROM feed_rebuild_task_changes WHERE run_id = $1 AND revision = 2",
		rebuildRunID).Scan(&kind); kindErr != nil || kind != rebuild.TaskUnpublished {
		t.Fatal("did not preserve the withdrawal", kind, kindErr)
	}
	if staleErr := store.SaveTaskPage(ctx, rebuildRunID, 0, rebuild.TaskPage{TaskCheckpoint: first}); !errors.Is(staleErr, repository.ErrTaskRebuildAdvanced) {
		t.Fatal("stale writer did not stop", staleErr)
	}
	activeAfter, err := store.ListPage(ctx, 20, nil)
	if err != nil || !reflect.DeepEqual(activeBefore, activeAfter) {
		t.Error("the shadow import changed the active feed", err)
	}
}

func BenchmarkTaskRebuildPage(b *testing.B) {
	pool := feedTestPool(b)
	store := repository.New(pool, false)
	const size = 100
	page := rebuild.TaskPage{
		TaskCheckpoint: rebuild.TaskCheckpoint{Watermark: size, Through: size},
		Changes:        make([]rebuild.TaskChange, size),
	}
	for i := range page.Changes {
		page.Changes[i] = rebuildChange(int64(i+1), rebuild.TaskPublished)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if err := store.SaveTaskPage(b.Context(), fmt.Sprintf("task-bench-%d", i), 0, page); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
