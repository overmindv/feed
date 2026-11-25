package rebuild

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	taskFixtureID = "de110000-0000-0000-0000-000000000001"
	versionID     = "de110000-0000-0000-0000-000000000002"
	fixtureRun    = "task-rebuild-test"
)

func taskChange(revision int64, eventType string) TaskChange {
	change := TaskChange{
		Revision: revision, EventID: fmt.Sprintf("de120000-0000-0000-0000-%012d", revision),
		EventType: eventType, TaskID: taskFixtureID, TaskVersionID: versionID,
		OccurredAt: time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC),
	}
	if eventType == TaskPublished {
		change.Title = "Fixture task"
	}

	return change
}

type taskStageFixture struct {
	checkpoint TaskCheckpoint
	saved      []TaskChange
	found      bool
}

func (s *taskStageFixture) LoadTaskCheckpoint(context.Context, string) (TaskCheckpoint, bool, error) {
	return s.checkpoint, s.found, nil
}

func (s *taskStageFixture) SaveTaskPage(_ context.Context, _ string, _ int64, page TaskPage) error {
	s.checkpoint, s.found = page.TaskCheckpoint, true
	s.saved = append(s.saved, page.Changes...)

	return nil
}

func TestStageTasksResumesAtTheSameWatermark(t *testing.T) {
	t.Parallel()

	token := strings.Repeat("x", 32)
	calls := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.Method != http.MethodGet || r.URL.Path != taskJournalPath ||
			r.Header.Get("Authorization") != "Bearer "+token || query.Get("limit") != "100" {
			t.Error("unexpected Tasks journal request")
		}
		call := calls.Add(1)
		if (call == 1 && (query.Get("after") != "0" || query.Has("watermark"))) ||
			(call == 2 && (query.Get("after") != "1" || query.Get("watermark") != "2")) {
			t.Error("unexpected journal cursor", query)
		}
		eventType := TaskPublished
		if call == 2 {
			eventType = TaskUnpublished
		}
		if err := json.NewEncoder(w).Encode(map[string]any{
			"watermark": 2, "through": call, "more": call == 1, "changes": []TaskChange{taskChange(int64(call), eventType)},
		}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	source, err := NewTaskJournal(server.URL, token, true)
	if err != nil {
		t.Fatal(err)
	}
	defer source.CloseIdleConnections()
	store := &taskStageFixture{}
	for _, want := range []int64{1, 2, 2} {
		checkpoint, stageErr := StageTasks(t.Context(), source, store, fixtureRun, 1)
		if stageErr != nil || checkpoint.Through != want || checkpoint.Watermark != 2 {
			t.Fatal("unexpected rebuild checkpoint", checkpoint, stageErr)
		}
	}
	if calls.Load() != 2 || len(store.saved) != 2 || store.saved[1].EventType != TaskUnpublished {
		t.Error("did not preserve journal changes exactly once", store.saved)
	}
}

func TestStageTasksRejectsIncompletePagesBeforeWriting(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		page TaskPage
	}{
		{"changed watermark", TaskPage{TaskCheckpoint: TaskCheckpoint{2, 2}, Changes: []TaskChange{taskChange(2, TaskPublished)}}},
		{"missing middle revision", TaskPage{TaskCheckpoint: TaskCheckpoint{3, 3}, Changes: []TaskChange{taskChange(3, TaskPublished)}}},
		{"out of order", TaskPage{TaskCheckpoint: TaskCheckpoint{3, 3}, Changes: []TaskChange{taskChange(3, TaskPublished), taskChange(2, TaskPublished)}}},
		{"no progress", TaskPage{TaskCheckpoint: TaskCheckpoint{3, 1}, More: true}},
		{"unknown event", TaskPage{TaskCheckpoint: TaskCheckpoint{3, 2}, More: true, Changes: []TaskChange{taskChange(2, "task.updated")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &taskStageFixture{checkpoint: TaskCheckpoint{Watermark: 3, Through: 1}, found: true}
			_, err := StageTasks(t.Context(), taskSourceFunc(func(context.Context, *int64, int64) (TaskPage, error) {
				return tc.page, nil
			}), store, fixtureRun, 1)
			if !errors.Is(err, ErrTaskJournalGap) || len(store.saved) != 0 || store.checkpoint.Through != 1 {
				t.Error("accepted an incomplete source page", err, store.checkpoint)
			}
		})
	}
}

type taskSourceFunc func(context.Context, *int64, int64) (TaskPage, error)

func (fn taskSourceFunc) ReadTaskPage(ctx context.Context, watermark *int64, after int64) (TaskPage, error) {
	return fn(ctx, watermark, after)
}

func TestTaskJournalRejectsRedirectWithoutSendingCredentials(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	token := strings.Repeat("x", 32)
	if _, err := NewTaskJournal(redirect.URL, token, false); err == nil {
		t.Fatal("accepted plaintext without an explicit opt-in")
	}
	source, err := NewTaskJournal(redirect.URL, token, true)
	if err != nil {
		t.Fatal(err)
	}
	defer source.CloseIdleConnections()
	_, err = source.ReadTaskPage(t.Context(), nil, 0)
	if err == nil || hits.Load() != 0 {
		t.Error("followed a Tasks journal redirect", err, hits.Load())
	}
}

func TestTaskJournalBoundsHTTPResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat(" ", maxTaskPage+1)))
	}))
	defer server.Close()
	source, err := NewTaskJournal(server.URL, strings.Repeat("x", 32), true)
	if err != nil {
		t.Fatal(err)
	}
	defer source.CloseIdleConnections()
	if _, err = source.ReadTaskPage(t.Context(), nil, 0); err == nil {
		t.Error("accepted an oversized Tasks response")
	}
}
