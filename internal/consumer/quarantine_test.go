package consumer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/overmindv/feed/internal/domain"
)

type activityFixture struct {
	rejections []domain.EventRejection
	saveErr    error
	applyErr   error
	applied    int
}

func (f *activityFixture) ApplyMaterialChange(context.Context, domain.MaterialChange) error {
	f.applied++

	return f.applyErr
}

func (f *activityFixture) ApplyCatalogChange(context.Context, domain.CatalogChange) error {
	f.applied++

	return f.applyErr
}

func (f *activityFixture) ApplyTaskPublication(context.Context, domain.TaskPublication) error {
	f.applied++

	return f.applyErr
}

func (f *activityFixture) SaveRejection(_ context.Context, rejected domain.EventRejection) (bool, error) {
	f.rejections = append(f.rejections, rejected)

	return f.saveErr == nil, f.saveErr
}

func TestQuarantineLogsOnlyMetadata(t *testing.T) {
	t.Parallel()

	store := &activityFixture{}
	var logs bytes.Buffer
	handle := NewHandler(store, slog.New(slog.NewJSONHandler(&logs, nil)), "catalog", "tasks", "")
	position := domain.EventPosition{Topic: "tasks", Partition: 2, Offset: 19}
	const payload = `{"event_type":"task.published","occurred_at":"fixture-sensitive-detail"}`
	if err := handle(t.Context(), position, "fixture-sensitive-key", []byte(payload)); err != nil {
		t.Fatal(err)
	}
	want := domain.EventRejection{Position: position, Source: "task", Reason: domain.RejectInvalidTask, PayloadSize: len(payload)}
	if len(store.rejections) != 1 || store.rejections[0] != want || store.applied != 0 {
		t.Fatal(store.rejections, store.applied)
	}
	if log := logs.String(); strings.Contains(log, "fixture-sensitive") ||
		!strings.Contains(log, `"reason":"invalid_task"`) || !strings.Contains(log, `"offset":19`) {
		t.Error(log)
	}
}

//nolint:paralleltest // subtests intentionally share one in-memory rejection fixture
func TestClassifyActivityRefusals(t *testing.T) {
	store := &activityFixture{}
	handle := NewHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)), "catalog", "tasks", "")
	cases := []struct {
		topic, data string
		want        domain.RejectionReason
	}{
		{"tasks", "{", domain.RejectInvalidJSON},
		{"tasks", `{"event_type":"private.detail"}`, domain.RejectUnknownType},
		{"tasks", strings.Repeat("x", maxEventSize+1), domain.RejectTooLarge},
		{"tasks", `{"event_type":"task.published","task_id":"11111111-1111-1111-1111-111111111111","occurred_at":"2026-09-12T10:00:00Z","title":"zero\u0000byte"}`, domain.RejectInvalidTask},
		{"catalog", `{"event_type":"catalog.course.created","aggregate_type":"course","aggregate_id":"bad"}`, domain.RejectInvalidCatalog},
		{"catalog", `{"event_type":"catalog.course.unpublised","aggregate_type":"course","aggregate_id":"11111111-1111-1111-1111-111111111111"}`, domain.RejectUnknownType},
		{"tasks", `{"event_type":"task.publised"}`, domain.RejectUnknownType},
		{"catalog", `{"event_type":"task.updated"}`, domain.RejectWrongSource},
	}
	for _, tc := range cases {
		t.Run(string(tc.want), func(t *testing.T) {
			if err := handle(t.Context(), domain.EventPosition{Topic: tc.topic}, "", []byte(tc.data)); err != nil {
				t.Fatal(err)
			}
			if latest := store.rejections[len(store.rejections)-1]; latest.Reason != tc.want {
				t.Error(latest.Reason)
			}
		})
	}
	if err := handle(t.Context(), domain.EventPosition{Topic: "catalog"}, "", []byte(`{"event_type":"catalog.topic.prerequisite.added","aggregate_type":"topic","aggregate_id":"11111111-1111-1111-1111-111111111111"}`)); err != nil {
		t.Fatal(err)
	}
	if len(store.rejections) != len(cases) || store.applied != 0 {
		t.Error("ignored event must not be quarantined or applied")
	}
}

func TestActivityStorageErrorsPropagate(t *testing.T) {
	t.Parallel()

	databaseErr := errors.New("database offline")
	store := &activityFixture{saveErr: databaseErr, applyErr: databaseErr}
	handle := NewHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil)), "catalog", "tasks", "")
	for _, value := range []string{"{", `{"event_type":"task.published","task_id":"11111111-1111-1111-1111-111111111111","occurred_at":"2026-09-12T10:00:00Z"}`} {
		if err := handle(t.Context(), domain.EventPosition{Topic: "tasks"}, "", []byte(value)); !errors.Is(err, databaseErr) {
			t.Fatal(err)
		}
	}
	if store.applied != 1 || len(store.rejections) != 1 {
		t.Fatal("failed application must not go to quarantine")
	}
}

func BenchmarkActivityDecode(b *testing.B) {
	key := "11111111-1111-1111-1111-111111111111"
	payload := []byte(`{"event_id":"22222222-2222-2222-2222-222222222222","event_type":"task.published","task_id":"11111111-1111-1111-1111-111111111111","occurred_at":"2026-09-12T10:00:00Z","title":"Binary search","task_type":"single_choice","difficulty":"basic"}`)
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := decodeEvent(key, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("check_topic", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := decodeForSource("task", key, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}
