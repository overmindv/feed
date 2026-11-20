package repository_test

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
)

const correctedTask = `{"event_id":"22110000-0000-0000-0000-000000000001","event_type":"task.published","task_id":"22110000-0000-0000-0000-000000000002","occurred_at":"2026-09-12T10:00:00Z","title":"Binary search"}`

func TestQuarantineRetryAndDuplicate(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool)
	handle := consumer.NewHandler(store, slog.New(slog.DiscardHandler), catalogTopic, taskTopic)
	p := domain.EventPosition{Topic: taskTopic, Partition: 1, Offset: 123}
	value := []byte(`{"event_type":"task.published","occurred_at":"bad time"}`)
	execQuarantineFixture(t, pool, "ALTER TABLE feed_event_quarantine ADD CONSTRAINT fixture_block_offset CHECK (record_offset < 100) NOT VALID")
	if err := handle(ctx, p, "bad key", value); err == nil {
		t.Fatal("failed rejection insert was acknowledged")
	}
	execQuarantineFixture(t, pool, "ALTER TABLE feed_event_quarantine DROP CONSTRAINT fixture_block_offset")
	for range 2 {
		if err := handle(ctx, p, "bad key", value); err != nil {
			t.Fatal(err)
		}
	}

	var source, reason string
	var size, count int
	err := pool.QueryRow(ctx, `
		SELECT source, reason, payload_size, count(*) OVER ()
		FROM feed_event_quarantine
		WHERE topic=$1 AND partition=$2 AND record_offset=$3`,
		p.Topic, p.Partition, p.Offset).Scan(&source, &reason, &size, &count)
	if err != nil || source != "task" || reason != "invalid_task" || size != len(value) || count != 1 {
		t.Fatal(source, reason, size, count, err)
	}
}

func TestQuarantineReplayIsValidatedAndAtomic(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool)
	p := domain.EventPosition{Topic: taskTopic, Partition: 1, Offset: 42}
	handle := consumer.NewHandler(store, slog.New(slog.DiscardHandler), catalogTopic, taskTopic)
	invalid := strings.Replace(correctedTask, "2026-09-12T10:00:00Z", "bad timestamp", 1)
	if err := handle(ctx, p, "", []byte(invalid)); err != nil {
		t.Fatal(err)
	}
	execQuarantineFixture(t, pool, "UPDATE feed_event_quarantine SET rejected_at = now() - interval '6 minutes'")
	if age, err := store.OldestRejectionAge(ctx); err != nil || age < 360 {
		t.Fatal(age, err)
	}
	for _, tc := range []struct {
		data string
		err  error
	}{
		{"{", domain.RejectInvalidJSON},
		{`{"event_type":"catalog.topic.updated","aggregate_type":"topic","aggregate_id":"11111111-1111-1111-1111-111111111111"}`, domain.RejectWrongSource},
		{`{"event_type":"task.updated"}`, domain.RejectUnknownType},
		{strings.Replace(correctedTask, "22110000-0000-0000-0000-000000000001", "22110000-0000-0000-0000-000000000099", 1), consumer.ErrIdentityMismatch},
		{strings.Repeat("x", 1<<20+1), domain.RejectTooLarge},
	} {
		if err := consumer.Replay(ctx, store, p, []byte(tc.data), true); !errors.Is(err, tc.err) {
			t.Error("wrong replay error", err, tc.err)
		}
	}

	// Если обновление журнала не прошло, откатываются также карточка и inbox.
	execQuarantineFixture(t, pool, "ALTER TABLE feed_event_quarantine ADD CONSTRAINT fixture_block_replay CHECK (replayed_at IS NULL) NOT VALID")
	if err := consumer.Replay(ctx, store, p, []byte(correctedTask), false); err == nil {
		t.Fatal("expected failure after applying the corrected event")
	}
	var cards, inbox int
	err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM feed_items), (SELECT count(*) FROM feed_event_inbox)").Scan(&cards, &inbox)
	if err != nil || cards != 0 || inbox != 0 {
		t.Fatal(cards, inbox, err)
	}
	execQuarantineFixture(t, pool, "ALTER TABLE feed_event_quarantine DROP CONSTRAINT fixture_block_replay")

	results := make(chan error, 2)
	for range cap(results) {
		go func() { results <- consumer.Replay(ctx, store, p, []byte(correctedTask), false) }()
	}
	var applied, duplicates int
	for range cap(results) {
		err := <-results
		switch {
		case err == nil:
			applied++
		case errors.Is(err, repository.ErrAlreadyReplayed):
			duplicates++
		default:
			t.Fatal(err)
		}
	}
	if applied != 1 || duplicates != 1 {
		t.Fatal(applied, duplicates)
	}
	rows, err := store.ListItems(ctx, 10, 0)
	if err != nil || len(rows) != 1 || rows[0].Title != "Новая задача: Binary search" {
		t.Fatal(rows, err)
	}
	var audited bool
	if err := pool.QueryRow(ctx, "SELECT replayed_by = session_user FROM feed_event_quarantine").Scan(&audited); err != nil || !audited {
		t.Fatal(audited, err)
	}
	if age, err := store.OldestRejectionAge(ctx); err != nil || age != 0 {
		t.Fatal(age, err)
	}
}

func TestQuarantineUnverifiedReplayRequiresConfirmation(t *testing.T) {
	ctx := t.Context()
	store := repository.New(feedTestPool(t))
	handle := consumer.NewHandler(store, slog.New(slog.DiscardHandler), catalogTopic, taskTopic)
	p := domain.EventPosition{Topic: taskTopic, Offset: 51}
	if err := handle(ctx, p, "", []byte("{")); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Replay(ctx, store, p, []byte(correctedTask), false); !errors.Is(err, consumer.ErrIdentityUnverified) {
		t.Fatal(err)
	}
	if err := consumer.Replay(ctx, store, p, []byte(correctedTask), true); err != nil {
		t.Fatal(err)
	}
}

func execQuarantineFixture(t testing.TB, pool *pgxpool.Pool, query string) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), query); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkQuarantineAge(b *testing.B) {
	pool := feedTestPool(b)
	const total = 100_000
	execQuarantineFixture(b, pool, fmt.Sprintf(`
		INSERT INTO feed_event_quarantine (topic, partition, record_offset, source, reason, payload_size, replayed_at, replayed_by)
		SELECT 'fixture', 0, n, 'task', 'invalid_task', 100, now(), session_user FROM generate_series(1,%d) n`, total))
	execQuarantineFixture(b, pool, `
		INSERT INTO feed_event_quarantine (topic, partition, record_offset, source, reason, payload_size)
		VALUES ('fixture', 1, 0, 'task', 'invalid_task', 100)`)
	execQuarantineFixture(b, pool, "ANALYZE feed_event_quarantine")
	store := repository.New(pool)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := store.OldestRejectionAge(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(total, "closed_rows")
}
