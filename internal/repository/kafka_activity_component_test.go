package repository_test

import (
	"crypto/sha256"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
)

//nolint:paralleltest // Миграции создают pgcrypto в общей тестовой базе.
func TestKafkaActivityTracksContinuity(t *testing.T) {
	pool := feedTestPool(t)
	store := repository.New(pool, false)
	delivery := domain.KafkaActivity{
		Group: activityTestGroup, Position: domain.EventPosition{Topic: taskTopic, Offset: 10},
		Fingerprint: sha256.Sum256([]byte("example")),
	}
	for _, step := range []struct {
		offset      int64
		highest     int64
		fingerprint [32]byte
		continuous  bool
	}{
		{10, 10, delivery.Fingerprint, true},
		{10, 10, delivery.Fingerprint, true},
		{11, 11, delivery.Fingerprint, true},
		{14, 14, delivery.Fingerprint, false},
		{15, 15, delivery.Fingerprint, false},
		{13, 15, delivery.Fingerprint, false},
	} {
		delivery.Position.Offset = step.offset
		delivery.Fingerprint = step.fingerprint
		if _, err := store.RecordKafkaActivity(t.Context(), delivery); err != nil {
			t.Fatal(err)
		}
		assertActivityHead(t, pool, delivery, 10, step.highest, step.continuous)
	}

	delivery.Position.Partition++
	delivery.Position.Offset = 15
	if _, err := store.RecordKafkaActivity(t.Context(), delivery); err != nil {
		t.Fatal(err)
	}
	assertActivityHead(t, pool, delivery, 15, 15, true)
	delivery.Fingerprint = sha256.Sum256([]byte("different record"))
	if _, err := store.RecordKafkaActivity(t.Context(), delivery); err != nil {
		t.Fatal(err)
	}
	assertActivityHead(t, pool, delivery, 15, 15, false)
	delivery.Group += "-other"
	if _, err := store.RecordKafkaActivity(t.Context(), delivery); err != nil {
		t.Fatal(err)
	}
	assertActivityHead(t, pool, delivery, 15, 15, true)
}

//nolint:paralleltest // Миграции создают pgcrypto в общей тестовой базе.
func TestKafkaActivityRollsBackBothWrites(t *testing.T) {
	pool := feedTestPool(t)
	store := repository.New(pool, false)
	handle := consumer.NewHandler(store, slog.New(slog.DiscardHandler), catalogTopic, taskTopic, "", activityTestGroup)
	p := domain.EventPosition{Topic: taskTopic, Offset: 47}

	execQuarantineFixture(t, pool, "ALTER TABLE feed_activity_delivery_heads ADD CHECK (highest_offset < 50)")
	for range 2 {
		if err := handle(t.Context(), p, "", []byte(correctedTask)); err != nil {
			t.Fatal(err)
		}
	}
	delivery := domain.KafkaActivity{Group: activityTestGroup, Position: p}
	assertActivityHead(t, pool, delivery, 47, 47, true)

	p.Offset = 48
	execQuarantineFixture(t, pool, "ALTER TABLE feed_event_quarantine ADD CONSTRAINT fixture_no_rejection CHECK (record_offset <> 48)")
	if err := handle(t.Context(), p, "", []byte("{")); err == nil {
		t.Fatal("handler acknowledged failed quarantine insert")
	}
	assertActivityHead(t, pool, delivery, 47, 47, true)
	execQuarantineFixture(t, pool, "ALTER TABLE feed_event_quarantine DROP CONSTRAINT fixture_no_rejection")
	for range 2 {
		if err := handle(t.Context(), p, "", []byte("{")); err != nil {
			t.Fatal(err)
		}
	}
	assertActivityHead(t, pool, delivery, 47, 48, true)

	p.Offset = 50
	if err := handle(t.Context(), p, "", []byte("{")); err == nil {
		t.Fatal("handler acknowledged failed position write")
	}
	publication := activityPublication(650)
	delivery.Position = p
	delivery.Task = &publication
	delivery.Fingerprint = sha256.Sum256([]byte("new task"))
	if _, err := store.RecordKafkaActivity(t.Context(), delivery); err == nil {
		t.Fatal("publication survived position constraint")
	}
	assertActivityHead(t, pool, delivery, 47, 48, true)

	var cards, inbox, quarantine int
	if err := pool.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM feed_items),
		       (SELECT count(*) FROM feed_event_inbox),
		       (SELECT count(*) FROM feed_event_quarantine)`).Scan(&cards, &inbox, &quarantine); err != nil {
		t.Fatal(err)
	}
	if cards != 1 || inbox != 1 || quarantine != 1 {
		t.Fatal("expected only committed writes, got cards, inbox, quarantine:", cards, inbox, quarantine)
	}
}

func assertActivityHead(t *testing.T, pool *pgxpool.Pool, delivery domain.KafkaActivity, first, highest int64, continuous bool) {
	t.Helper()
	p := delivery.Position
	var gotFirst, gotHighest int64
	var gotContinuous bool
	if err := pool.QueryRow(t.Context(), `
		SELECT first_offset, highest_offset, continuous FROM feed_activity_delivery_heads
		WHERE consumer_group = $1 AND topic = $2 AND partition = $3`,
		delivery.Group, p.Topic, p.Partition).Scan(&gotFirst, &gotHighest, &gotContinuous); err != nil {
		t.Fatal(err)
	}
	if gotFirst != first || gotHighest != highest || gotContinuous != continuous {
		t.Fatal("unexpected first, highest, continuous:", gotFirst, gotHighest, gotContinuous)
	}
}

func BenchmarkKafkaActivityWrite(b *testing.B) {
	for _, withPosition := range []bool{false, true} {
		name := "activity_only"
		if withPosition {
			name = "activity_and_position"
		}
		b.Run(name, func(b *testing.B) {
			store := repository.New(feedTestPool(b), false)
			delivery := domain.KafkaActivity{
				Group: activityTestGroup, Position: domain.EventPosition{Topic: taskTopic},
				Fingerprint: sha256.Sum256([]byte("benchmark")),
			}
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				publication := activityPublication(i)
				if withPosition {
					delivery.Position.Offset = int64(i)
					delivery.Task = &publication
					if _, err := store.RecordKafkaActivity(b.Context(), delivery); err != nil {
						b.Fatal(err)
					}
				} else if err := store.ApplyTaskPublication(b.Context(), publication); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func activityPublication(i int) domain.TaskPublication {
	return domain.TaskPublication{
		EventID: fixtureMaterialID(i),
		Item: domain.FeedItem{AggregateID: fixtureMaterialID(1), OccurredAt: materialFixtureTime(),
			Title: "Practice", RawPayload: []byte("{}")},
	}
}
