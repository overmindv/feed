package repository_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
)

//nolint:paralleltest // Использует отдельную схему с миграциями PostgreSQL.
func TestFeedPagesUnderInserts(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	feed := repository.New(pool, true)
	base := time.Date(2026, 11, 23, 19, 0, 0, 123456000, time.UTC)
	const sharedID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	const materialID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	const sharedMaterial = "/materials/" + materialID
	insertFeedCard(t, pool, sharedID, base, "/tasks/tied")
	insertFeedCard(t, pool, "cccccccc-cccc-cccc-cccc-cccccccccccc", base, "/tasks/first")
	insertFeedCard(t, pool, "dddddddd-dddd-dddd-dddd-dddddddddddd", base.Add(-time.Microsecond), "/tasks/last")
	execQuarantineFixture(t, pool, `
		INSERT INTO feed_material_state (material_id, id, aggregate_version, revision_id, title, occurred_at)
		VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',1, gen_random_uuid(),'Photo', now() - interval '1 day'),
		       ('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee',gen_random_uuid(),1,NULL,NULL,now())`)
	if _, err := pool.Exec(ctx, "UPDATE feed_material_state SET occurred_at=$1 WHERE material_id=$2", base, materialID); err != nil {
		t.Fatal(err)
	}
	execQuarantineFixture(t, pool, `
		INSERT INTO feed_items (source,event_key,kind,aggregate_type,aggregate_id,occurred_at,title,href,raw_payload)
		VALUES ('catalog','old','course.created','course','old',now(),'Unknown visibility','/courses/old','{}')`)

	first, err := feed.ListPage(ctx, 2, nil)
	if err != nil || !slices.Equal(cardPaths(first), []string{"/tasks/first", sharedMaterial}) || first.Next == nil || !first.Next.Material {
		t.Fatal("unexpected first page:", cardPaths(first), first.Next, err)
	}
	insertFeedCard(t, pool, "ffffffff-ffff-ffff-ffff-ffffffffffff", base.Add(time.Microsecond), "/tasks/inserted")
	second, err := feed.ListPage(ctx, 2, first.Next)
	if err != nil || !slices.Equal(cardPaths(second), []string{"/tasks/tied", "/tasks/last"}) || second.Next != nil {
		t.Fatal("following page shifted after an insert:", cardPaths(second), second.Next, err)
	}

	withoutMaterials := repository.New(pool, false)
	plain, err := withoutMaterials.ListPage(ctx, 3, nil)
	if err != nil || !slices.Equal(cardPaths(plain), []string{"/tasks/inserted", "/tasks/first", "/tasks/tied"}) || plain.Next == nil {
		t.Fatal("legacy flag exposed a material:", cardPaths(plain), plain.Next, err)
	}
	tail, err := withoutMaterials.ListPage(ctx, 3, plain.Next)
	if err != nil || !slices.Equal(cardPaths(tail), []string{"/tasks/last"}) || tail.Next != nil {
		t.Fatal("unexpected tail:", cardPaths(tail), tail.Next, err)
	}

	execQuarantineFixture(t, pool, `
		INSERT INTO feed_event_quarantine (topic,partition,record_offset,source,reason,payload_size)
		VALUES ('content.material.v1',0,10,'material','invalid_material',100)`)
	quarantined, err := feed.ListPage(ctx, 5, nil)
	want := []string{"/tasks/inserted", "/tasks/first", "/tasks/tied", "/tasks/last"}
	if err != nil || !slices.Equal(cardPaths(quarantined), want) || quarantined.Next != nil {
		t.Fatal("cursor exposed a quarantined material:", cardPaths(quarantined), err)
	}
}

func insertFeedCard(t *testing.T, pool *pgxpool.Pool, id string, occurredAt time.Time, path string) {
	t.Helper()

	if _, err := pool.Exec(t.Context(), `
		INSERT INTO feed_items (id,source,event_key,kind,aggregate_type,aggregate_id,occurred_at,title,href,raw_payload)
		VALUES ($1::uuid,'task',$1,'task.published','task',$1,$2,'Fixture',$3,'{}')`,
		id, occurredAt, path); err != nil {
		t.Fatal(err)
	}
}

func cardPaths(page domain.FeedPage) []string {
	paths := make([]string, 0, len(page.Items))
	for _, card := range page.Items {
		paths = append(paths, card.Href)
	}

	return paths
}

func BenchmarkFeedPageDeep(b *testing.B) {
	pool := feedTestPool(b)
	ctx := b.Context()
	if _, err := pool.Exec(ctx, `
		INSERT INTO feed_items (source,event_key,kind,aggregate_type,aggregate_id,occurred_at,title,href,raw_payload)
		SELECT 'task', n::text, 'task.published', 'task', n::text, now() - n*interval '1 second', 'Fixture','/tasks/'||n,'{}'
		FROM generate_series(1,80000) n`); err != nil {
		b.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO feed_material_state(material_id,aggregate_version,revision_id,title,occurred_at)
		SELECT gen_random_uuid(),1,gen_random_uuid(),'Fixture',now() - (n*4 + 0.5)*interval '1 second'
		FROM generate_series(1,20000) n`); err != nil {
		b.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE feed_items"); err != nil {
		b.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE feed_material_state"); err != nil {
		b.Fatal(err)
	}
	const offset = 50000
	const limit = 50
	store := repository.New(pool, true)
	previous, err := store.ListItems(ctx, 1, offset-1)
	if err != nil || len(previous) != 1 {
		b.Fatal("benchmark boundary missing:", err)
	}
	at := domain.FeedCursor{OccurredAt: previous[0].OccurredAt, ID: previous[0].ID, Material: previous[0].Source == "material"}
	firstOffset, err := store.ListItems(ctx, limit, offset)
	firstCursor, cursorErr := store.ListPage(ctx, limit, &at)
	if err != nil || cursorErr != nil || len(firstOffset) != limit || len(firstCursor.Items) != limit ||
		firstOffset[0].ID != firstCursor.Items[0].ID || firstOffset[limit-1].ID != firstCursor.Items[limit-1].ID {
		b.Fatal("benchmark pages do not match:", err, cursorErr)
	}
	b.Run("offset", func(b *testing.B) {
		benchmarkFeedPage(b, func(ctx context.Context) error {
			_, listErr := store.ListItems(ctx, limit, offset)

			return listErr
		})
	})
	b.Run("cursor", func(b *testing.B) {
		benchmarkFeedPage(b, func(ctx context.Context) error {
			_, listErr := store.ListPage(ctx, limit, &at)

			return listErr
		})
	})
}

func benchmarkFeedPage(b *testing.B, list func(context.Context) error) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		if err := list(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}
