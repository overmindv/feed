package repository_test

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/overmindv/content/pkg/api/content"
	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/rebuild"
	"github.com/overmindv/feed/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const materialRebuildID = "fixture-content-rebuild"
const materialStampTitle = "Published fixture"
const fixtureMaterialRevision = "de420000-0000-0000-0000-000000000001"
const materialFixtureTopic = "content.material.v1"

func fixtureMaterialID(i int) string {
	return fmt.Sprintf("de410000-0000-0000-0000-%012d", i)
}

type positionFixture struct{ calls int }

func (p *positionFixture) CaptureMaterialPosition(ctx context.Context) (rebuild.MaterialPosition, error) {
	p.calls++

	return rebuild.MaterialPosition{
		Topic: materialFixtureTopic, TopicID: "de410000000000000000000000000001",
		Partitions: []rebuild.MaterialPartition{{Partition: 0, NextOffset: 11, LeaderEpoch: 2}},
	}, nil
}

type contentSnapshotFixture struct {
	pb.UnimplementedContentServiceServer
	t     *testing.T
	token string
	check func(context.Context) error
	calls atomic.Int32
}

func (f *contentSnapshotFixture) SnapshotMaterials(ctx context.Context, req *pb.SnapshotMaterialsRequest) (*pb.SnapshotMaterialsResponse, error) {
	auth := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(auth) != 1 || auth[0] != "Bearer "+f.token || len(metadata.ValueFromIncomingContext(ctx, "x-user-id")) != 0 {
		f.t.Error("expected a catalog credential without an actor")

		return nil, status.Error(codes.Unauthenticated, "fixture authentication")
	}
	if err := f.check(ctx); err != nil {
		f.t.Error(err)

		return nil, status.Error(codes.FailedPrecondition, "Kafka position was not saved")
	}
	call := f.calls.Add(1)
	if req.GetPageSize() != 100 || (call == 1 && (req.Watermark != nil || req.GetAfterId() != "")) ||
		(call > 1 && (req.Watermark == nil || req.GetWatermark() != 102 || req.GetAfterId() != fixtureMaterialID(100))) {
		f.t.Error("unexpected snapshot cursor", req.GetWatermark(), req.GetAfterId())
	}
	if call == 1 {
		page := &pb.SnapshotMaterialsResponse{Watermark: 102, NextAfterId: fixtureMaterialID(100),
			Entries: make([]*pb.MaterialSnapshotEntry, 0, 100)}
		for i := 1; i <= 100; i++ {
			entry := &pb.MaterialSnapshotEntry{MaterialId: fixtureMaterialID(i), RevisionId: fixtureMaterialRevision,
				Version: int64(i - 1), Metadata: &pb.MaterialSnapshotMetadata{Title: materialStampTitle}}
			if i > 1 {
				entry.ActivityOccurredAt = timestamppb.New(materialFixtureTime())
			}
			page.Entries = append(page.Entries, entry)
		}

		return page, nil
	}
	if call == 2 {
		return &pb.SnapshotMaterialsResponse{Watermark: 103}, nil
	}

	return &pb.SnapshotMaterialsResponse{Watermark: 102, Entries: []*pb.MaterialSnapshotEntry{
		{MaterialId: fixtureMaterialID(101), RevisionId: fixtureMaterialRevision, Version: 101, Tombstone: true,
			Metadata: &pb.MaterialSnapshotMetadata{Title: "hidden title"}},
		{MaterialId: fixtureMaterialID(102), RevisionId: fixtureMaterialRevision, Version: 102,
			Metadata: &pb.MaterialSnapshotMetadata{Title: materialStampTitle}},
	}}, nil
}

func materialFixtureTime() time.Time {
	return time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
}

func serveMaterialSnapshot(t *testing.T, fixture *contentSnapshotFixture) *rebuild.MaterialSnapshot {
	t.Helper()
	rpc := grpc.NewServer()
	t.Cleanup(rpc.Stop)
	pb.RegisterContentServiceServer(rpc, fixture)
	server := httptest.NewUnstartedServer(rpc)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	ca := filepath.Join(t.TempDir(), "content-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := rebuild.NewMaterialSnapshot(server.Listener.Addr().String(), fixture.token, ca, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })

	return source
}

//nolint:paralleltest // Uses migrations and a dedicated PostgreSQL schema.
func TestContentRebuildKeepsKafkaPositionAndUnknownDates(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool, true)
	if err := store.ApplyMaterialChange(ctx, domain.MaterialChange{
		MaterialID: fixtureMaterialID(1), RevisionID: fixtureMaterialRevision, EventID: fixtureMaterialID(999),
		AggregateVersion: 1, OccurredAt: materialFixtureTime(), Title: materialStampTitle,
	}); err != nil {
		t.Fatal(err)
	}
	active, err := store.ListPage(ctx, 20, nil)
	if err != nil || len(active.Items) != 1 {
		t.Fatal("failed to seed live material", err)
	}

	fence := &positionFixture{}
	fixture := &contentSnapshotFixture{
		t: t, token: strings.Repeat("m", 32),
		check: func(ctx context.Context) error {
			var topic string
			var offset int64
			readErr := pool.QueryRow(ctx, `
				SELECT kafka_position->>'topic', (kafka_position->'partitions'->0->>'next_offset')::bigint
				FROM feed_rebuild_material_runs WHERE run_id = $1`, materialRebuildID).Scan(&topic, &offset)
			if readErr != nil || topic != materialFixtureTopic || offset != 11 {
				return errors.New("snapshot started without its original Kafka position")
			}

			return nil
		},
	}
	source := serveMaterialSnapshot(t, fixture)
	stage := func() (rebuild.MaterialCheckpoint, error) {
		return rebuild.StageMaterials(ctx, source, fence, store, materialRebuildID, 1)
	}
	first, err := stage()
	if err != nil || first.Watermark == nil || *first.Watermark != 102 || first.AfterID != fixtureMaterialID(100) ||
		first.Complete || first.Unresolved != 1 {
		t.Fatal("did not stage first snapshot page", first, err)
	}
	if _, moved := stage(); !errors.Is(moved, rebuild.ErrMaterialSnapshot) {
		t.Fatal("did not reject a changed watermark", moved)
	}
	_, duplicate := store.SaveMaterialPage(ctx, materialRebuildID, first, rebuild.MaterialPage{Watermark: 102,
		Rows: []rebuild.MaterialRow{{MaterialID: fixtureMaterialID(1), RevisionID: fixtureMaterialRevision,
			Title: materialStampTitle}}})
	if duplicate == nil {
		t.Fatal("accepted a duplicate staged material")
	}
	for range 2 {
		done, stageErr := stage()
		if stageErr != nil || !done.Complete || done.AfterID != fixtureMaterialID(102) || done.Unresolved != 2 {
			t.Fatal("did not resume from the last successful page", done, stageErr)
		}
	}
	if fence.calls != 1 || fixture.calls.Load() != 3 {
		t.Error("completed snapshot recaptured offsets or fetched another page")
	}
	if _, stale := store.SaveMaterialPage(ctx, materialRebuildID, first, rebuild.MaterialPage{Watermark: 102}); !errors.Is(stale, repository.ErrMaterialRebuildAdvanced) {
		t.Error("stale writer did not stop", stale)
	}
	var total, tombstones, unresolved, dated int
	if readErr := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE tombstone AND title IS NULL),
			count(*) FILTER (WHERE NOT tombstone AND occurred_at IS NULL),
			count(*) FILTER (WHERE occurred_at = $2)
		FROM feed_rebuild_material_rows WHERE run_id = $1`,
		materialRebuildID, materialFixtureTime(),
	).Scan(&total, &tombstones, &unresolved, &dated); readErr != nil || total != 102 || tombstones != 1 || unresolved != 2 || dated != 99 {
		t.Error("staged rows did not preserve dates and visibility", total, tombstones, unresolved, dated, readErr)
	}
	after, err := store.ListPage(ctx, 20, nil)
	if err != nil || !reflect.DeepEqual(active, after) {
		t.Error("Content snapshot changed the active feed", err)
	}
}

func BenchmarkMaterialRebuildPage(b *testing.B) {
	store := repository.New(feedTestPool(b), false)
	fence, err := (&positionFixture{}).CaptureMaterialPosition(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	const size = 100
	page := rebuild.MaterialPage{Watermark: size, Rows: make([]rebuild.MaterialRow, size)}
	at := materialFixtureTime()
	for i := range page.Rows {
		page.Rows[i] = rebuild.MaterialRow{MaterialID: fixtureMaterialID(i + 1), RevisionID: fixtureMaterialRevision,
			Title: materialStampTitle, Version: int64(i + 1), OccurredAt: &at}
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		id := fmt.Sprintf("content-bench-%d", i)
		b.StopTimer()
		before, startErr := store.StartMaterialCheckpoint(b.Context(), id, fence)
		if startErr != nil {
			b.Fatal(startErr)
		}
		b.StartTimer()
		if _, writeErr := store.SaveMaterialPage(b.Context(), id, before, page); writeErr != nil {
			b.Fatal(writeErr)
		}
		i++
	}
}
