package repository_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/rebuild"
	"github.com/overmindv/feed/internal/repository"
)

const replayRunID = "fixture-content-catch-up"

func replayOffset(position rebuild.MaterialPosition, offset int64) rebuild.MaterialPosition {
	position.Partitions = slices.Clone(position.Partitions)
	position.Partitions[0].NextOffset = offset

	return position
}

type replaySourceFixture struct {
	end     rebuild.MaterialPosition
	invalid bool
}

func (f *replaySourceFixture) ValidateRange(_ context.Context, state rebuild.MaterialReplayState) (rebuild.MaterialPosition, error) {
	if len(state.Target.Partitions) == 0 {
		return f.end, nil
	}

	return state.Target, nil
}

func (f *replaySourceFixture) ReadBatch(_ context.Context, state rebuild.MaterialReplayState) (rebuild.MaterialReplayBatch, error) {
	offset := state.Position.Partitions[0].NextOffset
	event := rebuild.MaterialReplayEvent{
		Offset: offset, EventID: fixtureMaterialID(8), MaterialID: fixtureMaterialID(1), Version: 6, OccurredAt: materialFixtureTime(),
	}
	if f.invalid {
		event.EventID = "invalid"
	}

	return rebuild.MaterialReplayBatch{Position: replayOffset(state.Position, offset+1), Events: []rebuild.MaterialReplayEvent{event}}, nil
}

func prepareReplayFixture(t testing.TB, store *repository.Postgres, id string, from rebuild.MaterialPosition) {
	t.Helper()
	snapshot, err := store.StartMaterialCheckpoint(t.Context(), id, from)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.LoadMaterialReplay(t.Context(), id); !errors.Is(err, rebuild.ErrMaterialReplaySnapshot) {
		t.Fatal("accepted an incomplete snapshot", err)
	}
	if _, err = store.SaveMaterialPage(t.Context(), id, snapshot, rebuild.MaterialPage{Watermark: 5}); err != nil {
		t.Fatal(err)
	}
}

//nolint:paralleltest // Uses migrations and a dedicated PostgreSQL schema.
func TestMaterialCatchUpRollsBackOffsetsAndLeavesLiveFeedIntact(t *testing.T) {
	ctx := t.Context()
	pool := feedTestPool(t)
	store := repository.New(pool, true)
	if err := store.ApplyMaterialChange(ctx, domain.MaterialChange{
		MaterialID: fixtureMaterialID(1), RevisionID: fixtureMaterialRevision, EventID: fixtureMaterialID(2),
		AggregateVersion: 5, OccurredAt: materialFixtureTime(), Title: materialStampTitle,
	}); err != nil {
		t.Fatal(err)
	}
	active, err := store.ListPage(ctx, 20, nil)
	if err != nil || len(active.Items) != 1 {
		t.Fatal("cannot seed active feed", err)
	}
	from, err := (&positionFixture{}).CaptureMaterialPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prepareReplayFixture(t, store, replayRunID, from)
	source := &replaySourceFixture{end: replayOffset(from, 13)}
	state, err := rebuild.CatchUpMaterials(ctx, replayRunID, 1, source, store)
	if err != nil || state.CaughtUp() || state.Position.Partitions[0].NextOffset != 12 {
		t.Fatal("did not persist first batch", state, err)
	}
	source.invalid = true
	if _, err = rebuild.CatchUpMaterials(ctx, replayRunID, 1, source, store); err == nil {
		t.Fatal("accepted a failed write")
	}
	saved, err := store.LoadMaterialReplay(ctx, replayRunID)
	if err != nil || !reflect.DeepEqual(state, saved) {
		t.Fatal("failed batch advanced the saved position", saved, err)
	}
	source.end, source.invalid = replayOffset(from, 15), false
	done, err := rebuild.CatchUpMaterials(ctx, replayRunID, 2, source, store)
	if err != nil || !done.CaughtUp() || done.Position.Partitions[0].NextOffset != 13 {
		t.Fatal("did not finish at the original cutoff", done, err)
	}
	err = store.SaveMaterialReplay(ctx, replayRunID, state, rebuild.MaterialReplayBatch{Position: done.Position})
	if !errors.Is(err, repository.ErrMaterialRebuildAdvanced) {
		t.Error("accepted stale offsets after retry", err)
	}
	var count, tombstones int
	if readErr := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE title IS NULL AND revision_id IS NULL AND version = 6)
		FROM feed_rebuild_material_events WHERE run_id = $1`, replayRunID,
	).Scan(&count, &tombstones); readErr != nil || count != 2 || tombstones != 2 {
		t.Error("replay lost physical offsets or tombstones", count, tombstones, readErr)
	}
	after, err := store.ListPage(ctx, 20, nil)
	if err != nil || !reflect.DeepEqual(active, after) {
		t.Error("material catch-up changed the live feed", err)
	}
}

func BenchmarkMaterialReplayBatch(b *testing.B) {
	store := repository.New(feedTestPool(b), false)
	from, err := (&positionFixture{}).CaptureMaterialPosition(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	const count = 100
	target := replayOffset(from, from.Partitions[0].NextOffset+count)
	batch := rebuild.MaterialReplayBatch{Position: target, Events: make([]rebuild.MaterialReplayEvent, count)}
	for i := range batch.Events {
		batch.Events[i] = rebuild.MaterialReplayEvent{
			Offset: from.Partitions[0].NextOffset + int64(i), EventID: fixtureMaterialID(i + 1),
			MaterialID: fixtureMaterialID(i + 1), Version: int64(i + 6), OccurredAt: materialFixtureTime(),
			Title: materialStampTitle, RevisionID: fixtureMaterialRevision,
		}
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		b.StopTimer()
		id := fmt.Sprintf("content-catch-up-%d", i)
		prepareReplayFixture(b, store, id, from)
		state, startErr := store.StartMaterialReplay(b.Context(), id, target)
		if startErr != nil {
			b.Fatal(startErr)
		}
		b.StartTimer()
		if err = store.SaveMaterialReplay(b.Context(), id, state, batch); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
