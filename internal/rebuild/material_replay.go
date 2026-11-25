package rebuild

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sort"
	"time"

	"github.com/overmindv/feed/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

const materialReplayBatchSize = 100

var (
	ErrMaterialReplaySnapshot = errors.New("complete the Content snapshot for this run before catching up")
	ErrMaterialReplayRange    = errors.New("saved Content Kafka range is no longer available")
	ErrMaterialReplayBatch    = errors.New("cannot read a contiguous batch of Content Kafka events")
)

type MaterialReplayState struct {
	Watermark int64
	Position  MaterialPosition
	Target    MaterialPosition
}

func (s MaterialReplayState) CaughtUp() bool {
	return sameMaterialLog(s.Position, s.Target) && slices.Equal(s.Position.Partitions, s.Target.Partitions)
}

type MaterialReplayEvent struct {
	Partition  int32     `json:"partition"`
	Offset     int64     `json:"offset"`
	EventID    string    `json:"event_id"`
	MaterialID string    `json:"material_id"`
	Version    int64     `json:"version"`
	OccurredAt time.Time `json:"occurred_at"`
	RevisionID string    `json:"revision_id,omitempty"`
	Title      string    `json:"title,omitempty"`
}

type MaterialReplayBatch struct {
	Position MaterialPosition
	Events   []MaterialReplayEvent
}

type MaterialReplayStore interface {
	LoadMaterialReplay(context.Context, string) (MaterialReplayState, error)
	StartMaterialReplay(context.Context, string, MaterialPosition) (MaterialReplayState, error)
	SaveMaterialReplay(context.Context, string, MaterialReplayState, MaterialReplayBatch) error
}

type MaterialReplaySource interface {
	ValidateRange(context.Context, MaterialReplayState) (MaterialPosition, error)
	ReadBatch(context.Context, MaterialReplayState) (MaterialReplayBatch, error)
}

type materialDecoder func(string, []byte) (domain.MaterialChange, error)

type KafkaMaterialReplay struct {
	info   *KafkaMaterialPositioner
	opts   []kgo.Opt
	decode materialDecoder
	reader *kgo.Client
}

func NewKafkaMaterialReplay(info *KafkaMaterialPositioner, opts []kgo.Opt, decode materialDecoder) *KafkaMaterialReplay {
	return &KafkaMaterialReplay{info: info, opts: slices.Clone(opts), decode: decode}
}

func (r *KafkaMaterialReplay) Close() {
	if r.reader != nil {
		r.reader.Close()
	}
}

func (r *KafkaMaterialReplay) ValidateRange(ctx context.Context, state MaterialReplayState) (MaterialPosition, error) {
	end, start, err := r.info.inspectLog(ctx, true)
	if err != nil {
		return MaterialPosition{}, err
	}
	target := state.Target
	if len(target.Partitions) == 0 {
		target = end
	}
	if !validReplayRange(state.Position, target, end, start) {
		return MaterialPosition{}, ErrMaterialReplayRange
	}

	return target, nil
}

func sameMaterialLog(a, b MaterialPosition) bool {
	if a.Topic != b.Topic || a.TopicID != b.TopicID || len(a.Partitions) == 0 || len(a.Partitions) != len(b.Partitions) {
		return false
	}
	for i, part := range a.Partitions {
		if part.Partition != b.Partitions[i].Partition || part.LeaderEpoch != b.Partitions[i].LeaderEpoch {
			return false
		}
	}

	return true
}

func validReplayRange(position, target, end MaterialPosition, start map[int32]int64) bool {
	if !sameMaterialLog(position, end) || !sameMaterialLog(target, end) {
		return false
	}
	for i, part := range position.Partitions {
		first, found := start[part.Partition]
		if !found || part.NextOffset < first || part.NextOffset > target.Partitions[i].NextOffset ||
			target.Partitions[i].NextOffset > end.Partitions[i].NextOffset {
			return false
		}
	}

	return true
}

func (r *KafkaMaterialReplay) ReadBatch(ctx context.Context, state MaterialReplayState) (MaterialReplayBatch, error) {
	topic := state.Position.Topic
	if r.reader == nil {
		if err := r.openReader(state); err != nil {
			return MaterialReplayBatch{}, err
		}
	}
	paused := make([]int32, 0, len(state.Position.Partitions))
	for i, part := range state.Position.Partitions {
		if part.NextOffset == state.Target.Partitions[i].NextOffset {
			paused = append(paused, part.Partition)
		}
	}
	if len(paused) != 0 {
		r.reader.PauseFetchPartitions(map[string][]int32{topic: paused})
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fetches := r.reader.PollRecords(ctx, materialReplayBatchSize)
	if fetches.Err() != nil {
		return MaterialReplayBatch{}, ErrMaterialReplayBatch
	}

	return decodeReplayBatch(state, fetches.RecordsAll(), r.decode)
}

func (r *KafkaMaterialReplay) openReader(state MaterialReplayState) error {
	assignments := make(map[int32]kgo.Offset, len(state.Position.Partitions))
	for i, part := range state.Position.Partitions {
		if part.NextOffset < state.Target.Partitions[i].NextOffset {
			assignments[part.Partition] = kgo.NewOffset().At(part.NextOffset).WithEpoch(part.LeaderEpoch)
		}
	}
	opts := append(slices.Clone(r.opts),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{state.Position.Topic: assignments}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxBytes(2<<20), kgo.FetchMaxPartitionBytes(1<<20), kgo.MaxConcurrentFetches(2), kgo.FetchMaxWait(200*time.Millisecond))
	reader, err := kgo.NewClient(opts...)
	if err != nil {
		return ErrMaterialReplayBatch
	}
	r.reader = reader

	return nil
}

func decodeReplayBatch(state MaterialReplayState, records iter.Seq[*kgo.Record], decode materialDecoder) (MaterialReplayBatch, error) {
	next := state.Position
	next.Partitions = slices.Clone(state.Position.Partitions)
	events := make([]MaterialReplayEvent, 0, materialReplayBatchSize)
	progress := false
	for record := range records {
		i := sort.Search(len(next.Partitions), func(i int) bool { return next.Partitions[i].Partition >= record.Partition })
		if record.Topic != next.Topic || i == len(next.Partitions) || next.Partitions[i].Partition != record.Partition {
			return MaterialReplayBatch{}, ErrMaterialReplayBatch
		}
		part := &next.Partitions[i]
		end := state.Target.Partitions[i].NextOffset
		if part.NextOffset == end && record.Offset >= end {
			continue
		}
		if record.Offset != part.NextOffset || record.Offset >= end || record.LeaderEpoch > part.LeaderEpoch {
			return MaterialReplayBatch{}, ErrMaterialReplayBatch
		}
		change, err := decode(string(record.Key), record.Value)
		if err != nil {
			return MaterialReplayBatch{}, ErrMaterialReplayBatch
		}
		part.NextOffset++
		progress = true
		if change.AggregateVersion > state.Watermark {
			events = append(events, MaterialReplayEvent{
				Partition: record.Partition, Offset: record.Offset, EventID: change.EventID,
				MaterialID: change.MaterialID, Version: change.AggregateVersion, OccurredAt: change.OccurredAt,
				RevisionID: change.RevisionID, Title: change.Title,
			})
		}
	}
	if !progress {
		return MaterialReplayBatch{}, ErrMaterialReplayBatch
	}

	return MaterialReplayBatch{Position: next, Events: events}, nil
}

func CatchUpMaterials(ctx context.Context, id string, maxBatches int, source MaterialReplaySource,
	store MaterialReplayStore) (MaterialReplayState, error) {
	if !ValidRunID(id) || maxBatches < 1 || maxBatches > MaxJournalPages {
		return MaterialReplayState{}, errors.New("invalid Content rebuild identifier or batch budget")
	}
	state, err := store.LoadMaterialReplay(ctx, id)
	if err != nil {
		return state, err
	}
	target, err := source.ValidateRange(ctx, state)
	if err != nil {
		return state, err
	}
	if len(state.Target.Partitions) == 0 {
		state, err = store.StartMaterialReplay(ctx, id, target)
		if err != nil {
			return state, err
		}
		if !slices.Equal(target.Partitions, state.Target.Partitions) {
			if _, err = source.ValidateRange(ctx, state); err != nil {
				return state, err
			}
		}
	}
	for range maxBatches {
		if state.CaughtUp() {
			break
		}
		batch, readErr := source.ReadBatch(ctx, state)
		if readErr != nil {
			return state, readErr
		}
		if saveErr := store.SaveMaterialReplay(ctx, id, state, batch); saveErr != nil {
			return state, saveErr
		}
		state.Position = batch.Position
	}
	if state.CaughtUp() {
		_, err = source.ValidateRange(ctx, state)
	}

	return state, err
}
