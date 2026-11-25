package rebuild

import (
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/overmindv/feed/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fixtureReplayPosition(offset int64) MaterialPosition {
	return MaterialPosition{
		Topic: materialPositionTopic, TopicID: "10000000000000000000000000000000",
		Partitions: []MaterialPartition{{Partition: 0, NextOffset: offset, LeaderEpoch: 2}},
	}
}

func fixtureReplayDecoder(_ string, raw []byte) (domain.MaterialChange, error) {
	version, err := strconv.ParseInt(string(raw), 10, 64)

	return domain.MaterialChange{AggregateVersion: version}, err
}

func TestMaterialReplayRangeRejectsLostAndChangedLogs(t *testing.T) {
	t.Parallel()

	position, target, end := fixtureReplayPosition(4), fixtureReplayPosition(7), fixtureReplayPosition(9)
	if !validReplayRange(position, target, end, map[int32]int64{0: 4}) {
		t.Fatal("rejected a replay range with a growing Kafka end offset")
	}
	if validReplayRange(position, target, end, map[int32]int64{0: 5}) ||
		validReplayRange(position, target, fixtureReplayPosition(6), map[int32]int64{0: 0}) {
		t.Error("accepted an expired cursor or truncated cutoff")
	}
	changed := fixtureReplayPosition(9)
	changed.Partitions[0].LeaderEpoch++
	if validReplayRange(position, target, changed, map[int32]int64{0: 0}) {
		t.Error("accepted a changed leader epoch")
	}
	changed = fixtureReplayPosition(9)
	changed.TopicID = "20000000000000000000000000000000"
	if validReplayRange(position, target, changed, map[int32]int64{0: 0}) {
		t.Error("accepted a recreated topic")
	}
}

func TestMaterialReplayAdvancesOnlyContiguousRecordsBelowCutoff(t *testing.T) {
	t.Parallel()

	state := MaterialReplayState{Watermark: 5, Position: fixtureReplayPosition(3), Target: fixtureReplayPosition(5)}
	before := &kgo.Record{Topic: materialPositionTopic, Offset: 3, Value: []byte("5")}
	newer := &kgo.Record{Topic: materialPositionTopic, Offset: 4, Value: []byte("6")}
	tail := &kgo.Record{Topic: materialPositionTopic, Offset: 5, Value: []byte("bad tail")}
	batch, err := decodeReplayBatch(state, slices.Values([]*kgo.Record{before, newer, tail}), fixtureReplayDecoder)
	if err != nil || len(batch.Events) != 1 || batch.Events[0].Offset != 4 ||
		batch.Position.Partitions[0].NextOffset != 5 || state.Position.Partitions[0].NextOffset != 3 {
		t.Fatal("unexpected replay batch", batch, err)
	}
	for _, rows := range [][]*kgo.Record{
		{newer}, {before, tail},
		{before, {Topic: materialPositionTopic, Offset: 4, Value: []byte("invalid")}},
		{before, {Topic: materialPositionTopic, Offset: 4, LeaderEpoch: 3, Value: []byte("7")}},
	} {
		bad, readErr := decodeReplayBatch(state, slices.Values(rows), fixtureReplayDecoder)
		if !errors.Is(readErr, ErrMaterialReplayBatch) || len(bad.Events) != 0 || len(bad.Position.Partitions) != 0 {
			t.Error("failed replay batch advanced the cursor or returned events", bad, readErr)
		}
	}
}
