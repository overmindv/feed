package rebuild

import (
	"context"
	"encoding/hex"
	"errors"
	"regexp"
	"slices"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	maxMaterialPartitions = 1024
	latestMaterialOffset  = -1
	firstMaterialOffset   = -2
)

var materialTopicName = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$")

var ErrMaterialPosition = errors.New("cannot capture a complete Kafka position for Content")

type MaterialPartition struct {
	Partition   int32 `json:"partition"`
	NextOffset  int64 `json:"next_offset"`
	LeaderEpoch int32 `json:"leader_epoch"`
}

type MaterialPosition struct {
	Topic      string              `json:"topic"`
	TopicID    string              `json:"topic_id"`
	Partitions []MaterialPartition `json:"partitions"`
}

type kafkaMaterialReader interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
	RequestSharded(context.Context, kmsg.Request) []kgo.ResponseShard
}

type KafkaMaterialPositioner struct {
	client kafkaMaterialReader
	topic  string
}

func NewKafkaMaterialPositioner(client *kgo.Client, topic string) (*KafkaMaterialPositioner, error) {
	if client == nil || !materialTopicName.MatchString(topic) {
		return nil, ErrMaterialPosition
	}

	return &KafkaMaterialPositioner{client: client, topic: topic}, nil
}

func (p *KafkaMaterialPositioner) CaptureMaterialPosition(ctx context.Context) (MaterialPosition, error) {
	position, _, err := p.inspectLog(ctx, false)

	return position, err
}

func (p *KafkaMaterialPositioner) inspectLog(ctx context.Context, includeStart bool) (MaterialPosition, map[int32]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	id, partitions, err := p.topology(ctx)
	if err != nil {
		return MaterialPosition{}, nil, err
	}
	offsets, err := p.readOffsets(ctx, partitions, latestMaterialOffset)
	if err != nil {
		return MaterialPosition{}, nil, err
	}
	var start map[int32]int64
	if includeStart {
		start, err = p.readOffsets(ctx, partitions, firstMaterialOffset)
		if err != nil {
			return MaterialPosition{}, nil, err
		}
	}
	lastID, lastPartitions, err := p.topology(ctx)
	if err != nil || id != lastID || !slices.Equal(partitions, lastPartitions) {
		return MaterialPosition{}, nil, ErrMaterialPosition
	}
	for i := range partitions {
		partitions[i].NextOffset = offsets[partitions[i].Partition]
	}

	return MaterialPosition{Topic: p.topic, TopicID: hex.EncodeToString(id[:]), Partitions: partitions}, start, nil
}

func (p *KafkaMaterialPositioner) topology(ctx context.Context) ([16]byte, []MaterialPartition, error) {
	query := kmsg.NewPtrMetadataRequest()
	query.AllowAutoTopicCreation = false
	query.Topics = []kmsg.MetadataRequestTopic{{Topic: &p.topic}}
	res, err := p.client.Request(ctx, query)
	reply, ok := res.(*kmsg.MetadataResponse)
	if err != nil || !ok || len(reply.Topics) != 1 {
		return [16]byte{}, nil, ErrMaterialPosition
	}
	topic := &reply.Topics[0]
	if topic.Topic == nil || *topic.Topic != p.topic || topic.ErrorCode != 0 || topic.IsInternal ||
		topic.TopicID == ([16]byte{}) || len(topic.Partitions) == 0 || len(topic.Partitions) > maxMaterialPartitions {
		return [16]byte{}, nil, ErrMaterialPosition
	}
	partitions := make([]MaterialPartition, 0, len(topic.Partitions))
	for _, partition := range topic.Partitions {
		if partition.ErrorCode != 0 || partition.Partition < 0 || partition.Leader < 0 || partition.LeaderEpoch < 0 {
			return [16]byte{}, nil, ErrMaterialPosition
		}
		partitions = append(partitions, MaterialPartition{Partition: partition.Partition, LeaderEpoch: partition.LeaderEpoch})
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Partition < partitions[j].Partition })
	for i := 1; i < len(partitions); i++ {
		if partitions[i-1].Partition == partitions[i].Partition {
			return [16]byte{}, nil, ErrMaterialPosition
		}
	}

	return topic.TopicID, partitions, nil
}

func (p *KafkaMaterialPositioner) readOffsets(ctx context.Context, partitions []MaterialPartition, timestamp int64) (map[int32]int64, error) {
	query := kmsg.NewPtrListOffsetsRequest()
	query.IsolationLevel = 1 // Use the last stable offset so pending transactions are not skipped.
	topic := kmsg.ListOffsetsRequestTopic{Topic: p.topic, Partitions: make([]kmsg.ListOffsetsRequestTopicPartition, len(partitions))}
	epochs := make(map[int32]int32, len(partitions))
	for i, partition := range partitions {
		epochs[partition.Partition] = partition.LeaderEpoch
		topic.Partitions[i] = kmsg.ListOffsetsRequestTopicPartition{
			Partition: partition.Partition, CurrentLeaderEpoch: partition.LeaderEpoch, Timestamp: timestamp,
		}
	}
	query.Topics = []kmsg.ListOffsetsRequestTopic{topic}
	offsets := make(map[int32]int64, len(partitions))
	for _, shard := range p.client.RequestSharded(ctx, query) {
		reply, ok := shard.Resp.(*kmsg.ListOffsetsResponse)
		if shard.Err != nil || !ok || len(reply.Topics) != 1 || reply.Topics[0].Topic != p.topic {
			return nil, ErrMaterialPosition
		}
		for _, partition := range reply.Topics[0].Partitions {
			epoch, expected := epochs[partition.Partition]
			if partition.ErrorCode != 0 || partition.Offset < 0 || !expected ||
				(timestamp == latestMaterialOffset && epoch != partition.LeaderEpoch) {
				return nil, ErrMaterialPosition
			}
			if _, duplicate := offsets[partition.Partition]; duplicate {
				return nil, ErrMaterialPosition
			}
			offsets[partition.Partition] = partition.Offset
		}
	}
	if len(offsets) != len(partitions) {
		return nil, ErrMaterialPosition
	}

	return offsets, nil
}
