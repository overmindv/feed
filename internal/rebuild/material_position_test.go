package rebuild

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const materialPositionTopic = "content.material.v1"

type materialBrokerFixture struct {
	metadataCalls int
	recreated     bool
	missing       bool
}

func (f *materialBrokerFixture) Request(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
	query, ok := req.(*kmsg.MetadataRequest)
	if !ok || query.AllowAutoTopicCreation || len(query.Topics) != 1 ||
		query.Topics[0].Topic == nil || *query.Topics[0].Topic != materialPositionTopic {
		return nil, errors.New("unexpected metadata request")
	}
	f.metadataCalls++
	id := byte(1)
	if f.recreated && f.metadataCalls == 2 {
		id = 2
	}
	topic := materialPositionTopic

	return &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{{
		Topic: &topic, TopicID: [16]byte{id},
		Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 1, LeaderEpoch: 3}, {Partition: 0, LeaderEpoch: 2}},
	}}}, nil
}

func (f *materialBrokerFixture) RequestSharded(_ context.Context, req kmsg.Request) []kgo.ResponseShard {
	query, ok := req.(*kmsg.ListOffsetsRequest)
	if !ok || query.IsolationLevel != 1 || len(query.Topics) != 1 || len(query.Topics[0].Partitions) != 2 ||
		query.Topics[0].Partitions[0].Timestamp != -1 {
		return []kgo.ResponseShard{{Err: errors.New("unexpected ListOffsets request")}}
	}
	partitions := []kmsg.ListOffsetsResponseTopicPartition{
		{Partition: 1, Offset: 9, LeaderEpoch: 3}, {Partition: 0, Offset: 0, LeaderEpoch: 2},
	}
	if f.missing {
		partitions = partitions[:1]
	}

	return []kgo.ResponseShard{{Resp: &kmsg.ListOffsetsResponse{
		Topics: []kmsg.ListOffsetsResponseTopic{{Topic: materialPositionTopic, Partitions: partitions}},
	}}}
}

func TestMaterialPositionRejectsMissingOrRecreatedPartitions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		mock materialBrokerFixture
	}{
		{"missing", materialBrokerFixture{missing: true}},
		{"recreated", materialBrokerFixture{recreated: true}},
		{"complete", materialBrokerFixture{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := KafkaMaterialPositioner{client: &tc.mock, topic: materialPositionTopic}
			position, err := p.CaptureMaterialPosition(t.Context())
			if tc.mock.missing || tc.mock.recreated {
				if !errors.Is(err, ErrMaterialPosition) {
					t.Error("accepted an incomplete Kafka position", err)
				}

				return
			}
			if err != nil || len(position.Partitions) != 2 || position.Partitions[0].Partition != 0 ||
				position.Partitions[1].NextOffset != 9 || len(position.TopicID) != 32 {
				t.Error("unexpected Kafka position", position, err)
			}
		})
	}
}

//nolint:paralleltest // Uses a dedicated loopback Kafka broker.
func TestMaterialPositionWithKafka(t *testing.T) {
	broker := os.Getenv("FEED_TEST_MATERIAL_KAFKA")
	if broker == "" {
		t.Skip("FEED_TEST_MATERIAL_KAFKA is not set")
	}
	if broker != "127.0.0.1:19092" {
		t.Fatal("expected a dedicated loopback Kafka broker on port 19092")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	topic := fmt.Sprintf("feed-position-test-%d", time.Now().UnixNano())
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}}
	reply, err := req.RequestWith(ctx, client)
	if err != nil || len(reply.Topics) != 1 || reply.Topics[0].ErrorCode != 0 {
		t.Fatal("cannot create fixture topic", err)
	}
	if err = client.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 2, Value: []byte("fixture")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	positioner, err := NewKafkaMaterialPositioner(client, topic)
	if err != nil {
		t.Fatal(err)
	}
	position, err := positioner.CaptureMaterialPosition(ctx)
	if err != nil || len(position.Partitions) != 3 || position.Partitions[0].NextOffset != 0 ||
		position.Partitions[1].NextOffset != 0 || position.Partitions[2].NextOffset != 1 {
		t.Error("did not capture every Kafka partition", position, err)
	}
}
