package config

import (
	"slices"
	"testing"
)

func setFeedEnv(t *testing.T) {
	t.Helper()
	t.Setenv("FEED_GATEWAY_TOKEN", "fixture")
	t.Setenv("KAFKA_CATALOG_ACTIVITY_TOPIC", "")
	t.Setenv("KAFKA_TASK_ACTIVITY_TOPIC", "")
	t.Setenv("KAFKA_CONTENT_LEARNING_TOPIC", "")
	t.Setenv("KAFKA_CONTENT_CONTRIBUTION_TOPIC", "")
	t.Setenv(materialCardsEnv, "")
}

func TestMaterialTopicOptIn(t *testing.T) {
	setFeedEnv(t)
	t.Setenv(materialTopicEnv, "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	want := make([]string, 0, 3)
	want = append(want, defaultCatalogActivityTopic, defaultTaskActivityTopic)
	if !slices.Equal(cfg.ActivityTopics(), want) {
		t.Fatal(cfg.ActivityTopics())
	}

	t.Setenv(materialTopicEnv, " content.material.v1 ")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.ActivityTopics(), append(want, "content.material.v1")) {
		t.Fatal(cfg.ActivityTopics())
	}
	if cfg.MaterialCardsEnabled {
		t.Fatal("ingestion must not enable serving material cards")
	}
	t.Setenv(materialCardsEnv, "true")
	cfg, err = Load()
	if err != nil || !cfg.MaterialCardsEnabled {
		t.Fatal(err)
	}
	t.Setenv(materialTopicEnv, "")
	if _, err = Load(); err == nil {
		t.Fatal("material cards require ingestion")
	}
}

func TestMaterialTopicCannotSubscribeToPrivateStreams(t *testing.T) {
	setFeedEnv(t)
	const privateTopic = "internal.reads.v1"
	t.Setenv("KAFKA_CONTENT_LEARNING_TOPIC", privateTopic)

	for _, topic := range []string{
		defaultContentLearningTopic, defaultContentReviewTopic, privateTopic,
		defaultTaskActivityTopic, defaultCatalogActivityTopic,
	} {
		t.Setenv(materialTopicEnv, topic)
		if _, err := Load(); err == nil {
			t.Error("accepted conflicting topic", topic)
		}
	}
}
