package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultCatalogActivityTopic = "catalog.activity.v1"
	defaultTaskActivityTopic    = "task.activity.v1"
	defaultContentLearningTopic = "content.learning.v1"
	defaultContentReviewTopic   = "content.contribution.v1"
	defaultConsumerGroup        = "feed-v1"
	materialTopicEnv            = "KAFKA_CONTENT_MATERIAL_TOPIC"
	materialCardsEnv            = "FEED_MATERIAL_CARDS_ENABLED"
)

// Config — runtime-конфигурация Feed. Инфраструктура (HTTP, Postgres, лог,
// Kafka-брокеры producer/consumer) — на parker.
type Config struct {
	GatewayToken          string // FEED_GATEWAY_TOKEN: валидирует X-Feed-Service-Token
	CatalogActivityTopic  string // KAFKA_CATALOG_ACTIVITY_TOPIC
	TaskActivityTopic     string // KAFKA_TASK_ACTIVITY_TOPIC
	MaterialActivityTopic string // KAFKA_CONTENT_MATERIAL_TOPIC, unset disables material activity
	MaterialCardsEnabled  bool
	ConsumerGroup         string // FEED_CONSUMER_GROUP
}

// Load читает, нормализует и проверяет бизнес-конфигурацию окружения Feed.
func Load() (Config, error) {
	showMaterials, err := strconv.ParseBool(env(materialCardsEnv, "false"))
	if err != nil {
		return Config{}, errors.New("FEED_MATERIAL_CARDS_ENABLED must be a boolean")
	}
	cfg := activityTopics()
	cfg.GatewayToken = strings.TrimSpace(os.Getenv("FEED_GATEWAY_TOKEN"))
	cfg.MaterialCardsEnabled = showMaterials
	cfg.ConsumerGroup = env("FEED_CONSUMER_GROUP", defaultConsumerGroup)
	if validateErr := cfg.Validate(); validateErr != nil {
		return Config{}, fmt.Errorf("validate config: %w", validateErr)
	}

	return cfg, nil
}

func activityTopics() Config {
	return Config{
		CatalogActivityTopic:  env("KAFKA_CATALOG_ACTIVITY_TOPIC", defaultCatalogActivityTopic),
		TaskActivityTopic:     env("KAFKA_TASK_ACTIVITY_TOPIC", defaultTaskActivityTopic),
		MaterialActivityTopic: strings.TrimSpace(os.Getenv(materialTopicEnv)),
	}
}

func MaterialRebuildTopic() (string, error) {
	cfg := activityTopics()
	if cfg.MaterialActivityTopic == "" {
		return "", errors.New("KAFKA_CONTENT_MATERIAL_TOPIC is required for the Content snapshot")
	}

	return cfg.MaterialActivityTopic, cfg.validateMaterialTopic()
}

// Validate проверяет обязательные и ограниченные настройки сервиса.
func (c Config) Validate() error {
	if c.GatewayToken == "" {
		return errors.New("FEED_GATEWAY_TOKEN не задан")
	}
	if c.CatalogActivityTopic == "" || c.TaskActivityTopic == "" {
		return errors.New("kafka activity topics обязательны")
	}
	if c.CatalogActivityTopic == c.TaskActivityTopic {
		return errors.New("catalog и task activity topics должны различаться")
	}
	if err := c.validateMaterialTopic(); err != nil {
		return err
	}
	if c.MaterialCardsEnabled && c.MaterialActivityTopic == "" {
		return errors.New("material cards require a material activity topic")
	}
	if c.ConsumerGroup == "" {
		return errors.New("FEED_CONSUMER_GROUP не задан")
	}

	return nil
}

func (c Config) validateMaterialTopic() error {
	if c.MaterialActivityTopic == "" {
		return nil
	}
	switch c.MaterialActivityTopic {
	case c.CatalogActivityTopic, c.TaskActivityTopic,
		defaultContentLearningTopic, defaultContentReviewTopic,
		env("KAFKA_CONTENT_LEARNING_TOPIC", defaultContentLearningTopic),
		env("KAFKA_CONTENT_CONTRIBUTION_TOPIC", defaultContentReviewTopic):
		return errors.New("material activity topic must be distinct from other activity topics")
	default:
		return nil
	}
}

func (c Config) ActivityTopics() []string {
	topics := []string{c.CatalogActivityTopic, c.TaskActivityTopic}
	if c.MaterialActivityTopic != "" {
		topics = append(topics, c.MaterialActivityTopic)
	}

	return topics
}

// env возвращает очищенную переменную окружения или fallback.
func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}
