package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	defaultCatalogActivityTopic = "catalog.activity.v1"
	defaultTaskActivityTopic    = "task.activity.v1"
	defaultConsumerGroup        = "feed-v1"
)

// Config — runtime-конфигурация Feed. Инфраструктура (HTTP, Postgres, лог,
// Kafka-брокеры producer/consumer) — на parker.
type Config struct {
	GatewayToken         string // FEED_GATEWAY_TOKEN: валидирует X-Feed-Service-Token
	CatalogActivityTopic string // KAFKA_CATALOG_ACTIVITY_TOPIC
	TaskActivityTopic    string // KAFKA_TASK_ACTIVITY_TOPIC
	ConsumerGroup        string // FEED_CONSUMER_GROUP
}

// Load читает, нормализует и проверяет бизнес-конфигурацию окружения Feed.
func Load() (Config, error) {
	cfg := Config{
		GatewayToken:         strings.TrimSpace(os.Getenv("FEED_GATEWAY_TOKEN")),
		CatalogActivityTopic: env("KAFKA_CATALOG_ACTIVITY_TOPIC", defaultCatalogActivityTopic),
		TaskActivityTopic:    env("KAFKA_TASK_ACTIVITY_TOPIC", defaultTaskActivityTopic),
		ConsumerGroup:        env("FEED_CONSUMER_GROUP", defaultConsumerGroup),
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
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
	if c.ConsumerGroup == "" {
		return errors.New("FEED_CONSUMER_GROUP не задан")
	}

	return nil
}

// env возвращает очищенную переменную окружения или fallback.
func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}
