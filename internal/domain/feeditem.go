package domain

import (
	"encoding/json"
	"time"
)

// FeedItem — одна запись ленты активностей, отрендеренная в заголовок/ссылку.
// EventKey определяет группу карточки, а не идентификатор Kafka-события.
// ActorUserID может быть пустым — у событий задач нет доверенного actor.
type FeedItem struct {
	ID            string
	Source        string
	EventKey      string
	Kind          string
	ActorUserID   string
	AggregateType string
	AggregateID   string
	OccurredAt    time.Time
	Title         string
	Text          string
	Href          string
	RawPayload    json.RawMessage
}

type CatalogChange struct {
	EventID          string
	ResourceKey      string
	CatalogRevision  int64
	AggregateVersion int64
	ScopeReset       bool
	Ancestors        []string
	Item             *FeedItem
}

type TaskPublication struct {
	EventID string
	Item    FeedItem
}
