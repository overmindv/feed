package domain

import (
	"encoding/json"
	"time"
)

// FeedItem — одна запись ленты активностей, отрендеренная в заголовок/ссылку.
// Source/EventKey образуют идемпотентный ключ (source='catalog'|'task',
// event_key=aggregate_id|task_id).
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
