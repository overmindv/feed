package domain

import (
	"encoding/json"
	"time"
)

// FeedItem — одна запись ленты активностей, отрендеренная в заголовок/ссылку.
// (Source, EventKey, Kind) образуют идемпотентный ключ: redelivery того же события
// дедуплицируется, а разные события одного агрегата (created/activated) сохраняются
// (source='catalog'|'task', event_key=aggregate_id|task_id).
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
