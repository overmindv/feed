package consumer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/overmindv/feed/internal/domain"
)

// catalogEnvelope — конверт события каталога от entities (topic catalog.activity.v1).
type catalogEnvelope struct {
	EventType     string          `json:"event_type"`
	OccurredAt    time.Time       `json:"occurred_at"`
	ActorUserID   string          `json:"actor_user_id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Payload       json.RawMessage `json:"payload"`
}

// catalogPayload — достаточное подмножество строки каталога для рендера ленты.
// Университет/программа/курс имеют .name, тема — .title; статус активен у всех.
type catalogPayload struct {
	Name   string `json:"name"`
	Title  string `json:"title"`
	Status string `json:"status"`
}

// taskEnvelope — конверт события публикации задачи от tasks (topic task.activity.v1).
type taskEnvelope struct {
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
	TaskID     string    `json:"task_id"`
	Title      string    `json:"title"`
	TopicID    *string   `json:"topic_id"`
	TaskType   string    `json:"task_type"`
	Difficulty string    `json:"difficulty"`
}

// aggregatorName — человекочитаемое имя aggregate для заголовков.
func aggregatorName(aggregateType string) string {
	switch aggregateType {
	case "university":
		return "университет"
	case "program":
		return "программа"
	case "course":
		return "курс"
	case "topic":
		return "тема"
	case "task":
		return "задача"
	default:
		return aggregateType
	}
}

// envelopeMeta — общее поле для дискриминации источника события.
// Продюсеры кодируют источник в имени event_type (catalog.* / task.*), поэтому
// источник определяется по нему, а не угадыванием по полям конверта.
type envelopeMeta struct {
	EventType string `json:"event_type"`
}

// Render преобразует сообщение из Kafka (value) в запись ленты.
// ok=false означает, что событие не должно попадать в ленту (отфильтровано).
func Render(key string, value []byte) (domain.FeedItem, bool, error) {
	var meta envelopeMeta
	if err := json.Unmarshal(value, &meta); err != nil {
		return domain.FeedItem{}, false, fmt.Errorf("parse feed envelope: %w", err)
	}
	switch {
	case strings.HasPrefix(meta.EventType, "catalog."):
		return renderCatalog(key, value)
	case strings.HasPrefix(meta.EventType, "task."):
		return renderTask(key, value)
	default:
		return domain.FeedItem{}, false, fmt.Errorf("unknown feed event_type %q", meta.EventType)
	}
}

// renderCatalog фильтрует события каталога и рендерит заголовок/ссылку.
// В ленту попадают: created всегда; updated — только когда сущность активна
// (публикация draft→active). Deleted/prerequisite и updated-черновики пропускаются.
func renderCatalog(key string, value []byte) (domain.FeedItem, bool, error) {
	var e catalogEnvelope
	if err := json.Unmarshal(value, &e); err != nil {
		return domain.FeedItem{}, false, fmt.Errorf("parse catalog envelope: %w", err)
	}
	if e.EventType == "" || e.AggregateID == "" {
		return domain.FeedItem{}, false, fmt.Errorf("catalog event без event_type/aggregate_id")
	}

	aggregate := e.AggregateType
	name := aggregateName(e.Payload)
	activated := strings.HasSuffix(e.EventType, ".updated") && payloadActive(e.Payload)

	var kind string
	switch {
	case strings.HasSuffix(e.EventType, ".created"):
		kind = aggregate + ".created"
	case activated:
		kind = aggregate + ".activated"
	default:
		// deleted / prerequisite.* / updated-не-active — в ленту не публикуем.
		return domain.FeedItem{}, false, nil
	}

	mask := "Новый %s: %s"
	if activated {
		mask = "Активирован %s: %s"
	}

	return domain.FeedItem{
		Source:        "catalog",
		EventKey:      e.AggregateID,
		Kind:          kind,
		ActorUserID:   e.ActorUserID,
		AggregateType: aggregate,
		AggregateID:   e.AggregateID,
		OccurredAt:    e.OccurredAt,
		Title:         fmt.Sprintf(mask, aggregatorName(aggregate), name),
		Text:          strings.TrimSpace(name),
		Href:          "/" + aggregate + "s/" + e.AggregateID,
		RawPayload:    e.Payload,
	}, true, nil
}

// renderTask рендерит публикацию задачи. Событий сразу «published»-типа нет —
// принимаем любой task-конверт как факт публикации.
func renderTask(key string, value []byte) (domain.FeedItem, bool, error) {
	var e taskEnvelope
	if err := json.Unmarshal(value, &e); err != nil {
		return domain.FeedItem{}, false, fmt.Errorf("parse task envelope: %w", err)
	}
	if e.TaskID == "" {
		return domain.FeedItem{}, false, fmt.Errorf("task event без task_id")
	}
	if e.EventType != "" && !strings.HasSuffix(e.EventType, "published") {
		// На будущее: если появятся другие task-события, их можно фильтровать здесь.
		return domain.FeedItem{}, false, nil
	}

	title := strings.TrimSpace(e.Title)
	if title == "" {
		title = "Задача"
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return domain.FeedItem{}, false, fmt.Errorf("marshal task envelope: %w", err)
	}

	return domain.FeedItem{
		Source:        "task",
		EventKey:      e.TaskID,
		Kind:          "task.published",
		ActorUserID:   "",
		AggregateType: "task",
		AggregateID:   e.TaskID,
		OccurredAt:    e.OccurredAt,
		Title:         "Новая задача: " + title,
		Text:          "",
		Href:          "/tasks/" + e.TaskID,
		RawPayload:    raw,
	}, true, nil
}

// aggregateName достаёт человекочитаемое имя из payload (name для каталога, title для темы).
func aggregateName(payload json.RawMessage) string {
	var p catalogPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return ""
	}
	if strings.TrimSpace(p.Name) != "" {
		return strings.TrimSpace(p.Name)
	}

	return strings.TrimSpace(p.Title)
}

// payloadActive определяет, стала ли сущность активной (опубликованной).
func payloadActive(payload json.RawMessage) bool {
	var p catalogPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return false
	}

	return p.Status == "active"
}
