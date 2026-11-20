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
	EventID          *string         `json:"event_id"`
	EventType        string          `json:"event_type"`
	OccurredAt       time.Time       `json:"occurred_at"`
	ActorUserID      string          `json:"actor_user_id"`
	AggregateType    string          `json:"aggregate_type"`
	AggregateID      string          `json:"aggregate_id"`
	AggregateVersion int64           `json:"aggregate_version"`
	CatalogRevision  int64           `json:"catalog_revision"`
	Visibility       *feedVisibility `json:"feed_visibility"`
	Payload          catalogPayload  `json:"payload"`
}

type feedVisibility struct {
	Visible    bool     `json:"visible"`
	Ancestors  []string `json:"ancestors"`
	ScopeReset bool     `json:"scope_reset"`
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
	EventID    *string   `json:"event_id,omitempty"`
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

type event struct {
	source  string
	task    *domain.TaskPublication
	catalog *domain.CatalogChange
}

const maxEventSize = 1 << 20

func decodeEvent(key string, value []byte) (event, error) {
	if len(value) > maxEventSize {
		return event{}, domain.RejectTooLarge
	}
	var meta envelopeMeta
	if err := json.Unmarshal(value, &meta); err != nil {
		return event{}, domain.RejectInvalidJSON
	}
	switch {
	case strings.HasPrefix(meta.EventType, "catalog."):
		change, err := decodeCatalog(key, value)
		if err != nil && err != domain.RejectUnknownType {
			err = domain.RejectInvalidCatalog
		}
		return event{source: "catalog", catalog: change}, err
	case strings.HasPrefix(meta.EventType, "task."):
		publication, err := decodeTaskPublication(key, value)
		if err != nil && err != domain.RejectUnknownType {
			err = domain.RejectInvalidTask
		}
		return event{source: "task", task: publication}, err
	default:
		return event{}, domain.RejectUnknownType
	}
}

func decodeCatalog(key string, value []byte) (*domain.CatalogChange, error) {
	var e catalogEnvelope
	if err := json.Unmarshal(value, &e); err != nil {
		return nil, fmt.Errorf("parse catalog envelope: %w", err)
	}
	if !catalogKind(e.AggregateType) || !validUUID(e.AggregateID) || (key != "" && key != e.AggregateID) {
		return nil, fmt.Errorf("invalid catalog aggregate key")
	}
	prefix := "catalog." + e.AggregateType + "."
	action, ok := strings.CutPrefix(e.EventType, prefix)
	if !ok {
		return nil, fmt.Errorf("catalog aggregate type does not match the event")
	}
	switch action {
	case "created", "updated", "unpublished", "deleted":
	case "prerequisite.added", "prerequisite.removed":
		if e.AggregateType == "topic" {
			return nil, nil
		}
		fallthrough
	default:
		return nil, domain.RejectUnknownType
	}
	if e.CatalogRevision == 0 && e.AggregateVersion == 0 {
		return nil, nil
	}
	if e.CatalogRevision < 1 || e.AggregateVersion < 1 {
		return nil, fmt.Errorf("invalid catalog event version")
	}
	eventID, err := parseEventID(e.EventID)
	if err != nil {
		return nil, err
	}
	change := &domain.CatalogChange{
		EventID:          eventID,
		ResourceKey:      e.AggregateType + ":" + e.AggregateID,
		CatalogRevision:  e.CatalogRevision,
		AggregateVersion: e.AggregateVersion,
	}
	if action == "deleted" || action == "unpublished" || e.Payload.Status != "active" ||
		e.Visibility == nil || !e.Visibility.Visible {
		return change, nil
	}
	if e.Visibility.Ancestors == nil || !validAncestorKeys(change.ResourceKey, e.Visibility.Ancestors) {
		return nil, fmt.Errorf("invalid catalog ancestors")
	}
	if e.OccurredAt.IsZero() || (e.ActorUserID != "" && !validUUID(e.ActorUserID)) {
		return nil, fmt.Errorf("invalid catalog activity metadata")
	}
	name := strings.TrimSpace(e.Payload.Name)
	if name == "" {
		name = strings.TrimSpace(e.Payload.Title)
	}
	if name == "" || strings.ContainsRune(name, 0) {
		return nil, fmt.Errorf("catalog event has no public title")
	}
	const maxTitleRunes = 256
	runes := 0
	for offset := range name {
		if runes == maxTitleRunes {
			name = name[:offset]
			break
		}
		runes++
	}

	aggregate := e.AggregateType
	kind := aggregate + ".created"
	mask := "Новый %s: %s"
	if action == "updated" {
		kind = aggregate + ".activated"
		mask = "Активирован %s: %s"
	}

	change.ScopeReset = e.Visibility.ScopeReset
	change.Ancestors = e.Visibility.Ancestors
	change.Item = &domain.FeedItem{
		Source:        "catalog",
		EventKey:      change.ResourceKey,
		Kind:          kind,
		ActorUserID:   e.ActorUserID,
		AggregateType: aggregate,
		AggregateID:   e.AggregateID,
		OccurredAt:    e.OccurredAt,
		Title:         fmt.Sprintf(mask, aggregatorName(aggregate), name),
		Text:          strings.TrimSpace(name),
		Href:          "/" + aggregate + "s/" + e.AggregateID,
	}

	return change, nil
}

func decodeTaskPublication(key string, value []byte) (*domain.TaskPublication, error) {
	var e taskEnvelope
	if err := json.Unmarshal(value, &e); err != nil {
		return nil, fmt.Errorf("parse task envelope: %w", err)
	}
	switch e.EventType {
	case "task.published":
	case "task.unpublished":
		return nil, nil
	default:
		return nil, domain.RejectUnknownType
	}
	if !validUUID(e.TaskID) || (key != "" && key != e.TaskID) || e.OccurredAt.IsZero() {
		return nil, fmt.Errorf("invalid task activity metadata")
	}
	if strings.ContainsRune(e.Title, 0) || strings.ContainsRune(e.TaskType, 0) ||
		strings.ContainsRune(e.Difficulty, 0) || (e.TopicID != nil && strings.ContainsRune(*e.TopicID, 0)) {
		return nil, fmt.Errorf("invalid task activity fields")
	}
	eventID, err := parseEventID(e.EventID)
	if err != nil {
		return nil, err
	}

	title := strings.TrimSpace(e.Title)
	if title == "" {
		title = "Задача"
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal task envelope: %w", err)
	}

	return &domain.TaskPublication{
		EventID: eventID,
		Item: domain.FeedItem{
			Source:        "task",
			EventKey:      e.TaskID,
			Kind:          "task.published",
			AggregateType: "task",
			AggregateID:   e.TaskID,
			OccurredAt:    e.OccurredAt,
			Title:         "Новая задача: " + title,
			Href:          "/tasks/" + e.TaskID,
			RawPayload:    raw,
		},
	}, nil
}

func parseEventID(value *string) (string, error) {
	if value == nil {
		return "", nil
	}
	if !validUUID(*value) {
		return "", fmt.Errorf("invalid event_id")
	}

	return *value, nil
}

func catalogKind(kind string) bool {
	switch kind {
	case "university", "program", "course", "topic":
		return true
	default:
		return false
	}
}

func validUUID(id string) bool {
	if len(id) != 36 || id == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for i := range len(id) {
		c := id[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return false
			}
		}
	}

	return true
}

func validAncestorKeys(self string, keys []string) bool {
	if len(keys) > 127 {
		return false
	}
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		kind, id, ok := strings.Cut(key, ":")
		if !ok || key == self || !catalogKind(kind) || !validUUID(id) {
			return false
		}
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}

	return true
}
