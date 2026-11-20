package consumer

import (
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/overmindv/feed/internal/domain"
)

const (
	sourceMaterial    = "material"
	materialPublished = "material.published"
	materialArchived  = "material.archived"
)

type materialEnvelope struct {
	EventID          string    `json:"event_id"`
	SchemaVersion    int       `json:"schema_version"`
	OccurredAt       time.Time `json:"occurred_at"`
	AggregateID      string    `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	RevisionID       string    `json:"revision_id"`
	Payload          struct {
		Tombstone *bool `json:"tombstone"`
		Metadata  *struct {
			Title string `json:"title"`
		} `json:"metadata"`
	} `json:"payload"`
}

func decodeMaterial(eventType, key string, value []byte) (*domain.MaterialChange, error) {
	if eventType != materialPublished && eventType != materialArchived {
		return nil, domain.RejectUnknownType
	}

	var e materialEnvelope
	if err := json.Unmarshal(value, &e); err != nil {
		return nil, domain.RejectInvalidMaterial
	}
	if e.SchemaVersion != 1 || !validUUID(e.EventID) || !validUUID(e.AggregateID) ||
		(key != "" && key != e.AggregateID) || e.AggregateVersion < 1 || e.OccurredAt.IsZero() ||
		e.Payload.Tombstone == nil || *e.Payload.Tombstone != (eventType == materialArchived) {
		return nil, domain.RejectInvalidMaterial
	}

	change := &domain.MaterialChange{
		MaterialID:       e.AggregateID,
		EventID:          e.EventID,
		AggregateVersion: e.AggregateVersion,
		OccurredAt:       e.OccurredAt,
	}
	if eventType == materialArchived {
		return change, nil
	}
	if !validUUID(e.RevisionID) || e.Payload.Metadata == nil {
		return nil, domain.RejectInvalidMaterial
	}
	title := strings.TrimSpace(e.Payload.Metadata.Title)
	if title == "" || strings.ContainsRune(title, 0) || utf8.RuneCountInString(title) > 256 {
		return nil, domain.RejectInvalidMaterial
	}
	change.RevisionID = e.RevisionID
	change.Title = "Опубликован материал: " + title

	return change, nil
}
