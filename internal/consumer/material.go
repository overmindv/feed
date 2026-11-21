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
	materialWithdrawn = "material.withdrawn"
	materialRestored  = "material.restored"
	materialCorrected = "material.corrected"
)

type materialEnvelope struct {
	EventID          string    `json:"event_id"`
	SchemaVersion    int       `json:"schema_version"`
	OccurredAt       time.Time `json:"occurred_at"`
	AggregateID      string    `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	RevisionID       string    `json:"revision_id"`
	Payload          struct {
		Tombstone             *bool  `json:"tombstone"`
		InvalidatedRevisionID string `json:"invalidated_revision_id"`
		Metadata              *struct {
			Title string `json:"title"`
		} `json:"metadata"`
	} `json:"payload"`
}

func decodeMaterial(eventType, key string, value []byte) (*domain.MaterialChange, error) {
	tombstone := eventType == materialArchived || eventType == materialWithdrawn
	switch eventType {
	case materialPublished, materialArchived, materialWithdrawn, materialRestored, materialCorrected:
	default:
		return nil, domain.RejectUnknownType
	}

	var e materialEnvelope
	if err := json.Unmarshal(value, &e); err != nil {
		return nil, domain.RejectInvalidMaterial
	}
	if e.SchemaVersion != 1 || !validUUID(e.EventID) || !validUUID(e.AggregateID) ||
		(key != "" && key != e.AggregateID) || e.AggregateVersion < 1 || e.OccurredAt.IsZero() ||
		e.Payload.Tombstone == nil || *e.Payload.Tombstone != tombstone {
		return nil, domain.RejectInvalidMaterial
	}
	if eventType == materialCorrected && (!validUUID(e.Payload.InvalidatedRevisionID) || e.Payload.InvalidatedRevisionID == e.RevisionID) {
		return nil, domain.RejectInvalidMaterial
	}
	if eventType == materialWithdrawn && (!validUUID(e.RevisionID) || e.Payload.InvalidatedRevisionID != e.RevisionID || e.Payload.Metadata != nil) {
		return nil, domain.RejectInvalidMaterial
	}

	change := &domain.MaterialChange{
		MaterialID:       e.AggregateID,
		EventID:          e.EventID,
		AggregateVersion: e.AggregateVersion,
		OccurredAt:       e.OccurredAt,
	}
	if tombstone {
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
