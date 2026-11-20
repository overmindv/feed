package consumer

import (
	"crypto/sha256"
	"encoding/json"
	"strings"

	"github.com/overmindv/feed/internal/domain"
)

const (
	eventIdentity  = "event"
	entityIdentity = "entity"
)

func rejectionIdentity(source, key string, value []byte) domain.RejectionIdentity {
	var ids struct {
		EventID     string `json:"event_id"`
		AggregateID string `json:"aggregate_id"`
		TaskID      string `json:"task_id"`
	}
	if len(value) <= maxEventSize && json.Unmarshal(value, &ids) == nil && validUUID(ids.EventID) {
		return hashIdentity(eventIdentity, ids.EventID)
	}
	if validUUID(key) {
		return hashIdentity(entityIdentity, key)
	}
	entity := ids.AggregateID
	if source == sourceTask {
		entity = ids.TaskID
	}
	if validUUID(entity) {
		return hashIdentity(entityIdentity, entity)
	}

	return domain.RejectionIdentity{}
}

func (e event) identity(kind string) domain.RejectionIdentity {
	var uuid string
	switch {
	case e.task != nil:
		uuid = e.task.Item.AggregateID
		if kind == eventIdentity {
			uuid = e.task.EventID
		}
	case e.catalog != nil:
		_, uuid, _ = strings.Cut(e.catalog.ResourceKey, ":")
		if kind == eventIdentity {
			uuid = e.catalog.EventID
		}
	}

	return hashIdentity(kind, uuid)
}

func hashIdentity(kind, uuid string) domain.RejectionIdentity {
	return domain.RejectionIdentity{Kind: kind, Hash: sha256.Sum256([]byte(uuid))}
}
