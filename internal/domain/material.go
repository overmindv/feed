package domain

import "time"

type MaterialChange struct {
	MaterialID       string
	EventID          string
	AggregateVersion int64
	OccurredAt       time.Time
	RevisionID       string
	Title            string
}
