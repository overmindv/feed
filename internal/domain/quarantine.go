package domain

type RejectionReason string

const (
	RejectTooLarge       RejectionReason = "too_large"
	RejectInvalidJSON    RejectionReason = "invalid_json"
	RejectUnknownType    RejectionReason = "unknown_type"
	RejectInvalidCatalog RejectionReason = "invalid_catalog"
	RejectInvalidTask    RejectionReason = "invalid_task"
	RejectWrongSource    RejectionReason = "wrong_source"
)

func (r RejectionReason) Error() string { return string(r) }

type EventPosition struct {
	Topic     string
	Partition int32
	Offset    int64
}

type EventRejection struct {
	Position    EventPosition
	Source      string
	Reason      RejectionReason
	PayloadSize int
	Identity    RejectionIdentity
}

type RejectionIdentity struct {
	Kind string
	Hash [32]byte
}
