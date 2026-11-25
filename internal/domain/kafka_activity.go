package domain

type KafkaActivity struct {
	Group       string
	Position    EventPosition
	Fingerprint [32]byte
	Catalog     *CatalogChange
	Task        *TaskPublication
	Material    *MaterialChange
	Rejection   *EventRejection
}
