package go_pg_message_queue

import "time"

type Message struct {
	SubTransactionID  string
	OwnerOrganization string
	TransactionID     string
	Pulse             time.Time
	CreateDateTime    time.Time
	RetryDateTime     time.Time
	Topic             string
	Status            int
	Retries           int
	Pulses            int
	Errors            int
	Payload           string
	ErrorMessages     string
	ContentId         string
	Priority          int
}

type Result struct {
	Error error
}
