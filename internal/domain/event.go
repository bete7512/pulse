package domain

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	JobSubmitted    EventType = "JOB_SUBMITTED"
	JobStarted      EventType = "JOB_STARTED"
	JobCompleted    EventType = "JOB_COMPLETED"
	JobFailed       EventType = "JOB_FAILED"
	JobRetried      EventType = "JOB_RETRIED"
	JobDeadLettered EventType = "JOB_DEAD_LETTERED"
	JOBCanceled     EventType = "JOB_CANCELED"
)

type Event struct {
	ID        string          `json:"id"`
	Type      EventType       `json:"type"` // the event kind (JOB_SUBMITTED, ...)
	JobId     string          `json:"job_id"`
	Sequence  int64           `json:"sequence"`
	Payload   json.RawMessage `json:"payload"`
	Message   string          `json:"message"`
	CreatedAt time.Time       `json:"created_at"`
	Topic     string          `json:"topic"`           // the job's topic (e.g. "send-email"), set on JobSubmitted
	// NextAttemptAt is set only on JobRetried events: the earliest time the job
	// becomes dispatchable again (backoff). nil for every other event type.
	NextAttemptAt *time.Time `json:"next_attempt_at"`
}
