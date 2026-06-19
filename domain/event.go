package domain

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	JobSubmitted EventType = "JOB_SUBMITTED"
	JobStarted   EventType = "JOB_STARTED"
	JobCompleted EventType = "JOB_COMPLETED"
	JobFailed    EventType = "JOB_FAILED"
	JOBCanceled  EventType = "JOB_CANCELED"
)

type Event struct {
	ID        string          `json:"id"`
	Type      EventType       `json:"type"`
	JobId     string          `json:"job_id"`
	Sequence  int64           `json:"sequence"`
	Payload   json.RawMessage `json:"payload"`
	Message   string          `json:"message"`
	CreatedAt time.Time       `json:"created_at"`
}
