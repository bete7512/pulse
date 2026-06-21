package domain

import (
	"encoding/json"
	"errors"
	"time"
)

// ErrInvalidTransition is returned by a Job command when the requested transition
// is illegal for the job's current (folded) status.
var ErrInvalidTransition = errors.New("invalid job state transition")

type JobStatus string

const (
	Pending   JobStatus = "PENDING"
	Running   JobStatus = "RUNNING"
	Completed JobStatus = "COMPLETED"
	Failed    JobStatus = "FAILED"
	Canceled  JobStatus = "CANCELED"
)

type Job struct {
	ID          string          `json:"id"`
	Topic       string          `json:"topic"` // job topic, carried from the JobSubmitted event
	Status      JobStatus       `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	Message     string          `json:"message"`
	SubmittedAt time.Time       `json:"submitted_at"`
	StartedAt   *time.Time      `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at"`
}

func RebuildJob(events []Event) Job {
	job := Job{}
	for _, event := range events {
		switch event.Type {
		case JobSubmitted:
			job.Status = Pending
			job.SubmittedAt = event.CreatedAt
			job.Payload = event.Payload
			job.Topic = event.Topic
		case JobStarted:
			job.Status = Running
			job.StartedAt = &event.CreatedAt
		case JobCompleted:
			job.Status = Completed
			job.CompletedAt = &event.CreatedAt
		case JobFailed:
			job.Status = Failed
			job.CompletedAt = &event.CreatedAt
			job.Payload = event.Payload
		case JOBCanceled:
			job.Status = Canceled
			job.CompletedAt = &event.CreatedAt
		default:
		}
		if job.Status != JobStatus("") {
			job.ID = event.JobId
			job.Message = event.Message
		}
	}
	return job
}

// Start transitions a pending job to running.
func (j Job) Start() (Event, error) {
	if j.Status != Pending { // the invariant
		return Event{}, ErrInvalidTransition // can't start a non-pending job
	}
	return Event{JobId: j.ID, Type: JobStarted}, nil
}

// Complete transitions a running job to completed.
func (j Job) Complete() (Event, error) {
	if j.Status != Running { // the invariant
		return Event{}, ErrInvalidTransition // can't complete a job that isn't running
	}
	return Event{JobId: j.ID, Type: JobCompleted}, nil
}

// Cancel transitions a non-terminal job to canceled.
func (j Job) Cancel() (Event, error) {
	if j.isTerminal() { // the invariant
		return Event{}, ErrInvalidTransition // can't cancel an already finished job
	}
	return Event{JobId: j.ID, Type: JOBCanceled}, nil
}

// Fail transitions a non-terminal job to failed, recording why in the event message.
func (j Job) Fail(reason string) (Event, error) {
	if j.isTerminal() { // the invariant
		return Event{}, ErrInvalidTransition // can't fail an already finished job
	}
	return Event{JobId: j.ID, Type: JobFailed, Message: reason}, nil
}

// isTerminal reports whether the job has reached a final, immutable status.
func (j Job) isTerminal() bool {
	switch j.Status {
	case Completed, Failed, Canceled:
		return true
	default:
		return false
	}
}
