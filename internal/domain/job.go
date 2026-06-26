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
	Pending      JobStatus = "PENDING"
	Running      JobStatus = "RUNNING"
	Completed    JobStatus = "COMPLETED"
	Failed       JobStatus = "FAILED"
	Retrying     JobStatus = "RETRYING"
	DeadLettered JobStatus = "DEAD_LETTERED"
	Canceled     JobStatus = "CANCELED"
)

// maxAttempts is how many times a job runs before it is dead-lettered.
const maxAttempts = 3

type Job struct {
	ID          string          `json:"id"`
	Topic       string          `json:"topic"` // job topic, carried from the JobSubmitted event
	Status      JobStatus       `json:"status"`
	Attempts    int             `json:"attempts"` // number of times the job has been started
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
			job.Attempts++
		case JobCompleted:
			job.Status = Completed
			job.CompletedAt = &event.CreatedAt
		case JobFailed:
			job.Status = Failed
			job.CompletedAt = &event.CreatedAt
		case JobRetried:
			job.Status = Retrying // dispatchable again
		case JobDeadLettered:
			job.Status = DeadLettered
		case JobCanceled:
			job.Status = Canceled
			job.CompletedAt = &event.CreatedAt
		default:
		}
		if job.Status != JobStatus("") {
			job.ID = event.JobId
			// Keep the last *meaningful* message: a failure reason set on JobFailed must
			// survive the empty-message JobRetried/JobDeadLettered that follows it.
			if event.Message != "" {
				job.Message = event.Message
			}
		}
	}
	return job
}

// Start transitions a dispatchable job (pending, or retrying after a failure) to running.
func (j Job) Start() (Event, error) {
	if j.Status != Pending && j.Status != Retrying { // the invariant
		return Event{}, ErrInvalidTransition // only a dispatchable job can start
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
	return Event{JobId: j.ID, Type: JobCanceled}, nil
}

// Fail records a running job's failure and decides its fate: retry while attempts
// remain, otherwise dead-letter. It returns both events so the transition appends
// them atomically, in order. now is injected so the backoff deadline is testable.
func (j Job) Fail(reason string, now time.Time) ([]Event, error) {
	if j.Status != Running { // the invariant
		return nil, ErrInvalidTransition // only a running job can fail
	}
	failed := Event{JobId: j.ID, Type: JobFailed, Message: reason} // record the failure
	if j.Attempts >= maxAttempts {
		return []Event{failed, {JobId: j.ID, Type: JobDeadLettered}}, nil // give up
	}
	nextAttemptAt := now.Add(backoff(j.Attempts)) // space the retry out
	return []Event{failed, {JobId: j.ID, Type: JobRetried, NextAttemptAt: &nextAttemptAt}}, nil
}

// backoff returns how long to wait before the n-th attempt's retry: 1s, 4s, 9s …
// (n² seconds), so repeated failures back off instead of hammering.
func backoff(attempts int) time.Duration {
	return time.Duration(attempts*attempts) * time.Second
}

// isTerminal reports whether the job has reached a final, immutable status.
func (j Job) isTerminal() bool {
	switch j.Status {
	case Completed, Canceled, DeadLettered:
		return true
	default:
		return false
	}
}
