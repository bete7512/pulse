package domain

import (
	"encoding/json"
	"time"
)

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
		case JobStarted:
			job.Status = Running
			job.StartedAt = &event.CreatedAt
		case JobCompleted:
			job.Status = Completed
			job.CompletedAt = &event.CreatedAt
		case JobFailed:
			job.Status = Failed
			job.CompletedAt = &event.CreatedAt
		case JOBCanceled:
			job.Status = Canceled
			job.CompletedAt = &event.CreatedAt
		default:
		}
		if job.Status != JobStatus("") {
			job.ID = event.JobId
			job.Message = event.Message
			job.Payload = event.Payload
		}
	}
	return job
}
