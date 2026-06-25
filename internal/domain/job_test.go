package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bete7512/pulse/internal/domain"
	"github.com/google/go-cmp/cmp"
)

func TestRebuildJob(t *testing.T) {

	startingTime := time.Now()
	testcases := []struct {
		name        string
		input       []domain.Event
		want        *domain.Job
		expectedErr error
	}{
		{
			name: "Get Pending",
			input: []domain.Event{
				{
					ID:       "fudsi-fasd-afsd-afsd-afsd",
					Type:     domain.JobSubmitted,
					JobId:    "fajhdsfghakjsgfkjasgfjkgadfs",
					Sequence: 1,
					Payload: func() []byte {
						payload := map[string]interface{}{
							"name": "bete",
						}

						bytes, _ := json.Marshal(payload)
						return bytes
					}(),
					Message:   "afds",
					CreatedAt: startingTime,
				},
			},
			want: &domain.Job{
				ID:          "fajhdsfghakjsgfkjasgfjkgadfs",
				Status:      domain.Pending,
				SubmittedAt: startingTime,
				Payload: func() []byte {
					payload := map[string]interface{}{
						"name": "bete",
					}

					bytes, _ := json.Marshal(payload)
					return bytes
				}(),
				Message: "afds",
			},
		},
		{
			name: "wrong type",
			input: []domain.Event{
				{
					ID:       "fudsi-fasd-afsd-afsd-afsd",
					Type:     domain.EventType("unknown"),
					JobId:    "fajhdsfghakjsgfkjasgfjkgadfs",
					Sequence: 1,
					Payload: func() []byte {
						payload := map[string]interface{}{
							"name": "bete",
						}

						bytes, _ := json.Marshal(payload)
						return bytes
					}(),
					Message:   "afds",
					CreatedAt: startingTime,
				},
			},
			want: &domain.Job{},
		},
		{
			name: "happy path: submitted→started→completed",
			input: []domain.Event{
				{JobId: "j", Type: domain.JobSubmitted, Topic: "send-email", CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobStarted, CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobCompleted, CreatedAt: startingTime},
			},
			want: &domain.Job{
				ID: "j", Topic: "send-email", Status: domain.Completed, Attempts: 1,
				SubmittedAt: startingTime, StartedAt: &startingTime, CompletedAt: &startingTime,
			},
		},
		{
			name: "retry then dead-letter keeps last failure reason",
			input: []domain.Event{
				{JobId: "j", Type: domain.JobSubmitted, CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobStarted, CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobFailed, Message: "boom", CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobRetried, CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobStarted, CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobFailed, Message: "again", CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobRetried, CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobStarted, CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobFailed, Message: "final", CreatedAt: startingTime},
				{JobId: "j", Type: domain.JobDeadLettered, CreatedAt: startingTime},
			},
			want: &domain.Job{
				ID: "j", Status: domain.DeadLettered, Attempts: 3, Message: "final",
				SubmittedAt: startingTime, StartedAt: &startingTime, CompletedAt: &startingTime,
			},
		},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			got := domain.RebuildJob(tt.input)
			if diff := cmp.Diff(tt.want, &got); diff != "" {
				t.Errorf("RebuildJob() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
