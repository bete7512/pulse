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
