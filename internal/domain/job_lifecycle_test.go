package domain_test

import (
	"testing"
	"time"

	"github.com/bete7512/pulse/internal/domain"
)

func TestJob_Start(t *testing.T) {
	tests := []struct {
		name    string
		status  domain.JobStatus
		wantErr bool
	}{
		{"from pending", domain.Pending, false},
		{"from retrying", domain.Retrying, false}, // a retried job is dispatchable
		{"from running rejected", domain.Running, true},
		{"from completed rejected", domain.Completed, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := domain.Job{ID: "j", Status: tt.status}.Start()
			if tt.wantErr {
				if err != domain.ErrInvalidTransition {
					t.Fatalf("err = %v, want ErrInvalidTransition", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if ev.Type != domain.JobStarted {
				t.Errorf("event type = %q, want JOB_STARTED", ev.Type)
			}
		})
	}
}

// TestJob_CompleteAndCancel tables both single-event terminal commands: each is legal only
// from a specific status and otherwise returns ErrInvalidTransition. The command is passed
// as a method expression so one loop body drives both.
func TestJob_CompleteAndCancel(t *testing.T) {
	tests := []struct {
		name     string
		status   domain.JobStatus
		call     func(domain.Job) (domain.Event, error)
		wantErr  bool
		wantType domain.EventType
	}{
		{"complete from running", domain.Running, domain.Job.Complete, false, domain.JobCompleted},
		{"complete from pending rejected", domain.Pending, domain.Job.Complete, true, ""},
		{"cancel from running", domain.Running, domain.Job.Cancel, false, domain.JobCanceled},
		{"cancel from terminal rejected", domain.Completed, domain.Job.Cancel, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev, err := tt.call(domain.Job{ID: "j", Status: tt.status})
			if tt.wantErr {
				if err != domain.ErrInvalidTransition {
					t.Fatalf("err = %v, want ErrInvalidTransition", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if ev.Type != tt.wantType {
				t.Errorf("event type = %q, want %q", ev.Type, tt.wantType)
			}
		})
	}
}

// TestJob_Fail tables the retry-vs-dead-letter decision, the backoff deadline, and the
// running-only invariant.
func TestJob_Fail(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name        string
		status      domain.JobStatus
		attempts    int
		wantErr     bool
		wantTypes   []domain.EventType
		wantMessage string        // checked on the JobFailed event when non-empty
		wantBackoff time.Duration // 0 ⇒ no JobRetried, so no next_attempt_at to check
	}{
		{
			name:        "retry while attempts remain",
			status:      domain.Running,
			attempts:    1,
			wantTypes:   []domain.EventType{domain.JobFailed, domain.JobRetried},
			wantMessage: "boom",
			wantBackoff: 1 * time.Second, // attempt 1 ⇒ 1²s
		},
		{
			name:      "dead-letter at max attempts",
			status:    domain.Running,
			attempts:  3,
			wantTypes: []domain.EventType{domain.JobFailed, domain.JobDeadLettered},
		},
		{
			name:    "only a running job can fail",
			status:  domain.Pending,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evs, err := domain.Job{ID: "j", Status: tt.status, Attempts: tt.attempts}.Fail("boom", now)
			if tt.wantErr {
				if err != domain.ErrInvalidTransition {
					t.Fatalf("err = %v, want ErrInvalidTransition", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if len(evs) != len(tt.wantTypes) {
				t.Fatalf("events = %+v, want types %v", evs, tt.wantTypes)
			}
			for i, wt := range tt.wantTypes {
				if evs[i].Type != wt {
					t.Errorf("event[%d] type = %q, want %q", i, evs[i].Type, wt)
				}
			}
			if tt.wantMessage != "" && evs[0].Message != tt.wantMessage {
				t.Errorf("failure message = %q, want %q", evs[0].Message, tt.wantMessage)
			}
			if tt.wantBackoff != 0 {
				want := now.Add(tt.wantBackoff)
				if evs[1].NextAttemptAt == nil || !evs[1].NextAttemptAt.Equal(want) {
					t.Errorf("next_attempt_at = %v, want %v", evs[1].NextAttemptAt, want)
				}
			}
		})
	}
}
