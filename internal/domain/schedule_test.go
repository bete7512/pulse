package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/bete7512/pulse/internal/domain"
)

func TestSchedule_Validate(t *testing.T) {
	tests := []struct {
		name    string
		sched   domain.Schedule
		wantErr bool
	}{
		{"once is valid", domain.Schedule{Topic: "t", Kind: domain.ScheduleOnce}, false},
		{"interval valid", domain.Schedule{Topic: "t", Kind: domain.ScheduleInterval, Interval: time.Minute}, false},
		{"cron valid", domain.Schedule{Topic: "t", Kind: domain.ScheduleCron, CronExpr: "0 * * * *"}, false},
		{"empty topic rejected", domain.Schedule{Kind: domain.ScheduleOnce}, true},
		{"interval must be positive", domain.Schedule{Topic: "t", Kind: domain.ScheduleInterval, Interval: 0}, true},
		{"cron expr required", domain.Schedule{Topic: "t", Kind: domain.ScheduleCron}, true},
		{"unknown kind rejected", domain.Schedule{Topic: "t", Kind: domain.ScheduleKind("weekly")}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sched.Validate()
			if tt.wantErr {
				if !errors.Is(err, domain.ErrInvalidSchedule) {
					t.Fatalf("err = %v, want ErrInvalidSchedule", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
		})
	}
}
