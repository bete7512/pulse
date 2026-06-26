package repos

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
)

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/eventrepo_mock.go -package=mocks github.com/bete7512/pulse/internal/repos EventRepo

// EventRepo is the append-only event log — the write side plus the queries dispatch
// and recovery depend on. Implemented by repos/postgres; mocked for service tests.
type EventRepo interface {
	Append(ctx context.Context, event domain.Event) (string, error)
	// AppendBatch appends events in order within a single transaction, so a multi-event
	// transition (e.g. Failed + Retried) lands atomically or not at all.
	AppendBatch(ctx context.Context, events []domain.Event) error
	LoadEventsForJob(ctx context.Context, jobID string) ([]domain.Event, error)
	// ListEventsByTopics returns each job's latest event when it is dispatchable
	// (JobSubmitted, or JobRetried whose next_attempt_at has passed), optionally filtered to topics.
	ListEventsByTopics(ctx context.Context, topics []string) ([]domain.Event, error)
	// OrphanedRunning returns running jobs (latest event JobStarted) with no liveness row,
	// started longer ago than grace — the watchdog's fallback for a failed liveness mark.
	OrphanedRunning(ctx context.Context, grace time.Duration) ([]string, error)
	// FiresBySchedule returns the JobSubmitted events a schedule spawned (its fire history),
	// newest first — lineage from the schedule_id tag.
	FiresBySchedule(ctx context.Context, scheduleID string) ([]domain.Event, error)
}
