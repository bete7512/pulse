package service

import (
	"context"
	"time"

	"github.com/bete7512/pulse/internal/domain"
)

// Test-only seams: expose the loop bodies (one tick of work) to the external
// service_test package without widening the public interfaces. Only compiled under
// `go test`; living in package service, they can reach the unexported concrete types
// behind the ProjectorService / WatchdogService / SchedulerService interfaces.

func Sweep(w WatchdogService, ctx context.Context) error { return w.(*watchdogService).sweep(ctx) }
func CatchUp(p ProjectorService, ctx context.Context) error {
	return p.(*projectorService).catchUp(ctx)
}
func Tick(s SchedulerService, ctx context.Context) error { return s.(*schedulerService).tick(ctx) }

// NextRun exposes the pure next-occurrence computation for table-driven testing.
func NextRun(s domain.Schedule, now time.Time) (time.Time, error) { return nextRun(s, now) }
