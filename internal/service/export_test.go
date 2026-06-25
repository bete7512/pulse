package service

import "context"

// Test-only seams: expose the loop bodies (one tick of work) to the external
// service_test package without widening the public interfaces. Only compiled under
// `go test`; living in package service, they can reach the unexported concrete types
// behind the ProjectorService / WatchdogService interfaces.

func Sweep(w WatchdogService, ctx context.Context) error { return w.(*watchdogService).sweep(ctx) }
func CatchUp(p ProjectorService, ctx context.Context) error {
	return p.(*projectorService).catchUp(ctx)
}
