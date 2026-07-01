package repos

import (
	"context"
	"time"
)

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/dispatchcontrolrepo_mock.go -package=mocks github.com/bete7512/pulse/internal/repos DispatchControlRepo

// DispatchControl is the current dispatch-pause state, read from the singleton
// dispatch_control row. It is mutable operational config, not an event.
type DispatchControl struct {
	Paused   bool
	Reason   string
	PausedAt *time.Time // when it was last paused; nil if never paused
}

// DispatchControlRepo reads and writes the singleton dispatch-pause switch.
type DispatchControlRepo interface {
	// Get returns the current pause state (the one dispatch_control row).
	Get(ctx context.Context) (DispatchControl, error)
	// SetPaused flips the switch: pausing stamps paused_at and records the reason;
	// resuming clears the reason but keeps paused_at as "last paused at".
	SetPaused(ctx context.Context, paused bool, reason string) error
}
