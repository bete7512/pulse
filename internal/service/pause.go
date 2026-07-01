package service

//go:generate go run go.uber.org/mock/mockgen -destination=mocks/pausecontrol_mock.go -package=mocks github.com/bete7512/pulse/internal/service PauseControlService

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/bete7512/pulse/internal/repos"
)

// DispatchGate is the in-memory pause switch the dispatcher reads on every tick — the fast
// path: an atomic load, never a DB call. The durable truth lives in the dispatch_control row;
// PauseControlService keeps this gate in sync. A nil *DispatchGate reads as "open" (never
// paused), so a dispatcher constructed without one dispatches normally.
type DispatchGate struct{ paused atomic.Bool }

func NewDispatchGate() *DispatchGate { return &DispatchGate{} }

// Paused reports whether dispatch is currently gated. nil-safe.
func (g *DispatchGate) Paused() bool { return g != nil && g.paused.Load() }

func (g *DispatchGate) set(v bool) { g.paused.Store(v) }

// PauseControlService owns the dispatch-pause switch: the admin writes (Pause/Resume/Status),
// a boot-time prime, and a refresh loop that converges this instance's gate onto the durable
// row (so a pause set on another instance is picked up within one interval).
type PauseControlService interface {
	Run(ctx context.Context)         // refresh loop: row → gate, every interval
	Prime(ctx context.Context) error // one synchronous read at boot (fail-safe)
	Pause(ctx context.Context, reason string) error
	Resume(ctx context.Context) error
	Status(ctx context.Context) (repos.DispatchControl, error)
}

type pauseControlService struct {
	repo     repos.DispatchControlRepo
	gate     *DispatchGate
	interval time.Duration
	logger   *slog.Logger
}

func NewPauseControl(repo repos.DispatchControlRepo, gate *DispatchGate, interval time.Duration, logger *slog.Logger) PauseControlService {
	if logger == nil {
		logger = slog.Default()
	}
	return &pauseControlService{repo: repo, gate: gate, interval: interval, logger: logger}
}

// Pause writes the durable switch then flips the local gate immediately, so the instance that
// served the request stops dispatching without waiting for its own next refresh. If the DB
// write fails the gate is left untouched and the error surfaces.
func (s *pauseControlService) Pause(ctx context.Context, reason string) error {
	if err := s.repo.SetPaused(ctx, true, reason); err != nil {
		return err
	}
	s.gate.set(true)
	return nil
}

func (s *pauseControlService) Resume(ctx context.Context) error {
	if err := s.repo.SetPaused(ctx, false, ""); err != nil {
		return err
	}
	s.gate.set(false)
	return nil
}

func (s *pauseControlService) Status(ctx context.Context) (repos.DispatchControl, error) {
	return s.repo.Get(ctx)
}

// Prime reads the durable state once at startup and sets the gate before the server accepts
// workers — so a pause set before a restart survives it. Fail-safe: the caller aborts startup
// on error rather than serve in an unknown dispatch state.
func (s *pauseControlService) Prime(ctx context.Context) error {
	ctrl, err := s.repo.Get(ctx)
	if err != nil {
		return err
	}
	s.gate.set(ctrl.Paused)
	return nil
}

// refresh is one tick of the loop: reconcile the gate to the durable row. Fail-open: on a read
// error it logs and keeps the last known gate value, so a transient DB blip can't halt dispatch.
func (s *pauseControlService) refresh(ctx context.Context) {
	ctrl, err := s.repo.Get(ctx)
	if err != nil {
		s.logger.Error("pause refresh failed", "error", err)
		return
	}
	s.gate.set(ctrl.Paused)
}

func (s *pauseControlService) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refresh(ctx)
		}
	}
}
