package grpc

import (
	"time"

	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/service"
)

// Server adapts the gRPC wire to the application service. It embeds the generated
// Unimplemented server for forward-compatibility and composes a dispatcher that owns the
// streaming poll→claim→send loop. Handlers are split by role across files:
//
//	producer.go    client-facing  — SubmitJob, GetJob
//	consumer.go    worker-facing  — StreamJobs, ReportResult, Heartbeat
//	dispatcher.go  the dispatch policy behind StreamJobs
type Server struct {
	pulsev1.UnimplementedPulseServiceServer
	svc        service.JobService
	schedules  service.ScheduleService
	dispatcher *dispatcher
}

// Option configures a Server at construction. Production callers pass none.
type Option func(*Server)

// WithDispatchInterval overrides the dispatcher's poll interval. Tests inject a small value
// so a StreamJobs round-trip doesn't wait the production 500ms for the first tick.
func WithDispatchInterval(d time.Duration) Option {
	return func(s *Server) { s.dispatcher.interval = d }
}

func New(svc service.JobService, schedules service.ScheduleService, opts ...Option) *Server {
	s := &Server{svc: svc, schedules: schedules, dispatcher: newDispatcher(svc, dispatchInterval)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
