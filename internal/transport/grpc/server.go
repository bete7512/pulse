package grpc

import (
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

func New(svc service.JobService, schedules service.ScheduleService) *Server {
	return &Server{svc: svc, schedules: schedules, dispatcher: newDispatcher(svc, dispatchInterval)}
}
