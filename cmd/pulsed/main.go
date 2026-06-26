package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bete7512/pulse/db/migrations"
	"github.com/bete7512/pulse/gen/pulsev1"
	"github.com/bete7512/pulse/internal/repos/postgres"
	"github.com/bete7512/pulse/internal/service"
	grpcserver "github.com/bete7512/pulse/internal/transport/grpc"
	"github.com/bete7512/pulse/pkg/common"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

const (
	grpcAddr         = ":50051"
	projectInterval  = 500 * time.Millisecond
	scheduleInterval = 1 * time.Second // how often the scheduler checks for due schedules
	shutdownTimeout  = 5 * time.Second

	// liveness/watchdog: a worker renews its liveness via heartbeats while running; if
	// it expires (TTL passed) the watchdog re-dispatches the job. livenessTTL should be
	// a few heartbeat intervals (SDK beats every ~10s). orphanGrace is the fallback
	// for running jobs whose best-effort liveness mark never landed.
	livenessTTL   = 30 * time.Second
	orphanGrace   = 2 * time.Minute
	watchInterval = 10 * time.Second
)

// Build metadata, injected at build time via -ldflags "-X main.version=… -X main.commit=…
// -X main.date=…". Defaults apply for `go run` / un-stamped builds.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	logger := newLogger()

	// .env is a local-dev convenience; in containers/CI config comes from the real
	// environment, so a missing file is fine — fall back to whatever env vars are set.
	_ = godotenv.Load()

	// ctx is cancelled on the first SIGINT/SIGTERM; stop() restores default signal
	// handling so a second signal force-quits instead of being swallowed.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := common.InitDbConnection(ctx, os.Getenv("DB_HOST"))
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer db.Close()

	// migration
	migrationService := migrations.NewMigrationsService(&migrations.Config{})
	if err = migrationService.Run(ctx, db, []string{"up"}); err != nil {
		log.Fatalf("failed to run migrate command %v", err)
	}
	svc, proj, wd, sched, scheduleAdmin := buildServices(db, logger)
	go proj.Run(ctx)
	go wd.Run(ctx)
	go sched.Run(ctx)

	gs, err := serveGRPC(svc, scheduleAdmin, logger)
	if err != nil {
		return err
	}

	logger.Info("pulse started successfully", "addr", grpcAddr, "version", version, "commit", commit, "date", date)
	<-ctx.Done()
	stop()
	shutdown(gs, logger)
	return nil
}

func newLogger() *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h)
}

// buildServices wires the read/write stack: event store, query reader, projector, the
// command/query service, the watchdog that recovers stuck jobs, and the scheduler that
// fires scheduled/recurring jobs.
func buildServices(db *pgxpool.Pool, logger *slog.Logger) (service.JobService, service.ProjectorService, service.WatchdogService, service.SchedulerService, service.ScheduleService) {
	events := postgres.NewEvent(db)
	jobStore := postgres.NewJob(db)
	live := postgres.NewLiveness(db)
	scheduleStore := postgres.NewSchedule(db)
	svc := service.New(events, jobStore, live, livenessTTL)
	proj := service.NewProjector(events, jobStore, projectInterval, logger)
	wd := service.NewWatchdog(live, events, svc, orphanGrace, watchInterval, logger)
	sched := service.NewScheduler(scheduleStore, events, scheduleInterval, logger)
	scheduleAdmin := service.NewScheduleService(scheduleStore, jobStore, events)
	return svc, proj, wd, sched, scheduleAdmin
}

// serveGRPC registers the Pulse service and starts serving in the background,
// returning the server so the caller can shut it down.
func serveGRPC(svc service.JobService, scheduleAdmin service.ScheduleService, logger *slog.Logger) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", grpcAddr, err)
	}
	gs := grpc.NewServer()
	pulsev1.RegisterPulseServiceServer(gs, grpcserver.New(svc, scheduleAdmin))

	go func() {
		if err := gs.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("grpc serve stopped", "error", err)
		}
	}()
	return gs, nil
}

// shutdown stops the server gracefully, but bounds the wait: GracefulStop waits on
// in-flight RPCs (e.g. a connected StreamJobs worker) without cancelling them, so
// it can block forever. After shutdownTimeout we force a hard stop.
func shutdown(gs *grpc.Server, logger *slog.Logger) {
	logger.Info("shutting down...")
	stopped := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(shutdownTimeout):
		logger.Warn("graceful stop timed out; forcing")
		gs.Stop()
	}
}
