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
	"github.com/bete7512/pulse/internal/eventstore"
	"github.com/bete7512/pulse/internal/projection"
	"github.com/bete7512/pulse/internal/query"
	"github.com/bete7512/pulse/internal/service"
	grpcserver "github.com/bete7512/pulse/internal/transport/grpc"
	"github.com/bete7512/pulse/pkg/common"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

const (
	grpcAddr        = ":50051"
	projectInterval = 500 * time.Millisecond
	shutdownTimeout = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	logger := newLogger()

	if err := godotenv.Load(); err != nil {
		return fmt.Errorf("load .env: %w", err)
	}

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
	svc, proj := buildServices(db, logger)
	go proj.Run(ctx)

	gs, err := serveGRPC(svc, logger)
	if err != nil {
		return err
	}

	logger.Info("pulse started successfully", "addr", grpcAddr)
	<-ctx.Done()
	stop()
	shutdown(gs, logger)
	return nil
}

func newLogger() *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h)
}

// buildServices wires the read/write stack: event store, query reader, projector,
// and the command/query service over them.
func buildServices(db *pgxpool.Pool, logger *slog.Logger) (*service.Service, *projection.Projector) {
	store := eventstore.NewPostgresEventStore(db)
	reader := query.New(db)
	proj := projection.New(store, db, projectInterval, logger)
	return service.New(store, reader), proj
}

// serveGRPC registers the Pulse service and starts serving in the background,
// returning the server so the caller can shut it down.
func serveGRPC(svc *service.Service, logger *slog.Logger) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", grpcAddr, err)
	}
	gs := grpc.NewServer()
	pulsev1.RegisterPulseServiceServer(gs, grpcserver.New(svc))

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
