package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bete7512/pulse/eventstore"
	"github.com/bete7512/pulse/pkg/common"
	"github.com/bete7512/pulse/projection"
	"github.com/bete7512/pulse/query"
	"github.com/bete7512/pulse/service"
	"github.com/bete7512/pulse/worker"
	"github.com/joho/godotenv"
)

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	jsonHandler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(jsonHandler)
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	dbHost := os.Getenv("DB_HOST")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	db, err := common.InitDbConnection(ctx, dbHost)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	store := eventstore.NewPostgresEventStore(db)
	query := query.New(db)
	proj := projection.New(store, db, 500*time.Millisecond, logger)

	svc := service.New(store, query)
	w := worker.New(svc)

	go w.Run(ctx)
	go proj.Run(ctx)
	
	logger.Info("pulse started succesfully")
	// TODO: in the future there must be gracefull shutdown
	<-ctx.Done()
}
