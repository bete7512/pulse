package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bete7512/pulse/eventstore"
	"github.com/bete7512/pulse/pkg/common"
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
	store := eventstore.NewPostgresEventStore(db)
	svc := service.New(store)
	w := worker.New(store)

	go w.Run(ctx)
	for i := 0; i < 6; i++ { // demo: submit a few jobs
		svc.Submit(ctx, []byte(`{"name":"abebe"}`))
	}

	logger.Info("pulse started succesfully")
	// TODO: in the future there must be gracefull shutdown
	<-ctx.Done()
}
