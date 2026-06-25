package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"

	"github.com/bete7512/pulse/db/migrations"
	"github.com/bete7512/pulse/pkg/common"
	"github.com/joho/godotenv"
)

func main() {

	flag.Parse()

	ctx := context.Background()
	// .env is a local-dev convenience; in containers/CI config comes from the real
	// environment, so a missing file is fine — fall back to whatever env vars are set.
	_ = godotenv.Load()
	dbHost := os.Getenv("DB_HOST")

	db, err := common.InitDbConnection(ctx, dbHost)
	migrationService := migrations.NewMigrationsService(&migrations.Config{})
	err = migrationService.Run(ctx, db, flag.Args())
	if err != nil {
		log.Fatalf("failed to run migrate command: %v", err)
	}

	slog.Info("migrate command completed successfully")
	os.Exit(0)
}
