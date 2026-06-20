package common

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func InitDbConnection(ctx context.Context, dbHost string) (*pgxpool.Pool, error) {

	dbpool, err := pgxpool.New(ctx, dbHost)
	if err != nil {
		return nil, err
	}

	err = dbpool.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return dbpool, nil
}
