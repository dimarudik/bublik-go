package integration_tests

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PostgresInstance struct {
	Container *postgres.PostgresContainer
	Pool      *pgxpool.Pool
}

func StartPostgresContainer(ctx context.Context, dbName, user, password string) (*PostgresInstance, error) {
	initScriptPath, err := filepath.Abs("../deploy/postgres/pg-init.sql")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve absolute path for init script: %w", err)
	}

	container, err := postgres.Run(ctx,
		"postgres:17",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(user),
		postgres.WithPassword(password),
		postgres.WithInitScripts(initScriptPath),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection string: %w", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to container db: %w", err)
	}

	return &PostgresInstance{
		Container: container,
		Pool:      pool,
	}, nil
}
