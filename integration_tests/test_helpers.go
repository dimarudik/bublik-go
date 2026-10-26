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

// PostgresInstance инкапсулирует контейнер и пул соединений к нему
type PostgresInstance struct {
	Container *postgres.PostgresContainer
	Pool      *pgxpool.Pool
}

// StartPostgresContainer поднимает чистый контейнер Postgres в Docker
func StartPostgresContainer(ctx context.Context, dbName, user, password string) (*PostgresInstance, error) {
	initScriptPath, err := filepath.Abs("../deploy/postgres/pg-init.sql")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve absolute path for init script: %w", err)
	}

	// Настраиваем параметры контейнера (константные образы, стратегии ожидания сокетов)
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

	// Получаем динамический connection string (порт мапится операционной системой случайно, чтобы не было конфликтов)
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection string: %w", err)
	}

	// Открываем пул соединений
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to container db: %w", err)
	}

	return &PostgresInstance{
		Container: container,
		Pool:      pool,
	}, nil
}

/*// SeedPostgresData создает таблицы и наполняет их данными для конкретного теста
func SeedPostgresData(ctx context.Context, pool *pgxpool.Pool, totalRows int) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users_src (
			id INT PRIMARY KEY,
			name VARCHAR(50),
			email VARCHAR(50),
			updated_at TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS users_trg (
			id INT PRIMARY KEY,
			name VARCHAR(50),
			email VARCHAR(50),
			updated_at TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS bublik_outbox (
			chunk_id BIGINT PRIMARY KEY,
			processed_at TIMESTAMP
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create tables in container: %w", err)
	}

	// Наполняем данными
	for i := 1; i <= totalRows; i++ {
		_, err = pool.Exec(ctx,
			"INSERT INTO users_src (id, name, email, updated_at) VALUES ($1, $2, $3, $4)",
			i, fmt.Sprintf("User_%d", i), fmt.Sprintf("user%d@integration.test", i), time.Now(),
		)
		if err != nil {
			return fmt.Errorf("failed to seed test row %d: %w", i, err)
		}
	}

	// Обновляем каталоги
	_, err = pool.Exec(ctx, "ANALYZE users_src;")
	return err
}
*/
