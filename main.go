package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"bublik-go/postgres"
)

func main() {
	ctx := context.Background()
	connStr := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

	log.Println("Инициализация пула соединений...")
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Ошибка пула: %v", err)
	}
	defer pool.Close()

	// Настраиваем тестовую среду
	setupTables(ctx, pool)

	// 1. Декларативно объявляем правила маппинга (Аналог JSON конфига из Java)
	config := MappingConfig{
		FromSchema: "public",
		FromTable:  "users_src",
		ToSchema:   "public",
		ToTable:    "users_trg",
		Columns: map[string]string{
			"id":         "id",
			"name":       "name",
			"email":      "email",
			"updated_at": "updated_at",
		},
	}

	// 2. Создаем компоненты и передаем их в конструктор Engine
	source := postgres.NewPostgresSource(pool)
	target := postgres.NewPostgresTarget(pool)
	engine := NewEngine(source, target)

	// 3. Запускаем движок!
	// Параметры: контекст, конфиг, chunkSize = 3 строки, threadCount = 3 параллельных воркера
	log.Println("Старт движка миграции Bublik-Go...")
	result, err := engine.Run(ctx, config, 3, 3)
	if err != nil {
		log.Fatalf("Движок завершился с ошибкой: %v", err)
	}

	fmt.Printf("\n====================================\n")
	fmt.Printf("МИГРАЦИЯ УСПЕШНО ВЫПОЛНЕНА ДВИЖКОМ\n")
	fmt.Printf("Всего перенесено строк: %d\n", result.TotalRows)
	fmt.Printf("Время работы пайплайна: %v\n", result.Duration)
	fmt.Printf("====================================\n")
}

// Функция setupTables остается без изменений внизу файла main.go...
func setupTables(ctx context.Context, pool *pgxpool.Pool) {
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS users_src;")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS users_trg;")

	_, err := pool.Exec(ctx, `
		CREATE TABLE users_src (
			id INT PRIMARY KEY,
			name VARCHAR(50),
			email VARCHAR(50),
			updated_at TIMESTAMP
		);
		CREATE TABLE users_trg (
			id INT PRIMARY KEY,
			name VARCHAR(50),
			email VARCHAR(50),
			updated_at TIMESTAMP
		);
	`)
	if err != nil {
		log.Fatalf("Ошибка создания таблиц: %v", err)
	}

	for i := 1; i <= 10; i++ {
		_, err = pool.Exec(ctx,
			"INSERT INTO users_src (id, name, email, updated_at) VALUES ($1, $2, $3, $4)",
			i, fmt.Sprintf("User_%d", i), fmt.Sprintf("user%d@bublik.dev", i), time.Now(),
		)
		if err != nil {
			log.Fatalf("Ошибка наполнения тестовых данных: %v", err)
		}
	}

	_, err = pool.Exec(ctx, "ANALYZE users_src;")
	if err != nil {
		log.Fatalf("Ошибка выполнения ANALYZE: %v", err)
	}
}
