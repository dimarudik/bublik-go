package main

import (
	"bublik-go/core"
	"bublik-go/postgres"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

	// 1. Читаем настройки подключений из YAML
	props, err := core.LoadConnectionProperties("pg2pg.yaml")
	if err != nil {
		log.Fatalf("Ошибка загрузки свойств подключения: %v", err)
	}

	// 2. Читаем маппинги таблиц из JSON
	configs, err := core.LoadMappingConfigs("columnOrder.json")
	if err != nil {
		log.Fatalf("Ошибка загрузки маппингов: %v", err)
	}

	log.Printf("Конфиги успешно загружены! Потоков из YAML: %d, Таблиц для миграции: %d", props.ThreadCount, len(configs))

	// 3. Создаем компоненты движка
	source := postgres.NewPostgresSource(pool)
	target := postgres.NewPostgresTarget(pool)
	engine := core.NewEngine(source, target)

	log.Println("Старт движка миграции Bublik-Go...")

	// Общая статистика по всей сессии миграции
	var totalRows int64
	sessionStart := time.Now()

	// 4. Итерируемся по списку таблиц из файла (как в App.java)
	for _, config := range configs {
		log.Printf("--- Запуск миграции таблицы %s.%s -> %s.%s ---",
			config.FromSchemaName, config.FromTableName, config.ToSchemaName, config.ToTableName)

		// Подставляем динамические параметры:
		// config — текущая таблица из файла
		// 3 — размер чанка (позже вынесем в параметры командной строки)
		// props.ThreadCount — количество потоков воркеров строго из YAML файла!
		result, err := engine.Run(ctx, config, 3, props.ThreadCount)
		if err != nil {
			log.Printf("[Ошибка] Миграция таблицы %s провалена: %v", config.FromTableName, err)
			continue // Переходим к следующей таблице, не роняя всё приложение
		}

		totalRows += result.TotalRows
	}

	fmt.Printf("\n====================================\n")
	fmt.Printf("МИГРАЦИЯ УСПЕШНО ВЫПОЛНЕНА ДВИЖКОМ\n")
	fmt.Printf("Всего перенесено строк во всех таблицах: %d\n", totalRows)
	fmt.Printf("Общее время работы сессии: %v\n", time.Since(sessionStart))
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
