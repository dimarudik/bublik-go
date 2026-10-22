package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	// Импортируем наш собственный подпакет.
	// Если при go mod init вы указали просто bublik-go, путь будет "bublik-go/postgres"
	"bublik-go/postgres"
)

func main() {
	ctx := context.Background()

	// 1. Строка подключения к вашей локальной базе Postgres.
	// Подставьте свои параметры: пользователь, пароль, хост, порт и имя БД.
	connStr := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

	log.Println("Подключение к PostgreSQL...")
	conn, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Не удалось подключиться к БД: %v", err)
	}
	defer conn.Close()

	// 2. Подготавливаем окружение: создаем исходную и целевую таблицы для теста
	log.Println("Создание тестовых таблиц...")
	setupTables(ctx, conn)

	// 3. Инициализируем компоненты нашей библиотеки Бублик-Go
	source := postgres.NewPostgresSource(conn)
	target := postgres.NewPostgresTarget(conn)

	// Описываем метаданные миграции
	schemaName := "public"
	sourceTable := "users_src"
	targetTable := "users_trg"
	columns := []string{"id", "name", "email", "updated_at"}
	pkColumns := []string{"id"} // Ключ для ON CONFLICT DO UPDATE

	// 4. НАРЕЗКА ЧАНКОВ (Сердце Бублика)
	// Укажем chunkSize = 3. Так как мы вставили 10 строк, у нас должно получиться около 4 чанков.
	log.Println("Расчет и нарезка физических чанков по ctid...")
	chunks, err := source.CreateChunks(ctx, schemaName, sourceTable, 3)
	if err != nil {
		log.Fatalf("Ошибка нарезки чанков: %v", err)
	}
	fmt.Printf("Успешно сформировано чанков: %d\n", len(chunks))

	// 5. ЗАПУСК ПАЙПЛАЙНА МИГРАЦИИ
	totalMigrated := int64(0)
	startTime := time.Now()

	for _, chunk := range chunks {
		fmt.Printf("--- Обработка Чанка #%d (Страницы: %d -> %d) ---\n", chunk.ID, chunk.StartPage, chunk.EndPage)

		// Создаем асинхронный Go-канал для стриминга строк (Буфер на 100 строк)
		rowChan := make(chan []any, 100)

		// Запускаем горутину-продюсер для чтения данных чанка (Аналог фонового потока в Java)
		go func(ch postgres.ChunkRange) {
			defer close(rowChan) // Обязательно закрываем канал, когда чтение завершено!
			err := source.StreamChunk(ctx, schemaName, sourceTable, columns, ch, rowChan)
			if err != nil {
				log.Printf("[Ошибка] Не удалось вычитать чанк %d: %v\n", ch.ID, err)
			}
		}(chunk)

		// Основной поток выступает в роли Консьюмера — вычитывает канал и делает UPSERT пачками
		inserted, err := target.WriteChunkUpsert(ctx, targetTable, columns, pkColumns, rowChan)
		if err != nil {
			log.Fatalf("[Критическая Ошибка] Запись чанка %d провалена: %v. Транзакция откачена.", chunk.ID, err)
		}

		fmt.Printf("Чанк #%d успешно зафиксирован (Exactly-Once). Записано строк: %d\n", chunk.ID, inserted)
		totalMigrated += inserted
	}

	fmt.Printf("\n=== МИГРАЦИЯ ЗАВЕРШЕНА ===\nВсего перенесено строк: %d\nВремя выполнения: %v\n", totalMigrated, time.Since(startTime))
}

// Вспомогательная функция для создания и наполнения тестовой структуры
func setupTables(ctx context.Context, pool *pgxpool.Pool) {
	// Удаляем старые таблицы, если они были
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS users_src;")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS users_trg;")

	// Создаем источник и приемник
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

	// Вставляем 10 тестовых записей в исходную таблицу
	for i := 1; i <= 10; i++ {
		_, err = pool.Exec(ctx,
			"INSERT INTO users_src (id, name, email, updated_at) VALUES ($1, $2, $3, $4)",
			i, fmt.Sprintf("User_%d", i), fmt.Sprintf("user%d@bublik.dev", i), time.Now(),
		)
		if err != nil {
			log.Fatalf("Ошибка наполнения тестовых данных: %v", err)
		}
	}

	// Запускаем ANALYZE, чтобы Postgres обновил системный каталог pg_class.
	// Без этого relpages/reltuples будут равны 0, и ctid-нарезка не сможет рассчитать плотность.
	_, err = pool.Exec(ctx, "ANALYZE users_src;")
	if err != nil {
		log.Fatalf("Ошибка выполнения ANALYZE: %v", err)
	}
}
