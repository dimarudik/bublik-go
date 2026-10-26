package integration_tests

import (
	"context"
	"testing"
	"time"

	"bublik-go/core"
	"bublik-go/postgres"
)

func TestIntegration_PostgresToPostgres_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропускаем тяжелый стресс-тест в коротком режиме")
	}

	ctx := context.Background()
	totalRows := 100000
	chunkSize := 5000
	threadCount := 10

	t.Log("Инициализация Docker-контейнера для стресс-теста...")
	pgInst, err := StartPostgresContainer(ctx, "bublik_stress", "postgres", "postgres")
	if err != nil {
		t.Fatalf("Не удалось поднять контейнер: %v", err)
	}
	defer func() {
		pgInst.Pool.Close()
		_ = pgInst.Container.Terminate(ctx)
	}()

	config := core.Config{
		FromSchemaName: "public",
		FromTableName:  "users_src",
		ToSchemaName:   "public",
		ToTableName:    "users_trg",
		ColumnToColumn: map[string]string{
			"id":         "id",
			"name":       "name",
			"email":      "email",
			"updated_at": "updated_at",
		},
	}

	source := postgres.NewPostgresSource(pgInst.Pool)
	target := postgres.NewPostgresTarget(pgInst.Pool)
	engine := core.NewEngine(source, target)

	t.Logf("Запуск параллельного Движка (Воркеров: %d, Размер чанка: %d строк)...", threadCount, chunkSize)

	startTime := time.Now()

	result, err := engine.Run(ctx, config, chunkSize, threadCount)
	if err != nil {
		t.Fatalf("Движок рухнул под нагрузкой: %v", err)
	}

	duration := time.Since(startTime)
	t.Logf("Стресс-миграция завершена за %v. Перенесено строк по отчету: %d", duration, result.TotalRows)

	if result.TotalRows != int64(totalRows) {
		t.Errorf("Движок потерял строки по дороге! Ожидалось %d, перенесено %d", totalRows, result.TotalRows)
	}

	var targetCount int
	err = pgInst.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM users_trg").Scan(&targetCount)
	if err != nil {
		t.Fatalf("Не удалось выполнить проверочный SELECT: %v", err)
	}

	if targetCount != totalRows {
		t.Errorf("Физическая проверка данных провалена! В базе-приемнике %d строк вместо %d", targetCount, totalRows)
	}

	var outboxCount int
	err = pgInst.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM bublik_outbox").Scan(&outboxCount)
	if err != nil {
		t.Fatalf("Не удалось проверить таблицу метаданных bublik_outbox: %v", err)
	}

	expectedChunks := totalRows / chunkSize
	t.Logf("В базе зафиксировано %d мета-записей чанков (Ожидалось: %d)", outboxCount, expectedChunks)
}
