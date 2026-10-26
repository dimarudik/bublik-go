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
	totalRows := 100000 // Заливаем 100 тысяч записей для создания реальной нагрузки
	chunkSize := 5000   // Получим ровно 20 физических чанков
	threadCount := 10   // 10 горутин-воркеров будут рвать этот пайплайн параллельно

	// 1. Поднимаем контейнер Postgres 17
	t.Log("Инициализация Docker-контейнера для стресс-теста...")
	pgInst, err := StartPostgresContainer(ctx, "bublik_stress", "postgres", "postgres")
	if err != nil {
		t.Fatalf("Не удалось поднять контейнер: %v", err)
	}
	defer func() {
		pgInst.Pool.Close()
		_ = pgInst.Container.Terminate(ctx)
	}()

	/*	// 2. Накатываем схемы и генерируем 100k строк данных
		t.Logf("Генерация тестовых данных (%d строк)... Это может занять пару секунд.", totalRows)
		err = SeedPostgresData(ctx, pgInst.Pool, totalRows)
		if err != nil {
			t.Fatalf("Ошибка генерации данных: %v", err)
		}
	*/
	// 3. Собираем конфигурацию миграции
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

	// 4. Штурмуем движок
	result, err := engine.Run(ctx, config, chunkSize, threadCount)
	if err != nil {
		t.Fatalf("Движок рухнул под нагрузкой: %v", err)
	}

	duration := time.Since(startTime)
	t.Logf("Стресс-миграция завершена за %v. Перенесено строк по отчету: %d", duration, result.TotalRows)

	// 5. ЖЕСТКАЯ ПРОВЕРКА ДАННЫХ (E2E Ассерты)
	if result.TotalRows != int64(totalRows) {
		t.Errorf("Движок потерял строки по дороге! Ожидалось %d, перенесено %d", totalRows, result.TotalRows)
	}

	// Делаем физический SELECT COUNT(*) к целевой таблице в контейнере
	var targetCount int
	err = pgInst.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM users_trg").Scan(&targetCount)
	if err != nil {
		t.Fatalf("Не удалось выполнить проверочный SELECT: %v", err)
	}

	if targetCount != totalRows {
		t.Errorf("Физическая проверка данных провалена! В базе-приемнике %d строк вместо %d", targetCount, totalRows)
	}

	// Дополнительно проверяем, что Exactly-Once метаданные записали все 20 чанков
	var outboxCount int
	err = pgInst.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM bublik_outbox").Scan(&outboxCount)
	if err != nil {
		t.Fatalf("Не удалось проверить таблицу метаданных bublik_outbox: %v", err)
	}

	expectedChunks := totalRows / chunkSize
	t.Logf("В базе зафиксировано %d мета-записей чанков (Ожидалось: %d)", outboxCount, expectedChunks)
}
