package integration_tests

import (
	"bublik-go/core"
	"bublik-go/postgres"
	"context"
	"testing"
)

func TestIntegration_PostgresToPostgres_Upsert(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропускаем интеграционный тест в коротком режиме")
	}

	ctx := context.Background()

	t.Log("Запуск Docker-контейнера Postgres через Testcontainers...")
	pgInst, err := StartPostgresContainer(ctx, "postgres", "postgres", "postgres")
	if err != nil {
		t.Fatalf("Не удалось поднять тестовое окружение: %v", err)
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
		FromTaskName:   "users_task",
		ColumnToColumn: map[string]string{
			"id":         "id",
			"name":       "name",
			"email":      "email",
			"updated_at": "updated_at",
		},
	}

	// 1. Создаем доменную модель дефолтной таблицы чанков для этого теста [1.12]
	chunkTable := core.Table{SchemaName: "public", TableName: "bublik_chunk"}
	outboxTable := core.Table{SchemaName: "public", TableName: "bublik_outbox"}

	// 2. Инициализируем компоненты с учетом новых параметров
	source := postgres.NewPostgresSource(pgInst.Pool, chunkTable)
	target := postgres.NewPostgresTarget(pgInst.Pool, outboxTable)
	engine := core.NewEngine(source, target)

	t.Log("Старт движка миграции...")

	// 3. Передаем source последним аргументом в качестве провайдера метаданных [1.12]
	configs := []core.Config{config}
	result, err := engine.Run(ctx, configs, 10_000, 4, source)
	if err != nil {
		t.Fatalf("Движок завершился со сбоем: %v", err)
	}

	if result.TotalRows != 10 {
		t.Errorf("Движок отрапортовал о переносе %d строк, а ожидалось 10", result.TotalRows)
	}

	var targetCount int
	err = pgInst.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM users_trg").Scan(&targetCount)
	if err != nil {
		t.Fatalf("Не удалось проверить таблицу users_trg: %v", err)
	}

	if targetCount != 10 {
		t.Errorf("Физическая проверка провалена! В users_trg найдено %d строк вместо 10", targetCount)
	}
	t.Log("Интеграционный тест pg -> pg успешно пройден!")
}
