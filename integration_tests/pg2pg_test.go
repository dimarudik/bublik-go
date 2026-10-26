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

	// 1. Запускаем изолированный контейнер базы данных
	t.Log("Запуск Docker-контейнера Postgres через Testcontainers...")
	pgInst, err := StartPostgresContainer(ctx, "bublik_test", "tester", "secret_pass")
	if err != nil {
		t.Fatalf("Не удалось поднять тестовое окружение: %v", err)
	}
	// Гарантированно тушим контейнер и закрываем пулы ПОСЛЕ завершения теста
	defer func() {
		pgInst.Pool.Close()
		_ = pgInst.Container.Terminate(ctx)
	}()

	// 2. Накатываем тестовую схему и заливаем 10 строк
	t.Log("Наполнение СУБД тестовыми данными...")
	err = SeedPostgresData(ctx, pgInst.Pool, 10)
	if err != nil {
		t.Fatalf("Ошибка подготовки данных: %v", err)
	}

	// 3. Конфигурируем наш Бублик-Go на работу с контейнером
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

	// 4. ЗАПУСКАЕМ ДВИЖОК
	t.Log("Старт движка миграции...")
	result, err := engine.Run(ctx, config, 3, 2)
	if err != nil {
		t.Fatalf("Движок завершился со сбоем: %v", err)
	}

	// 5. ПРОВЕРКА (ASSERTIONS)
	if result.TotalRows != 10 {
		t.Errorf("Движок отрапортовал о переносе %d строк, а ожидалось 10", result.TotalRows)
	}

	// Делаем физический SELECT к нашей базе в контейнере, чтобы проверить, долетели ли данные
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
