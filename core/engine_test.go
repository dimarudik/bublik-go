package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

// =========================================================================
// СОЗДАЕМ MOCK-ОБЪЕКТЫ (Заглушки вместо реального Postgres)
// =========================================================================

// MockSource реализует интерфейс core.SourceStorage
type MockSource struct {
	mockChunks []any
	mockRows   [][]any
}

func (m *MockSource) CreateChunks(ctx context.Context, schema string, table string, chunkSize int) ([]any, error) {
	// Имитируем, что нарезали 2 абстрактных чанка (например, просто числа 1 и 2)
	return m.mockChunks, nil
}

func (m *MockSource) StreamChunk(ctx context.Context, schema string, table string, columns []string, chunk any, out chan<- []any) error {
	// Имитируем потоковое чтение: отправляем заготовленные строки в канал
	for _, row := range m.mockRows {
		out <- row
	}
	return nil
}

// MockTarget реализует интерфейс core.TargetStorage
type MockTarget struct {
	rowsWritten int64
}

func (m *MockTarget) WriteChunkUpsert(ctx context.Context, table string, columns []string, pkColumns []string, rows <-chan []any) (int64, error) {
	var count int64
	// Вычитываем строки из канала, как это делает реальный Target
	for range rows {
		count++
	}
	m.rowsWritten += count
	return count, nil
}

// =========================================================================
// САМ UNIT-ТЕСТ (Аналог @Test в JUnit)
// =========================================================================

func TestEngine_Run_Success(t *testing.T) {
	ctx := context.Background()

	// 1. Готовим фейковые данные для теста
	fakeChunks := []any{int64(1), int64(2)} // 2 чанка
	fakeRows := [][]any{
		{1, "Иван", "ivan@dev.ru", time.Now()},
		{2, "Петр", "petr@dev.ru", time.Now()},
	}

	// Инициализируем наши моки
	mockSrc := &MockSource{mockChunks: fakeChunks, mockRows: fakeRows}
	mockTrg := &MockTarget{}

	// Создаем Engine, подсовывая ему моки вместо реального Postgres
	engine := NewEngine(mockSrc, mockTrg)

	// Настраиваем фейковый конфиг
	cfg := Config{
		FromSchemaName: "public",
		FromTableName:  "test_src",
		ToSchemaName:   "public",
		ToTableName:    "test_trg",
		ColumnToColumn: map[string]string{"id": "id", "name": "name"},
	}

	// 2. ЗАПУСКАЕМ ДВИЖОК
	// Тестируем пачку из 2 воркеров
	result, err := engine.Run(ctx, cfg, 3, 2)

	// 3. ПРОВЕРЯЕМ РЕЗУЛЬТАТЫ (Assertions)
	if err != nil {
		t.Fatalf("Ожидался успешный запуск движка, но получена ошибка: %v", err)
	}

	// Математика: 2 чанка по 2 строки в каждом = всего должно быть обработано 4 строки
	expectedRows := int64(4)
	if result.TotalRows != expectedRows {
		t.Errorf("Неверное количество строк в результате движка. Ожидалось: %d, Получено: %d", expectedRows, result.TotalRows)
	}

	if mockTrg.rowsWritten != expectedRows {
		t.Errorf("Target не зафиксировал нужный объем строк. Ожидалось: %d, Получено: %d", expectedRows, mockTrg.rowsWritten)
	}
}

// MockErrorTarget имитирует целевую базу данных, которая падает с ошибкой на определенном чанке.
type MockErrorTarget struct {
	rowsWritten int64
}

func (m *MockErrorTarget) WriteChunkUpsert(ctx context.Context, table string, columns []string, pkColumns []string, rows <-chan []any) (int64, error) {
	// Вычитываем канал, чтобы горутина-продюсер (Source) не зависла
	var count int64
	for range rows {
		count++
	}

	// Имитируем сбой: если прилетели данные, выкидываем ошибку базы данных (например, обрыв сетевого соединения)
	return 0, errors.New("failed to write to postgres: connection reset by peer")
}

// TestEngine_Run_WithTargetError проверяет, что движок корректно обрабатывает сбои записи
func TestEngine_Run_WithTargetError(t *testing.T) {
	ctx := context.Background()

	// Готовим 2 фейковых чанка
	fakeChunks := []any{int64(1), int64(2)}
	fakeRows := [][]any{
		{1, "Тест", "test@bublik.dev", time.Now()},
	}

	mockSrc := &MockSource{mockChunks: fakeChunks, mockRows: fakeRows}
	// Подсовываем наш новый сбойный Target
	mockErrTrg := &MockErrorTarget{}

	engine := NewEngine(mockSrc, mockErrTrg)

	cfg := Config{
		FromSchemaName: "public",
		FromTableName:  "test_src",
		ToSchemaName:   "public",
		ToTableName:    "test_trg",
		ColumnToColumn: map[string]string{"id": "id"},
	}

	// Запускаем движок
	result, err := engine.Run(ctx, cfg, 3, 2)

	// Проверяем контракты:
	// 1. Сам движок не должен падать с паникой или возвращать глобальную ошибку,
	//    так как он должен дать шанс обработаться другим чанкам.
	if err != nil {
		t.Fatalf("Движок не должен возвращать критическую ошибку, если упал отдельный воркер: %v", err)
	}

	// 2. Так как Target на все чанки возвращал ошибку, итоговое количество успешно
	//    перенесенных строк должно быть строго равно 0.
	if result.TotalRows != 0 {
		t.Errorf("Ожидалось 0 успешно перенесенных строк из-за ошибок БД, но получено: %d", result.TotalRows)
	}
}
