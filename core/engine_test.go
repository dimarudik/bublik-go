package core

import (
	"context"
	"errors"
	"testing"
)

// =========================================================================
// ОБНОВЛЕННЫЕ MOCK-ОБЪЕКТЫ ДЛЯ ТЕСТОВ ЯДРА
// =========================================================================

type MockSource struct {
	mockChunks []any
	mockRows   [][]any
}

func (m *MockSource) CreateChunks(ctx context.Context, schema string, table string, chunkSize int) ([]any, error) {
	return m.mockChunks, nil
}

func (m *MockSource) StreamChunk(ctx context.Context, schema string, table string, columns []string, chunk any, out chan<- []any) error {
	for _, row := range m.mockRows {
		out <- row
	}
	return nil
}

// Наш тестовый провайдер метаданных
type MockMetadataProvider struct{}

func (m *MockMetadataProvider) EnrichTableMetadata(ctx context.Context, schema, table string) (Table, error) {
	// Имитируем, что вычитали из системных каталогов СУБД две колонки: id и name
	return Table{
		SchemaName: schema,
		TableName:  table,
		Columns: []Column{
			{Position: 1, Name: "id", Type: "integer"},
			{Position: 2, Name: "name", Type: "varchar"},
		},
		PkColumns: []Column{
			{Position: 1, Name: "id", Type: "integer"},
		},
	}, nil
}

type MockTarget struct {
	rowsWritten int64
}

// Поправили сигнатуру: добавили chunkID int64 первым параметром!
func (m *MockTarget) WriteChunkUpsert(ctx context.Context, chunkID int64, table string, columns []string, pkColumns []string, rows <-chan []any) (int64, error) {
	var count int64
	for range rows {
		count++
	}
	m.rowsWritten += count
	return count, nil
}

type MockErrorTarget struct{}

// Мок для симуляции ошибок записи в БД
func (m *MockErrorTarget) WriteChunkUpsert(ctx context.Context, chunkID int64, table string, columns []string, pkColumns []string, rows <-chan []any) (int64, error) {
	for range rows {
		// Сжигаем канал
	}
	return 0, errors.New("failed to write to postgres: connection reset by peer")
}

// =========================================================================
// UNIT-ТЕСТЫ
// =========================================================================

func TestEngine_Run_Success(t *testing.T) {
	ctx := context.Background()

	// Используем нашу новую полиморфную структуру Chunk[int64] на генериках!
	fakeChunks := []any{
		Chunk[int64]{ID: 1, Start: 0, End: 5},
		Chunk[int64]{ID: 2, Start: 5, End: 10},
	}
	fakeRows := [][]any{
		{1, "Иван"},
		{2, "Петр"},
	}

	mockSrc := &MockSource{mockChunks: fakeChunks, mockRows: fakeRows}
	mockTrg := &MockTarget{}
	metaProvider := &MockMetadataProvider{}

	engine := NewEngine(mockSrc, mockTrg)

	cfg := Config{
		FromSchemaName: "public",
		FromTableName:  "test_src",
		ToSchemaName:   "public",
		ToTableName:    "test_trg",
		ColumnToColumn: map[string]string{"id": "id", "name": "name"},
	}

	// Передаем metaProvider последним аргументом
	result, err := engine.Run(ctx, cfg, 3, 2, metaProvider)
	if err != nil {
		t.Fatalf("Ожидался успешный запуск движка, но получена ошибка: %v", err)
	}

	expectedRows := int64(4) // 2 чанка * 2 строки
	if result.TotalRows != expectedRows {
		t.Errorf("Неверное количество строк. Ожидалось: %d, Получено: %d", expectedRows, result.TotalRows)
	}
}

func TestEngine_Run_WithTargetError(t *testing.T) {
	ctx := context.Background()

	fakeChunks := []any{Chunk[int64]{ID: 1, Start: 0, End: 5}}
	fakeRows := [][]any{{1, "Тест"}}

	mockSrc := &MockSource{mockChunks: fakeChunks, mockRows: fakeRows}
	mockErrTrg := &MockErrorTarget{}
	metaProvider := &MockMetadataProvider{}

	engine := NewEngine(mockSrc, mockErrTrg)

	cfg := Config{
		FromSchemaName: "public",
		FromTableName:  "test_src",
		ToSchemaName:   "public",
		ToTableName:    "test_trg",
		ColumnToColumn: map[string]string{"id": "id"},
	}

	result, err := engine.Run(ctx, cfg, 3, 2, metaProvider)
	if err != nil {
		t.Fatalf("Движок не должен возвращать критическую ошибку при сбое отдельного чанка: %v", err)
	}

	if result.TotalRows != 0 {
		t.Errorf("Ожидалось 0 успешно перенесенных строк, получено: %d", result.TotalRows)
	}
}
