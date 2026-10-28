package postgres

import (
	"bublik-go/core"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresTarget struct {
	conn        *pgxpool.Pool
	outboxTable core.Table
}

func NewPostgresTarget(conn *pgxpool.Pool, outboxTable core.Table) *PostgresTarget {
	return &PostgresTarget{
		conn:        conn,
		outboxTable: outboxTable,
	}
}

func (ps *PostgresTarget) CreateOutboxTable(ctx context.Context) error {
	outboxTableName := ps.outboxTable.FullName()
	createTableSQL := fmt.Sprintf(DDL_CREATE_OUTBOX_TABLE, outboxTableName)
	if _, err := ps.conn.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create outbox table: %w", err)
	}
	return nil
}

func (pt *PostgresTarget) WriteChunkUpsert(ctx context.Context, chunkID int64, table string, columns []string, pkColumns []string, rows <-chan []any) (int64, error) {
	tx, err := pt.conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // Авто-откат при панике или ошибке

	// 2. Генерируем SQL-запрос для инкрементального UPSERT
	sqlUpsert, err := buildUpsertSQL(table, columns, pkColumns)
	if err != nil {
		return 0, err
	}

	var recordCount int64
	batchSize := 1000

	// В pgx для пакетных операций используется pgx.Batch (аналог addBatch/executeBatch в JDBC)
	batch := &pgx.Batch{}

	// 3. Вычитываем строки из Go-канала
	for row := range rows {
		batch.Queue(sqlUpsert, row...)
		recordCount++

		// Если набрали пачку (Batch Size), отправляем в базу
		if batch.Len() >= batchSize {
			br := tx.SendBatch(ctx, batch)
			if err := br.Close(); err != nil { // br.Close() выполняет батч и проверяет ошибки на стороне БД
				return 0, fmt.Errorf("batch execution failed: %w", err)
			}
			batch = &pgx.Batch{} // Очищаем батч для следующей порции
		}
	}

	// Сбрасываем остатки данных в канале, если они есть
	if batch.Len() > 0 {
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return 0, fmt.Errorf("final batch execution failed: %w", err)
		}
	}

	// 4. Имитируем insertProcessedChunkInfo (Запись в outbox строго в этой же транзакции!)
	// TODO: Когда появится таблица метаданных, раскомментируем этот вызов:
	// err = pt.insertProcessedChunkInfo(ctx, tx, chunkID)

	// 5. Атомарный коммит дельты данных
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit tx: %w", err)
	}

	return recordCount, nil
}

// buildUpsertSQL динамически собирает строку "INSERT ... ON CONFLICT DO UPDATE"
func buildUpsertSQL(table string, columns []string, pkColumns []string) (string, error) {
	if len(pkColumns) == 0 {
		return "", fmt.Errorf("для выполнения UPSERT на таблице %s обязан быть первичный ключ (PK)", table)
	}

	// Собираем плейсхолдеры ($1, $2, $3...) вместо знаков '?' в JDBC
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	// Собираем секцию DO UPDATE SET col1 = EXCLUDED.col1, ...
	// Исключаем из обновления колонки, входящие в первичный ключ
	var updateClauses []string
	pkMap := make(map[string]bool)
	for _, pk := range pkColumns {
		pkMap[strings.ToLower(pk)] = true
	}

	for _, col := range columns {
		if !pkMap[strings.ToLower(col)] {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}

	// Финальная сборка SQL-запроса
	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(pkColumns, ", "),
		strings.Join(updateClauses, ", "),
	)

	return sql, nil
}
