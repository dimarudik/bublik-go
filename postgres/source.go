package postgres

import (
	"bublik-go/core"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSource отвечает за нарезку чанков и чтение данных из исходной БД.
type PostgresSource struct {
	conn       *pgxpool.Pool
	chunkTable core.Table
}

// NewPostgresSource создает новый экземпляр исходного хранилища.
func NewPostgresSource(conn *pgxpool.Pool, chunkTable core.Table) *PostgresSource {
	return &PostgresSource{
		conn:       conn,
		chunkTable: chunkTable,
	}
}

// ChunkRange представляет границы физического чанка по ctid
type ChunkRange struct {
	ID        int64
	StartPage int64
	EndPage   int64
}

/*func (ps *PostgresSource) CreateChunks(ctx context.Context, schema string, table string, chunkSize int) ([]any, error) {
	chunkTableName := ps.chunkTable.FullName()
	taskName := fmt.Sprintf("task_%s_%s", strings.ToLower(schema), strings.ToLower(table))

	// 1. Атомарно создаем персистентную таблицу чанков в БД (если её еще нет)
	createTableSQL := fmt.Sprintf(DDL_CREATE_CHUNK_TABLE, chunkTableName)
	_, err := ps.conn.Exec(ctx, createTableSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure chunk table exists: %w", err)
	}

	// 2. Вычитываем статистику relpages из pg_class для исходной таблицы
	var relPages int64
	var relTuples float64
	err = ps.conn.QueryRow(ctx, "SELECT relpages::bigint, reltuples FROM pg_class WHERE relnamespace::regnamespace::text = $1 AND relname = $2", schema, table).Scan(&relPages, &relTuples)
	if err != nil {
		return nil, fmt.Errorf("failed to read table pages from pg_class: %w", err)
	}

	if relPages == 0 && relTuples > 0 {
		relPages = 1
	}

	log.Printf("[PostgresSource] Таблица %s.%s содержит блоков страниц relpages: %d, строк reltuples: %.0f",
		schema, table, relPages, relTuples)

	// 3. Открываем транзакцию, чтобы проверить, нет ли уже нарезанных чанков для этой задачи
	tx, err := ps.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin chunk initialization tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var existingCount int64
	checkQuery := fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE task_name = $1", chunkTableName)
	err = tx.QueryRow(ctx, checkQuery, taskName).Scan(&existingCount)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing chunks: %w", err)
	}

	// 4. Если чанков в базе нет — выполняем массовую нарезку через generate_series прямо на стороне СУБД!
	if existingCount == 0 {
		log.Printf("[PostgresSource] База чиста. Выполняем генерацию чанков в таблице %s...", chunkTableName)

		insertQuery := fmt.Sprintf(DML_BATCH_INSERT_CHUNK_TABLE, chunkTableName)
		log.Print(insertQuery)
		_, err = tx.Exec(ctx, insertQuery,
			chunkSize,    // $1: для n + $1
			0,            // $2: copied (0)
			taskName,     // $3: task_name
			schema,       // $4: schema_name
			table,        // $5: table_name
			"UNASSIGNED", // $6: status
			0,            // $7: required (0)
			0,            // $8: start для generate_series (начинаем с 0 страницы)
			relPages,     // $9: stop для generate_series (до relPages блоков)
			chunkSize,    // $10: step для generate_series (шагаем по размеру чанка)
		)
		if err != nil {
			return nil, fmt.Errorf("failed to batch insert chunks via generate_series: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit chunk generation tx: %w", err)
		}
	} else {
		log.Printf("[PostgresSource] Найдено %d ранее нарезанных чанков в БД. Возобновляем миграцию.", existingCount)
	}

	// 5. Вычитываем из БД все неназначенные задачи для передачи в движок
	selectQuery := fmt.Sprintf("SELECT chunk_id, start_page, end_page FROM %s WHERE task_name = $1 AND status = 'UNASSIGNED' ORDER BY chunk_id", chunkTableName)
	rows, err := ps.conn.Query(ctx, selectQuery, taskName)
	if err != nil {
		return nil, fmt.Errorf("failed to select unassigned chunks from db: %w", err)
	}
	defer rows.Close()

	var chunks []any
	for rows.Next() {
		var ch ChunkRange
		if err := rows.Scan(&ch.ID, &ch.StartPage, &ch.EndPage); err != nil {
			return nil, err
		}
		// Упаковываем в полиморфный тип core.Chunk[int64] на генериках!
		chunks = append(chunks, core.Chunk[int64]{
			ID:       ch.ID,
			TaskName: taskName,
			Start:    ch.StartPage,
			End:      ch.EndPage,
			Status:   core.ChunkStatusNew,
		})
	}

	return chunks, nil
}
*/
// StreamChunk выполняет эффективный Tid Range Scan для конкретного чанка и отправляет данные в Go-канал.
func (ps *PostgresSource) StreamChunk(ctx context.Context, schema string, table string, columns []string, chunk any, out chan<- []any) error {
	// Распаковываем универсальный интерфейс в нашу строго типизированную структуру на генериках
	ch := chunk.(core.Chunk[int64])

	// Экранируем имена колонок
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = `"` + col + `"`
	}
	colsSQL := strings.Join(escapedCols, ", ")

	// Формируем SQL-запрос с фильтрацией по физическим ctid-адресам (Tid Range Scan)
	query := fmt.Sprintf(
		`SELECT %s FROM "%s"."%s" WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid`,
		colsSQL, schema, table, ch.Start, ch.End,
	)

	// Выполняем потоковое чтение
	rows, err := ps.conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute ctid scan query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("failed to scan row values: %w", err)
		}

		select {
		case out <- values:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return rows.Err()
}

// EnrichTableMetadata вычитывает структуру колонок и ПК прямо из информационного каталога базы данных
func (ps *PostgresSource) EnrichTableMetadata(ctx context.Context, schema, table string) (core.Table, error) {
	t := core.Table{SchemaName: schema, TableName: table}

	columnsQuery := `
		SELECT ordinal_position, column_name, data_type, is_nullable 
		FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`

	rows, err := ps.conn.Query(ctx, columnsQuery, schema, table)
	if err != nil {
		return t, fmt.Errorf("failed to read columns metadata: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col core.Column
		var isNullableStr string
		err := rows.Scan(&col.Position, &col.Name, &col.Type, &isNullableStr)
		if err != nil {
			return t, err
		}
		col.IsNullable = (isNullableStr == "YES")
		t.Columns = append(t.Columns, col)
	}

	pkQuery := `
		SELECT kcu.ordinal_position, kcu.column_name 
		FROM information_schema.table_constraints tco
		JOIN information_schema.key_column_usage kcu 
		  ON kcu.constraint_name = tco.constraint_name
		 AND kcu.table_schema = tco.table_schema
		 AND kcu.table_name = tco.table_name
		WHERE tco.constraint_type = 'PRIMARY KEY' AND tco.table_schema = $1 AND tco.table_name = $2
		ORDER BY kcu.ordinal_position`

	pkRows, err := ps.conn.Query(ctx, pkQuery, schema, table)
	if err != nil {
		return t, fmt.Errorf("failed to read PK metadata: %w", err)
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var col core.Column
		err := pkRows.Scan(&col.Position, &col.Name)
		if err != nil {
			return t, err
		}
		t.PkColumns = append(t.PkColumns, col)
	}

	return t, nil
}

// SliceAndPersistChunks атомарно создает таблицу и заливает туда чанки для конкретной таблицы (Фаза 1)
func (ps *PostgresSource) SliceAndPersistChunks(ctx context.Context, cfg core.Config, chunkSize int) error {
	chunkTableName := ps.chunkTable.FullName()
	taskName := fmt.Sprintf("task_%s_%s", strings.ToLower(cfg.FromSchemaName), strings.ToLower(cfg.FromTableName))

	var relPages int64
	var relTuples float64
	err := ps.conn.QueryRow(ctx, "SELECT relpages::bigint, reltuples FROM pg_class WHERE relnamespace::regnamespace::text = $1 AND relname = $2", cfg.FromSchemaName, cfg.FromTableName).Scan(&relPages, &relTuples)
	if err != nil {
		return fmt.Errorf("failed to read table pages from pg_class: %w", err)
	}
	if relPages == 0 && relTuples > 0 {
		relPages = 1
	}

	tx, err := ps.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	insertQuery := fmt.Sprintf(DML_BATCH_INSERT_CHUNK_TABLE, chunkTableName)
	_, err = tx.Exec(ctx, insertQuery, chunkSize, taskName, cfg.FromSchemaName, cfg.FromTableName, 0, relPages, chunkSize)
	if err != nil {
		return fmt.Errorf("failed to batch insert chunks via generate_series: %w", err)
	}
	return tx.Commit(ctx)
}

// FetchAvailableChunks выгребает ВСЕ неназначенные чанки по списку наших задач (Фаза 2)
func (ps *PostgresSource) FetchAvailableChunks(ctx context.Context, configs []core.Config) ([]any, error) {
	chunkTableName := ps.chunkTable.FullName()

	// Собираем все имена задач для миграции
	var taskNames []string
	for _, cfg := range configs {
		taskNames = append(taskNames, fmt.Sprintf("task_%s_%s", strings.ToLower(cfg.FromSchemaName), strings.ToLower(cfg.FromTableName)))
	}

	// Для простоты вычитки делаем запрос, выбирающий UNASSIGNED чанки по списку задач
	// В реальном Бублике тут используется SKIP LOCKED внутри пула, мы вычитаем пакет задач для старта
	selectQuery := fmt.Sprintf("SELECT chunk_id, start_page, end_page, task_name, schema_name, table_name FROM %s WHERE status = 'UNASSIGNED' ORDER BY chunk_id", chunkTableName)
	rows, err := ps.conn.Query(ctx, selectQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []any
	for rows.Next() {
		var ch ChunkRange
		var taskName, schema, table string
		if err := rows.Scan(&ch.ID, &ch.StartPage, &ch.EndPage, &taskName, &schema, &table); err != nil {
			return nil, err
		}

		// Фильтруем только те задачи, которые есть в текущей сессии запуска
		for _, tn := range taskNames {
			if tn == taskName {
				chunks = append(chunks, core.Chunk[int64]{
					ID:       ch.ID,
					TaskName: taskName,
					Start:    ch.StartPage,
					End:      ch.EndPage,
					Status:   core.ChunkStatusNew,
				})
				break
			}
		}
	}
	return chunks, nil
}
