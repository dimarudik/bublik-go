package postgres

import (
	"context"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSource отвечает за нарезку чанков и чтение данных из исходной БД.
type PostgresSource struct {
	conn *pgxpool.Pool
}

// NewPostgresSource создает новый экземпляр исходного хранилища.
func NewPostgresSource(conn *pgxpool.Pool) *PostgresSource {
	return &PostgresSource{conn: conn}
}

// ChunkRange представляет границы физического чанка по ctid
type ChunkRange struct {
	ID        int64
	StartPage int64
	EndPage   int64
}

// CreateChunks оценивает размер таблицы в страницах (блоках) и нарезает её на ctid-диапазоны.
// chunkSize здесь означает желаемое количество строк на чанк (например, 50 000).
func (ps *PostgresSource) CreateChunks(ctx context.Context, schema string, table string, chunkSize int) ([]ChunkRange, error) {
	// 1. Запрашиваем у системного каталога Postgres количество физических страниц (relpages)
	// и приблизительное количество строк (reltuples).
	query := `
		SELECT relpages, reltuples 
		FROM pg_class c 
		JOIN pg_namespace n ON n.oid = c.relnamespace 
		WHERE n.nspname = $1 AND c.relname = $2`

	var relPages int64
	var relTuples float64

	err := ps.conn.QueryRow(ctx, query, schema, table).Scan(&relPages, &relTuples)
	if err != nil {
		return nil, fmt.Errorf("failed to get table stats from pg_class: %w", err)
	}

	// Если таблица пустая или статистика еще не собиралась (ANALYZE)
	if relPages == 0 {
		relPages = 1 // Обработаем хотя бы нулевую страницу
	}

	// 2. Рассчитываем, сколько строк примерно помещается в одну страницу (плотность данных)
	var avgRowsPerPage float64 = 1
	if relTuples > 0 && relPages > 0 {
		avgRowsPerPage = relTuples / float64(relPages)
	}

	// Вычисляем, сколько страниц нужно объединить в один чанк, чтобы набрать нужный chunkSize строк
	pagesPerChunk := int64(math.Ceil(float64(chunkSize) / avgRowsPerPage))
	if pagesPerChunk <= 0 {
		pagesPerChunk = 1
	}

	// 3. Нарезаем диапазоны страниц
	var chunks []ChunkRange
	var chunkID int64 = 1

	for startPage := int64(0); startPage < relPages; startPage += pagesPerChunk {
		endPage := startPage + pagesPerChunk
		if endPage > relPages {
			endPage = relPages
		}

		chunks = append(chunks, ChunkRange{
			ID:        chunkID,
			StartPage: startPage,
			EndPage:   endPage,
		})
		chunkID++
	}

	return chunks, nil
}

// StreamChunk выполняет эффективный Tid Range Scan для конкретного чанка и отправляет данные в Go-канал.
func (ps *PostgresSource) StreamChunk(ctx context.Context, schema string, table string, columns []string, chunk ChunkRange, out chan<- []any) error {
	// Формируем имена колонок для SELECT
	colsSQL := `"` + columns[0] + `"`
	for i := 1; i < len(columns); i++ {
		colsSQL += `, "` + columns[i] + `"`
	}

	// Строим SQL-запрос с фильтрацией по физическим ctid-адресам (Tid Range Scan)
	// Важно: Валидный формат ctid на границах — '(page_id, 0)'
	query := fmt.Sprintf(
		`SELECT %s FROM %s.%s WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid`,
		colsSQL, schema, table, chunk.StartPage, chunk.EndPage,
	)

	// Выполняем потоковое чтение
	rows, err := ps.conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute ctid scan query: %w", err)
	}
	defer rows.Close()

	// Читаем строки по одной
	for rows.Next() {
		// pgx позволяет динамически считывать строку в срез any
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("failed to scan row values: %w", err)
		}

		// Передаем строку в канал (воркер на стороне Target её сразу подхватит)
		select {
		case out <- values:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return rows.Err()
}
