package postgres

import (
	"context"
	"fmt"
	"math"
	"strings"

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
func (ps *PostgresSource) CreateChunks(ctx context.Context, schema string, table string, chunkSize int) ([]any, error) {
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
	var chunks []any
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
func (ps *PostgresSource) StreamChunk(ctx context.Context, schema string, table string, columns []string, chunk any, out chan<- []any) error {
	// 1. Распаковываем пустой интерфейс any в конкретную структуру Postgres
	ch := chunk.(ChunkRange)

	// 2. Идиоматичный и безопасный сбор колонок: экранируем каждую колонку кавычками
	escapedCols := make([]string, len(columns))
	for i, col := range columns {
		escapedCols[i] = `"` + col + `"`
	}
	// strings.Join склеит массив через запятую (аналог String.join в Java)
	colsSQL := strings.Join(escapedCols, ", ")

	// 3. Строим SQL-запрос. ИСПОЛЬЗУЕМ ПЕРЕМЕННУЮ ch вместо chunk!
	query := fmt.Sprintf(
		`SELECT %s FROM "%s"."%s" WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid`,
		colsSQL, schema, table, ch.StartPage, ch.EndPage,
	)

	// Выполняем потоковое чтение
	rows, err := ps.conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute ctid scan query: %w", err)
	}
	defer rows.Close()

	// Читаем строки по одной
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
