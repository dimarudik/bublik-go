package core

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// Engine теперь зависит ТОЛЬКО от абстрактных интерфейсов нашего ядра.
// Никакого импорта пакета "postgres" здесь больше нет!
type Engine struct {
	source SourceStorage
	target TargetStorage
}

func NewEngine(src SourceStorage, trg TargetStorage) *Engine {
	return &Engine{
		source: src,
		target: trg,
	}
}

type Result struct {
	TotalRows int64
	Duration  time.Duration
}

// Run запускает двухфазный конвейер миграции: сначала полная нарезка всех таблиц, затем параллельное выполнение
func (e *Engine) Run(ctx context.Context, configs []Config, chunkSize int, threadCount int, sourceMetadataProvider interface {
	EnrichTableMetadata(ctx context.Context, schema, table string) (Table, error)
}) (*Result, error) {
	startTime := time.Now()

	// =========================================================================
	// ФАЗА 1: ГЕНЕРАЦИЯ ПОЛНОГО РЕЕСТРА ЗАДАЧ (Pre-Slicing)
	// =========================================================================
	log.Println("[Engine] === СТАРТ ФАЗЫ 1: Нарезка и фиксация чанков для ВСЕХ таблиц ===")

	// Хранилище динамических метаданных колонок, чтобы не вычитывать их повторно в воркерах
	tableMetaMap := make(map[string]Table)

	for _, cfg := range configs {
		// 1.1. Вычитываем структуру метаданных СУБД для кэша движка
		meta, err := sourceMetadataProvider.EnrichTableMetadata(ctx, cfg.FromSchemaName, cfg.FromTableName)
		if err != nil {
			return nil, fmt.Errorf("failed to enrich metadata for %s: %w", cfg.FromTableName, err)
		}
		taskName := fmt.Sprintf("task_%s_%s", strings.ToLower(cfg.FromSchemaName), strings.ToLower(cfg.FromTableName))
		tableMetaMap[taskName] = meta

		// 1.2. Нарезаем и атомарно персистим чанки в БД. Теперь вся сессия запишется ДО стартов потоков!
		err = e.source.SliceAndPersistChunks(ctx, cfg, chunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to slice and persist chunks for table %s: %w", cfg.FromTableName, err)
		}
	}

	// =========================================================================
	// ФАЗА 2: ВЫЧИТКА РЕЕСТРА И КОНКУРЕНТНОЕ ВЫПОЛНЕНИЕ (Execution Phase)
	// =========================================================================
	log.Println("[Engine] === СТАРТ ФАЗЫ 2: Конкурентная обработка общего пула задач ===")

	chunks, err := e.source.FetchAvailableChunks(ctx, configs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch available chunks from registry: %w", err)
	}
	log.Printf("[Engine] Из персистентного реестра БД успешно вычитано задач: %d. Запуск %d воркеров...", len(chunks), threadCount)

	if len(chunks) == 0 {
		return &Result{TotalRows: 0, Duration: time.Since(startTime)}, nil
	}

	// Наполняем общую очередь задач
	chunkQueue := make(chan any, len(chunks))
	for _, chunk := range chunks {
		chunkQueue <- chunk
	}
	close(chunkQueue)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalMigrated int64

	for w := 1; w <= threadCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for chunk := range chunkQueue {
				var currentChunkID int64
				var taskName string
				switch ch := chunk.(type) {
				case Chunk[int64]:
					currentChunkID = ch.ID
					taskName = ch.TaskName
				default:
					currentChunkID = time.Now().UnixNano()
				}

				// Извлекаем предвычисленные метаданные колонок из нашего кэша Фазы 1
				meta := tableMetaMap[taskName]

				var srcColumns []string
				var trgColumns []string
				for _, col := range meta.Columns {
					srcColumns = append(srcColumns, col.Name)
					trgColumns = append(trgColumns, col.Name) // Для простоты мапим 1 в 1
				}
				var pkColumns []string
				for _, pk := range meta.PkColumns {
					pkColumns = append(pkColumns, pk.Name)
				}

				rowChan := make(chan []any, 1000)

				// Потоковый Продюсер
				go func(ch any, s, t string) {
					defer close(rowChan)
					_ = e.source.StreamChunk(ctx, s, t, srcColumns, ch, rowChan)
				}(chunk, meta.SchemaName, meta.TableName)

				// Потоковый Консьюмер (Запись)
				inserted, writeErr := e.target.WriteChunkUpsert(ctx, currentChunkID, meta.TableName, trgColumns, pkColumns, rowChan)
				if writeErr != nil {
					log.Printf("[Воркер #%d][Ошибка] Чанк #%d провален: %v", workerID, currentChunkID, writeErr)
					continue
				}

				mu.Lock()
				totalMigrated += inserted
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	return &Result{TotalRows: totalMigrated, Duration: time.Since(startTime)}, nil
}
