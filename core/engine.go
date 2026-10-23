package core

import (
	"context"
	"fmt"
	"log"
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

func (e *Engine) Run(ctx context.Context, cfg MappingConfig, chunkSize int, threadCount int) (*Result, error) {
	startTime := time.Now()

	var srcColumns []string
	var trgColumns []string
	for srcCol, trgCol := range cfg.Columns {
		srcColumns = append(srcColumns, srcCol)
		trgColumns = append(trgColumns, trgCol)
	}
	pkColumns := []string{"id"}

	// Вызов CreateChunks через интерфейс
	log.Printf("[Engine] Нарезка чанков для таблицы %s.%s...", cfg.FromSchema, cfg.FromTable)
	chunks, err := e.source.CreateChunks(ctx, cfg.FromSchema, cfg.FromTable, chunkSize)
	if err != nil {
		return nil, fmt.Errorf("failed to slice chunks: %w", err)
	}
	log.Printf("[Engine] Сформировано чанков: %d. Запуск пула горутин...", len(chunks))

	// Очередь теперь гоняет пустые интерфейсы 'any'
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
				rowChan := make(chan []any, 1000)

				// Потоковый продюсер
				go func(ch any) {
					defer close(rowChan)
					_ = e.source.StreamChunk(ctx, cfg.FromSchema, cfg.FromTable, srcColumns, ch, rowChan)
				}(chunk)

				// Потоковый консьюмер
				inserted, err := e.target.WriteChunkUpsert(ctx, cfg.ToTable, trgColumns, pkColumns, rowChan)
				if err != nil {
					log.Printf("[Воркер #%d][Ошибка Записи]: %v", workerID, err)
					continue
				}

				mu.Lock()
				totalMigrated += inserted
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()

	return &Result{
		TotalRows: totalMigrated,
		Duration:  time.Since(startTime),
	}, nil
}
