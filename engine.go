package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"bublik-go/postgres"
)

// Engine — это центральный оркестратор миграции данных (Аналог Core модуля в Java).
type Engine struct {
	source *postgres.PostgresSource
	target *postgres.PostgresTarget
}

// NewEngine — конструктор для нашего движка.
func NewEngine(src *postgres.PostgresSource, trg *postgres.PostgresTarget) *Engine {
	return &Engine{
		source: src,
		target: trg,
	}
}

// Result содержит статистику выполнения миграции.
type Result struct {
	TotalRows int64
	Duration  time.Duration
}

// Run запускает параллельный конвейер миграции на основе переданной конфигурации.
// Параметр threadCount — это полный аналог threadCount из оригинального Бублика.
func (e *Engine) Run(ctx context.Context, cfg MappingConfig, chunkSize int, threadCount int) (*Result, error) {
	startTime := time.Now()

	// 1. Извлекаем список колонок (ключи мапы columns — это source, значения — target)
	var srcColumns []string
	var trgColumns []string
	for srcCol, trgCol := range cfg.Columns {
		srcColumns = append(srcColumns, srcCol)
		trgColumns = append(trgColumns, trgCol)
	}

	// Для простоты пока считаем, что ПК — это всегда колонка "id".
	// В будущем вынесем это в MappingConfig, как в Java-версии.
	pkColumns := []string{"id"}

	// 2. Нарезаем физические чанки по ctid через Source
	log.Printf("[Engine] Нарезка чанков по ctid для таблицы %s.%s...", cfg.FromSchema, cfg.FromTable)
	chunks, err := e.source.CreateChunks(ctx, cfg.FromSchema, cfg.FromTable, chunkSize)
	if err != nil {
		return nil, fmt.Errorf("failed to slice chunks: %w", err)
	}
	log.Printf("[Engine] Сформировано чанков: %d. Запуск воркеров (threadCount = %d)...", len(chunks), threadCount)

	// 3. Создаем потокобезопасный канал для распределения чанков между воркерами
	chunkQueue := make(chan postgres.ChunkRange, len(chunks))
	for _, chunk := range chunks {
		chunkQueue <- chunk
	}
	close(chunkQueue) // Закрываем канал: воркеры поймут, когда задачи закончатся

	// 4. Инициализируем средства синхронизации конкурентности
	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalMigrated int64

	// 5. Запускаем пул горутин-воркеров (Worker Pool)
	for w := 1; w <= threadCount; w++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			// Каждый воркер конкурентно выхватывает чанк из общей очереди
			for chunk := range chunkQueue {
				log.Printf("[Воркер #%d] Взял чанк #%d (Страницы ctid: %d -> %d)",
					workerID, chunk.ID, chunk.StartPage, chunk.EndPage)

				// Внутренний потоковый буфер для строк между Source и Target
				rowChan := make(chan []any, 1000)

				// Запускаем под-горутину чтения (Продюсер)
				go func(ch postgres.ChunkRange) {
					defer close(rowChan)
					err := e.source.StreamChunk(ctx, cfg.FromSchema, cfg.FromTable, srcColumns, ch, rowChan)
					if err != nil {
						log.Printf("[Воркер #%d][Ошибка Чтения] Чанк #%d: %v", workerID, ch.ID, err)
					}
				}(chunk)

				// Основной поток воркера выступает как Консьюмер и льет данные через UPSERT
				inserted, err := e.target.WriteChunkUpsert(ctx, cfg.ToTable, trgColumns, pkColumns, rowChan)
				if err != nil {
					log.Printf("[Воркер #%d][Критическая Ошибка Записи] Чанк #%d провален: %v", workerID, chunk.ID, err)
					continue // В реальном Бублике тут пишется статус PROCESSED_WITH_ERROR в outbox
				}

				// Безопасно обновляем атомарный счетчик строк
				mu.Lock()
				totalMigrated += inserted
				mu.Unlock()

				log.Printf("[Воркер #%d] Чанк #%d успешно зафиксирован. Записано строк: %d", workerID, chunk.ID, inserted)
			}
		}(w)
	}

	// Ожидаем завершения всех горутин из пула threadCount
	wg.Wait()

	return &Result{
		TotalRows: totalMigrated,
		Duration:  time.Since(startTime),
	}, nil
}
