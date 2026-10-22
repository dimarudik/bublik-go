package main

import "context"

// RowData представляет собой одну строку данных из БД.
type RowData []any

// SourceStorage описывает логику чтения данных из исходной БД.
type SourceStorage interface {
	// CreateChunks разбивает таблицу на куски и возвращает список ID чанков
	CreateChunks(ctx context.Context, cfg MappingConfig, chunkSize int) ([]string, error)
	// StreamChunk читает данные конкретного чанка и отправляет строки в канал
	StreamChunk(ctx context.Context, chunkID string, out chan<- RowData) error
}

// TargetStorage описывает логику записи данных в целевую БД.
type TargetStorage interface {
	// WriteChunk записывает пачку данных из канала в целевую БД
	WriteChunk(ctx context.Context, chunkID string, rows <-chan RowData) error
}
