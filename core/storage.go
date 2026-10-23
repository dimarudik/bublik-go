package core

import (
	"context"
)

type SourceStorage interface {
	// CreateChunks нарезает физические или логические диапазоны данных
	// Мы возвращаем срез анонимных интерфейсов 'any', чтобы это могли быть
	// и ctid-диапазоны для Postgres, и ROWID-диапазоны для Oracle!
	CreateChunks(ctx context.Context, schema string, table string, chunkSize int) ([]any, error)

	// StreamChunk читает данные и шлет их в канал
	StreamChunk(ctx context.Context, schema string, table string, columns []string, chunk any, out chan<- []any) error
}

// TargetStorage описывает абстрактный приемник данных
type TargetStorage interface {
	// WriteChunkUpsert пишет данные в режиме UPSERT
	WriteChunkUpsert(ctx context.Context, table string, columns []string, pkColumns []string, rows <-chan []any) (int64, error)
}
