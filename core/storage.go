package core

import (
	"context"
)

type SourceStorage interface {
	SliceAndPersistChunks(ctx context.Context, cfg Config, chunkSize int) error

	FetchAvailableChunks(ctx context.Context, configs []Config) ([]any, error)

	StreamChunk(ctx context.Context, schema string, table string, columns []string, chunk any, out chan<- []any) error
}

// TargetStorage описывает абстрактный приемник данных
type TargetStorage interface {
	// WriteChunkUpsert пишет данные в режиме UPSERT
	WriteChunkUpsert(ctx context.Context, chunkID int64, table string, columns []string, pkColumns []string, rows <-chan []any) (int64, error)
}
