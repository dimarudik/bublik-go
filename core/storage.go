package core

import (
	"context"
)

type SourceStorage interface {
	CreateChunkTable(ctx context.Context) error
	SliceAndPersistChunks(ctx context.Context, cfg Config, chunkSize int) error
	FetchAvailableChunks(ctx context.Context, configs []Config) ([]any, error)
	StreamChunk(ctx context.Context, schema string, table string, columns []string, chunk any, out chan<- []any) error
}

type TargetStorage interface {
	CreateOutboxTable(ctx context.Context) error
	WriteChunkUpsert(ctx context.Context, chunkID int64, table string, columns []string, pkColumns []string, rows <-chan []any) (int64, error)
}
