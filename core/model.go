package core

import "time"

// Column описывает метаданные конкретного столбца в СУБД.
type Column struct {
	Position   int
	Name       string
	Type       string
	IsNullable bool
}

// Table представляет собой доменную модель таблицы (аналог Table.java).
type Table struct {
	SchemaName string
	TableName  string
	Columns    []Column
	PkColumns  []Column
}

// FullName возвращает сквозное имя таблицы "schema.table".
func (t Table) FullName() string {
	if t.SchemaName == "" {
		return t.TableName
	}
	return t.SchemaName + "." + t.TableName
}

// ChunkStatus описывает жизненный цикл чанка (аналог ChunkStatus enum в Java).
type ChunkStatus string

const (
	ChunkStatusNew                ChunkStatus = "NEW"
	ChunkStatusAssigned           ChunkStatus = "ASSIGNED"
	ChunkStatusLocked             ChunkStatus = "LOCKED"
	ChunkStatusProcessed          ChunkStatus = "PROCESSED"
	ChunkStatusProcessedWithError ChunkStatus = "PROCESSED_WITH_ERROR"
)

// ChunkMetadata описывает персистентную структуру чанка для таблицы метаданных (аналог Chunk.java).
type Chunk[T any] struct {
	ID       int64
	TaskName string
	Start    T
	End      T
	Status   ChunkStatus
	UUID     string
	ErrorMsg string
	UpdateTs time.Time
}
