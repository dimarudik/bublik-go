package core

type ConnectionProperties struct {
	ThreadCount    int               `json:"threadCount" yaml:"threadCount"`
	FromProperties map[string]string `json:"fromProperties" yaml:"fromProperties"`
	ToProperties   map[string]string `json:"toProperties" yaml:"toProperties"`
}

type Config struct {
	FromSchemaName   string            `json:"fromSchemaName" yaml:"fromSchemaName"`
	FromTableName    string            `json:"fromTableName" yaml:"fromTableName"`
	FromTaskName     string            `json:"fromTaskName" yaml:"fromTaskName"`
	ToSchemaName     string            `json:"toSchemaName" yaml:"toSchemaName"`
	ToTableName      string            `json:"toTableName" yaml:"toTableName"`
	ColumnToColumn   map[string]string `json:"columnToColumn" yaml:"columnToColumn"`
	FetchWhereClause string            `json:"fetchWhereClause,omitempty" yaml:"fetchWhereClause,omitempty"`
	// При необходимости сюда легко добавляются остальные поля из Java рекорда (withTTL, timestamp и др.)
}

func (cp *ConnectionProperties) GetChunkTable() Table {
	schema := cp.FromProperties["chunkSchemaName"]
	table := cp.FromProperties["chunkTableName"]
	if schema == "" {
		schema = "public"
	}
	if table == "" {
		table = "bublik_chunk"
	}
	return Table{SchemaName: schema, TableName: table}
}

func (cp *ConnectionProperties) GetOutboxTable() Table {
	schema := cp.ToProperties["outboxSchemaName"]
	table := cp.ToProperties["outboxTableName"]
	if schema == "" {
		schema = "public"
	}
	if table == "" {
		table = "bublik_outbox"
	}
	return Table{SchemaName: schema, TableName: table}
}
