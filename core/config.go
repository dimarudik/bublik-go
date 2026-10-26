package core

// ConnectionProperties содержит настройки подключений и потоков (аналог ConnectionProperty.java).
// Описываем теги `json` и `yaml`, чтобы один и тот же объект мог парсить оба формата.
type ConnectionProperties struct {
	ThreadCount    int               `json:"threadCount" yaml:"threadCount"`
	FromProperties map[string]string `json:"fromProperties" yaml:"fromProperties"`
	ToProperties   map[string]string `json:"toProperties" yaml:"toProperties"`
}

// Config задает правила переноса данных для конкретной таблицы (полный аналог Config.java).
type Config struct {
	FromSchemaName   string            `json:"fromSchemaName" yaml:"fromSchemaName"`
	FromTableName    string            `json:"fromTableName" yaml:"fromTableName"`
	ToSchemaName     string            `json:"toSchemaName" yaml:"toSchemaName"`
	ToTableName      string            `json:"toTableName" yaml:"toTableName"`
	ColumnToColumn   map[string]string `json:"columnToColumn" yaml:"columnToColumn"`
	FetchWhereClause string            `json:"fetchWhereClause,omitempty" yaml:"fetchWhereClause,omitempty"`
	// При необходимости сюда легко добавляются остальные поля из Java рекорда (withTTL, timestamp и др.)
}
