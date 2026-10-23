package core

// MappingConfig задает правила переноса данных для конкретной таблицы.
type MappingConfig struct {
	FromSchema string            `json:"fromSchema"`
	FromTable  string            `json:"fromTable"`
	ToSchema   string            `json:"toSchema"`
	ToTable    string            `json:"toTable"`
	Columns    map[string]string `json:"columns"` // Маппинг "source_col" -> "target_col"
}
