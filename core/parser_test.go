package core

import (
	"os"
	"testing"
)

func TestLoadConnectionProperties(t *testing.T) {
	// 1. Создаем временный YAML-файл, имитирующий pg2pg.yaml
	yamlData := `
threadCount: 10
fromProperties:
  url: jdbc:postgresql://localhost:5432/postgres
  user: test
toProperties:
  url: jdbc:postgresql://localhost:5432/target
  outboxTableName: _outbox_
`
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Гарантированно удаляем файл после теста

	if _, err := tmpFile.Write([]byte(yamlData)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// 2. Тестируем наш парсер
	props, err := LoadConnectionProperties(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConnectionProperties failed: %v", err)
	}

	// 3. Проверяем утверждения (Assertions)
	if props.ThreadCount != 10 {
		t.Errorf("Expected threadCount 10, got %d", props.ThreadCount)
	}
	if props.FromProperties["user"] != "test" {
		t.Errorf("Expected from user 'test', got '%s'", props.FromProperties["user"])
	}
}

func TestLoadMappingConfigs(t *testing.T) {
	// Имитируем columnOrder.json
	jsonData := `[
		{
			"fromSchemaName": "test",
			"fromTableName": "a",
			"toSchemaName": "test",
			"toTableName": "b"
		}
	]`
	tmpFile, err := os.CreateTemp("", "mapping-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(jsonData)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	configs, err := LoadMappingConfigs(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadMappingConfigs failed: %v", err)
	}

	if len(configs) != 1 {
		t.Fatalf("Expected 1 config, got %d", len(configs))
	}
	if configs[0].FromTableName != "a" || configs[0].ToTableName != "b" {
		t.Errorf("Mapping parsing values mismatch")
	}
}
