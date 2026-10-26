package core

import (
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadConnectionProperties читает YAML-файл конфигурации подключений.
func LoadConnectionProperties(filename string) (*ConnectionProperties, error) {
	// В Go чтение файла возвращает срез байт []byte (аналог чтения в byte[] или InputStream)
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read properties file: %w", err)
	}

	props := &ConnectionProperties{}
	// yaml.Unmarshal парсит байты прямо в структуру по YAML-тегам
	err = yaml.Unmarshal(data, props)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	return props, nil
}

// LoadMappingConfigs читает JSON-файл с массивом конфигураций таблиц.
func LoadMappingConfigs(filename string) ([]Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read mapping file: %w", err)
	}

	var configs []Config
	// json.Unmarshal парсит байты в срез структур по JSON-тегам
	err = json.Unmarshal(data, &configs)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return configs, nil
}
