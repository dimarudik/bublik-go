package main

import (
	"bublik-go/core"
	"bublik-go/postgres"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

func main() {
	// Создаем корневую команду CLI (Аналог Options и Parser из Apache Commons CLI)
	var (
		configFile  string
		mappingFile string
		chunkSize   int
		mode        string
	)

	var rootCmd = &cobra.Command{
		Use:   "bublik-go",
		Short: "Bublik-Go — высокопроизводительная утилита миграции данных",
		Long:  `Инфраструктурный инструмент переноса данных между СУБД с поддержкой Exactly-Once, физического чанкования и параллельных воркеров.`,
		// Функция RunE выполнится только тогда, когда аргументы командной строки успешно распарсятся
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			// 1. Читаем настройки подключений из YAML-файла, переданного через флаг -c / --config
			props, err := core.LoadConnectionProperties(configFile)
			if err != nil {
				return fmt.Errorf("ошибка загрузки свойств подключения: %w", err)
			}

			// 2. Читаем маппинги таблиц из JSON-файла, переданного через флаг -m / --mapping
			configs, err := core.LoadMappingConfigs(mappingFile)
			if err != nil {
				return fmt.Errorf("ошибка загрузки маппингов таблиц: %w", err)
			}

			log.Printf("[CLI] Загружен конфиг '%s' (воркеров: %d). Найдено таблиц для переноса: %d",
				configFile, props.ThreadCount, len(configs))
			log.Printf("[CLI] Режим выполнения (режим передан через флаг): %s", mode)

			// 3. Подключаемся к базе данных
			// Берем URL из секции toProperties нашего YAML, как в Java-версии
			connStr, ok := props.ToProperties["url"]
			if !ok || connStr == "" {
				connStr = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" // дефолт
			}

			log.Println("[CLI] Инициализация пула соединений СУБД...")
			pool, err := pgxpool.New(ctx, connStr)
			if err != nil {
				return fmt.Errorf("ошибка инициализации пула: %w", err)
			}
			defer pool.Close()

			// Временная настройка таблиц для тестов (можно убрать в продакшене)
			//setupTables(ctx, pool)

			// 4. Инициализируем компоненты движка
			source := postgres.NewPostgresSource(pool)
			target := postgres.NewPostgresTarget(pool)
			engine := core.NewEngine(source, target)

			totalRows := int64(0)
			sessionStart := time.Now()

			// 5. Потоковый цикл по конфигурациям таблиц
			for _, config := range configs {
				log.Printf("[CLI] Накатываем пайплайн: %s.%s -> %s.%s",
					config.FromSchemaName, config.FromTableName, config.ToSchemaName, config.ToTableName)

				// Запускаем движок!
				// Передаем динамический chunkSize из флага -k и threadCount из YAML файла
				result, err := engine.Run(ctx, config, chunkSize, props.ThreadCount)
				if err != nil {
					log.Printf("[Ошибка] Таблица %s пропущена из-за сбоя: %v", config.FromTableName, err)
					continue
				}
				totalRows += result.TotalRows
			}

			fmt.Printf("\n====================================\n")
			fmt.Printf("МИГРАЦИЯ УСПЕШНО ВЫПОЛНЕНА ДВИЖКОМ\n")
			fmt.Printf("Всего перенесено строк: %d\n", totalRows)
			fmt.Printf("Общее время работы сессии: %v\n", time.Since(sessionStart))
			fmt.Printf("====================================\n")

			return nil
		},
	}

	// Регистрируем флаги (Аналог Option.builder() из Java)
	rootCmd.Flags().StringVarP(&configFile, "config", "c", "pg2pg.yaml", "путь к YAML-файлу свойств подключения")
	rootCmd.Flags().StringVarP(&mappingFile, "mapping", "m", "columnOrder.json", "путь к JSON-файлу маппингов таблиц")
	rootCmd.Flags().IntVarP(&chunkSize, "chunk", "k", 3, "размер физического чанка (количество строк)")
	rootCmd.Flags().StringVarP(&mode, "mode", "s", "UPSERT", "режим выполнения (COPY или UPSERT)")

	// Указываем, какие флаги являются обязательными для работы
	_ = rootCmd.MarkFlagRequired("config")
	_ = rootCmd.MarkFlagRequired("mapping")

	// Запускаем CLI приложение
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
