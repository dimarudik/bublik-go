package main

import (
	"bublik-go/core"
	"bublik-go/postgres"
	"context"
	"fmt"
	"log"
	"os"

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

			// Извлекаем доменную модель таблицы чанков из YAML-конфига (кастомную или дефолтную) [1.12]
			chunkTable := props.GetChunkTable()
			log.Printf("[CLI] Служебная таблица метаданных чанков: %s", chunkTable.FullName())

			// 4. Инициализируем компоненты движка
			// Передаем chunkTable вторым аргументом в конструктор пакета postgres! [1.12]
			source := postgres.NewPostgresSource(pool, chunkTable)
			target := postgres.NewPostgresTarget(pool)
			engine := core.NewEngine(source, target)

			//totalRows := int64(0)
			//sessionStart := time.Now()

			// Внутри main.go в методе RunE:
			log.Println("Старт двухфазного движка миграции Bublik-Go...")

			// 5. Запускаем двухфазный движок миграции для всего массива таблиц скопом
			log.Println("[CLI] Старт двухфазного движка миграции Bublik-Go...")
			result, err := engine.Run(ctx, configs, chunkSize, props.ThreadCount, source)
			if err != nil {
				return fmt.Errorf("критическая ошибка движка миграции: %w", err)
			}

			// ТЕПЕРЬ МЫ ИСПОЛЬЗУЕМ ПЕРЕМЕННУЮ result, И КОМПИЛЯТОР БУДЕТ ДОВОЛЕН!
			fmt.Printf("\n====================================\n")
			fmt.Printf("МИГРАЦИЯ УСПЕШНО ВЫПОЛНЕНА ДВИЖКОМ\n")
			fmt.Printf("Всего перенесено строк во всех таблицах: %d\n", result.TotalRows)
			fmt.Printf("Общее время работы сессии: %v\n", result.Duration)
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
