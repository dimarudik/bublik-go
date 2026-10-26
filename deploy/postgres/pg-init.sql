-- Создаем таблицы источника и приемника
CREATE TABLE IF NOT EXISTS users_src (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(50),
    updated_at TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS users_trg (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(50),
    updated_at TIMESTAMP
    );

-- Создаем таблицу метаданных Exactly-Once аутбокса
CREATE TABLE IF NOT EXISTS bublik_outbox (
    chunk_id BIGINT PRIMARY KEY,
    processed_at TIMESTAMP
);

-- МГНОВЕННАЯ ГЕНЕРАЦИЯ 100 000 СТРОК (Вместо медленного цикла for)
INSERT INTO users_src (id, name, email, updated_at)
SELECT
    n AS id,
    'User_' || n AS name,
    'user' || n || '@integration.test' AS email,
    NOW() AS updated_at
FROM generate_series(1, 100000) AS n;

-- Обновляем статистику системного каталога для корректной нарезки ctid чанков движком
ANALYZE users_src;
