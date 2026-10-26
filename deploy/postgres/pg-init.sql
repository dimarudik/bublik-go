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

CREATE TABLE IF NOT EXISTS bublik_outbox (
    chunk_id BIGINT PRIMARY KEY,
    processed_at TIMESTAMP
);

INSERT INTO users_src (id, name, email, updated_at)
SELECT
    n AS id,
    'User_' || n AS name,
    'user' || n || '@integration.test' AS email,
    NOW() AS updated_at
FROM generate_series(1, 100000) AS n;

ANALYZE users_src;
