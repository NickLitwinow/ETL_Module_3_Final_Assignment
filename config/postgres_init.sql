-- Создание таблицы для хранения сессий пользователей
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id VARCHAR PRIMARY KEY,
    user_id INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_visited TEXT[],
    device VARCHAR,
    actions TEXT[]
);

-- Таблица для истории изменения цен товаров
CREATE TABLE IF NOT EXISTS product_price_history (
    product_id VARCHAR PRIMARY KEY,
    price_changes JSONB,
    current_price NUMERIC,
    currency VARCHAR(3)
);

-- Таблица для логов событий
CREATE TABLE IF NOT EXISTS event_logs (
    event_id VARCHAR PRIMARY KEY,
    timestamp TIMESTAMP,
    event_type VARCHAR,
    details TEXT
);

-- Таблица для обращений в поддержку
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id VARCHAR PRIMARY KEY,
    user_id INTEGER,
    status VARCHAR,
    issue_type VARCHAR,
    messages TEXT[],
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Таблица для рекомендаций пользователям
CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id INTEGER PRIMARY KEY,
    recommended_products TEXT[],
    last_updated TIMESTAMP
);

-- Таблица для очереди модерации отзывов
CREATE TABLE IF NOT EXISTS moderation_queue (
    review_id VARCHAR PRIMARY KEY,
    user_id INTEGER,
    product_id VARCHAR,
    review_text TEXT,
    rating INTEGER,
    moderation_status VARCHAR,
    flags TEXT[],
    submitted_at TIMESTAMP
);

-- Таблица для хранения поисковых запросов
CREATE TABLE IF NOT EXISTS search_queries (
    query_id VARCHAR PRIMARY KEY,
    user_id INTEGER,
    query_text TEXT,
    timestamp TIMESTAMP,
    filters TEXT[],
    results_count INTEGER
);

-- Пример создания аналитической витрины для активности пользователей
DROP TABLE IF EXISTS user_activity_mart;
CREATE TABLE user_activity_mart AS
SELECT
    user_id,
    COUNT(session_id) AS session_count,
    MIN(start_time) AS first_session,
    MAX(end_time) AS last_session
FROM user_sessions
GROUP BY user_id;

-- Пример создания аналитической витрины для обращений в поддержку
DROP TABLE IF EXISTS support_tickets_mart;
CREATE TABLE support_tickets_mart AS
SELECT
    user_id,
    COUNT(ticket_id) AS ticket_count,
    SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS closed_tickets,
    SUM(CASE WHEN status <> 'closed' THEN 1 ELSE 0 END) AS open_tickets
FROM support_tickets
GROUP BY user_id;