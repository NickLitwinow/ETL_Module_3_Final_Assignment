from pymongo import MongoClient
import random
from faker import Faker
import datetime

# Создаём экземпляр Faker для генерации данных.
fake = Faker()
# Устанавливаем соединение с MongoDB по адресу localhost:27017.
# В данном случае, используется база данных "etl_db".
client = MongoClient("mongodb://localhost:27017/")
db = client["etl_db"]

def generate_user_sessions(n=100):
    """
    Генерирует данные для коллекции user_sessions.

    Параметры:
        n (int): Количество сессий для генерации (по умолчанию 100).

    Для каждой сессии:
      - Генерируется уникальный идентификатор session_id.
      - Генерируется случайный идентификатор пользователя (user_id) от 1 до 50.
      - Определяются время начала сессии (start_time) и время окончания сессии (end_time),
            где end_time рассчитывается как start_time плюс случайное количество минут (от 1 до 120).
      - Генерируется список посещённых страниц (pages_visited) с использованием случайных URL.
      - Случайным образом выбирается тип устройства (desktop, mobile, tablet).
      - Генерируется список действий (actions), выбранных из набора: click, scroll, input, hover.

    После генерации данные вставляются в коллекцию user_sessions в MongoDB.
    """
    sessions = []
    for _ in range(n):
        start_time = fake.date_time_this_month()
        end_time = start_time + datetime.timedelta(minutes=random.randint(1, 120))
        session = {
            "session_id": fake.uuid4(),
            "user_id": random.randint(1, 50),
            "start_time": start_time,
            "end_time": end_time,
            "pages_visited": [fake.url() for _ in range(random.randint(1, 10))],
            "device": random.choice(["desktop", "mobile", "tablet"]),
            "actions": [random.choice(["click", "scroll", "input", "hover"]) for _ in range(random.randint(1, 5))]
        }
        sessions.append(session)
    db.user_sessions.insert_many(sessions)
    print("UserSessions inserted.")

def generate_product_price_history(n=50):
    """
    Генерирует данные для коллекции product_price_history.

    Параметры:
        n (int): Количество товаров для генерации истории цен (по умолчанию 50).

    Для каждого товара:
        - Генерируется уникальный идентификатор product_id.
        - Создаётся список изменений цен (price_changes), где для каждого изменения:
            * Генерируется дата изменения (date) в текущем году.
            * Генерируется цена (price) в диапазоне от 10 до 1000 с округлением до 2 знаков.
        - Текущая цена (current_price) устанавливается равной цене последнего изменения.
        - Случайным образом выбирается валюта (USD, EUR, RUB).

    После генерации данные вставляются в коллекцию product_price_history в MongoDB.
    """
    histories = []
    for _ in range(n):
        price_changes = []
        for _ in range(random.randint(1, 5)):
            price_changes.append({
            "date": fake.date_time_this_year(),
                "price": round(random.uniform(10, 1000), 2)
            })
        history = {
            "product_id": fake.uuid4(),
            "price_changes": price_changes,
            "current_price": price_changes[-1]["price"],
            "currency": random.choice(["USD", "EUR", "RUB"])
        }
        histories.append(history)
    db.product_price_history.insert_many(histories)
    print("ProductPriceHistory inserted.")

def generate_event_logs(n=200):
    """
    Генерирует данные для коллекции event_logs.

    Параметры:
        n (int): Количество логов событий для генерации (по умолчанию 200).

    Для каждого лог-записи:
      - Генерируется уникальный идентификатор event_id.
      - Генерируется время события (timestamp) в текущем году.
      - Выбирается тип события из заданного набора (login, logout, purchase, error).
      - Генерируется краткое текстовое описание события (details).

    После генерации данные вставляются в коллекцию event_logs.
    """
    logs = []
    for _ in range(n):
        log = {
            "event_id": fake.uuid4(),
            "timestamp": fake.date_time_this_year(),
            "event_type": random.choice(["login", "logout", "purchase", "error"]),
            "details": fake.sentence()
        }
        logs.append(log)
    db.event_logs.insert_many(logs)
    print("EventLogs inserted.")

def generate_support_tickets(n=100):
    """
    Генерирует данные для коллекции support_tickets.

    Параметры:
        n (int): Количество обращений в поддержку для генерации (по умолчанию 100).

    Для каждого тикета:
      - Генерируется уникальный идентификатор ticket_id.
      - Генерируется случайный идентификатор пользователя (user_id).
      - Выбирается статус тикета (open, closed, pending).
      - Выбирается тип проблемы (technical, billing, general).
      - Генерируется список сообщений, связанных с тикетом.
      - Генерируются времена создания (created_at) и обновления (updated_at) тикета.

    После генерации данные вставляются в коллекцию support_tickets.
    """
    tickets = []
    for _ in range(n):
        ticket = {
            "ticket_id": fake.uuid4(),
            "user_id": random.randint(1, 50),
            "status": random.choice(["open", "closed", "pending"]),
            "issue_type": random.choice(["technical", "billing", "general"]),
            "messages": [fake.sentence() for _ in range(random.randint(1, 5))],
            "created_at": fake.date_time_this_year(),
            "updated_at": fake.date_time_this_year()
        }
        tickets.append(ticket)
    db.support_tickets.insert_many(tickets)
    print("SupportTickets inserted.")

def generate_user_recommendations(n=80):
    """
    Генерирует данные для коллекции user_recommendations.

    Параметры:
        n (int): Количество рекомендаций для генерации (по умолчанию 80).

    Для каждой рекомендации:
      - Случайным образом выбирается идентификатор пользователя (user_id).
      - Генерируется список рекомендованных товаров (recommended_products) — случайное количество идентификаторов.
      - Генерируется время последнего обновления рекомендаций (last_updated).

    После генерации данные вставляются в коллекцию user_recommendations.
    """
    recommendations = []
    for _ in range(n):
        recommendation = {
            "user_id": random.randint(1, 50),
            "recommended_products": [fake.uuid4() for _ in range(random.randint(1, 5))],
            "last_updated": fake.date_time_this_year()
        }
        recommendations.append(recommendation)
    db.user_recommendations.insert_many(recommendations)
    print("UserRecommendations inserted.")

def generate_moderation_queue(n=70):
    """
    Генерирует данные для коллекции moderation_queue.

    Параметры:
        n (int): Количество отзывов для генерации (по умолчанию 70).

    Для каждого отзыва:
      - Генерируется уникальный идентификатор review_id.
      - Случайным образом выбирается идентификатор пользователя (user_id).
      - Генерируется идентификатор товара (product_id).
      - Генерируется текст отзыва (review_text) с ограничением в 200 символов.
      - Генерируется оценка товара (rating) от 1 до 5.
      - Выбирается статус модерации (pending, approved, rejected).
      - Генерируется список флагов, например, для определения спама или оскорбительного содержания.
      - Генерируется время отправки отзыва (submitted_at).

    После генерации данные вставляются в коллекцию moderation_queue.
    """
    reviews = []
    for _ in range(n):
        review = {
            "review_id": fake.uuid4(),
            "user_id": random.randint(1, 50),
            "product_id": fake.uuid4(),
            "review_text": fake.text(max_nb_chars=200),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(["pending", "approved", "rejected"]),
            "flags": [random.choice(["spam", "offensive", "none"]) for _ in range(random.randint(0, 2))],
            "submitted_at": fake.date_time_this_year()
        }
        reviews.append(review)
    db.moderation_queue.insert_many(reviews)
    print("ModerationQueue inserted.")

def generate_search_queries(n=150):
    """
    Генерирует данные для коллекции search_queries.

    Параметры:
        n (int): Количество поисковых запросов для генерации (по умолчанию 150).

    Для каждого запроса:
      - Генерируется уникальный идентификатор query_id.
      - Генерируется случайный идентификатор пользователя (user_id).
      - Генерируется текст запроса (query_text) — случайное слово.
      - Генерируется время запроса (timestamp) в текущем году.
      - Генерируется список применённых фильтров (filters) — случайное количество от 0 до 3.
      - Генерируется количество найденных результатов (results_count) в диапазоне от 0 до 100.

    После генерации данные вставляются в коллекцию search_queries.
    """
    queries = []
    for _ in range(n):
        query = {
            "query_id": fake.uuid4(),
            "user_id": random.randint(1, 50),
            "query_text": fake.word(),
            "timestamp": fake.date_time_this_year(),
            "filters": [fake.word() for _ in range(random.randint(0, 3))],
            "results_count": random.randint(0, 100)
        }
        queries.append(query)
    db.search_queries.insert_many(queries)
    print("SearchQueries inserted.")

if __name__ == "__main__":
    generate_user_sessions()
    generate_product_price_history()
    generate_event_logs()
    generate_support_tickets()
    generate_user_recommendations()
    generate_moderation_queue()
    generate_search_queries()