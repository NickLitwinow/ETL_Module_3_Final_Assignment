# ETL Module 3 Final Assignment

В рамках проекта выполнены следующие задачи:

- **Генерация данных**  
  Созданы тестовые данные в нереляционной базе данных MongoDB для следующих сущностей:
  - **UserSessions** – сессии пользователей
  - **ProductPriceHistory** – история изменения цен
  - **EventLogs** – логи событий
  - **SupportTickets** – обращения в поддержку
  - **UserRecommendations** – рекомендации пользователям
  - **ModerationQueue** – очередь модерации отзывов
  - **SearchQueries** – поисковые запросы

- **Репликация данных**  
  Настроен ETL-процесс с использованием Apache Airflow для репликации данных из MongoDB в PostgreSQL. Процесс включает следующие этапы:
  - **Extract** – извлечение данных из MongoDB
  - **Transform** – очистка, устранение дублей, приведение форматов дат
  - **Load** – загрузка обработанных данных в PostgreSQL

- **Построение аналитических витрин**  
  На основе данных в PostgreSQL созданы аналитические витрины:
  - **User Activity Mart** – агрегированная витрина активности пользователей
  - **Support Tickets Mart** – витрина обращений в поддержку

---

1. Сборка и запуск контейнеров:
Для сборки и запуска всех сервисов используйте команду:
`docker-compose up -d --build`
Контейнеры будут запущены:
  PostgreSQL – база данных для загрузки данных и аналитических витрин.
  MongoDB – база данных для генерации исходных данных.
  Redis – брокер сообщений для Airflow.
  Airflow-сервисы (init, scheduler, webserver, worker, flower) – для выполнения ETL-процессов и создания витрин.

2.	Генерация тестовых данных:
После запуска контейнеров выполните скрипт генерации данных:
`python generate_data.py`

При успешном выполнении скрипта будет:
```
UserSessions inserted.
ProductPriceHistory inserted.
EventLogs inserted.
SupportTickets inserted.
UserRecommendations inserted.
ModerationQueue inserted.
SearchQueries inserted.

Process finished with exit code 0
```

Скриншоты примеров работы:

<img width="627" alt="Screenshot 2025-03-16 at 20 51 20" src="https://github.com/user-attachments/assets/bed4d504-e9a9-4463-9892-f146871f8e4b" />

<img width="1440" alt="Screenshot 2025-03-16 at 20 48 21" src="https://github.com/user-attachments/assets/6361f094-4c83-49d7-8813-52c684c4bb2b" />

<img width="1440" alt="Screenshot 2025-03-16 at 20 49 11" src="https://github.com/user-attachments/assets/40413880-656f-424f-b6af-81b766770f97" />

<img width="1440" alt="Screenshot 2025-03-16 at 20 48 39" src="https://github.com/user-attachments/assets/44d43b7c-1fed-4158-9411-9ca986524839" />

<img width="1440" alt="Screenshot 2025-03-16 at 20 49 29" src="https://github.com/user-attachments/assets/0733b3f9-33ea-4b3b-8ec7-fb17c5fd312a" />

<img width="1440" alt="Screenshot 2025-03-18 at 14 21 25" src="https://github.com/user-attachments/assets/15a4aa2d-a1a7-46a7-8df0-a503a34dc72c" />

<img width="1148" alt="Screenshot 2025-03-16 at 20 50 44" src="https://github.com/user-attachments/assets/b22fcb9c-c57b-46e2-be22-5be71fe4fa9f" />

