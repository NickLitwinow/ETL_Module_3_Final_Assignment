from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


# Определение стандартных аргументов для DAG
# - owner: владелец DAG (обычно имя пользователя или роль)
# - start_date: дата начала выполнения DAG; все запуски будут начинаться с этой даты
# - depends_on_past: если True, каждый запуск будет ожидать успешного выполнения предыдущего
# - retry_delay: задержка между попытками в случае ошибки выполнения задачи
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5)
}


# Создание DAG (Directed Acyclic Graph) с именем 'analytical_marts'
# schedule_interval='@daily' указывает, что DAG будет запускаться ежедневно
# catchup=False означает, что пропущенные запуски не будут выполняться при перезапуске DAG
dag = DAG('analytical_marts', default_args=default_args, schedule_interval='@daily', catchup=False)

# ---------------------------------------------------------------------
# Оператор для создания аналитической витрины активности пользователей
# ---------------------------------------------------------------------
# Данная витрина (user_activity_mart) агрегирует данные по сессиям пользователей.
# SQL-запрос выполняет следующие действия:
# 1. Удаляет таблицу user_activity_mart, если она существует (DROP TABLE IF EXISTS),
#    чтобы избежать конфликтов при повторном запуске DAG.
# 2. Создаёт новую таблицу user_activity_mart, в которой агрегируются данные из таблицы user_sessions.
#    Для каждого пользователя (user_id) вычисляется:
#    - Количество сессий (session_count),
#    - Время начала первой сессии (first_session),
#    - Время окончания последней сессии (last_session).
create_user_activity_mart = PostgresOperator(
    task_id='create_user_activity_mart',
    postgres_conn_id='postgres_default',
    sql="""
    DROP TABLE IF EXISTS user_activity_mart;
    CREATE TABLE user_activity_mart AS
    SELECT 
        user_id,
        COUNT(session_id) AS session_count,
        MIN(start_time) AS first_session,
        MAX(end_time) AS last_session
    FROM user_sessions
    GROUP BY user_id;
    """,
    dag=dag
)

# ---------------------------------------------------------------------
# Оператор для создания аналитической витрины эффективности работы поддержки
# ---------------------------------------------------------------------
# Данная витрина (support_tickets_mart) агрегирует данные по обращениям в поддержку.
# SQL-запрос выполняет следующие действия:
# 1. Удаляет таблицу support_tickets_mart, если она существует.
# 2. Создаёт новую таблицу support_tickets_mart, в которой агрегируются данные из таблицы support_tickets.
#    Для каждого пользователя (user_id) вычисляются:
#    - Общее количество тикетов (ticket_count),
#    - Количество закрытых тикетов (closed_tickets),
#    - Количество тикетов, не закрытых (open_tickets), с использованием конструкции CASE.
create_support_tickets_mart = PostgresOperator(
    task_id='create_support_tickets_mart',
    postgres_conn_id='postgres_default',
    sql="""
    DROP TABLE IF EXISTS support_tickets_mart;
    CREATE TABLE support_tickets_mart AS
    SELECT 
        user_id,
        COUNT(ticket_id) AS ticket_count,
        SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS closed_tickets,
        SUM(CASE WHEN status <> 'closed' THEN 1 ELSE 0 END) AS open_tickets
    FROM support_tickets
    GROUP BY user_id;
    """,
    dag=dag
)

# ---------------------------------------------------------------------
# Определение последовательности выполнения задач (зависимости)
# ---------------------------------------------------------------------
# Здесь мы указываем, что задача создания витрины активности (create_user_activity_mart)
# должна выполниться до задачи создания витрины поддержки (create_support_tickets_mart).
create_user_activity_mart >> create_support_tickets_mart