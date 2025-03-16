from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pymongo
import psycopg2
from datetime import datetime

# Определение стандартных аргументов для DAG.
# Эти аргументы применяются ко всем задачам внутри DAG.
# - owner: владелец DAG (обычно имя пользователя или роль)
# - start_date: дата начала выполнения DAG; все запуски будут начинаться с этой даты
# - depends_on_past: если True, каждый запуск будет ожидать успешного выполнения предыдущего
# - retries: кол-во попыток после неудачного рана
# - retry_delay: задержка между попытками в случае ошибки выполнения задачи
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание объекта DAG с именем 'etl_replication'.
# schedule_interval='@hourly' задаёт запуск DAG каждый час,
# catchup=False означает, что пропущенные запуски не будут выполнены.
dag = DAG('etl_replication', default_args=default_args, schedule_interval='@hourly', catchup=False)

# Функция convert_objectid:
# Рекурсивно преобразует объекты MongoDB ObjectId в строки,
# а также все datetime объекты в ISO-форматированные строки.
def convert_objectid(doc):
    if '_id' in doc:
        doc['_id'] = str(doc['_id'])
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
        elif isinstance(value, list):
            new_list = []
            for item in value:
                if isinstance(item, datetime):
                    new_list.append(item.isoformat())
                elif isinstance(item, dict):
                    new_list.append(convert_objectid(item))
                else:
                    new_list.append(item)
            doc[key] = new_list
        elif isinstance(value, dict):
            doc[key] = convert_objectid(value)
    return doc

# Функция extract_data:
# Извлекает данные из MongoDB, преобразует документы (ObjectId и datetime),
# и сохраняет результат в XCom для последующей обработки.
def extract_data(**kwargs):
    """Извлекаем данные из MongoDB"""
    client = pymongo.MongoClient("mongodb://mongo:27017/")
    db = client["etl_db"]
    data = {
        "user_sessions": [convert_objectid(doc) for doc in db.user_sessions.find()],
        "product_price_history": [convert_objectid(doc) for doc in db.product_price_history.find()],
        "event_logs": [convert_objectid(doc) for doc in db.event_logs.find()],
        "support_tickets": [convert_objectid(doc) for doc in db.support_tickets.find()],
        "user_recommendations": [convert_objectid(doc) for doc in db.user_recommendations.find()],
        "moderation_queue": [convert_objectid(doc) for doc in db.moderation_queue.find()],
        "search_queries": [convert_objectid(doc) for doc in db.search_queries.find()]
    }
    kwargs['ti'].xcom_push(key='raw_data', value=data)
    return data

# Функция transform_data:
# Выполняет преобразование данных, полученных из MongoDB.
# Пример: удаление дубликатов сессий по session_id и преобразование дат в строковый формат.
def transform_data(**kwargs):
    """Выполняем очистку и трансформацию данных"""
    data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract_task')
    unique_sessions = {session['session_id']: session for session in data.get('user_sessions', [])}
    data['user_sessions'] = list(unique_sessions.values())

    # Преобразуем datetime объекты в строковый формат (ISO), если они ещё не являются строками.
    for session in data['user_sessions']:
        if session.get('start_time') and not isinstance(session['start_time'], str):
            session['start_time'] = session['start_time'].isoformat()
        if session.get('end_time') and not isinstance(session['end_time'], str):
            session['end_time'] = session['end_time'].isoformat()
    kwargs['ti'].xcom_push(key='transformed_data', value=data)
    return data


# Функция load_data:
# Загружает преобразованные данные в PostgreSQL.
# Здесь демонстрируется загрузка данных в таблицу user_sessions.
def load_data(**kwargs):
    """Загружаем данные в PostgreSQL"""
    data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_task')
    conn = psycopg2.connect("dbname=etl_db user=postgres password=postgres host=postgres")
    cur = conn.cursor()

    # Пример загрузки данных в таблицу user_sessions.
    # Для каждой сессии выполняем SQL-запрос INSERT.
    # Конструкция ON CONFLICT (session_id) DO NOTHING предотвращает дублирование записей.
    for session in data.get('user_sessions', []):
        cur.execute("""
            INSERT INTO user_sessions (session_id, user_id, start_time, end_time, pages_visited, device, actions)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING;
        """, (
            session['session_id'],
            session['user_id'],
            session['start_time'],
            session['end_time'],
            session['pages_visited'],
            session['device'],
            session['actions']
        ))
    conn.commit()
    cur.close()
    conn.close()

# Определение задач (tasks) с использованием PythonOperator:

# Задача extract_task:
# Выполняет функцию extract_data для извлечения данных из MongoDB.
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

# Задача transform_task:
# Выполняет функцию transform_data для очистки и преобразования извлечённых данных.
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

# Задача load_task:
# Выполняет функцию load_data для загрузки данных в PostgreSQL.
load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# Определение порядка выполнения задач:
# Сначала выполняется extract_task, затем transform_task, после чего load_task.
extract_task >> transform_task >> load_task