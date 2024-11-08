from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime, timedelta

# Функция для записи данных в ClickHouse
def insert_into_clickhouse():
    # Подключение к ClickHouse
    client = Client(host='89.169.131.57', port=8123, user='admin', password='@dmin5545', database='default')

    # Данные для вставки
    data = [
        (1, 'Alice', 'Region1'),
        (2, 'Bob', 'Region2'),
        (3, 'Charlie', 'Region3')
    ]

    # Вставка данных в таблицу NewTable
    client.execute('INSERT INTO NewTable (id, name, region) VALUES', data)
    print("Данные успешно записаны в таблицу NewTable")

# Определение DAG
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'insert_clickhouse_dag',
    default_args=default_args,
    description='DAG для вставки данных в таблицу ClickHouse',
    schedule_interval=None,  # Запуск вручную
    start_date=datetime(2024, 11, 8),
    catchup=False,
) as dag:

    insert_data_task = PythonOperator(
        task_id='insert_data_clickhouse',
        python_callable=insert_into_clickhouse
    )

    insert_data_task
