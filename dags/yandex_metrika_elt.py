import os
import sys
import locale
import datetime
from datetime import date, datetime, timedelta
import dateutil.relativedelta
import pandasql as ps
import clickhouse_connect
import json
from clickhouse_driver import Client
from airflow.operators.python import PythonOperator
from airflow import DAG

def get_clickhouse_client():
    return Client(host='89.169.131.57', port=8123, user='admin', password='@dmin5545', database='default')

def create_metrika():
    # Подключение к ClickHouse
    client = get_clickhouse_client()

    # Создание таблицы ClickHouse
    create_table_sql = '''CREATE OR REPLACE TABLE test_metrika (
            id UUID,
            name Nullable(String),
            region Nullable(String)
        ) ENGINE = MergeTree
        ORDER BY id
    ''' 

    # SQL-запрос на вставку данных в ClickHouse
    insert_table_sql = '''INSERT INTO test_metrika (id, name, region) VALUES 
    ('123e4567-e89b-12d3-a456-426614174000', 'John Doe', 'North America'),
    ('987f6543-a21b-45c7-b123-321f56789abc', 'Jane Smith', 'Europe')'''

    # Выполнение операций в ClickHouse
    client.execute(create_table_sql)
    client.execute(insert_table_sql)

# Определение DAG для Airflow
with DAG(
    'test_metrika',  # название DAG
    default_args={'retries': 1},
    description='Insert into test_metrika values',
    schedule_interval="15 07 * * *",  # расписание DAG
    start_date=datetime(2024, 11, 8),
    catchup=False,
    tags=['metrika', 'test'],
    max_active_runs=1,
) as dag:

    create_metrika_task = PythonOperator(
        task_id='create_metrika_task',
        python_callable=create_metrika
    )

    create_metrika_task
