from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import requests
import json
import datetime

# Параметры для Яндекс Метрики
YANDEX_METRIKA_API_URL = "https://api-metrika.yandex.net/stat/v1/data"
TOKEN = 'y0_AgAAAAAFuz4YAAyiOAAAAAEVm2J0AAArzYyUk_RG5b57qNKnt-oQVoxLBg' #ваш_токен_яндекс_метрики
COUNTER_ID = '93714785' #ваш_id_счётчика

# Параметры ClickHouse
CLICKHOUSE_HOST = '10.6.68.16:8123' #ваш_host_clickhouse
CLICKHOUSE_USER = 'EAndrosenko' #ваш_пользователь
CLICKHOUSE_PASSWORD = 'P6lsL06wYc' #ваш_пароль
CLICKHOUSE_DATABASE = 'default' #ваша_база_данных

# Функция для получения данных из Яндекс Метрики
def fetch_yandex_metrika_data(ds, **kwargs):
    headers = {
        'Authorization': f'OAuth {TOKEN}',
    }
    params = {
        'ids': COUNTER_ID,
        'metrics': 'ym:s:visits,ym:s:pageviews,ym:s:users',
        'date1': ds,
        'date2': ds,
        'dimensions': 'ym:s:date,ym:s:deviceCategory',
        'limit': 100000
    }
    response = requests.get(YANDEX_METRIKA_API_URL, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return data['data']

# Функция для вставки данных в ClickHouse
def insert_data_to_clickhouse(ds, **kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_yandex_metrika_data')
    client = Client(host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database=CLICKHOUSE_DATABASE)
    rows = [
        (item['dimensions'][0]['name'], item['dimensions'][1]['name'], item['metrics'][0], item['metrics'][1], item['metrics'][2])
        for item in data
    ]
    client.execute('''
        INSERT INTO your_table (date, device, visits, pageviews, users) VALUES
    ''', rows)

# DAG для выгрузки данных
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    'yandex_metrika_to_clickhouse',
    default_args=default_args,
    description='DAG для выгрузки данных из Яндекс Метрики в ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_yandex_metrika_data',
        python_callable=fetch_yandex_metrika_data,
        provide_context=True
    )

    insert_data = PythonOperator(
        task_id='insert_data_to_clickhouse',
        python_callable=insert_data_to_clickhouse,
        provide_context=True
    )

    fetch_data >> insert_data
