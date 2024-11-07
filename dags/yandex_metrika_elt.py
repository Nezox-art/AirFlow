from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
import pandas as pd
import requests
import logging
import json
from urllib.parse import urlencode
import io

log_mapping = {
    'hits': {
        'fields': 'ym:pv:watchID, ym:pv:counterID, ym:pv:date, ym:pv:title, ym:pv:URL',
        'create_query': '''
            CREATE TABLE IF NOT EXISTS {table_name} (
                watch_id UInt64,
                counter_id UInt32,
                date Date,
                title String,
                url String
            ) ENGINE = MergeTree()
            PARTITION BY date
            ORDER BY date
        '''
    }
}

def get_clickhouse_client():
    return Client(host='89.169.131.57', port=8123, user='admin', password='@dmin5545', database='default')

def create_log_request(yandex_metrika_token, yandex_metrika_counter_id, start_date, end_date, source):
    selected_fields = log_mapping[source]['fields']
    url_params = urlencode({'date1': start_date, 'date2': end_date, 'source': source, 'fields': selected_fields})
    url = f'https://api-metrika.yandex.net/management/v1/counter/{yandex_metrika_counter_id}/logrequests?' + url_params
    headers = {'Authorization': f'OAuth {yandex_metrika_token}'}
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()['log_request']['request_id']

def download_data(request_id, yandex_metrika_token, yandex_metrika_counter_id):
    url_template = 'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part_number}/download'
    headers = {'Authorization': f'OAuth {yandex_metrika_token}'}
    part_number = 0
    data_frames = []
    while True:
        url = url_template.format(counter_id=yandex_metrika_counter_id, request_id=request_id, part_number=part_number)
        response = requests.get(url, headers=headers)
        if response.status_code == 204:
            break
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        data_frames.append(df)
        part_number += 1
    return pd.concat(data_frames)

def load_data_to_clickhouse(df, table_name, source):
    client = get_clickhouse_client()
    create_query = log_mapping[source]['create_query'].format(table_name=table_name)
    client.execute(create_query)
    client.execute(f"INSERT INTO {table_name} VALUES", df.to_dict('records'))

def clean_data(request_id, yandex_metrika_token, yandex_metrika_counter_id):
    url = f'https://api-metrika.yandex.net/management/v1/counter/{yandex_metrika_counter_id}/logrequest/{request_id}/clean'
    headers = {'Authorization': f'OAuth {yandex_metrika_token}'}
    requests.post(url, headers=headers).raise_for_status()

def etl_task(**kwargs):
    logging.info(f"Starting ETL task for date: {kwargs['ds']}")
    yandex_metrika_token = 'y0_AgAAAAAFuz4YAAyiOAAAAAEVm2J0AAArzYyUk_RG5b57qNKnt-oQVoxLBg'
    yandex_metrika_counter_id = '93714785'
    start_date = (datetime.strptime(kwargs['ds'], '%Y-%m-%d') - timedelta(days=6)).strftime('%Y-%m-%d')
    end_date = kwargs['ds']
    source = 'hits'
    table_name = 'metrika_hits'
    
    request_id = create_log_request(yandex_metrika_token, yandex_metrika_counter_id, start_date, end_date, source)
    df = download_data(request_id, yandex_metrika_token, yandex_metrika_counter_id)
    load_data_to_clickhouse(df, table_name, source)
    clean_data(request_id, yandex_metrika_token, yandex_metrika_counter_id)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 24),
    'depends_on_past': False,
    'catchup': False
}

with DAG(
    'yandex_metrika_to_clickhouse',
    default_args=default_args,
    description='ETL pipeline from Yandex Metrika to ClickHouse',
    schedule_interval="15 07 * * *",
    catchup=False,
    tags=['clickhouse', 'yandex', 'metrika'],
    max_active_runs=1
) as dag:
    
    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=etl_task,
        op_kwargs={},
    )
