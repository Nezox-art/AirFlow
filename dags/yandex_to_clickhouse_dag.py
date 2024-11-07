# /Airflow/dag/yandex_to_clickhouse_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from plugins.export_yametrica.yandex_metrika_etl import YandexMetrikaToClickHouse

# Параметры
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'yandex_metrika_to_clickhouse',
    default_args=default_args,
    description='Load data from Yandex Metrika to ClickHouse',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

def load_data():
    metrika_client = YandexMetrikaToClickHouse(
        clickhouse_host='89.169.131.57',
        clickhouse_port=8123,
        clickhouse_user='admin',
        clickhouse_password='@dmin5545',
        yandex_oauth_token='y0_AgAAAAAFuz4YAAyiOAAAAAEVm2J0AAArzYyUk_RG5b57qNKnt-oQVoxLBg',
        yandex_counter_id=93714785,
    )
    metrika_client.run_etl()

load_task = PythonOperator(
    task_id='load_yandex_metrika_data',
    python_callable=load_data,
    dag=dag,
)
