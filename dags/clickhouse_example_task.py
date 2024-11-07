from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

default_args = {
'owner': 'rfathutdinov',
'catchup': False,
'description': """
Пример DAG с подключением к Clickhouse
"""
}


with DAG(dag_id='ch_test',
schedule=None,
start_date=days_ago(0),
tags=['clickhouse', 'example'],
default_args=default_args
) as dag:

clickhouse_example_task = ClickHouseOperator(
task_id='clickhouse_example_task',
sql='SELECT version();',
clickhouse_conn_id='clickhouse_datago',
)

clickhouse_example_task
