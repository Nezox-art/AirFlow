from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from clickhouse_driver import Client
import requests
import logging

# Подключение к ClickHouse
def get_clickhouse_client(host='89.169.131.57', port=8123, user='admin', password='@dmin5545'):
    return Client(host=host, port=port, user=user, password=password, database='default')

# Функция для создания таблицы и вставки данных
def create_ods_registration_amoCRM():
    client = get_clickhouse_client()

    select_agents_sql = '''
    SELECT account.id as id, a.created_at as created_at, account.updated_at as updated_at, 
           account.last_action_date as last_action_date, d.name AS role_class, a.company_id as company_id
    FROM dombook_pgsql_acl.Agent AS a
    INNER JOIN dombook_pgsql_acl.Account AS account ON a.id = account.id
    INNER JOIN dombook_pgsql_main.Dictionary AS d ON d.id = account.service_environment_id
    WHERE a.deleted_at IS NULL AND account.is_enabled = 1
    '''

    # Получение данных агентов
    agents = pd.DataFrame(client.execute(select_agents_sql, settings={"max_block_size": 10000}), 
                          columns=['id', 'created_at', 'updated_at', 'last_action_date', 'role_class', 'company_id'])

    agents['created_at'] = pd.to_datetime(agents['created_at']).dt.date.astype(str)

    # Форматирование дат
    agents = agents.apply(lambda col: col.dt.strftime('%Y-%m-%d') if col.dtype == 'datetime64[ns]' else col)

    select_companies_sql = '''
    SELECT field_1542147 AS company_id, name AS company_name, field_1541623 AS region
    FROM dombook_mysql_samolet_new.ap_mirror_samoletplus1_companies 
    WHERE field_1541629 LIKE '%Партнер%'
    '''

    # Получение данных компаний
    companies = pd.DataFrame(client.execute(select_companies_sql, settings={"max_block_size": 10000}), 
                             columns=['company_id', 'company_name', 'region'])

    companies = companies.dropna().drop_duplicates()
    agents = agents.merge(companies, how='left', on='company_id')

    # Создание таблицы в ClickHouse
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS ods.dombook_crm_registration_agents (
        id UUID,
        created_at Nullable(Date),
        updated_at Nullable(DateTime),
        last_action_date Nullable(DateTime),
        role_class Nullable(String),
        company_id Nullable(UUID),
        company_name Nullable(String),
        region Nullable(String)
    ) ENGINE = MergeTree
    ORDER BY id
    '''

    client.execute(create_table_sql)

    # Вставка данных
    data_json = agents.to_dict(orient='records')
    client.execute('INSERT INTO ods.dombook_crm_registration_agents VALUES', data_json)

# Функция уведомления об ошибках в Telegram
def failure_callback(context):
    bot_token = 'YOUR_TELEGRAM_BOT_TOKEN'
    chat_id = 'YOUR_CHAT_ID'
    message = f"Task failed: {context['task_instance_key_str']}"
    try:
        requests.post(f'https://api.telegram.org/bot{bot_token}/sendMessage', data={'chat_id': chat_id, 'text': message})
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {str(e)}")

# Определение DAG
with DAG(
    'dombook_ods_registration_agent',
    default_args={'retries': 1},
    description='Insert into ods.dombook_crm_registration_agents',
    schedule_interval="15 07 * * *",
    start_date=datetime(2024, 10, 24),
    catchup=False,
    tags=['amoCRM', 'ods', 'dombook'],
    max_active_runs=1,
    on_failure_callback=failure_callback,
) as dag:

    create_ods_registration_agent = PythonOperator(
        task_id='create_ods_registration_agent',
        python_callable=create_ods_registration_amoCRM,
    )

    create_ods_registration_agent
