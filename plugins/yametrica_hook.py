import requests
from airflow.hooks.base import BaseHook

class YandexMetricaHook(BaseHook):

    def __init__(self, yandex_metrika_token, yandex_metrika_counter_id, start_date, end_date, source, table_name, type_query='insert', clickhouse_connection='new_click'):
        self.yandex_metrika_token = yandex_metrika_token
        self.yandex_metrika_counter_id = yandex_metrika_counter_id
        self.start_date = start_date
        self.end_date = end_date
        self.source = source
        self.table_name = table_name
        self.type_query = type_query 
        self.clickhouse_connection = clickhouse_connection
    

    def load_data_to_clickhouse(self):
        """Запрос данных из Яндекс Метрики и загрузка их в ClickHouse"""
        
        # Логика передачи данных в ClickHouse (предполагается, что используем download_metrica_to_clickhouse)
        download_metrica_to_clickhouse(
            yandex_metrika_token = self.yandex_metrika_token,
            yandex_metrika_counter_id = self.yandex_metrika_counter_id,
            start_date = self.start_date,
            end_date = self.end_date,
            source = self.source,
            table_name = self.table_name,  
            type_query = self.type_query,
            clickhouse_connection = self.clickhouse_connection
        )


    
