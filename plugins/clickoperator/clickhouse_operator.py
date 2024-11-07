from airflow.models.baseoperator import BaseOperator
from plugins.clickoperator.clickhouse_hook import ClickhouseHook


class ClickhouseExecuteOperator(BaseOperator):


    def __init__(self, conn_id, sql, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.query = sql

    def execute(self, context):
        hook = ClickhouseHook(self.conn_id)
        return hook.execute_action(self.query)
