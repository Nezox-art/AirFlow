from airflow.plugins_manager import AirflowPlugin
from clickoperator.clickhouse_operator import ClickhouseExecuteOperator
from clickoperator.clickhouse_hook import ClickhouseHook


class AirflowClickhousePlugin(AirflowPlugin):
    name = "clickhouse_plugin"  # does not need to match the package name
    operators = [ClickhouseExecuteOperator]
    sensors = []
    hooks = [ClickhouseHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
