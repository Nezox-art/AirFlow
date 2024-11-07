from airflow.plugins_manager import AirflowPlugin
from plugins.export_yametrica.yametrica_hook import YandexMetricaHook

class AirflowYametricaPlugin(AirflowPlugin):
    name = "yametrica_plugin"  
    operators = []
    sensors = []
    hooks = [YandexMetricaHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []
