from airflow.plugins_manager import AirflowPlugin
from selenium_plugin.operators import SeleniumOperator

class SeleniumPlugin(AirflowPlugin):
    name = "selenium_plugin"
    operators = [SeleniumOperator]
