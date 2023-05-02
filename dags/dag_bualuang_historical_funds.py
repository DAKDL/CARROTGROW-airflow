from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import TaskInstance
from selenium_plugin.operators import SeleniumOperator
from typing import Dict, Any
from selenium_web_crawler.bualuang_crawler import BualuangFundsCrawler
from config import DRIVER_PATH, ROOT_URL
from airflow.utils.task_group import TaskGroup


class BualuangFetchTableDAG(DAG):
    def __init__(self, driver_path: str, root_url: str, dag_id: str, schedule_interval: str,
                 default_args: Dict[str, Any] = None) -> None:
        super().__init__(dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval, catchup=False)

        # Define tasks
        self.driver_path = driver_path
        self.root_url = root_url
        self.create_task()


    def create_task(self):
        task1 = SeleniumOperator(
            task_id="selenium_task",
            process_browser=self.task_crawl_all_funds_name,
            do_xcom_push=True,
            dag=self
        )
        task1_result = task1.execute(None)
        tg = TaskGroup(group_id='processes', dag=self)
        for idx, item in enumerate(task1_result):
            tg.add(SeleniumOperator(
                task_id=f"selenium_task_craw_table_{idx}",
                process_browser=self.task_crawl_table,
                process_args=(item, ),
                dag=self
                ))

        task1 >> tg

    def task_crawl_all_funds_name(self, driver):
        BFC = BualuangFundsCrawler(driver, "get_all_fund", self.root_url)
        fund_goto = BFC.start()
        BFC.close()
        return fund_goto

    def task_crawl_table(self, driver, fund_name):
        print(fund_name)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dag_import_timeout': timedelta(minutes=2)
}

my_dag = BualuangFetchTableDAG(driver_path=DRIVER_PATH, root_url=ROOT_URL, dag_id='crawl-historical-nav', schedule_interval='@daily',
                               default_args=default_args)
