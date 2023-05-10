from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import TaskInstance
from selenium_plugin.operators import SeleniumOperator
from typing import Dict, Any
from selenium_web_crawler.bualuang_crawler import BualuangFundsCrawler, BualuangTableCrawler
from config import DRIVER_PATH, ROOT_URL
from airflow.utils.task_group import TaskGroup
import math
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

class BualuangGetAllFundsDAG(DAG):
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

        save_result = PythonOperator(
            task_id='save_result',
            python_callable=BualuangGetAllFundsDAG.save_task1_result_to_file,
            provide_context=True,
            dag=self
        )

        task1 >> save_result

    def task_crawl_all_funds_name(self, driver):
        BFC = BualuangFundsCrawler(driver, "get_all_fund", self.root_url)
        fund_goto = BFC.start()
        BFC.close()
        return fund_goto

    @staticmethod
    def save_task1_result_to_file(task_instance):
        task1_result = task_instance.xcom_pull(task_ids="selenium_task")
        with open('/opt/airflow/output/task1_result.json', 'w') as f:
            json.dump(task1_result, f)


class BualuangFetchTableDAG(DAG):
    def __init__(self, driver_path: str, root_url: str, dag_id: str, schedule_interval: str,
                 default_args: Dict[str, Any] = None) -> None:
        super().__init__(dag_id=dag_id, default_args=default_args, schedule_interval=schedule_interval, catchup=False)

        # Define tasks
        self.driver_path = driver_path
        self.root_url = root_url
        self.create_task()

    def create_task(self):
        wait_for_file = FileSensor(
            task_id='wait_for_file',
            filepath='/opt/airflow/output/task1_result.json',
            mode='poke',
            timeout=60 * 60 * 6,  # Timeout after 6 hours, adjust as needed
            poke_interval=60 * 5,  # Check every 5 minutes, adjust as needed
            dag=self
        )



        with open('/opt/airflow/output/task1_result.json', 'r') as f:
            task1_result = json.load(f)
        task_count = len(task1_result)
        tasks_per_group = 10
        num_task_groups = math.ceil(task_count / tasks_per_group)

        previous_tg = None
        for tg_idx in range(num_task_groups):
            with TaskGroup(group_id=f'processes_{tg_idx}', dag=self) as tg:
                start_idx = tg_idx * tasks_per_group
                end_idx = min((tg_idx + 1) * tasks_per_group, task_count)
                for idx in range(start_idx, end_idx):
                    task = SeleniumOperator(
                        task_id=f"selenium_task_craw_table_{idx}",
                        process_browser=self.task_crawl_table,
                        process_args=(task1_result[idx],),
                        dag=self
                    )
            if previous_tg is not None:
                previous_tg >> tg
            else:
                wait_for_file >> tg

            previous_tg = tg



    def task_crawl_table(self, driver, fund_name):
        btc = BualuangTableCrawler(driver, fund_name, self.root_url)
        btc.start()
        results = btc.table.copy()

        # Lower columns name
        results.columns = results.columns.str.lower()
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        insert_cmd = """INSERT INTO historical_nav 
        (date, nav, nav_unit, selling_price, redemption_price, change, funds_name) 
        VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        values = []
        for _, row in results.iterrows():
            values.append((
                row['date'], row['nav'], row['navperunit'],
                row['sellingprice'], row['redemptionprice'], row['change'], row['fund_name']
            ))
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.executemany(insert_cmd, values)
        conn.commit()
        cur.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'timeout': timedelta(hours=1),
    'execution_timeout': timedelta(hours=2),
    'dag_import_timeout': timedelta(hours=1)
}
get_funds_dag = BualuangGetAllFundsDAG(driver_path=DRIVER_PATH, root_url=ROOT_URL, dag_id='crawl-funds-nav', schedule_interval='@daily',
                               default_args=default_args)
fetch_table_dag = BualuangFetchTableDAG(driver_path=DRIVER_PATH, root_url=ROOT_URL, dag_id='crawl-historical-nav', schedule_interval='@daily',
                               default_args=default_args)
