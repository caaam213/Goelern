import os
import sys


sys.path.insert(0, "/opt/airflow/")
from etl.extraction.crawling_voc_list_urls import CrawlingVocListUrls

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


sys.path.insert(0, "/opt/")
from utils.utils_mongo.operation_mongo import create_connection

default_args = {
    "owner": "airflow",
    "depends_on_past": False,  
    "start_date": datetime(2024, 10, 29, 0, 0, 0),  
    "email_on_failure": False,  
    "email_on_retry": False,  
    "retries": 1,
    "retry_delay": timedelta(minutes=5), 
}

dag = DAG(
    "create_parameters_dag",
    default_args=default_args,
    description="A DAG that runs every month at midnight to create parameters for the scraper",
    schedule_interval=timedelta(days=30),
    catchup=False,
)

# Parameters
mongo_hook = create_connection(os.getenv("MONGO_CONN_ID")) 
lang = "de"

crawling_obj = CrawlingVocListUrls()
parameter_creator_task = PythonOperator(
    task_id="parameter_creator",
    python_callable=crawling_obj.run,
    op_kwargs={
        "mongo_hook": mongo_hook,
        "lang": lang,
    },
    dag=dag,
)

