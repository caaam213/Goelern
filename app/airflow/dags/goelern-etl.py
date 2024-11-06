import os
import sys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator



sys.path.insert(0, "/opt/")
from utils.utils_mongo.operation_mongo import create_connection

sys.path.insert(0, "/opt/airflow/")
from etl.extraction.scrap_vocabulary import ScrapVocabulary

default_args = {
    "owner": "airflow",
    "depends_on_past": False,  # Define if it depends on past DAG runs
    "start_date": datetime(2024, 10, 29, 0, 0, 0),  # Run everyday
    "email_on_failure": False,  # Do not send email if something went wrong
    "email_on_retry": False,  # Do not send email on retry
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # Delay between retries
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    "goelern_etl_dags",
    default_args=default_args,
    description="A DAG which runs every day at midnight to scrap Vocabulary Data",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Data extraction

# Parameters
scraping_obj = ScrapVocabulary()
mongo_hook = create_connection(os.environ["MONGO_CONN_ID"])
scrap_url = "https://fichesvocabulaire.com/liste-vocabulaire-allemand-le-temps"
lang = "de"


extraction_task = PythonOperator(
    task_id="extract_vocabulary",
    python_callable=scraping_obj.run,
    op_kwargs={
        "mongo_hook": mongo_hook,
        "lang":lang,
        "scrap_url": scrap_url
    },
    dag=dag,
)

extraction_task