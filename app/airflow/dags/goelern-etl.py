import logging
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/")

from utils.utils_mongo.operation_mongo import create_connection, find_data

sys.path.insert(0, "/opt/airflow/")
from etl.extraction.scrap_vocabulary import ScrapVocabulary
from etl.transformation.process_vocabulary import ProcessVocabulary

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
    "goelern_etl_dags",
    default_args=default_args,
    description="A DAG that runs every day at midnight to scrap vocabulary data",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Parameters
mongo_hook = create_connection(os.getenv("MONGO_CONN_ID")) 
select_parameter = PythonOperator(
    task_id="select_parameter",
    python_callable=find_data,
    op_kwargs={
        "mongo_hook":mongo_hook,
        "db_name": os.environ["MONGO_DB_DEV"], 
        "collection_name": "parameters", 
        "query_filter":{"status": "WAITING"},
        "only_one": True,
        "projection":{"_id": 0,"language": 1, "scrap_url": 1}
    },
    
    
    dag=dag,
)

# Data extraction
def extract_vocabulary(mongo_hook, **kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids="select_parameter")
    logging.info(f"Data: {data}")
    
    if data:
        lang = data.get("language")
        scrap_url = data.get("scrap_url")
        logging.info(f"Language: {lang}, Scrap URL: {scrap_url}")
        
        scraping_obj.run(mongo_hook=mongo_hook, lang=lang, scrap_url=scrap_url)
    else:
        raise ValueError("Data `lang` et `scrap_url` are missing")

scraping_obj = ScrapVocabulary()
extraction_task = PythonOperator(
    task_id="extract_vocabulary",
    python_callable=extract_vocabulary,
    op_kwargs={"mongo_hook": mongo_hook},
    dag=dag,
)

# Data transformation
process_obj = ProcessVocabulary()
process_data = PythonOperator(
    task_id="process_data",
    python_callable=process_obj.run,
    op_kwargs={"mongo_hook": mongo_hook,},
    dag=dag,
)


select_parameter >> extraction_task >> process_data