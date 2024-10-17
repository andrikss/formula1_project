from airflow import DAG
from airflow.operators.bash import BashOperator  
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 14),
    'retries': 0
}

dag = DAG(
    'data_scraping_dag',
    default_args=default_args,
    description='DAG for scraping driver data',
    schedule_interval='@daily',
)

fetch_and_insert_task = BashOperator(
    task_id='fetch_and_insert_drivers',
    bash_command='python3 /opt/airflow/scripts/scraping_data.py',  
    dag = dag
)


fetch_and_insert_task
