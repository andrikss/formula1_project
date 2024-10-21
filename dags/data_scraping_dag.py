from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
from kafka import KafkaProducer
import pandas as pd
import json
import logging
import sys
sys.path.append('/opt/airflow/scripts')

from scraping_data import fetch_driver_data, transform_driver_data


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 21),
    'retries': 3
}

def main():
    producer = None

    try:
        bootstrap_servers = 'kafka-broker-1:9092'
        logging.info(f'Connecting to Kafka at {bootstrap_servers}')

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        drivers = fetch_driver_data()  
        if drivers:
            drivers_df = transform_driver_data(drivers)
            send_dataframe_to_kafka(drivers_df, 'drivers_topic', producer)

        else:       
            logging.warning('No driver data fetched.')

    except Exception as e:
        logging.error(f'Error occurred: {e}')

    finally:
        if producer:
            producer.close() 

def send_dataframe_to_kafka(dataframe, topic_name, producer):
    for _, row in dataframe.iterrows():
        producer.send(topic_name, row.to_dict())
        logging.info(f"Sent data to Kafka topic {topic_name}: {row.to_dict()}")

with DAG(
    'scraping_and_producer_dag',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_and_send_task = PythonOperator(
        task_id='fetch_and_send_drivers',
        python_callable=main
    )

    fetch_and_send_task
