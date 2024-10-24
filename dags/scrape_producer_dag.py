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
from fetch_data import (

    fetch_driver_standings_data,
    fetch_constructor_standings_data,
    fetch_race_results
)

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

        years_range_1 = range(2000, 2012)  # 2000 to 2011 inclusive
        years_range_2 = range(2022, 2025)  # 2022 to 2024 inclusive
        years = list(years_range_1) + list(years_range_2)

        for year in years:
            # Fetch and send driver standings
            driver_data = fetch_driver_standings_data(year)
            logging.info(f'Driver standings data fetched for year {year}: {driver_data}')
            
            if driver_data:
                producer.send('driver_standings_topic', value=driver_data)
                producer.flush()
            else:
                logging.warning(f'No driver standings data fetched for year {year}.')

            # Fetch and send constructor standings
            constructor_data = fetch_constructor_standings_data(year)
            logging.info(f'Constructor standings data fetched for year {year}: {constructor_data}')
            
            if constructor_data:
                producer.send('constructor_standings_topic', value=constructor_data)
                producer.flush()
            else:
                logging.warning(f'No constructor standings data fetched for year {year}.')

            # Fetch and send race results
            race_results = fetch_race_results(year)
            logging.info(f'Race results data fetched for year {year}: {race_results}')
            
            if race_results:
                race_results_df = pd.DataFrame(race_results['races'])
                send_dataframe_to_kafka(race_results_df, 'race_results_topic', producer)
            else:
                logging.warning(f'No race results data fetched for year {year}.')

        # Send trigger message
        trigger_message = {"trigger_scraping": True}
        producer.send('trigger_topic', value=trigger_message)
        producer.flush()
        logging.info(f"Sent trigger message to 'trigger_topic': {trigger_message}")


    except Exception as e:
        logging.error(f'Error occurred: {e}')

    finally:
        if producer:
            producer.close() 

def send_dataframe_to_kafka(df, topic, producer, batch_size=10):
    batch = []
    for _, row in df.iterrows():
        batch.append(row.to_dict())
        if len(batch) == batch_size:
            producer.send(topic, value=batch)
            producer.flush()
            batch = []
    if batch:
        producer.send(topic, value=batch)
        producer.flush()


with DAG(
    'scraping_and_producer_dag',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_and_send_task = PythonOperator(
        task_id='fetch_and_send',
        python_callable=main
    )

    fetch_and_send_task
