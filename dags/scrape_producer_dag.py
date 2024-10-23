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

from fetch_data import fetch_driver_data, fetch_constructor_data, fetch_circuit_data, fetch_race_data, fetch_location_data


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

        # fetch drivers data 
        drivers = fetch_driver_data()  
        logging.info(f'Driver data fetched: {drivers}')

        if drivers:
            drivers_df = pd.DataFrame(drivers) 
            send_dataframe_to_kafka(drivers_df, 'drivers_topic', producer)
        else:
            logging.warning('No driver data fetched.')

        # fetch constructors data
        constructors = fetch_constructor_data()  
        logging.info(f'Constructor data fetched: {constructors}')

        if constructors:
            constructors_df = pd.DataFrame(constructors) 
            send_dataframe_to_kafka(constructors_df, 'constructors_topic', producer)
        else:
            logging.warning('No constructor data fetched.')

        # fetch circuit data 
        circuits = fetch_circuit_data()  
        logging.info(f'Circuit data fetched: {circuits}')

        if circuits:
            circuits_df = pd.DataFrame(circuits) 
            send_dataframe_to_kafka(circuits_df, 'circuit_topic', producer)  
        else:
            logging.warning('No circuit data fetched.')

        # fetch race data 
        races = fetch_race_data()  
        logging.info(f'Race data fetched: {races}')

        if races:
            races_df = pd.DataFrame(races)
            send_dataframe_to_kafka(races_df, 'race_topic', producer) 
        else:
            logging.warning('No race data fetched.')

        # fetch location data
        locations = fetch_location_data()
        logging.info(f'Location data fetched: {locations}')

        if locations:
            locations_df = pd.DataFrame(locations)
            send_dataframe_to_kafka(locations_df, 'location_topic', producer)  
        else:
            logging.warning('No location data fetched.')

        # telling listener there is new messages
        trigger_message = {"trigger_scraping": True}
        producer.send('trigger_topic', trigger_message)
        logging.info(f"Sent trigger message to 'trigger_topic': {trigger_message}")

    except Exception as e:
        logging.error(f'Error occurred: {e}')

    finally:
        if producer:
            producer.close() 


def send_dataframe_to_kafka(dataframe, topic_name, producer):
    logging.info(f'Sending data to Kafka topic: {topic_name}')
    print(f'Sending data to Kafka topic: {topic_name}')  
    
    for _, row in dataframe.iterrows():  
        producer.send(topic_name, row.to_dict())
        logging.info(f"Sent data to Kafka topic {topic_name}: {row.to_dict()}")
        print(f"Sent data to Kafka topic {topic_name}: {row.to_dict()}")  


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
