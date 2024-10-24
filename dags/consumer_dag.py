from airflow import DAG
from airflow.decorators import task
from pendulum import datetime
from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import psycopg2
import os
import pandas as pd
from datetime import timedelta
import sys
sys.path.append('/opt/airflow/scripts')
from transform_insert_data import  transform_insert_driver_standings, transform_insert_constructor_standings, transform_insert_race_results

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'retries': 0,
}

with DAG(
    'consumer_dag',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def consume_kafka_messages():
        logging.info("Starting Kafka Consumers...")
    
        # Set up individual Kafka consumers for each topic
        driver_consumer = KafkaConsumer(
            'driver_standings_topic',
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-driver-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=60000, 
            max_poll_interval_ms=100000
        )

        constructor_consumer = KafkaConsumer(
            'constructor_standings_topic',
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-constructor-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=60000, 
            max_poll_interval_ms=100000
        )

        race_consumer = KafkaConsumer(
            'race_results_topic',
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-race-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=60000, 
            max_poll_interval_ms=100000
        )
        driver_standings_messages = []
        constructor_standings_messages = []
        race_results_messages = []
        batch_size = 10  # Adjust batch size as needed

        for driver_message in driver_consumer:
            logging.info(f"Consumed message from driver_standings_topic: {driver_message.value}")
            driver_standings_messages.extend(driver_message.value)
            if len(driver_standings_messages) >= batch_size:
                break

        for constructor_message in constructor_consumer:
            logging.info(f"Consumed message from constructor_standings_topic: {constructor_message.value}")
            constructor_standings_messages.extend(constructor_message.value)
            if len(constructor_standings_messages) >= batch_size:
                break

        for race_message in race_consumer:
            logging.info(f"Consumed message from race_results_topic: {race_message.value}")
            race_results_messages.extend(race_message.value)
            if len(race_results_messages) >= batch_size:
                break

        driver_consumer.close()
        constructor_consumer.close()
        race_consumer.close()
        logging.info("Kafka Consumer closed.")

        # Return the data as dictionaries to pass between tasks
        return {
            "driver_standings_data": driver_standings_messages,
            "constructor_standings_data": constructor_standings_messages,
            "race_results_data": race_results_messages
        }

    @task
    def load_data_to_db(data):
        logging.info("Establishing connection to the database...")

        driver_standings_data = data['driver_standings_data']
        constructor_standings_data = data['constructor_standings_data']
        race_results_data = data["race_results_data"]

        with psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'race_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'andrea1'),
            host=os.getenv('DB_HOST', 'postgres'),
            port=os.getenv('DB_PORT', '5432')
        ) as conn:
            cursor = conn.cursor()

            if driver_standings_data:
                logging.info(f"Processing driver_standings_data with {len(driver_standings_data)} records")
                for standing in driver_standings_data:
                    logging.info(f"Processing standing: {standing}")
                    transform_insert_driver_standings(standing, cursor)
                logging.info("Driver standings data loaded to the database.")
            else:
                logging.warning("No driver standings data found.")
                
            if constructor_standings_data:
                for standing in constructor_standings_data:
                    transform_insert_constructor_standings(standing, cursor)
                logging.info("Constructor standings data loaded to the database.")
            else:
                logging.warning("No constructor standings data found.")

            for race in race_results_data:
              transform_insert_race_results(race, cursor)
            


            # Commit the changes
            conn.commit()
            cursor.close()

        logging.info("All data loaded to the database.")


    # Define tasks in DAG
    consume_task = consume_kafka_messages()
    load_task = load_data_to_db(consume_task)

    # Set task dependencies
    consume_task >> load_task 
