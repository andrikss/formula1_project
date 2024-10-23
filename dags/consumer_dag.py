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
from transform_insert_data import transform_insert_drivers, transform_insert_constructors, transform_insert_circuits, transform_insert_races, transform_insert_locations

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

        # consumers for each topic
        drivers_consumer = KafkaConsumer(
            'drivers_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000, 
            max_poll_interval_ms=600000
        )

        constructors_consumer = KafkaConsumer(
            'constructors_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000, 
            max_poll_interval_ms=100000
        )

        circuits_consumer = KafkaConsumer(
            'circuit_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000, 
            max_poll_interval_ms=100000
        )

        races_consumer = KafkaConsumer(
            'race_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000, 
            max_poll_interval_ms=100000
        )

        locations_consumer = KafkaConsumer(
            'location_topic',  # Dodaj consumer za location_topic
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000, 
            max_poll_interval_ms=100000
        )

        logging.info("Kafka Consumers started.")

        drivers_messages = []
        constructors_messages = []
        circuits_messages = []
        races_messages = []
        locations_messages = []

        for message in drivers_consumer:
            logging.info(f"Consumed driver message: {message.value}")
            print(f"Consumed driver message: {message.value}") 
            drivers_messages.append(message.value)

        for message in constructors_consumer:
            logging.info(f"Consumed constructor message: {message.value}")
            print(f"Consumed constructor message: {message.value}") 
            constructors_messages.append(message.value)

        for message in circuits_consumer:
            logging.info(f"Consumed circuit message: {message.value}")
            print(f"Consumed circuit message: {message.value}") 
            circuits_messages.append(message.value)

        for message in races_consumer:
            logging.info(f"Consumed race message: {message.value}")
            print(f"Consumed race message: {message.value}") 
            races_messages.append(message.value)

        for message in locations_consumer:
            logging.info(f"Consumed location message: {message.value}")
            print(f"Consumed location message: {message.value}") 
            locations_messages.append(message.value)

        # Convert messages to DataFrame
        drivers_df = pd.DataFrame(drivers_messages)
        constructors_df = pd.DataFrame(constructors_messages)
        circuits_df = pd.DataFrame(circuits_messages)
        races_df = pd.DataFrame(races_messages)
        locations_df = pd.DataFrame(locations_messages)  


        # Replace NaN with None
        drivers_df = drivers_df.where(pd.notna(drivers_df), None)
        constructors_df = constructors_df.where(pd.notna(constructors_df), None)
        circuits_df = circuits_df.where(pd.notna(circuits_df), None)
        races_df = races_df.where(pd.notna(races_df), None)
        locations_df = locations_df.where(pd.notna(locations_df), None)

        # close the consumers
        drivers_consumer.close()
        constructors_consumer.close()
        circuits_consumer.close()
        races_consumer.close()
        locations_consumer.close()
        
        logging.info("Kafka Consumers closed.")

        # returning the data as dict
        return {
            "drivers_df": drivers_df.to_dict(orient='records'),
            "constructors_df": constructors_df.to_dict(orient='records'),
            "circuits_df": circuits_df.to_dict(orient='records'),
            "races_df": races_df.to_dict(orient='records'),
            "locations_df": locations_df.to_dict(orient='records')
        }

    @task
    def load_data_to_db(data):
        logging.info("Establishing connection to the database...")
        
        drivers_data = data["drivers_df"]
        constructors_data = data["constructors_df"]
        circuits_data = data["circuits_df"]  
        races_data = data['races_df']
        locations_data = data['locations_df']

        with psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'race_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'andrea1'),
            host=os.getenv('DB_HOST', 'postgres'),
            port=os.getenv('DB_PORT', '5432')
        ) as conn:
            cursor = conn.cursor()

            # Transform and insert drivers data
            transform_insert_drivers(drivers_data, cursor)
            logging.info("Driver data loaded to the database.")

            # Transform and insert constructors data
            transform_insert_constructors(constructors_data, cursor)
            logging.info("Constructor data loaded to the database.")

            # Transform and insert circuit data
            transform_insert_circuits(circuits_data, cursor)
            logging.info("Circuit data loaded to the database.")

            # Transform and insert races data
            transform_insert_races(races_data, cursor)  
            logging.info("Race data loaded to the database.")

            # Transform and insert locations data
            transform_insert_locations(locations_data, cursor)  
            logging.info("Location data loaded to the database.")

            # Commit the changes
            conn.commit()
            cursor.close()

        logging.info("All data loaded to the database.")


    # Define tasks in DAG
    consume_task = consume_kafka_messages()
    load_task = load_data_to_db(consume_task)

    # Set task dependencies
    consume_task >> load_task 
