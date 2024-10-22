from airflow import DAG
from airflow.decorators import task
from pendulum import datetime
from kafka import KafkaConsumer
import json
import logging
import psycopg2
import os
from datetime import timedelta
import sys
sys.path.append('/opt/airflow/scripts')
from transform_insert_data import transform_insert_drivers

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
    def consume_from_kafka():
        logging.info("Starting Kafka Consumer...")
        print("Starting Kafka Consumer...")  

        consumer = KafkaConsumer(
            'drivers_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000  
        )

        logging.info("Kafka Consumer started.")
        print("Kafka Consumer started.")  

        messages_consumed = 0
        max_messages = 100  

        messages = []

        try:
            for message in consumer:
                logging.info(f"Consumed message: {message.value}")
                print(f"Consumed message: {message.value}") 
                messages.append(message.value)  

                messages_consumed += 1
                if messages_consumed >= max_messages:
                    logging.info("Reached maximum number of messages to consume.")
                    print("Reached maximum number of messages to consume.")  
                    break
            else:
                logging.info("No more messages to consume.")
                print("No more messages to consume.") 

        except Exception as e:
            logging.error(f"An error occurred in Kafka consumer: {e}")
            print(f"An error occurred in Kafka consumer: {e}")  

        finally:
            consumer.close()
            logging.info("Kafka Consumer closed.")
            print("Kafka Consumer closed.") 

        # establish a connection to the PostgreSQL database
        with psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'race_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'andrea1'),
            host=os.getenv('DB_HOST', 'postgres'),
            port=os.getenv('DB_PORT', '5432')
        ) as conn:
            cursor = conn.cursor()

            # combined transformation and insertion function
            transform_insert_drivers(messages, cursor)

            # commit the changes
            conn.commit()
            cursor.close()

        logging.info("Kafka consumption, transformation, and insertion completed.")

   
    consume_task = consume_from_kafka()
