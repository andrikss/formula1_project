from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from kafka import KafkaConsumer
import json
import logging
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 21),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def listen_to_kafka():
    logging.info("Initializing Kafka consumer...")
    consumer = None

    try:
        # consumer that acts like listener
        consumer = KafkaConsumer(
            'trigger_topic',  
            bootstrap_servers='kafka-broker-1:9092',  
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  
            enable_auto_commit=True,
            group_id='airflow-listener-group',
            consumer_timeout_ms=30000  
        )

        logging.info("Listening for messages on 'trigger_topic'...")

        # Listen for messages
        for message in consumer:
            logging.info(f"Received message: {message.value}")
            if message.value.get('trigger_scraping') == True:
                logging.info("Trigger condition met. Returning True to trigger the DAG.")
                return True

        logging.info("No relevant message received. Skipping DAG run.")
        return False  

    except Exception as e:
        logging.error(f"An error occurred while listening to Kafka: {e}")
        return False 
    finally:
        if consumer:
            try:
                consumer.close()
                logging.info("Kafka Consumer closed successfully.")
            except Exception as e:
                logging.error(f"Error while closing Kafka Consumer: {e}")


with DAG(
    'listener_dag',
    default_args=default_args,
    schedule_interval='* * * * *',  
    catchup=False,
    max_active_runs=1,
) as dag:

    # task to listen
    listen_for_messages = PythonOperator(
        task_id='listen_for_kafka_message',
        python_callable=listen_to_kafka,
        retries=2
    )

    # task to trigger consumption dag
    trigger_consumer_dag = TriggerDagRunOperator(
        task_id='trigger_consumer_dag',
        trigger_dag_id='consumer_dag',  
        wait_for_completion=False,
        conf={"trigger": "kafka_message_received"},  
        trigger_rule="all_success"  
    )

    listen_for_messages >> trigger_consumer_dag
