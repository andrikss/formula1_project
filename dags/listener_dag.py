from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from kafka import KafkaConsumer, KafkaProducer
import json
import psycopg2
import logging
import sys
import os
sys.path.append('/opt/airflow/scripts')
from staging import check_drivers_staging_and_trigger
from datetime import timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 21),
    'retries': 0
}

def listen_to_kafka_and_check():
    logging.info("Initializing Kafka consumer for trigger_topic...")
    consumer = None
    conn = None  

    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'postgres'),  
            database=os.getenv('DB_NAME', 'race_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'andrea1'),
            port=os.getenv('DB_PORT', '5432')
        )

        consumer = KafkaConsumer(
            'trigger_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='airflow-listener-group',
            consumer_timeout_ms=30000,
            max_poll_interval_ms=300000
        )

        logging.info("Listening for trigger message on 'trigger_topic'...")

        for message in consumer:
            logging.info(f"Received message: {message.value}")

            if message.value.get('trigger_scraping'):
                logging.info("Trigger scraping detected. Returning TRUE")
                consumer.close()  
                return True

        logging.info("No relevant trigger message received. Skipping DAG run.")
        return False  

    except Exception as e:
        logging.error(f"An error occurred while listening to Kafka: {e}")
        return False

    finally:
        if consumer:
            try:
                consumer.close()
                logging.info("Kafka Consumer for 'trigger_topic' closed successfully.")
            except Exception as e:
                logging.error(f"Error while closing Kafka Consumer: {e}")
        if conn:
            conn.close()

def set_trigger_to_false():
    logging.info("Setting trigger_scraping to False in trigger_topic...")

    try:
        # producer
        producer = KafkaProducer(
            bootstrap_servers='kafka-broker-1:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # false - its sent
        trigger_message = {"trigger_scraping": False}
        producer.send('trigger_topic', trigger_message)
        producer.close()

        logging.info("Sent trigger_scraping=False message to 'trigger_topic'.")

    except Exception as e:
        logging.error(f"An error occurred while setting trigger to False: {e}")

def decide_trigger(**context):
    if context['ti'].xcom_pull(task_ids='listen_for_kafka_and_check'):
        return 'trigger_consumer_dag'
    else:
        return 'skip_trigger'

with DAG(
    'listener_dag',
    default_args=default_args,
    schedule_interval='* * * * *',  
    catchup=False,
    max_active_runs=1,
) as dag:

    listen_for_messages_and_check = PythonOperator(
        task_id='listen_for_kafka_and_check',
        python_callable=listen_to_kafka_and_check,
        retries=2
    )

    skip_trigger = DummyOperator(
        task_id='skip_trigger'
    )

    decide = BranchPythonOperator(
        task_id='decide_trigger',
        python_callable=decide_trigger,
        provide_context=True
    )

    trigger_consumer_dag = TriggerDagRunOperator(
        task_id='trigger_consumer_dag',
        trigger_dag_id='consumer_dag',
        wait_for_completion=False,
        conf={"trigger": "kafka_message_received"},
        trigger_rule="all_success"
    )

    set_trigger_false_task = PythonOperator(
        task_id='set_trigger_to_false',
        python_callable=set_trigger_to_false
    )

    # Task dependencies
    listen_for_messages_and_check >> decide >> [trigger_consumer_dag, skip_trigger]
    trigger_consumer_dag >> set_trigger_false_task
