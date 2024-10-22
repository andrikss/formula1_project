from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from kafka import KafkaConsumer
import json
import logging
from datetime import timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 21),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def listen_to_kafka_and_check():
    logging.info("Initializing Kafka consumer for trigger_topic...")
    consumer = None

    try:
        # listen on trigger topic
        consumer = KafkaConsumer(
            'trigger_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='airflow-listener-group',
            consumer_timeout_ms=30000
        )

        logging.info("Listening for trigger message on 'trigger_topic'...")

        for message in consumer:
            logging.info(f"Received message: {message.value}")

            # if its true then we have some data in producer
            if message.value.get('trigger_scraping'):
                logging.info("Trigger scraping detected. Switching to 'drivers_topic'...")
                consumer.close()  

                # go to drivers topic to look for duplicates
                return listen_to_drivers_topic()

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


def listen_to_drivers_topic():
    logging.info("Initializing Kafka consumer for drivers_topic...")
    consumer = None

    try:
        consumer = KafkaConsumer(
            'drivers_topic', 
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='airflow-listener-group',
            consumer_timeout_ms=30000
        )

        logging.info("Listening for driver data on 'drivers_topic'...")

        for message in consumer:
            new_data = message.value.get('driverId')
            if new_data:
                previous_data = Variable.get("previous_driver_data", default_var=None)

                if previous_data is not None and previous_data == str(new_data):
                    logging.info(f"Data has not changed: {new_data}. Skipping DAG trigger.")
                    return False  

                logging.info(f"New data detected: {new_data}. Triggering DAG.")
                Variable.set("previous_driver_data", str(new_data))
                return True  

        logging.info("No new driver data found. Skipping DAG run.")
        return False 

    except Exception as e:
        logging.error(f"An error occurred while consuming driver data: {e}")
        return False

    finally:
        if consumer:
            try:
                consumer.close()
                logging.info("Kafka Consumer for 'drivers_topic' closed successfully.")
            except Exception as e:
                logging.error(f"Error while closing Kafka Consumer: {e}")

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

    listen_for_messages_and_check >> decide >> [trigger_consumer_dag, skip_trigger]
