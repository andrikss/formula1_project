from airflow import DAG
from pendulum import datetime
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
import json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 21),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def consume_messages():
    """Funkcija za konzumiranje poruka iz Kafka topica"""
    consumer = KafkaConsumer(
        'trigger_topic',
        bootstrap_servers='kafka-broker-1:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000  # 10 sekundi timeout
    )
    
    for message in consumer:
        message_content = message.value
        print(f"Primljena poruka: {message_content}")
        # Ako se poruka odnosi na pokretanje scrapping-a, vrati True
        if message_content.get('trigger_scraping') == True:
            return True

    return False

with DAG(
    'listener_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
) as dag:

    listen_for_trigger = PythonOperator(
        task_id='listen_for_trigger',
        python_callable=consume_messages
    )

    trigger_scraping = TriggerDagRunOperator(
        task_id='trigger_scraping_dag',
        trigger_dag_id='scraping_and_producer_dag',  # Ime DAG-a za scraping
        wait_for_completion=False,
        conf={"trigger": "scraping_triggered"},
    )

    listen_for_trigger >> trigger_scraping
