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
from transform_insert_data import transform_insert_drivers, transform_insert_constructors, transform_insert_circuits, transform_insert_races, transform_insert_locations, transform_insert_driver_standings, get_race_info, transfrom_insert_constructor_standings, transform_insert_race_results

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

        driver_standings_consumer = KafkaConsumer(
            'driver_standings_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000, 
            max_poll_interval_ms=100000
        )

        constructor_standings_consumer = KafkaConsumer(
            'constructor_standings_topic',  
            bootstrap_servers='kafka-broker-1:9092',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airflow-consumer-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000, 
            max_poll_interval_ms=100000
        )

        race_results_consumer = KafkaConsumer(
            'race_results_topic',  
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
        driver_standings_messages = []
        constructor_standings_messages = []
        race_results_messages = []

        for message in drivers_consumer:
            logging.info(f"Consumed driver message: {message.value}")
            drivers_messages.append(message.value)

        for message in constructors_consumer:
            logging.info(f"Consumed constructor message: {message.value}")
            constructors_messages.append(message.value)

        for message in circuits_consumer:
            logging.info(f"Consumed circuit message: {message.value}")
            circuits_messages.append(message.value)

        for message in races_consumer:
            logging.info(f"Consumed race message: {message.value}")
            races_messages.append(message.value)

        for message in locations_consumer:
            logging.info(f"Consumed location message: {message.value}")
            locations_messages.append(message.value)

        for message in driver_standings_consumer:
            logging.info(f"Consumed driver standings message: {message.value}")
            driver_standings_messages.append(message.value)

        for message in constructor_standings_consumer:
            logging.info(f"Consumed constructor standings message: {message.value}") 
            constructor_standings_messages.append(message.value)

        for message in race_results_consumer:
            logging.info(f"Consumed race results message: {message.value}")
            race_results_messages.append(message.value)

        # Convert messages to DataFrame
        drivers_df = pd.DataFrame(drivers_messages)
        constructors_df = pd.DataFrame(constructors_messages)
        circuits_df = pd.DataFrame(circuits_messages)
        races_df = pd.DataFrame(races_messages)
        locations_df = pd.DataFrame(locations_messages)  
        driver_standings_df = pd.DataFrame(driver_standings_messages)
        constructor_standings_df = pd.DataFrame(constructor_standings_messages)
        race_results_df = pd.DataFrame(race_results_messages)


        # Replace NaN with None
        drivers_df = drivers_df.where(pd.notna(drivers_df), None)
        constructors_df = constructors_df.where(pd.notna(constructors_df), None)
        circuits_df = circuits_df.where(pd.notna(circuits_df), None)
        races_df = races_df.where(pd.notna(races_df), None)
        locations_df = locations_df.where(pd.notna(locations_df), None)
        driver_standings_df = driver_standings_df.where(pd.notna(driver_standings_df), None)
        constructor_standings_df = constructor_standings_df.where(pd.notna(constructor_standings_df), None)
        race_results_df = race_results_df.where(pd.notna(race_results_df), None)

        # close the consumers
        drivers_consumer.close()
        constructors_consumer.close()
        circuits_consumer.close()
        races_consumer.close()
        locations_consumer.close()
        driver_standings_consumer.close()
        race_results_consumer.close()

        logging.info("Kafka Consumers closed.")

        # returning the data as dict
        return {
            "drivers_df": drivers_df.to_dict(orient='records'),
            "constructors_df": constructors_df.to_dict(orient='records'),
            "circuits_df": circuits_df.to_dict(orient='records'),
            "races_df": races_df.to_dict(orient='records'),
            "locations_df": locations_df.to_dict(orient='records'),
            "driver_standings_df": driver_standings_df.to_dict(orient='records'),
            "constructor_standings_df": constructor_standings_df.to_dict(orient='records'),
            "race_results_df": race_results_df.to_dict(orient='records')
        }

    @task
    def load_data_to_db(data):
        logging.info("Establishing connection to the database...")
        
        drivers_data = data["drivers_df"]
        constructors_data = data["constructors_df"]
        circuits_data = data["circuits_df"]  
        races_data = data['races_df']
        locations_data = data['locations_df']
        driver_standings_data = data['driver_standings_df']
        constructor_standings_data = data['constructor_standings_df']
        race_results_data = data["race_results_df"]

        # for now its stays like this 
        if driver_standings_data:
            season = driver_standings_data[0].get('season')
            round = driver_standings_data[0].get('round')

            ds_race = get_race_info(season, round)
            logging.info(f"Fetched race info for season {season}, round {round}: {ds_race}")
        else:
            logging.warning("No driver standings data found.")
            ds_race = None

        # for constructor standings
        if constructor_standings_data:
            season = constructor_standings_data[0].get('season')
            round = constructor_standings_data[0].get('round')

            cs_race = get_race_info(season, round)
            logging.info(f"Fetched race info for season {season}, round {round}: {cs_race}")
        else:
            logging.warning("No constructor standings data found.")
            cs_race = None

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

            # Transform and insert driver standings data
            transform_insert_driver_standings(driver_standings_data, cursor, ds_race)  
            logging.info("Driver standings data loaded to the database.")

            # Transform and insert constructor standings data
            transfrom_insert_constructor_standings(constructor_standings_data, cursor, cs_race)  
            logging.info("Constructor standings data loaded to the database.")

            # Transform and insert race results data
            if race_results_data:
                for race in race_results_data:
                    logging.info(f"Processing race results for season {race['season']}, round {race['round']}")
                    transform_insert_race_results(race['results'], cursor, race)
            else:
                logging.warning("No race results found.")


            # Commit the changes
            conn.commit()
            cursor.close()

        logging.info("All data loaded to the database.")


    # Define tasks in DAG
    consume_task = consume_kafka_messages()
    load_task = load_data_to_db(consume_task)

    # Set task dependencies
    consume_task >> load_task 
