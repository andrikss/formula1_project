from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
from airflow.models import Variable
from kafka import KafkaProducer
import pandas as pd
import json
import logging
import sys
sys.path.append('/opt/airflow/scripts')
from fetch_data import (

    fetch_driver_standings_data,
    fetch_constructor_standings_data,
    fetch_race_results,
    fetch_latest_race_date,
    fetch_driver_standings_data_by_round,
    fetch_constructor_standings_data_by_round
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 21),
    'retries': 3
}

def check_for_new_data(**context):
    last_scraped_race_date = Variable.get("last_scraped_race_date", default_var=None)

    # full scrape
    if last_scraped_race_date is None:
        context['ti'].xcom_push(key='scrape_all_data', value=True)
        context['ti'].xcom_push(key='skip_scraping', value=False)
    else:
        latest_race_date = fetch_latest_race_date()

        # incremental scrape
        if latest_race_date > last_scraped_race_date:
            context['ti'].xcom_push(key='scrape_all_data', value=False)
            context['ti'].xcom_push(key='last_scraped_race_date', value=last_scraped_race_date)
            context['ti'].xcom_push(key='latest_race_date', value=latest_race_date)
            context['ti'].xcom_push(key='skip_scraping', value=False)
        else:
            # skip
            context['ti'].xcom_push(key='scrape_all_data', value=False)
            context['ti'].xcom_push(key='skip_scraping', value=True)

def filter_races_by_date(data, last_scraped_race_date):
    races = data.get("races", [])

    last_date = datetime.strptime(last_scraped_race_date, "%Y-%m-%d")
    filtered_races = []

    for race in races:
        race_date_str = race.get("date", "")
        try:
            race_date = datetime.strptime(race_date_str, "%Y-%m-%d")
            if race_date > last_date:
                filtered_races.append(race)
            else:
                logging.info(f"Race '{race['raceName']}' skipped due to date {race_date_str}.")
        except ValueError:
            logging.warning(f"Invalid date format '{race_date_str}' in race: {race}")

    result = {
        "races": filtered_races
    }
    
    return result


def scrape_all_data(producer):
    # Function to scrape all data across all years
    years_range_1 = range(2000, 2012)  
    years_range_2 = range(2022, 2025) 
    years = list(years_range_1) + list(years_range_2)

    latest_fetched_race_date = None

    for year in years:
        driver_data = fetch_driver_standings_data(year)
        logging.info(f'Driver standings data fetched for year {year}: {driver_data}')
       
        if driver_data:
            producer.send('driver_standings_topic', value=driver_data)
            producer.flush()

        constructor_data = fetch_constructor_standings_data(year)
        logging.info(f'Constructor standings data fetched for year {year}: {constructor_data}')
       
        if constructor_data:
            producer.send('constructor_standings_topic', value=constructor_data)
            producer.flush()

        race_results = fetch_race_results(year)
        logging.info(f'Race results data fetched for year {year}: {race_results}')
       
        if race_results:
            race_results_df = pd.DataFrame(race_results['races'])
            send_dataframe_to_kafka(race_results_df, 'race_results_topic', producer)
            # Update latest fetched race date
            latest_fetched_race_date = max([race['date'] for race in race_results['races']])

    # Set the Airflow variable to the latest race date from full scrape
    if latest_fetched_race_date:
        Variable.set("last_scraped_race_date", latest_fetched_race_date)

    # tell listener to trigger consumer 
    trigger_message = {"trigger_scraping": True}
    producer.send('trigger_topic', value=trigger_message)
    producer.flush()
    logging.info(f"Sent trigger message to 'trigger_topic': {trigger_message}")

def scrape_new_data(producer, last_scraped_race_date, latest_race_date, **context):
    # Function to scrape data incrementally by year, but filter out older records
    last_scraped_year = int(last_scraped_race_date[:4])
    latest_year = int(latest_race_date[:4])

    latest_fetched_race_date = None

    for year in range(last_scraped_year, latest_year + 1):
        logging.info(f'Scraping data for year {year}')

        # filtering fetched results
        race_results = fetch_race_results(year)
        if year == last_scraped_year:
            filtered_race_results = filter_races_by_date(race_results, last_scraped_race_date)
        else:
            filtered_race_results = race_results
       
        if not filtered_race_results.get("races"):
            # data on the api is not yet updated
            context['ti'].xcom_push(key='skip_scraping', value=True)
            return  # exit the function immediately 

        race_results_df = pd.DataFrame(filtered_race_results['races'])
        send_dataframe_to_kafka(race_results_df, 'race_results_topic', producer)
        # update last fetched race for variable
        latest_fetched_race_date = max([race['date'] for race in race_results['races']])

        for race in filtered_race_results['races']:
            round_number = race['round']

             # fetch driver standings by round
            driver_data = fetch_driver_standings_data_by_round(year, round_number)
            if driver_data:
                print(f"Fetched driver standings for year {year}, round {round_number}: {driver_data}")
                producer.send('driver_standings_topic', value=driver_data)
                producer.flush()

            # fetch constructor standings by round
            constructor_data = fetch_constructor_standings_data_by_round(year, round_number)
            if constructor_data:
                print(f"Fetched constructor standings for year {year}, round {round_number}: {constructor_data}")
                producer.send('constructor_standings_topic', value=constructor_data)
                producer.flush()

    # set the Airflow variable to the latest race date from incremental scrape
    if latest_fetched_race_date:
        Variable.set("last_scraped_race_date", latest_fetched_race_date)

    # tell listener to trigger consumer 
    trigger_message = {"trigger_scraping": True}
    producer.send('trigger_topic', value=trigger_message)
    producer.flush()
    logging.info(f"Sent trigger message to 'trigger_topic': {trigger_message}")


def main(**context):
    scrape_all_data_flag = context['ti'].xcom_pull(key='scrape_all_data')
    skip_scraping = context['ti'].xcom_pull(key='skip_scraping')
    print(f"scrape_all_data_flag: {scrape_all_data_flag}, type: {type(scrape_all_data_flag)}")
    print(f"skip_scraping: {skip_scraping}, type: {type(skip_scraping)}")

    # if its true - skip
    if skip_scraping:
        logging.info("No new data to scrape. Skipping scraping and production.")
        return

    producer = None

    try:
        bootstrap_servers = 'kafka-broker-1:9092'
        logging.info(f'Connecting to Kafka at {bootstrap_servers}')

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        if scrape_all_data_flag:
            logging.info("Scraping all data as this is the first run.")
            scrape_all_data(producer)
        else:
            # last and latest race needed for incremental scraping
            last_scraped_race_date = context['ti'].xcom_pull(key='last_scraped_race_date')
            latest_race_date = context['ti'].xcom_pull(key='latest_race_date')

            logging.info(f"Scraping new data from {last_scraped_race_date} to {latest_race_date}")
            scrape_new_data(producer, last_scraped_race_date, latest_race_date, **context)

            skip_scraping = context['ti'].xcom_pull(key='skip_scraping')
            if skip_scraping:
                logging.info("There is new data, but still not uploaded to the main API.")
                return

    except Exception as e:
        print(f'Type of e: {type(e)}') 
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

    check_new_data_task = PythonOperator(
        task_id='check_for_new_data',
        python_callable=check_for_new_data,
        provide_context=True
    )

    fetch_and_send_task = PythonOperator(
            task_id='fetch_and_send',
            python_callable=main,
            provide_context=True
        )

    check_new_data_task >> fetch_and_send_task