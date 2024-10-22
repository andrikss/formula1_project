import json
import logging
import pandas as pd
import numpy as np
import pendulum
from kafka import KafkaConsumer
import logging
import json
from airflow.decorators import dag, task
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from airflow.hooks.postgres_hook import PostgresHook

# adapter for numpy.int64
def adapt_numpy_int64(n):
    return AsIs(int(n))

register_adapter(np.int64, adapt_numpy_int64)

@dag(
    schedule='0 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

def etl_dag():

    # RESTART THE DB 
    @task()
    def restart_db():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            truncate_query = """
                TRUNCATE TABLE fact_raceresults, fact_driverstanding, fact_constructorstanding, dimensionsession, dimensionrace, dimensiondriver, 
                dimensionconstructor, dimensioncircuit, dimensionlocation, dimensionstatus  
                CASCADE;
            """
            cursor.execute(truncate_query)
            conn.commit()
            print("Tables truncated successfully!")
        except Exception as e:
            print(f"Error occurred during database reset: {e}")
        finally:
            cursor.close()
            conn.close()

    # EXTRACT
    @task()
    def extract():
        df = pd.read_csv('./data/dataEngineeringDataset.csv', low_memory=False) 
        return df
    
    # DRIVER
    @task()
    def transform_driver(df):
        df_driver = df[['driverId', 'forename', 'surname', 'dob', 'nationality']]
        df_driver_cleaned = df_driver.drop_duplicates()
        return df_driver_cleaned
    
    # CONSTRUCTOR
    @task()
    def transform_constructor(df):
        df_constr = df[['constructorId', 'name', 'nationality_constructors']]

        df_constr= df_constr.rename(columns={
                'nationality_constructors': 'nationality'
            })

        # no more duplicates
        df_constr_cleaned = df_constr.drop_duplicates()

        df_constr_cleaned['constructorId'] = df_constr_cleaned['constructorId'].apply(int)
        df_constr_cleaned['name'] = df_constr_cleaned['name'].astype(str)  # Konvertuj u string
        df_constr_cleaned['nationality'] = df_constr_cleaned['nationality'].astype(str)  # Konvertuj u string

        return df_constr_cleaned

    # CIRCUIT
    @task()
    def transform_circuit(df):
        df_circuit = df[['circuitId', 'name_y', 'lat', 'lng', 'alt']]

        df_circuit = df_circuit.rename(columns={
            'name_y': 'name',
        })

        df_circuit.replace(r'\\N', np.nan, regex=True, inplace=True)

        # no more duplicates
        df_circuit_cleaned = df_circuit.drop_duplicates()
        return df_circuit_cleaned

    # RACE 
    @task()
    def transform_race(df):
        df_race = df[['raceId', 'name_x', 'round', 'time_races', 'date']]
        df_race = df_race.rename(columns={
            'name_x': 'name',
            'time_races': 'time'
        })

        df_race_cleaned = df_race.drop_duplicates()
        return df_race_cleaned

    # LOCATION 
    @task()
    def transform_location(df):
        df_location = df[['location', 'country']]

        # cleaning duplicates and reseting index
        df_location_cleaned = df_location.drop_duplicates().reset_index(drop=True)

        # adding the first column as index of pandas df (ag id)
        df_location_cleaned.insert(0, 'locationId', df_location_cleaned.index)

        return df_location_cleaned

    # FACT DRIVER STANDING
    @task()
    def transform_driver_standing(df):
        df_driver_standing = df[['driverStandingsId', 'raceId', 'driverId', 'points_driverstandings', 'position_driverstandings', 'wins']]
        df_driver_standing = df_driver_standing.rename(columns={
            'driverStandingsId' : 'driverStandingId',
            'points_driverstandings': 'points',
            'position_driverstandings': 'position',
            })
        df_driver_standing['points'] = pd.to_numeric(df_driver_standing['points'], errors='coerce')
        df_driver_standing_cleaned = df_driver_standing.drop_duplicates()
        return df_driver_standing_cleaned

    # FACT CONSTRUCTOR STANDING
    @task()
    def transform_constructor_standing(df):
        df_constructor_standing = df[['constructorStandingsId', 'raceId', 'constructorId', 'points_constructorstandings', 'position_constructorstandings', 'wins_constructorstandings']]
        df_constructor_standing = df_constructor_standing.rename(columns={
            'constructorStandingsId' : 'constructorStandingId',
            'points_constructorstandings': 'points',
            'position_constructorstandings': 'position',
            'wins_constructorstandings': 'wins'
        })
        df_constructor_standing['points'] = pd.to_numeric(df_constructor_standing['points'], errors='coerce')
        df_constructor_standing_cleaned = df_constructor_standing.drop_duplicates()
        return df_constructor_standing_cleaned

    # STATUS 
    @task()
    def transform_status(df):
        df_status = df[['statusId', 'status']]   

        df_status_cleaned = df_status.drop_duplicates()
        return df_status_cleaned

    # FACT RACE RESULTS 
    @task()
    def transform_race_results(df):
        # making locationId column that references to locationDimension table
        df_location = df[['location']].drop_duplicates().reset_index(drop=True)
        df_location['locationId'] = df_location.index
        df = df.merge(df_location, on='location', how='left')

        # since we need to sum laps duration of race by drivers
        df_cleaned = df[['raceId', 'driverId', 'lap', 'milliseconds_laptimes']].drop_duplicates()     

        # summing and renaming the milliseconds_laptimes_x and y
        lap_times = (
            df_cleaned.groupby(['raceId', 'driverId'], as_index=False)['milliseconds_laptimes']
            .sum()
            .rename(columns={'milliseconds_laptimes': 'total_laptimes'})
        )

        # summing total pitstops duration of race by drivers, and renaming
        pitstop_times = (
        df[['raceId', 'driverId', 'milliseconds_pitstops']]
        .groupby(['raceId', 'driverId'], as_index=False)['milliseconds_pitstops']
        .sum()
        .rename(columns={'milliseconds_pitstops': 'pitStopDuration'})
        )

        # merging these two df into original df
        df = df.merge(lap_times, on=['raceId', 'driverId'], how='left')
        df = df.merge(pitstop_times, on=['raceId', 'driverId'], how='left')

        # copy of relevant columns
        df_race_results = df[['resultId', 'raceId', 'driverId', 'constructorId',
                        'circuitId', 'locationId', 'statusId', 'grid', 
                        'positionText', 'points', 'laps', 
                        'milliseconds', 'total_laptimes', 
                        'pitStopDuration', 'fastestLap', 'fastestLapTime', 
                        'fastestLapSpeed']].copy()

        # renaming
        df_race_results = df_race_results.rename(columns={
        'grid': 'startPosition',
        'positionText': 'endPosition',
        'milliseconds' : 'duration'
        })

        df_race_results.replace({'\\N': None}, inplace=True)

        # assuring that endPosition is a string bc of D N E and R possibilities 
        df_race_results['endPosition'] = df_race_results['endPosition'].astype(str)

        df_race_results['points'] = pd.to_numeric(df_race_results['points'], errors='coerce')
        df_race_results['points'].fillna(0, inplace=True)
        df_race_results['laps'] = df_race_results['laps'].astype(int)

        df_race_results.dropna(subset=['duration', 'fastestLap', 'fastestLapTime', 'fastestLapSpeed'], inplace=True)

        # Convert duration and fastestLap columns
        df_race_results['duration'] = df_race_results['duration'].astype(str).str.replace("'", "").astype(float)
        df_race_results['fastestLap'] = df_race_results['fastestLap'].astype(str).str.replace("'", "").astype(float).astype(int)

        def convert_time_to_milliseconds(time_str):
            if pd.isna(time_str):  
                return None
            minutes, seconds = time_str.split(':')
            total_seconds = int(minutes) * 60 + float(seconds)
            total_milliseconds = total_seconds * 1000 
            return total_milliseconds

        # conversion to milliseconds
        df_race_results['fastestLapTime'] = df_race_results['fastestLapTime'].apply(convert_time_to_milliseconds)

        df_race_results['fastestLapSpeed'] = df_race_results['fastestLapSpeed'].astype(str).str.replace("'", "").astype(float)

        # formula for average lap time by driver for each race 
        df_race_results['averageLapTime'] = (
            df_race_results['total_laptimes'] / df_race_results['laps']
        ).round(3)

        # dont need this column anymore
        df_race_results = df_race_results.drop(columns=['total_laptimes'])

        # rank calc f(x)
        def calculate_rank(start, end):
            try:
                return int(start) - int(end)
            except ValueError:
                return None

        df_race_results['rank'] = df_race_results.apply(
            lambda row: calculate_rank(row['startPosition'], row['endPosition']), axis=1
        )

        # deleting duplicates
        df_race_results_cleaned = df_race_results.drop_duplicates()

        # \n values 
        df_race_results_cleaned.replace({'\\N': None}, inplace=True)

        # remembering order of columns
        current_columns = df_race_results_cleaned.columns.tolist()

        current_columns.remove('rank')
        current_columns.remove('pitStopDuration')
            
        # want my rank to be after endPosition
        end_pos_index = current_columns.index('endPosition') + 1
        current_columns.insert(end_pos_index, 'rank')
            
        # the last column is pitStopDuration
        current_columns.append('pitStopDuration')
            
        # new order of columns
        df_race_results_cleaned = df_race_results_cleaned[current_columns]

        return df_race_results_cleaned
      
    # SESSION 
    @task()
    def transform_session(df):
        session_data = []

        # types of sessions and their columns
        session_types = {
            'fp1': ['fp1_date', 'fp1_time'],
            'fp2': ['fp2_date', 'fp2_time'],
            'fp3': ['fp3_date', 'fp3_time'],
            'quali': ['quali_date', 'quali_time'],
            'sprint': ['sprint_date', 'sprint_time']
        }

        # iterating and making rows
        for session_type, columns in session_types.items():
            date_col, time_col = columns
            session_df = df[['raceId', date_col, time_col]].copy()
            session_df = session_df.dropna(subset=[date_col, time_col])  

            session_df['sessionType'] = session_type
            session_df = session_df.rename(columns={date_col: 'sessionDate', time_col: 'sessionTime'})
            session_data.append(session_df)

        # concating 
        df_sessions = pd.concat(session_data, ignore_index=True)

        df_sessions_cleaned = df_sessions.drop_duplicates()
        df_sessions_cleaned.replace({'\\N': None}, inplace=True)

        # ag id
        df_sessions_cleaned = df_sessions_cleaned.reset_index(drop=True)
        df_sessions_cleaned['sessionId'] = df_sessions_cleaned.index

        # reordering
        df_sessions_cleaned = df_sessions_cleaned[['sessionId', 'raceId', 'sessionType', 'sessionDate', 'sessionTime']]

        return df_sessions_cleaned

    # LOAD ALL DATA
    @task()
    def load_data(driver_df, constructor_df, race_df, circuit_df, location_df, driver_standing_df, constructor_standing_df, status_df, session_df, race_results_df): 
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
    
        # load driver
        driver_records = driver_df.to_records(index = False)
        driver_values = list(driver_records)

        insert_driver_query = """
            INSERT INTO dimensiondriver (driverId, name, surname, dob, nationality)
            VALUES %s
            ON CONFLICT (driverId) DO NOTHING;  
        """
        execute_values(cursor, insert_driver_query, driver_values)

        # load constructor 
        constructor_records = constructor_df.to_records(index = False)
        constructor_values = list(constructor_records)

        insert_constructor_query = """
            INSERT INTO dimensionconstructor (constructorId, name, nationality)
            VALUES %s
            ON CONFLICT (constructorId) DO NOTHING;
        """
        execute_values(cursor, insert_constructor_query, constructor_values)

        # load race 
        race_records = race_df.to_records(index = False)
        race_values = list(race_records)

        insert_race_query = """
            INSERT INTO dimensionrace (raceId, name, round, time, date)
            VALUES %s
            ON CONFLICT (raceId) DO NOTHING;
        """
        execute_values(cursor, insert_race_query, race_values)

        # load circuit
        circuit_records = circuit_df.to_records(index = False)
        circuit_values = list(circuit_records)

        insert_circuit_query = """
            INSERT INTO dimensioncircuit (circuitId, name, lat, lng, alt)
            VALUES %s
            ON CONFLICT (circuitId) DO NOTHING;
        """
        execute_values(cursor, insert_circuit_query, circuit_values)

        # load location
        location_records = location_df.to_records(index = False)
        location_values = list(location_records)

        insert_location_query = """
            INSERT INTO dimensionlocation (locationId, location, country)
            VALUES %s
            ON CONFLICT (locationId) DO NOTHING;
        """
        execute_values(cursor, insert_location_query, location_values)

        # load status
        status_records = status_df.to_records(index = False)
        status_values = list(status_records)

        insert_status_query = """
            INSERT INTO dimensionstatus (statusId, status)
            VALUES %s
            ON CONFLICT (statusId) DO NOTHING;
        """
        execute_values(cursor, insert_status_query, status_values)

        # load session
        session_records = session_df.to_records(index = False)
        session_values = list(session_records)

        insert_session_query = """
            INSERT INTO dimensionsession (sessionId, raceId, sessionType, sessionDate, sessionTime)
            VALUES %s
            ON CONFLICT (sessionId) DO NOTHING;
        """
        execute_values(cursor, insert_session_query, session_values)

        # load driver standings
        driver_standing_records = driver_standing_df.to_records(index = False)
        driver_standing_values = list(driver_standing_records)

        insert_driver_standing_query = """
            INSERT INTO fact_driverstanding (driverStandingId, raceId, driverId, points, position, wins)
            VALUES %s
            ON CONFLICT (driverStandingId) DO NOTHING;
        """
        execute_values(cursor, insert_driver_standing_query, driver_standing_values)

        # load constructor standing
        constructor_standing_records = constructor_standing_df.to_records(index = False)
        constructor_standing_values = list(constructor_standing_records)

        insert_constructor_standing_query = """
            INSERT INTO fact_constructorstanding (constructorStandingId, raceId, constructorId, points, position, wins)
            VALUES %s
            ON CONFLICT (constructorStandingId) DO NOTHING;
        """
        execute_values(cursor, insert_constructor_standing_query, constructor_standing_values)

        # load race results 
        race_results_records = race_results_df.to_records(index = False)
        race_results_values = list(race_results_records)

        insert_race_results_query = """
            INSERT INTO fact_raceresults (
                    resultId, raceId, driverId, constructorId, circuitId, locationId, statusId,
                    startPosition, endPosition, rank, points, laps, duration, 
                    fastestLap, fastestLapTime, fastestLapSpeed, averageLapTime, pitStopDurationTotal
                )
                VALUES %s
                ON CONFLICT (resultId) DO NOTHING;
        """
        execute_values(cursor, insert_race_results_query, race_results_values)

        conn.commit()
        cursor.close()
        conn.close()
        print("Data loaded successfully!")

    # TASKS
    db_reset = restart_db()

    data_frame = extract()

    driver_df = transform_driver(data_frame)
    constructor_df = transform_constructor(data_frame)
    circuit_df = transform_circuit(data_frame)
    race_df = transform_race(data_frame)
    location_df = transform_location(data_frame)
    driver_standing_df = transform_driver_standing(data_frame)
    constructor_standing_df = transform_constructor_standing(data_frame)
    status_df = transform_status(data_frame)
    session_df = transform_session(data_frame)
    race_results_df = transform_race_results(data_frame)

    load_data_task = load_data(
        driver_df,
        constructor_df,
        race_df,
        circuit_df,
        location_df,
        driver_standing_df,
        constructor_standing_df,
        status_df,
        session_df,
        race_results_df,
    )

    
    # dependencies between tasks
    db_reset >> data_frame >> [
        driver_df,
        constructor_df,
        circuit_df,
        race_df,
        location_df,
        driver_standing_df,
        constructor_standing_df,
        status_df,
        session_df,
        race_results_df,
    ] >> load_data_task


dag_instance = etl_dag()