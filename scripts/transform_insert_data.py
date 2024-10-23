import requests
import random
import hashlib
from psycopg2 import sql
import json
import os
import pandas as pd
from datetime import datetime


# generate id from string
def generate_id_from_string(string_id, cursor, table_name, id_column):

    # sql query for checking
    check_id_query = sql.SQL(f"""
        SELECT {id_column}
        FROM {table_name}
        WHERE {id_column} = %s
    """)          

    # making unique id from string
    ascii_sum = sum(ord(char) for char in string_id)
    unique_id = abs(ascii_sum) % (10 ** 3)

    # checking if that id exists
    cursor.execute(check_id_query, (unique_id,))
    existing_row = cursor.fetchone()

    attempts = 0
    # if it does - generate new driverId
    while existing_row and attempts < 20:  
        unique_id = generate_unique_id(unique_id, attempts)
        cursor.execute(check_id_query, (unique_id,))
        existing_row = cursor.fetchone()
        attempts += 1

    if attempts >= 20:
    # try to make a new ID, that doesnt already exist, for 20 times, this is just borderline case
        print(f"Could not generate a unique  ID. Skipping.") 
        return None

    return unique_id

# generate unique id
def generate_unique_id(string_id, attempts=0):
    base_str = f"{string_id}_{attempts}_{random.randint(1, 10000)}"
    hashed_value = hashlib.sha256(base_str.encode()).hexdigest()[:3]
    return int(hashed_value, 16)

# transform and insert drivers
def transform_insert_drivers(drivers, cursor):               
                # sql queries
                check_name_query = sql.SQL("""
                    SELECT driverId, dob, nationality
                    FROM DimensionDriver
                    WHERE name = %s AND surname = %s
                """)

                insert_query = sql.SQL("""
                    INSERT INTO DimensionDriver (driverId, name, surname, dob, nationality)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (driverId) DO NOTHING;
                """)

                update_query = sql.SQL("""
                    UPDATE DimensionDriver
                    SET dob = %s, nationality = %s
                    WHERE driverId = %s;
                """)

                for driver in drivers:
                    original_driver_id = driver['driverId'] 
                    name = driver['givenName']
                    surname = driver['familyName']
                    dob = datetime.strptime(driver['dateOfBirth'], '%Y-%m-%d').date() if 'dateOfBirth' in driver else None
                    nationality = driver['nationality']

                    # checking if name and surname already exist 
                    cursor.execute(check_name_query, (name, surname))
                    name_check = cursor.fetchone()

                    if name_check:
                        existing_dob, existing_nationality = name_check[1], name_check[2]

                        # if row has the same name and surname, but different dob OR nat - update it
                        # there can be drivers that have same name and surname
                        # so - if this row has same name and surname, but different dob AND nationality - its probabaly a completely new driver.
                    
                        if (existing_dob != dob) or (existing_nationality != nationality):                            
                            if (existing_dob != dob) and (existing_nationality != nationality):
                                driver_id = generate_id_from_string(f"{name}_{surname}_{dob}_{nationality}", cursor, "dimensiondriver", "driverId")                               
                                cursor.execute(insert_query, (driver_id, name, surname, dob, nationality))
                                print(f"Inserted new driver with different DOB and nationality: {driver_id}, {name}, {surname}, {dob}, {nationality}.")
                            else:
                                cursor.execute(update_query, (dob, nationality, name_check[0]))
                                print(f"Updated driver {name} {surname} with new DOB or nationality.")
                        else:
                            print(f"Driver {name} {surname} already exists with the same details. Skipping.")
                    else:         
                        driver_id = generate_id_from_string(f"{name}_{surname}_{dob}_{nationality}", cursor, "dimensiondriver", "driverId")              
                        cursor.execute(insert_query, (driver_id, name, surname, dob, nationality))
                        print(f"Inserted new driver: {driver_id}, {name}, {surname}, {dob}, {nationality}")

                print("Database operations completed successfully.")

# transform and insert constructors                
def transform_insert_constructors(constructors, cursor):
     # SQL queries
    check_query = sql.SQL("""
            SELECT constructorId
            FROM DimensionConstructor 
            WHERE name = %s AND nationality = %s
        """)

    insert_query = sql.SQL("""
        INSERT INTO DimensionConstructor (constructorId, name, nationality)
        VALUES (%s, %s, %s) ON CONFLICT (constructorId) DO NOTHING;
    """)

    for constructor in constructors:
        name = constructor['name'] if pd.notna(constructor['name']) else None
        nationality = constructor['nationality'] if pd.notna(constructor['nationality']) else None

        cursor.execute(check_query, (name, nationality))
        name_check = cursor.fetchone()

        if name_check:
            print('Skipping duplicate constructor')
            continue
        else:
            # unique id for new constructor
            constructor_id = generate_id_from_string(f"{name}_{nationality}", cursor, "DimensionConstructor", "constructorId")
            cursor.execute(insert_query, (constructor_id, name, nationality))
            print(f"Inserted new constructor: {constructor_id}, {name}, {nationality}.")

# fetch altitude with lat and long
def fetch_altitude(lat, long):
    url = f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{long}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data and 'results' in data:
            return data['results'][0]['elevation']
    return None

# transform and insert circuits
def transform_insert_circuits(circuits, cursor):
    # SQL queries
    check_circuit_query = sql.SQL("""
        SELECT circuitId, lat, lng, alt
        FROM DimensionCircuit
        WHERE name = %s
    """)

    insert_query = sql.SQL("""
        INSERT INTO DimensionCircuit (circuitId, name, lat, lng, alt)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (circuitId) DO NOTHING;
    """)

    update_query = sql.SQL("""
        UPDATE DimensionCircuit
        SET lat = %s, lng = %s, alt = %s
        WHERE circuitId = %s;
    """)

    for circuit in circuits:
        circuit_name = circuit['circuitName']
        lat = float(circuit['Location']['lat'])
        lng = float(circuit['Location']['long'])

        # calc alt from api
        alt = fetch_altitude(lat, lng)
        if alt is None:
            print(f"Could not fetch altitude for {circuit_name}. Skipping.")
            continue

        cursor.execute(check_circuit_query, (circuit_name,))
        existing_circuit = cursor.fetchone()

        if existing_circuit:
            existing_lat, existing_lng, existing_alt = existing_circuit[1], existing_circuit[2], existing_circuit[3]

             # sum = 1 if one of columns is changed, sum > 1 if there are more
            columns_changed = sum([
                existing_lat != lat,        
                existing_lng != lng, 
                existing_alt != alt
            ])

            # update - if only one column is changed (sum = 1), if there are more - do the insertion query (sum > 1)
            if columns_changed > 1:
                circuit_id = generate_id_from_string(f"{circuit_name}_{lat}_{lng}_{alt}", cursor, "dimensioncircuit", "circuitId")
                cursor.execute(insert_query, (circuit_id, circuit_name, lat, lng, alt))
                print(f"Inserted new circuit with different lat, lng, and alt: {circuit_id}, {circuit_name}, {lat}, {lng}, {alt}.")        

            elif columns_changed == 1:
                cursor.execute(update_query, (lat, lng, alt, existing_circuit[0]))
                print(f"Updated circuit {circuit_name} with new lat, lng, or alt.")

            else:
                # so sum = 0, there are no changes - its a duplicate
                print(f"Circuit {circuit_name} already exists with the same details. Skipping.")
        else:          
            circuit_id = generate_id_from_string(f"{circuit_name}_{lat}_{lng}_{alt}", cursor, "dimensioncircuit", "circuitId")
            cursor.execute(insert_query, (circuit_id, circuit_name, lat, lng, alt))
            print(f"Inserted new circuit: {circuit_id}, {circuit_name}, {lat}, {lng}, {alt}.")

# fetching information for sessions 
def fetch_race_info_for_sessions(season, round):
    url = f"http://ergast.com/api/f1/{season}/{round}.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        race_info = data['MRData']['RaceTable']['Races'][0]

        sessions_info = {
            'raceName': race_info['raceName'],
            'round': race_info['round'],
            'date': race_info['date'],
            'time': race_info.get('time'),
            'fp1_date': race_info.get('FirstPractice', {}).get('date'),  
            'fp1_time': race_info.get('FirstPractice', {}).get('time'),
            'fp2_date': race_info.get('SecondPractice', {}).get('date'),
            'fp2_time': race_info.get('SecondPractice', {}).get('time'),
            'fp3_date': race_info.get('ThirdPractice', {}).get('date'),
            'fp3_time': race_info.get('ThirdPractice', {}).get('time'),
            'quali_date': race_info.get('Qualifying', {}).get('date'),
            'quali_time': race_info.get('Qualifying', {}).get('time'),
        }
        return sessions_info
    else:
        print("Error fetching race data from Ergast API:", response.status_code)
        return None

def transform_insert_sessions(race_info, race_id, cursor):
    sessions = [
        {'type': 'fp1', 'date': race_info['fp1_date'], 'time': race_info['fp1_time']},
        {'type': 'fp2', 'date': race_info['fp2_date'], 'time': race_info['fp2_time']},
        {'type': 'fp3', 'date': race_info['fp3_date'], 'time': race_info['fp3_time']},
        {'type': 'quali', 'date': race_info['quali_date'], 'time': race_info['quali_time']}
    ]

    for session in sessions:
        check_duplicate_query = sql.SQL("""
                SELECT sessionId FROM dimensionsession
                WHERE raceId = %s AND sessiontype = %s AND sessiondate = %s AND sessiontime = %s;
            """)
        cursor.execute(check_duplicate_query, (race_id, session['type'], session['date'], session['time']))
        existing_session = cursor.fetchone()

        if existing_session:
                        print(f"Duplicate session found: {session['type']} for race ID {race_id} on {session['date']} at {session['time']}. Skipping insert.")
                        continue 

        cursor.execute("SELECT MAX(sessionId) FROM dimensionsession;")
        max_session_id = cursor.fetchone()[0] or 0 
        session_id = max_session_id + 1

        insert_query = sql.SQL("""
            INSERT INTO dimensionsession (sessionId, raceId, sessiontype, sessiondate, sessiontime)
            VALUES (%s, %s, %s, %s, %s);
        """)

        cursor.execute(insert_query, (session_id, race_id, session['type'], session['date'], session['time']))
        print(f"Inserted session: {session['type']} for race ID {race_id} on {session['date']} at {session['time']}.")

# transform and insert race
def transform_insert_races(races, cursor):
    insert_query = sql.SQL(""" 
        INSERT INTO DimensionRace (raceId, name, round, date, time)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (raceId) DO NOTHING;
    """)

    check_duplicate_query = sql.SQL("""
        SELECT raceId
        FROM DimensionRace
        WHERE name = %s AND round = %s AND date = %s;
    """)

    for race in races:
        season = race['season']
        round_num = race['round']
        race_name = race['raceName']
        race_date = race['date']
        race_time = None  # this api got no information about time

        race_datetime = datetime.strptime(race_date, '%Y-%m-%d')  

        # if the race did not happen yet, dont insert it 
        if race_datetime > datetime.now():
            print(f"Race {race_name}, round {round_num}, date {race_date} is in the future. Skipping insert.")
            continue

        cursor.execute(check_duplicate_query, (race_name, round_num, race_date))
        existing_race = cursor.fetchone()

        if existing_race:
            print(f"Duplicate found: {race_name}, round {round_num}, date {race_date}. Skipping insert.")
        else:
            # get race id
            race_id = generate_id_from_string(f"{race_name}_{round_num}", cursor, "dimensionrace", "raceId")

            # get info about sessions
            race_info = fetch_race_info_for_sessions(season, round_num)
            
            # first make new race bc of the RIC
            cursor.execute(insert_query, (race_id, race_name, round_num, race_date, race_time))
            print(f"Inserted race: {race_id}, {race_name}, round {round_num}, date {race_date}.")

            # then insert session info for new race 
            transform_insert_sessions(race_info, race_id, cursor)

# f-on for getting last location id (bc it's autogenerated)
def get_next_location_id(cursor):
    cursor.execute("SELECT MAX(locationId) FROM DimensionLocation;")
    max_id = cursor.fetchone()[0]
    return max_id + 1 if max_id is not None else 1

# transform and insert location
def transform_insert_locations(locations, cursor):
    for locality, country in locations:
        cursor.execute(
            """
            SELECT locationId
            FROM DimensionLocation
            WHERE location = %s AND country = %s;
            """,
            (country, locality)
        )
    
        existing_location = cursor.fetchone()

        if existing_location:
            print(f"Duplicate location found: Country={country}, Location={locality}. Skipping insert.")
        else:
            location_id = get_next_location_id(cursor)
            cursor.execute(
                """
                INSERT INTO DimensionLocation (locationId, location, country)
                VALUES (%s, %s, %s);
                """,
                (location_id, locality, country)
            )
            print(f"Inserted location: ID={location_id}, Country={country}, Location={locality}")

