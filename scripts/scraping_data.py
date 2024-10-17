import requests
import psycopg2
import random
import hashlib
from psycopg2 import sql
import json
import os
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

# fetch from driver api 
def fetch_driver_data():
    url = "http://ergast.com/api/f1/drivers.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['MRData']['DriverTable']['Drivers']
    else:
        print("Error fetching data from API")
        return []

# drivers into db
def insert_drivers_into_db(drivers, cursor):               
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

# fetch from constructor api
def fetch_constructor_data():
    url = "http://ergast.com/api/f1/constructors.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['MRData']['ConstructorTable']['Constructors']
    else:
        print("Error fetching data from API")
        return []
    
# constructors into db
def insert_constructors_into_db(constructors, cursor):
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
        name = constructor['name']
        nationality = constructor['nationality']

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

# fetch circuit data
def fetch_circuit_data():
    url = "http://ergast.com/api/f1/circuits.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['MRData']['CircuitTable']['Circuits']
    else:
        print("Error fetching circuit data from API")
        return []

# fetch altitude with lat and long
def fetch_altitude(lat, long):
    url = f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{long}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data and 'results' in data:
            return data['results'][0]['elevation']
    return None

# insert circuits into db
def insert_circuits_into_db(circuits, cursor):
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

# get race info for sessions 
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
    
# insert into session dim table
def insert_sessions_into_db(race_info, race_id, cursor):
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

# fetch races from api
def fetch_races_data():
    url = "http://ergast.com/api/f1/races.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data['MRData']['RaceTable']['Races']
    else:
        print(f"Error fetching data: {response.status_code}")
        return []
    
# insert races into db
def insert_races_into_db(races, cursor):
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
            insert_sessions_into_db(race_info, race_id, cursor)

# fetch location data
def fetch_locations_data():
    url = "http://ergast.com/api/f1/races.json"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        races = data['MRData']['RaceTable']['Races']
        locations = [
            (race['Circuit']['Location']['locality'], race['Circuit']['Location']['country'])
            for race in races
        ]
        return locations
    else:
        print("Error fetching data from Ergast API:", response.status_code)
        return []
    
def insert_location_into_db(locations, cursor):
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

def get_next_location_id(cursor):
    cursor.execute("SELECT MAX(locationId) FROM DimensionLocation;")
    max_id = cursor.fetchone()[0]
    return max_id + 1 if max_id is not None else 1

# fetch driver standings
def fetch_driver_standings_data():
    url = "http://ergast.com/api/f1/current/driverstandings.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        season = data['MRData']['StandingsTable']['season']
        round = data['MRData']['StandingsTable']['StandingsLists'][0]['round']
        driver_standings = data['MRData']['StandingsTable']['StandingsLists'][0]['DriverStandings']
        return season, round, driver_standings
    else:
        print("Error fetching data from Ergast API:", response.status_code)
        return None, None, []

# race info we got from season and round
def fetch_race_info(season, round):
    url = f"http://ergast.com/api/f1/{season}/{round}.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        race_info = data['MRData']['RaceTable']['Races'][0]

        circuit_info = race_info['Circuit']

        return {
            'raceName': race_info['raceName'],
            'round' : race_info['round'],
            'season' : race_info['season'],
            'date': race_info['date'],
            'time' : race_info['time'],
            'circuit': {
                'circuitName': circuit_info['circuitName'],
                'lat': circuit_info['Location']['lat'],
                'long': circuit_info['Location']['long'],
                'locality': circuit_info['Location']['locality'],
                'country': circuit_info['Location']['country']
              }
        }
    else:
        print("Error fetching race data from Ergast API:", response.status_code)
        return None

# query for finding id
def get_race_id(cursor, race_name, race_date, round):
    cursor.execute(
        """
        SELECT raceId
        FROM DimensionRace
        WHERE name = %s AND date = %s AND round = %s;
        """,
        (race_name, race_date, round)
    )
    result = cursor.fetchone()
    return result[0] if result else None  

# query for finding id
def get_driver_id(cursor, given_name, family_name, date_of_birth, nationality):
    cursor.execute(
        """
        SELECT driverId
        FROM DimensionDriver
        WHERE name = %s AND surname = %s AND dob = %s AND nationality = %s;
        """,
        (given_name, family_name, date_of_birth, nationality)
    )
    result = cursor.fetchone()
    return result[0] if result else None

# update race time? check this, probs can opt 
def update_race_time(cursor, race_id, race_time):
    cursor.execute(
        """
        UPDATE DimensionRace
        SET time = %s
        WHERE raceId = %s;
        """,
        (race_time, race_id)
    )
    print(f"Updated race time for RaceID={race_id} to {race_time}")

# insert driver standings into db
def insert_driver_standings_into_db(driver_standings, cursor, race):

    race_id = get_race_id(cursor, race['raceName'], race['date'], race['round'])

    if race_id is None:
        race_id = generate_id_from_string(f"{race['raceName']}", cursor, "dimensionrace", "raceId")  
        insert_race_query = sql.SQL(""" 
            INSERT INTO DimensionRace (raceId, name, round, time, date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (raceId) DO NOTHING;
        """)

        # first we insert new race
        cursor.execute(insert_race_query, (race_id, race['raceName'], race['round'], race['time'],  race['date']))
        print(f"Inserted new race: RaceID={race_id}, RaceName={race['raceName']}, Round={race['round']}, Date={race['date']}, Time={race['time']}")

        # then we insert sessions for that race
        race_info = fetch_race_info_for_sessions(race['season'], race['round'])
        insert_sessions_into_db(race_info, race_id, cursor)

        cursor.connection.commit()
    else:           
        update_race_time(cursor, race_id, race['time'])

    for standing in driver_standings:

        points = float(standing['points'])  
        position = int(standing['positionText']) 
        wins = int(standing['wins'])  

        driver = standing['Driver']  
        given_name = driver['givenName']
        family_name = driver['familyName']
        date_of_birth = datetime.strptime(driver['dateOfBirth'], '%Y-%m-%d').date()
        nationality = driver['nationality']

        # find driver id 
        driver_id = get_driver_id(cursor, given_name, family_name, date_of_birth, nationality)

        # if it is None - insert it into dimensionDriver 
        if driver_id is None:
            driver_id = generate_id_from_string(f"{given_name}_{family_name}", cursor, "dimensiondriver", "driverId")
            insert_driver_query = sql.SQL("""
                INSERT INTO DimensionDriver (driverId, name, surname, dob, nationality)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (driverId) DO NOTHING;
            """)
            cursor.execute(insert_driver_query, (driver_id, given_name, family_name, date_of_birth, nationality))
            print(f"Inserted new driver: DriverID={driver_id}, GivenName={given_name}, FamilyName={family_name}, DateOfBirth={date_of_birth}, Nationality={nationality}")

        # check duplicates 
        cursor.execute(
            """
            SELECT driverStandingId
            FROM fact_DriverStanding
            WHERE raceId = %s AND driverId = %s AND position = %s AND points = %s AND wins = %s;
            """,
            (race_id, driver_id, position, points, wins)
        )

        existing_count = cursor.fetchone()

        if existing_count:
            print(f"Duplicate entry found for RaceID={race_id}, DriverID={driver_id}, Position={position}, Points={points}, Wins={wins}, skipping.")
        else:
            driver_standing_id = generate_id_from_string(f"{driver['givenName']}_{driver['familyName']}", cursor, "fact_driverstanding", "driverStandingId")
            cursor.execute(
                """
                INSERT INTO fact_DriverStanding (driverStandingId, raceId, driverId, points, position, wins)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (driver_standing_id, race_id, driver_id, points, position, wins)
            )
            print(f"Inserted driver standing: DriverStandingID={driver_standing_id}, RaceID={race_id}, DriverID={driver_id}, Position={position}, Points={points}, Wins={wins}")

# get constructor id by name and nationality
def get_constructor_id(cursor, constructor_name, nationality):
    cursor.execute(
        """
        SELECT constructorId
        FROM DimensionConstructor
        WHERE name = %s AND nationality = %s;
        """,
        (constructor_name, nationality)
    )
    result = cursor.fetchone()
    return result[0] if result else None

# fetch constructor standings
def fetch_constructor_standings_data():
    url = "http://ergast.com/api/f1/current/constructorstandings.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        season = data['MRData']['StandingsTable']['season']
        round = data['MRData']['StandingsTable']['StandingsLists'][0]['round']
        constructor_standings = data['MRData']['StandingsTable']['StandingsLists'][0]['ConstructorStandings']
        return season, round, constructor_standings
    else:
        print("Error fetching data from Ergast API:", response.status_code)
        return None, None, []
    
# insert constructor standings 
def insert_constructor_standings_into_db(constructor_standings, cursor, race):

    race_id = get_race_id(cursor, race['raceName'], race['date'], race['round'])

    if race_id is None:
        race_id = generate_id_from_string(f"{race['raceName']}", cursor, "dimensionrace", "raceId")  
        insert_race_query = sql.SQL(""" 
            INSERT INTO DimensionRace (raceId, name, round, time, date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (raceId) DO NOTHING;
        """)
        # inserting new race
        cursor.execute(insert_race_query, (race_id, race['raceName'], race['round'], race['time'], race['date']))
        print(f"Inserted new race: RaceID={race_id}, RaceName={race['raceName']}, Round={race['round']}, Date={race['date']}, Time={race['time']}")
        
         # then we insert sessions for that race
        race_info = fetch_race_info_for_sessions(race['season'], race['round'])
        insert_sessions_into_db(race_info, race_id, cursor)

        cursor.connection.commit()
    else:           
        update_race_time(cursor, race_id, race['time'])

    for standing in constructor_standings:
        points = float(standing['points'])  
        position = int(standing['positionText'])  
        wins = int(standing['wins'])
        
        constructor = standing['Constructor']  
        constructor_name = constructor['name']
        constructor_nationality = constructor['nationality']
        constructor_id = constructor['constructorId']
       
        
        constructor_id = get_constructor_id(cursor, constructor_name, constructor_nationality)

        if constructor_id is None:
            # if there is no constructor insert it into dim table (ric)
            constructor_id = generate_id_from_string(f"{constructor_name}_{constructor_nationality}", cursor, "dimensiondriver", "driverId")
            insert_constructor_query = sql.SQL(""" 
                INSERT INTO DimensionConstructor (constructorId, name, nationality)
                VALUES (%s, %s, %s)
                ON CONFLICT (constructorId) DO NOTHING;
            """)
            cursor.execute(insert_constructor_query, (constructor_id, constructor_name, constructor_nationality))
            print(f"Inserted new constructor: ConstructorID={constructor_id}, Name={constructor_name}, Nationality={constructor_nationality}")

        # Check for duplicates in fact_ConstructorStanding
        cursor.execute(
            """
            SELECT constructorStandingId
            FROM fact_ConstructorStanding
            WHERE raceId = %s AND constructorId = %s AND position = %s AND points = %s AND wins = %s;
            """,
            (race_id, constructor_id, position, points, wins)
        )

        existing_count = cursor.fetchone()

        if existing_count:
            print(f"Duplicate entry found for RaceID={race_id}, ConstructorID={constructor_id}, Position={position}, Points={points}, skipping.")
        else:
            constructor_standing_id = generate_id_from_string(f"{constructor_name}", cursor, "fact_constructorstanding", "constructorStandingId")
            cursor.execute(
                """
                INSERT INTO fact_ConstructorStanding (constructorStandingId, raceId, constructorId, points, position, wins)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (constructor_standing_id, race_id, constructor_id, points, position, wins)
            )
            print(f"Inserted constructor standing: ConstructorStandingID={constructor_standing_id}, RaceID={race_id}, ConstructorID={constructor_id}, Position={position}, Points={points}")

# fetch race results
def fetch_race_results(year):
    url = f"http://ergast.com/api/f1/{year}/results.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        races = data['MRData']['RaceTable']['Races']
        
        race_info = [] 

        for race in races:
            race_details = {
                'season': race['season'],
                'round': race['round'],
                'raceName': race['raceName'],
                'date': race['date'],
                'time': race.get('time', 'N/A'),  # Default to 'N/A' if time is not available
                'circuit': {
                    'circuitName': race['Circuit']['circuitName'],
                    'location': {
                        'lat': race['Circuit']['Location']['lat'],
                        'long': race['Circuit']['Location']['long'],
                        'locality': race['Circuit']['Location']['locality'],
                        'country': race['Circuit']['Location']['country']
                    }
                },
                'results': []  # List to store results of the race
            }

            for result in race['Results']:

                driver_info = {
                    'positionText': result['positionText'],
                    'points': result['points'],
                    'driver': {
                        'givenName': result['Driver']['givenName'],
                        'familyName': result['Driver']['familyName'],
                        'dateOfBirth': result['Driver']['dateOfBirth'],
                        'nationality': result['Driver']['nationality'],
                    },
                    'constructor': {
                        'name': result['Constructor']['name'],
                        'nationality': result['Constructor']['nationality'],
                    },
                    
                    'grid': result['grid'],
                    'laps': result['laps'],
                    'status': result['status'],
                    'time': result.get('Time', {}).get('millis', 'N/A'),
                    'fastestLap': {
                        'rank': result.get('FastestLap', {}).get('rank', 'N/A'),
                        'lap': result.get('FastestLap', {}).get('lap', 'N/A'),
                        'time': result.get('FastestLap', {}).get('Time', {}).get('time', 'N/A'),
                    }
                }
                race_details['results'].append(driver_info)

            race_info.append(race_details)

        return {
            'races': race_info
        }
    else:
        print("Error fetching data from Ergast API:", response.status_code)
        return None


# get circuit id 
def get_circuit_id(cursor, circuit_name, lat, lng):
    query = sql.SQL("""
        SELECT circuitId 
        FROM DimensionCircuit
        WHERE name = %s AND lat = %s AND lng = %s;
    """)
    
    cursor.execute(query, (circuit_name, lat, lng))
    
    result = cursor.fetchone()
    
    if result:
        return result[0]  
    else:
        return None

# get location id 
def get_location_id(cursor, locality, country):
    query = sql.SQL("""
        SELECT locationId 
        FROM DimensionLocation
        WHERE location = %s AND country = %s;
    """)

    cursor.execute(query, (locality, country))

    result = cursor.fetchone()

    if result:
        return result[0] 
    else:
        return None

# get status id 
def get_status_id(cursor, status_name):
    query = sql.SQL("""
        SELECT statusId 
        FROM DimensionStatus
        WHERE status = %s;
    """)

    cursor.execute(query, (status_name,))

    result = cursor.fetchone()

    if result:
        return result[0] 
    else:
        return None
    
# time conversion 
def convert_time_to_milliseconds(time_string):
    milliseconds = 0

    # Split the time string into minutes and seconds
    time_parts = time_string.split(':')
    
    # Check if the time string is in the format "MM:SS.sss"
    if len(time_parts) == 2:  # MM:SS.sss
        minutes = int(time_parts[0])  # Get the minutes
        seconds_millis = time_parts[1]  # Get the seconds and milliseconds
    else:
        # If there's no minutes part, it's in the format "SS.sss"
        minutes = 0
        seconds_millis = time_parts[0]

    # Further split seconds and milliseconds
    seconds_parts = seconds_millis.split('.')
    seconds = int(seconds_parts[0])  # Get the seconds

    # Check if milliseconds exist
    if len(seconds_parts) > 1:
        millis = int(seconds_parts[1])  # Get the milliseconds
    else:
        millis = 0  # Default to 0 if not provided

    # Calculate total milliseconds
    milliseconds = (minutes * 60 * 1000) + (seconds * 1000) + millis

    return milliseconds

# insert race results into db 
def insert_race_results_into_db(race_results, cursor, race):

    # does the race exist ? if it doesnt insert new one into race dim table
    race_id = get_race_id(cursor, race['raceName'], race['date'], race['round'])

    if race_id is None:
        race_id = generate_id_from_string(f"{race['raceName']}", cursor, "dimensionrace", "raceId")  

        insert_race_query = sql.SQL(""" 
            INSERT INTO DimensionRace (raceId, name, round, time, date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (raceId) DO NOTHING;
        """)

        # inserting new race
        cursor.execute(insert_race_query, (race_id, race['raceName'], race['round'], race['time'], race['date']))
        print(f"Inserted new race: RaceID={race_id}, RaceName={race['raceName']}, Round={race['round']}, Date={race['date']}, Time={race['time']}")
        
         # then we insert sessions for that race
        race_info = fetch_race_info_for_sessions(race['season'], race['round'])
        insert_sessions_into_db(race_info, race_id, cursor)

        cursor.connection.commit()
    else:           
        update_race_time(cursor, race_id, race['time'])

    # then circuit
    circuit_info = race['circuit']
    circuit_id = get_circuit_id(cursor, circuit_info['circuitName'], circuit_info['location']['lat'], circuit_info['location']['long'])

    if circuit_id is None: 
        circuit_id = generate_id_from_string(f"{circuit_info['circuitName']}", cursor, 'dimensionCircuit', 'circuitId')
        
        insert_circuit_query = sql.SQL("""
        INSERT INTO DimensionCircuit (circuitId, name, lat, lng, alt)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (circuitId) DO NOTHING;
    """)
        
        cursor.execute(insert_circuit_query, (circuit_id, circuit_info['circuitName'], circuit_info['location']['lat'], circuit_info['location']['lng'], None))

    # then location
    location_id = get_location_id(cursor, circuit_info['location']['locality'], circuit_info['location']['country'])
    if location_id is None:
        location_id =  get_next_location_id(cursor)
        cursor.execute(
                """
                INSERT INTO DimensionLocation (locationId, location, country)
                VALUES (%s, %s, %s);
                """,
                (location_id, circuit_info['locality'], circuit_info['country'])
            )
    
    print('iznad fora sam')
    for result in race_results:
            # DRIVER 
            print('driver')
            driver_info = result['driver']
            driver_id = get_driver_id(cursor, driver_info['givenName'], driver_info['familyName'], driver_info['dateOfBirth'], driver_info['nationality'])

            if driver_id is None: 
                driver_id = generate_id_from_string(f"{driver_info['givenName']}_{driver_info['familyName']}", cursor, "dimensiondriver", "driverId")
                insert_driver_query = sql.SQL("""
                    INSERT INTO DimensionDriver (driverId, name, surname, dob, nationality)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (driverId) DO NOTHING;
                """)
                cursor.execute(insert_driver_query, (driver_id,  driver_info['givenName'], driver_info['familyName'], driver_info['dateOfBirth'], driver_info['nationality']))
                print(f"Inserted new driver: DriverID={driver_id}, GivenName={driver_info['givenName']}, FamilyName={driver_info['familyName']}, DateOfBirth={driver_info['dateOfBirth']}, Nationality={driver_info['nationality']}")

            # CONSTRUCTOR
            print('constructor')
            constructor_info = result['constructor']
            constructor_id = get_constructor_id(cursor, constructor_info['name'], constructor_info['nationality'])

            if constructor_id is None: 
                constructor_id = generate_id_from_string(f"{constructor_info['name']}_{constructor_info['nationality']}", cursor, "dimensiondriver", "driverId")
                insert_constructor_query = sql.SQL(""" 
                    INSERT INTO DimensionConstructor (constructorId, name, nationality)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (constructorId) DO NOTHING;
                """)
                cursor.execute(insert_constructor_query, (constructor_id, constructor_info['name'], constructor_info['nationality']))
                print(f"Inserted new constructor: ConstructorID={constructor_id}, Name={constructor_info['name']}, Nationality={constructor_info['nationality']}")

        
            # STATUS 
            status_id = get_status_id(cursor, result['status'])
            print('status')
            if status_id is None:
                status_id = generate_id_from_string(result['status'], cursor, "dimensionStatus", "statusId")

                insert_status_query = sql.SQL("""
                    INSERT INTO DimensionStatus (statusId, status)
                    VALUES (%s, %s)
                    ON CONFLICT (statusId) DO NOTHING;
                """)

                cursor.execute(insert_status_query, (status_id, result['status']))
                print(f"Inserted new status: StatusID={status_id}, Name={result['status']}")

            startPosition = int(result['grid'])
            endPosition = int(result['positionText'])
            rank = startPosition - endPosition
            rank = int(rank)
            # because end position is string in our DB becuase of 'D N E R' 
            endPosition = str(result['positionText'])
            points = float(result['points'])
            laps = int(result['laps'])


            # hard coding 
            # will think about this later
            duration = result.get('Time', {}).get('millis', None)
            if duration is not None:
                duration = convert_time_to_milliseconds(duration)
                duration = int(duration)
            else:
                duration = 0

            fastestLap = result.get('FastestLap', {}).get('lap', None)
            if fastestLap is not None:
                fastestLap = int(fastestLap)
            else: 
                fastestLap = 0

            fastestLapTimeString = result.get('FastestLap', {}).get('Time', {}).get('time', '0:0.0')
            fastestLapTime = convert_time_to_milliseconds(fastestLapTimeString)
            fastestLapTime = str(fastestLapTime)

            # TO DO 
            fastestLapSpeed = 415.200
            averageLapTime = 9000.04
            pitStopDurationTotal = 28981

            # checking duplicates 
            check_query = sql.SQL("""
                SELECT resultId FROM Fact_RaceResults
                WHERE raceId = %s AND driverId = %s AND constructorId = %s AND circuitId = %s AND locationId = %s 
                AND startPosition = %s AND endPosition = %s AND laps = %s;
                """)
            cursor.execute(check_query, (race_id, driver_id, constructor_id, circuit_id, location_id, startPosition, endPosition, laps))
            existing_result = cursor.fetchone()

            if existing_result is None: 
                # finally, add the result
                result_id = generate_id_from_string(f"{race_id}_{driver_id}_{constructor_id}_{startPosition}_{endPosition}", cursor, "fact_raceresults", "resultId")
                print(f"Result ID: {result_id} (Type: {type(result_id).__name__})")
                print(f"Race ID: {race_id} (Type: {type(race_id).__name__})")
                print(f"Driver ID: {driver_id} (Type: {type(driver_id).__name__})")
                print(f"Constructor ID: {constructor_id} (Type: {type(constructor_id).__name__})")
                print(f"Location ID: {location_id} (Type: {type(location_id).__name__})")
                print(f"Status ID: {status_id} (Type: {type(status_id).__name__})")
                print(f"Start Position: {startPosition} (Type: {type(startPosition).__name__})")
                print(f"End Position: {endPosition} (Type: {type(endPosition).__name__})")
                print(f"Rank: {rank} (Type: {type(rank).__name__})")
                print(f"Points: {points} (Type: {type(points).__name__})")
                print(f"Laps: {laps} (Type: {type(laps).__name__})")
                print(f"Duration: {duration} (Type: {type(duration).__name__})")
                print(f"Fastest Lap: {fastestLap} (Type: {type(fastestLap).__name__})")
                print(f"Fastest Lap Time: {fastestLapTime} (Type: {type(fastestLapTime).__name__})")
                print(f"Fastest Lap Speed: {fastestLapSpeed} (Type: {type(fastestLapSpeed).__name__})")
                print(f"Average Lap Time: {averageLapTime} (Type: {type(averageLapTime).__name__})")
                print(f"Pit Stop Duration Total: {pitStopDurationTotal} (Type: {type(pitStopDurationTotal).__name__})")
                print('')
                insert_query = sql.SQL("""
                INSERT INTO Fact_RaceResults (resultId, raceId, driverId, constructorId, circuitId, locationId, statusId, startPosition, endPosition, rank, points, laps, duration, fastestLap, fastestLapTime, fastestLapSpeed, averageLapTime, pitStopDurationTotal)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (resultId) DO NOTHING;
            """)
            
                cursor.execute(insert_query, (
                    result_id, race_id, driver_id, constructor_id, circuit_id, location_id, status_id, startPosition, endPosition, rank, points, 
                    laps, duration, fastestLap, fastestLapTime, fastestLapSpeed, averageLapTime, pitStopDurationTotal
                ))
                print(f"Inserted new race result: ResultID={result_id}, DriverID={driver_id}, ConstructorID={constructor_id}, Start={startPosition}, End={endPosition}, Laps={laps}")
            else:
                print(f"Result already exists for DriverID={driver_id}, ConstructorID={constructor_id}, Start={startPosition}, End={endPosition}, Laps={laps}")

# main function / connecting 
def main():
    try:
        with psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'race_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'andrea1'),
            host=os.getenv('DB_HOST', 'postgres'),
            port=os.getenv('DB_PORT', '5432')
        ) as conn:
            with conn.cursor() as cursor:
                # fetch drivers and insert them
                drivers = fetch_driver_data()
                if drivers:
                    insert_drivers_into_db(drivers, cursor)

                # fetch and insert constructors
                constructors = fetch_constructor_data()
                if constructors: 
                    insert_constructors_into_db(constructors, cursor) 

                # fetch and insert circuits 
                circuits = fetch_circuit_data()
                if circuits:
                    insert_circuits_into_db(circuits, cursor)

                # fetch and insert races
                races = fetch_races_data()
                if races:
                    insert_races_into_db(races, cursor)

                # fetch and insert locations
                locations = fetch_locations_data()
                if locations: 
                    insert_location_into_db(locations, cursor)

                # fetch and insert driver standings 
                season, round, driverStandings = fetch_driver_standings_data()
                ds_race = fetch_race_info(season, round)
                if driverStandings:
                    insert_driver_standings_into_db(driverStandings, cursor, ds_race)

                # fetch and insert constructor standings 
                season, round, constructorStandings = fetch_constructor_standings_data()
                cs_race = fetch_race_info(season, round)
                if constructorStandings:
                    insert_constructor_standings_into_db(constructorStandings, cursor, cs_race)

                # we want to fetch results from this year
                now = datetime.now()
                year = now.year

                # fetch and insert race results
                raceResults = fetch_race_results(year)
                if raceResults:
                    for race in raceResults['races']:
                        season = race['season']
                        round = race['round']
                        insert_race_results_into_db(race['results'], cursor, race)

                conn.commit()
                print("Database operations completed successfully.")

    except psycopg2.Error as e:
        print("Database error:", e)
    except Exception as e:
        print("Unexpected error:", e)

# entry point
if __name__ == '__main__':
    main()
