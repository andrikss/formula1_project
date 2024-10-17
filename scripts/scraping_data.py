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
            race_id = generate_id_from_string(f"{race_name}_{round_num}", cursor, "dimensionrace", "raceId")
            cursor.execute(insert_query, (race_id, race_name, round_num, race_date, race_time))
            print(f"Inserted race: {race_id}, {race_name}, round {round_num}, date {race_date}.")

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
        return {
            'raceName': race_info['raceName'],
            'round' : race_info['round'],
            'date': race_info['date'],
            'time' : race_info['time']
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
        cursor.execute(insert_race_query, (race_id, race['raceName'], race['round'], race['time'],  race['date']))
        print(f"Inserted new race: RaceID={race_id}, RaceName={race['raceName']}, Round={race['round']}, Date={race['date']}, Time={race['time']}")
        cursor.connection.commit()
    else:           
        update_race_time(cursor, race_id, race['time'])

    for standing in driver_standings:

        points = float(standing['points'])  
        position = int(standing['position']) 
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
                race = fetch_race_info(season, round)
                if driverStandings:
                    insert_driver_standings_into_db(driverStandings, cursor, race)


                conn.commit()
                print("Database operations completed successfully.")

    except psycopg2.Error as e:
        print("Database error:", e)
    except Exception as e:
        print("Unexpected error:", e)

# entry point
if __name__ == '__main__':
    main()
