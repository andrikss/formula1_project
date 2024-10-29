import requests
import psycopg2
import random
import hashlib
from psycopg2 import sql
import json
import os
from datetime import datetime

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

# fetch races from api
def fetch_race_data():
    url = "http://ergast.com/api/f1/races.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data['MRData']['RaceTable']['Races']
    else:
        print(f"Error fetching data: {response.status_code}")
        return []
    
# fetch location data
def fetch_location_data():
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
    
# fetch driver standings
def fetch_driver_standings_data(year):
    url = f"http://ergast.com/api/f1/{year}/driverStandings.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        standings_table = data['MRData']['StandingsTable']

        if standings_table['StandingsLists']:
            standings_list = standings_table['StandingsLists'][0]
            season = standings_list['season']
            round_number = standings_list['round']
            driver_standings = standings_list['DriverStandings']

            # Ensure driver_standings is always a list
            if not isinstance(driver_standings, list):
                driver_standings = [driver_standings]

            standings_info = []

            for standing in driver_standings:
                driver_info = {
                    'position': standing['position'],
                    'positionText': standing['positionText'],
                    'points': standing['points'],
                    'wins': standing['wins'],
                    'driver': {
                        'driverId': standing['Driver']['driverId'],
                        'givenName': standing['Driver']['givenName'],
                        'familyName': standing['Driver']['familyName'],
                        'dateOfBirth': standing['Driver']['dateOfBirth'],
                        'nationality': standing['Driver']['nationality']
                    }
                }
                standings_info.append(driver_info)

            return {
                'season': season,
                'round': round_number,
                'driver_standings': standings_info
            }
        else:
            print(f"No driver standings data available for the year {year}.")
            return None
    else:
        print("Error fetching data from Ergast API:", response.status_code)
        return None

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

# fetch constructor standings
def fetch_constructor_standings_data(year):
    url = f"http://ergast.com/api/f1/{year}/constructorStandings.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        standings_table = data['MRData']['StandingsTable']

        if standings_table['StandingsLists']:
            standings_list = standings_table['StandingsLists'][0]
            season = standings_list['season']
            round_number = standings_list['round']
            constructor_standings = standings_list['ConstructorStandings']

            if not isinstance(constructor_standings, list):
                constructor_standings = [constructor_standings]

            standings_info = []
            
            for standing in constructor_standings:
                constructor_info = {
                    'position': standing['position'],
                    'positionText': standing['positionText'],
                    'points': standing['points'],
                    'wins': standing['wins'],
                    'constructor': {
                        'constructorId': standing['Constructor']['constructorId'],
                        'name': standing['Constructor']['name'],
                        'nationality': standing['Constructor']['nationality']
                    }
                }
                standings_info.append(constructor_info)

            return {
                'season': season,
                'round': round_number,
                'constructor_standings': standings_info
            }
        else:
            print(f"No constructor standings data available for the year {year}.")
            return None
    else:
        print("Error fetching data from Ergast API:", response.status_code)
        return None

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

        if 'Results' in race:
            for result in race['Results']:
                driverId = result['Driver']['driverId']
                laps = int(result['laps'])
                # calculate avg lap speed
                averageLapSpeed = findAverageLapSpeed(year, race['round'], driverId, laps)

                # calculate total pit stop duration
                pitStopDurationTotal = findPitStopDurationTotal(year, race['round'], driverId)

                driver_info = {
                    'positionText': result['positionText'],
                    'points': result['points'],
                    'driver': {
                        'driverId': result['Driver']['driverId'],
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
                        'speed': result.get('FastestLap', {}).get('AverageSpeed', {}).get('speed', 'N/A'),
                    },
                    'averageLapSpeed': averageLapSpeed,
                    'totalPitStopDuration': pitStopDurationTotal
                }
                race_details['results'].append(driver_info)

            race_info.append(race_details)

        return {
            'races': race_info
        }
    else:
        print("Error fetching data from Ergast API:", response.status_code)
        return None

# fetch last race date 
def fetch_latest_race_date():
    try:
        api_url = "http://ergast.com/api/f1/current/last/results.json"
        
        response = requests.get(api_url)
        response.raise_for_status()
        
        race_data = response.json()
        
        races = race_data["MRData"]["RaceTable"]["Races"]
        
        if races:
            last_race = races[-1] # getting the last race in list
            last_race_date = last_race["date"] 
            return last_race_date
        else:
            print("No races found in the response.")
            return None
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch the last race date: {e}")
        return None
    
# fetch driver standings by year and round num
def fetch_driver_standings_data_by_round(year, round_number):
    url = f"http://ergast.com/api/f1/{year}/{round_number}/driverStandings.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        standings_table = data['MRData']['StandingsTable']

        if standings_table['StandingsLists']:
            standings_list = standings_table['StandingsLists'][0]
            season = standings_list['season']
            round_number = standings_list['round']
            driver_standings = standings_list['DriverStandings']

            # Ensure driver_standings is always a list
            if not isinstance(driver_standings, list):
                driver_standings = [driver_standings]

            standings_info = []

            for standing in driver_standings:
                driver_info = {
                    'position': standing['position'],
                    'positionText': standing['positionText'],
                    'points': standing['points'],
                    'wins': standing['wins'],
                    'driver': {
                        'driverId': standing['Driver']['driverId'],
                        'givenName': standing['Driver']['givenName'],
                        'familyName': standing['Driver']['familyName'],
                        'dateOfBirth': standing['Driver']['dateOfBirth'],
                        'nationality': standing['Driver']['nationality']
                    }
                }
                standings_info.append(driver_info)

            return {
                'season': season,
                'round': round_number,
                'driver_standings': standings_info
            }
        else:
            print(f"No driver standings data available for year {year}, round {round_number}.")
            return None
    else:
        print("Error fetching data from API:", response.status_code)
        return None
    

# fetch constructor standings by year and num
def fetch_constructor_standings_data_by_round(year, round_number):
    url = f"http://ergast.com/api/f1/{year}/{round_number}/constructorStandings.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        standings_table = data['MRData']['StandingsTable']

        if standings_table['StandingsLists']:
            standings_list = standings_table['StandingsLists'][0]
            season = standings_list['season']
            round_number = standings_list['round']
            constructor_standings = standings_list['ConstructorStandings']

            # Ensure constructor_standings is always a list
            if not isinstance(constructor_standings, list):
                constructor_standings = [constructor_standings]

            standings_info = []
            
            for standing in constructor_standings:
                constructor_info = {
                    'position': standing['position'],
                    'positionText': standing['positionText'],
                    'points': standing['points'],
                    'wins': standing['wins'],
                    'constructor': {
                        'constructorId': standing['Constructor']['constructorId'],
                        'name': standing['Constructor']['name'],
                        'nationality': standing['Constructor']['nationality']
                    }
                }
                standings_info.append(constructor_info)

            # Return a dictionary with the retrieved data
            return {
                'season': season,
                'round': round_number,
                'constructor_standings': standings_info
            }
        else:
            print(f"No constructor standings data available for year {year}, round {round_number}.")
            return None
    else:
        print("Error fetching data from API:", response.status_code)
        return None
    
# for new column we added in model 
def findAverageLapSpeed(year, round_num, driverId, laps):
    url = f"http://ergast.com/api/f1/{year}/{round_num}/laps.json"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        laps_data = data['MRData']['RaceTable']['Races'][0].get('Laps', [])

        # Check if laps data exists
        if not laps_data:
            print(f"No lap data found for year {year}, round {round_num}, driver {driverId}.")
            return None

        total_lap_time = sum(
            convert_time_to_milliseconds(timing['time'])
            for lap_data in laps_data
            for timing in lap_data['Timings']
            if timing['driverId'] == driverId
        )

        average_lap_time = total_lap_time / laps if laps > 0 else 0
        print(f"Average Lap Speed for driver {driverId} in {year} round {round_num}: {average_lap_time:.3f} seconds")
        return round(average_lap_time, 3)
    else:
        print(f"Error fetching data from Ergast API for laps: {response.status_code}")
        return None
    
def convert_duration_to_milliseconds(duration):
    # Ako je format minuta:sekunde.milisekunde (npr. "1:09.761")
    if ':' in duration:
        minutes, seconds = duration.split(':')
        total_milliseconds = (int(minutes) * 60 + float(seconds)) * 1000
    else:
        # Ako je format samo sekunde.milisekunde (npr. "21.958")
        total_milliseconds = float(duration) * 1000
    
    return total_milliseconds

def findPitStopDurationTotal(year, round_num, driverId):
    url = f"http://ergast.com/api/f1/{year}/{round_num}/pitstops.json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        races = data['MRData']['RaceTable']['Races']

        if not races:
            print("No race data found.")
            return None

        total_duration = 0
        # iterate through each race (even though it should only contain one race for the given year and round, but the info is nested like that)
        for race in races:
            for pit_stop in race.get('PitStops', []):
                if pit_stop['driverId'] == driverId:
                    # Konvertuj i saberi vreme trajanja pit stopova
                    duration_ms = convert_duration_to_milliseconds(pit_stop['duration'])
                    total_duration += duration_ms

        print(f"Total pit stop duration for driver {driverId}: {total_duration} milliseconds")
        return round(total_duration, 3)
    else:
        print("Error fetching data from Ergast API:", response.status_code)
        return None
    
# time conversion 
def convert_time_to_milliseconds(time_string):

    if not time_string or time_string == 'N/A':
        return None

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