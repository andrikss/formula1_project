import psycopg2
import logging

def insert_into_drivers_staging(driver_id, connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO StagingDrivers (driverId)
            VALUES (%s)
            ON CONFLICT (driverId) DO NOTHING;
            """,
            (driver_id,)
        )
    connection.commit()

def check_drivers_staging_and_trigger(driver_id, connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT driverId FROM StagingDrivers WHERE driverId = %s;", (driver_id,))
        result = cursor.fetchone()
        
        if result is None:
            logging.info(f"New driverId detected: {driver_id}. Triggering DAG.")
            insert_into_drivers_staging(driver_id, connection)
            return True 
        else:
            logging.info(f"DriverId {driver_id} already processed. Skipping DAG trigger.")
            return False 
