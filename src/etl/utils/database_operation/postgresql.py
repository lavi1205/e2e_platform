
import os,sys
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from logger.logger_config import get_logger

logger = get_logger(__name__)


# Function to create the table if it does not exist
def create_table_if_not_exists(conn):
    try:
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS trip_summary (
            uuid TEXT PRIMARY KEY,
            pickup_date DATE,
            total_passenger_count INTEGER,
            total_distance FLOAT,
            total_fare FLOAT,
            avg_trip_distance FLOAT,
            avg_fare_amount FLOAT
        );
        '''
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
    except KeyError as e:
        logger.error(f"Can not create table. Error: ".format(e))

# Function to read data from a Parquet file and insert it into the database
def read_and_write_to_db(parquet_file_path, conn):
    try:
        df = pd.read_parquet(parquet_file_path)
        insert_query = '''
        INSERT INTO parquet_data (uuid, pickup_date, total_passenger_count, total_distance, total_fare, avg_trip_distance, avg_fare_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (uuid) DO NOTHING;
        '''
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['uuid'],
                row['pickup_date'],
                row['total_passenger_count'],
                row['total_distance'],
                row['total_fare'],
                row['avg_trip_distance'],
                row['avg_fare_amount']
            ))
        conn.commit()
        cursor.close()
        logger.info(f"Data from {parquet_file_path} has been written to the database.")
    except Exception as e:
        logger.error(f"Failed to write data from {parquet_file_path}: {e}")