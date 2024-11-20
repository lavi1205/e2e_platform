import os
import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


# Import custom modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.etl.utils.aws.aws_login import create_postgresql_client
from src.etl.utils.database_operation.postgresql import create_table_if_not_exists,read_and_write_to_db
from src.etl.logger.logger_config import get_logger
from src.etl.setting.setting import STAGING_FOLDER, MAX_WORKERS


# Setup logging
script_dir = os.path.dirname(__file__)
staging_folder = os.path.join(script_dir, '..', STAGING_FOLDER)
logger = get_logger(__name__)

# Function to handle concurrent load of parquet files
def load_parquet_files():
    # List all parquet files in the staging folder
    parquet_files = [f for f in Path(staging_folder).glob('*.parquet')]
    # Create a PostgreSQL client
    conn = create_postgresql_client(aws_access_key="",aws_secret_key="", region_name="")

    # Create the table if it does not exist
    create_table_if_not_exists(conn)

    # Use ThreadPoolExecutor to process files concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(read_and_write_to_db, file, conn) for file in parquet_files]

        # Wait for all threads to complete
        for future in futures:
            future.result()  # This will raise exceptions if any

    # Close the PostgreSQL connection
    conn.close()

def main():
    # Call the function to handle the processing
    load_parquet_files()

if __name__ == "__main__":
    main()
