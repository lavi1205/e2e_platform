BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

TIME_SLEEP = 1

PARQUET_ENGINE = "pyarrow"

DOWNLOAD_FOLDER = 'download_data'

STAGING_FOLDER = 'staging_data'

MONTHS_TO_DOWNLOAD = [1, 2, 3]

YEARS_TO_DOWNLOAD = [2023]

COLUMNS_TO_KEEP = [
'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 
'passenger_count', 'trip_distance', 'RatecodeID', 
'PULocationID', 'DOLocationID', 'payment_type', 
'fare_amount', 'tip_amount', 'total_amount', 
]

MAX_WORKERS = 4

CORE = "core"