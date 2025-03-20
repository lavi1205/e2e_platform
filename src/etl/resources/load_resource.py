# load_resource.py
import os
import glob
import pandas as pd
import psycopg2
from dagster import ConfigurableResource, AssetExecutionContext
from datetime import datetime

class ParquetPostgresLoader(ConfigurableResource):
    host: str
    port: int
    dbname: str
    user: str
    password: str
    output_folder: str

    def connect(self, context: AssetExecutionContext):
        """Establish and return the PostgreSQL connection, storing it in the instance."""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            context.log.info("Connection success: ", self.user)
            return self.conn
        except Error as e:
            context.log.error("Connection error")

    def load_parquet_files(self, context: AssetExecutionContext):
        """
        Reads all Parquet files from the specified output_folder directory and loads
        their data into the PostgreSQL table 'trip_summary'.
        """
        conn = psycopg2.connect(
            host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password 
        )
        parquet_files = glob.glob(os.path.join(self.output_folder, "*.parquet"))
        if not parquet_files:
            context.log.info(f"No Parquet files found in directory: {self.output_folder}")
            return
        required_columns = [
            "uuid",
            "pickup_date",
            "total_passenger_count",
            "total_distance",
            "total_fare",
            "avg_trip_distance",
            "avg_fare_amount",
        ]
        for file_path in parquet_files:
            context.log.info(f"Processing file: {file_path}")
            try:
                df = pd.read_parquet(file_path)
            except Exception as e:
                context.log.info(f"Error reading {file_path}: {e}")
                continue
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                context.log.info(f"File {file_path} is missing required columns: {missing_cols}. Skipping.")
                continue
            if pd.api.types.is_datetime64_any_dtype(df["pickup_date"]):
                df["pickup_date"] = df["pickup_date"].dt.date
            else:
                df["pickup_date"] = df["pickup_date"].apply(
                    lambda x: datetime.fromisoformat(x).date() if isinstance(x, str) else x
                )
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute(
                        """
                        INSERT INTO trip_summary 
                            (uuid, pickup_date, total_passenger_count, total_distance, total_fare, avg_trip_distance, avg_fare_amount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (uuid) DO UPDATE SET 
                            pickup_date = EXCLUDED.pickup_date,
                            total_passenger_count = EXCLUDED.total_passenger_count,
                            total_distance = EXCLUDED.total_distance,
                            total_fare = EXCLUDED.total_fare,
                            avg_trip_distance = EXCLUDED.avg_trip_distance,
                            avg_fare_amount = EXCLUDED.avg_fare_amount
                        """,
                        (
                            row["uuid"],
                            row["pickup_date"],
                            row["total_passenger_count"],
                            row["total_distance"],
                            row["total_fare"],
                            row["avg_trip_distance"],
                            row["avg_fare_amount"],
                        ),
                    )
                # Commit after processing each file
                conn.commit()
            context.log.info(f"Data from file {file_path} loaded successfully.")
