import os
import uuid
import re
import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
from dagster import ConfigurableResource, AssetExecutionContext


class ParquetTransformResource(ConfigurableResource):
    source_folder: str
    output_folder: str
    max_workers: int

    def ensure_staging_folder_exists(self, context:AssetExecutionContext):
        """Ensures the output folder exists."""
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            context.log.info(f"Created output folder: {self.output_folder}")
        return f"Folder already exist in the folder"
        
    def process_parquet_file(self, file_path, context:AssetExecutionContext):
        """Processes a single Parquet file."""
        file_name = os.path.basename(file_path)
        parquet_data = pq.read_table(file_path)
        df = parquet_data.to_pandas()
        month_in_file = re.search(r'-(\d{2})\.parquet$', file_name).group(1)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df = df[df['tpep_pickup_datetime'].dt.month == int(month_in_file)]
        df['pickup_date'] = df['tpep_pickup_datetime'].dt.date
        summary = df.groupby('pickup_date').agg(
            total_passenger_count=('passenger_count', 'count'),
            total_distance=('trip_distance', 'sum'),
            total_fare=('fare_amount', 'sum'),
            avg_trip_distance=('trip_distance', 'mean'),
            avg_fare_amount=('fare_amount', 'mean')
        ).reset_index()
        context.log.info(f"Generated summary statistics for file: {file_name} success")
        summary['uuid'] = [str(uuid.uuid4()) for _ in range(len(summary))]
        summary = summary[['uuid', 'pickup_date', 'total_passenger_count', 'total_distance', 'total_fare', 'avg_trip_distance', 'avg_fare_amount']]
        output_file_name = f"summary_{file_name}"
        output_file_path = os.path.join(self.output_folder, output_file_name)
        summary.to_parquet(output_file_path, index=False)
        context.log.info(f"Saved summary to Parquet file: {output_file_name}")
        
        return f"Successfully processed {file_name}"

    # def transform_parquet_files(self, context:AssetExecutionContext):
    #     """Transforms multiple Parquet files in parallel."""
    #     parquet_files = [os.path.join(self.source_folder, f) for f in os.listdir(self.source_folder) if f.endswith(".parquet")]
    #     context.log.info(f"Found {len(parquet_files)} Parquet files to process.")
        
    #     with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
    #         futures = {executor.submit(self.process_parquet_file, file, context): file for file in parquet_files}
    #         for future in as_completed(futures):
    #             result = future.result()
    #             context.log.info(result)
        
    #     context.log.info(f"Transformation complete. Summaries saved in {self.output_folder}")

    def transform_parquet_files(self, context: AssetExecutionContext):
        """Transforms multiple Parquet files sequentially."""
        # Get all Parquet files in the source folder
        parquet_files = [os.path.join(self.source_folder, f) for f in os.listdir(self.source_folder) if f.endswith(".parquet")]
        context.log.info(f"Found {len(parquet_files)} Parquet files to process.")
        
        # Process each file sequentially
        for file_path in parquet_files:
            try:
                result = self.process_parquet_file(file_path, context)
                context.log.info(result)
            except Exception as e:
                context.log.error(f"Error processing file {file_path}: {e}")
        
        context.log.info(f"Transformation complete. Summaries saved in {self.output_folder}")

