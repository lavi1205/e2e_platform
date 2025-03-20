from dagster import asset, AssetExecutionContext
from src.etl.resources.load_resource import ParquetPostgresLoader
import psycopg2

@asset(deps=["transform_parquet_files"])
def load_data_to_database(context: AssetExecutionContext, load_resource: ParquetPostgresLoader):
    """
    Ensures the staging folder exists using the ParquetTransformResource.

    Args:
        context: Dagster context for logging.
        transform_resource (ParquetTransformResource): Instance of the resource.
    """
    load_resource.load_parquet_files(context)
