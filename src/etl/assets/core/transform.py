from dagster import asset, AssetExecutionContext
from src.etl.resources.transform_resource import ParquetTransformResource


@asset(deps=["download_parquet_data"])
def ensure_staging_folder_exists(context: AssetExecutionContext, transform_resource: ParquetTransformResource):
    """
    Ensures the staging folder exists using the ParquetTransformResource.

    Args:
        context: Dagster context for logging.
        transform_resource (ParquetTransformResource): Instance of the resource.
    """
    transform_resource.ensure_staging_folder_exists(context)


@asset(deps=[ensure_staging_folder_exists])
def transform_parquet_files(context: AssetExecutionContext, transform_resource: ParquetTransformResource):
    """
    Transforms multiple Parquet files using the ParquetTransformResource.

    Args:
        context: Dagster context for logging.
        transform_resource (ParquetTransformResource): Instance of the resource.
    """
    transform_resource.transform_parquet_files(context)
