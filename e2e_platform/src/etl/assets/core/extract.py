from dagster import asset, AssetExecutionContext
from src.etl.resources.ts_resources import ParquetDownloadResource

@asset
def ensure_download_folder_exists(context:AssetExecutionContext, download_resource: ParquetDownloadResource):
    """
    Ensures the download folder exists using the ParquetDownloadResource.

    Args:
        context: Dagster context for logging.
        download_resource (ParquetDownloadResource): Instance of the resource.
    """
    download_resource.ensure_download_folder_exists(context)


@asset(deps=[ensure_download_folder_exists])
def download_parquet_data(context:AssetExecutionContext, download_resource: ParquetDownloadResource):
    """
    Downloads Parquet files using the ParquetDownloadResource.

    Args:
        context: Dagster context for logging.
        download_resource (ParquetDownloadResource): Instance of the resource.
    """
    download_resource.download_parquet_data(context)
