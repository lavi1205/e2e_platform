from dagster import Definitions, load_assets_from_package_module
from src.etl.setting.setting import CORE, BASE_URL, YEARS_TO_DOWNLOAD, MONTHS_TO_DOWNLOAD, DOWNLOAD_FOLDER, STAGING_FOLDER, MAX_WORKERS
from src.etl.assets import core
from src.etl.resources.ts_resources import ParquetDownloadResource
from src.etl.resources.transform_resource import ParquetTransformResource
from src.etl.resources.upload_resource import ParquetUploadResource
from src.etl.resources.load_resource import ParquetPostgresLoader
import os, sys, psycopg2

core_assets = load_assets_from_package_module(core, group_name=CORE)


RESOURCES_LOCAL = {
    "download_resource": ParquetDownloadResource(
        base_url=BASE_URL,
        years=YEARS_TO_DOWNLOAD,
        months=MONTHS_TO_DOWNLOAD,
        download_folder=DOWNLOAD_FOLDER
    ),
    "transform_resource" : ParquetTransformResource(
    source_folder=DOWNLOAD_FOLDER,
    output_folder=STAGING_FOLDER,
    max_workers=MAX_WORKERS
    ),
    "upload_resource": ParquetUploadResource(
        aws_access_key=os.environ.get("AWS_ACCESS_KEY", "test"),
        aws_secret_key=os.environ.get("AWS_SECRET_KEY", "test1"),
        region_name=os.environ.get("REGION", "test_region"),
        source_folder=STAGING_FOLDER,
        bucket_name=os.environ.get("S3_BUCKET", "test_bucket"),
        s3_folder="firmware/test/",
        batch_size=10
    ),
    "load_resource": ParquetPostgresLoader(
        host="172.18.0.2",
        port="5432",
        dbname="trip_summary",
        user="postgres",
        password="postgres",
        output_folder=STAGING_FOLDER
    )
}
resources_by_deployment_name = {
    # "prod": RESOURCES_PROD,
    # "staging": RESOURCES_STAGING,
    "local": RESOURCES_LOCAL,
}

all_assets = [*core_assets]
defs = Definitions(
    assets = all_assets,
    resources=resources_by_deployment_name['local'],
)
