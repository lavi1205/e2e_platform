from dagster import Definitions, load_assets_from_package_module
from src.etl.setting.setting import CORE, BASE_URL, YEARS_TO_DOWNLOAD, MONTHS_TO_DOWNLOAD, DOWNLOAD_FOLDER, STAGING_FOLDER, MAX_WORKERS
from src.etl.assets import core
from src.etl.resources.ts_resources import ParquetDownloadResource
from src.etl.resources.transform_resource import ParquetTransformResource
from src.etl.resources.upload_resource import ParquetUploadResource
import os

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
        aws_access_key=os.environ["AWS_ACCESS_KEY"],
        aws_secret_key=os.environ["AWS_SECRET_KEY"],
        region_name=os.environ["REGION"],
        source_folder=STAGING_FOLDER,
        bucket_name=os.environ["S3_BUCKET"],
        s3_folder="firmware/test/",
        batch_size=10
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
