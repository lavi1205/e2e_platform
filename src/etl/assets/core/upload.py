import os
from dagster import asset, AssetExecutionContext
from src.etl.resources.upload_resource import ParquetUploadResource


@asset(deps=["transform_parquet_files"])
def upload_to_s3(context: AssetExecutionContext, upload_resource: ParquetUploadResource):
    """
    Upload Parquet files from the source folder to S3 in batches.

    Args:
        context (OpExecutionContext): Dagster context for logging and execution.
        upload_resource (ParquetUploadResource): Configurable resource for S3 upload.
    """
    # Extract configuration from the resource
    source_folder = upload_resource.source_folder
    bucket_name = upload_resource.bucket_name
    s3_folder = upload_resource.s3_folder
    batch_size = upload_resource.batch_size

    # Create the S3 client
    s3_client = upload_resource.create_s3_client()

    # Find all Parquet files in the source folder
    parquet_files = [file_name for file_name in os.listdir(source_folder) if file_name.endswith(".parquet")]

    if not parquet_files:
        context.log.error("No Parquet files found in the source folder.")
        return "No files to upload."

    # Batch processing
    for i in range(0, len(parquet_files), batch_size):
        batch = parquet_files[i : i + batch_size]
        context.log.info(f"Processing batch {i // batch_size + 1}: {batch}")

        for parquet_file in batch:
            file_path = os.path.join(source_folder, parquet_file)
            try:
                s3_key = os.path.join(s3_folder, parquet_file)
                s3_client.upload_file(file_path, bucket_name, s3_key)
                context.log.info(f"Successfully uploaded {parquet_file} to S3 bucket {bucket_name}.")
            except FileNotFoundError:
                context.log.error(f"File not found: {parquet_file}")
            except Exception as e:
                context.log.error(f"Failed to upload {parquet_file}: {str(e)}")

    context.log.info(f"All batches uploaded successfully to bucket {bucket_name}.")
    return f"All batches uploaded successfully to {bucket_name}."
