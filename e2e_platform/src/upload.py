import os
import sys
from botocore.exceptions import NoCredentialsError
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.etl.logger.logger_config import get_logger
from src.etl.setting.setting import STAGING_FOLDER
from src.etl.utils.aws.aws_login import create_s3_client
logger = get_logger(__name__)
script_dir = os.path.dirname(__file__)
staging_folder = os.path.join(script_dir, '..', STAGING_FOLDER)

def upload_to_s3_batch(s3_client, source_folder, bucket_name, s3_folder, batch_size=10):
    """
    Upload Parquet files from a source folder to an S3 bucket in batches.

    Args:
        s3_client: Pre-configured boto3 S3 client.
        source_folder: Path to the folder containing Parquet files.
        bucket_name: Name of the S3 bucket.
        s3_folder: Folder in S3 to store the files.
        batch_size: Number of files to upload in each batch (default is 10).

    return: Success or failure message for each batch.
    """
    # Find all Parquet files in the source folder
    parquet_files = [file_name for file_name in os.listdir(source_folder) if file_name.endswith('.parquet')]
    
    if not parquet_files:
        logger.error("No Parquet files found in the source folder.")
        return "No files to upload."

    # Batch processing
    for i in range(0, len(parquet_files), batch_size):
        batch = parquet_files[i:i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1}: {batch}")

        for parquet_file in batch:
            file_path = os.path.join(source_folder, parquet_file)
            try:
                s3_key = os.path.join(s3_folder, parquet_file)
                s3_client.upload_file(file_path, bucket_name, s3_key)
                logger.info(f"Successfully uploaded {parquet_file} to S3 bucket {bucket_name}.")
            except FileNotFoundError:
                logger.error(f"File not found: {parquet_file}")
            except NoCredentialsError:
                logger.error(f"Credentials not available for AWS.")
                return "Credentials error occurred."

    return f"All batches uploaded successfully to {bucket_name}."



source_folder = staging_folder
# Example usage
if __name__ == "__main__":
    aws_access_key = ""
    aws_secret_key = ""
    region_name = ""
    
    # Set up S3 client
    s3_client = create_s3_client(aws_access_key, aws_secret_key, region_name)
    
    # Upload files in batches
    source_folder = staging_folder
    bucket_name = ""
    s3_folder = ""
    
    upload_to_s3_batch(s3_client, source_folder, bucket_name, s3_folder, batch_size=10)
