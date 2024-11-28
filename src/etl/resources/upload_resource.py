from dagster import ConfigurableResource
import boto3
from botocore.exceptions import NoCredentialsError


class ParquetUploadResource(ConfigurableResource):
    
    source_folder: str
    bucket_name: str
    s3_folder: str
    aws_access_key: str
    aws_secret_key: str
    region_name: str
    batch_size: int = 10

    def create_s3_client(self):
        """
        Create an S3 client using provided credentials.

        Returns:
            boto3.client: Configured S3 client.
        """
        try:
            return boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.region_name,
            )
        except NoCredentialsError:
            raise Exception("AWS credentials are missing or invalid.")
