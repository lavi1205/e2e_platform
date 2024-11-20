import sys,os
import boto3, psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__),"../.."))
from logger.logger_config import get_logger
# from setting.setting import ENDPOINT , PORT, USER, REGION, DBNAME
logger = get_logger(__name__)

ENDPOINT = os.environ["ENDPOINT"]
PORT = os.environ["PORT"]
USER = os.environ["USER"]
REGION = os.environ["REGION"]
DBNAME = os.environ["DBNAME"]
PASSWORD = os.environ["PASSWORD"]
# Separate client setup
def create_s3_client(aws_access_key, aws_secret_key, region_name):
    """
    Create and return a boto3 S3 client.

    Args:
        aws_access_key: AWS Access Key ID.
        aws_secret_key: AWS Secret Access Key.
        region_name: AWS region.

    return: boto3 S3 client.
    """
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key,
                             aws_secret_access_key=aws_secret_key,
                             region_name=region_name)
    logger.info("S3 client setup successful")
    return s3_client

def create_postgresql_client(aws_access_key, aws_secret_key, region_name):
    """
    Create and return boto3 Postgresql client.

    Args:
        aws_access_key: AWS Access Key ID.
        aws_secret_key: AWS Secret Access Key.
        region_name: AWS region.

    return: boto3 Postgresql client.
    """
    # client = boto3.client('rds', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,region_name=region_name)
    try:
        connection =  psycopg2.connect(host=ENDPOINT, user=USER, password=PASSWORD, port=PORT, database=DBNAME)
        logger.info(f"Postgresql connection estabished successfully with endpoint {ENDPOINT}, port {PORT}")
        return connection
    except Exception as e:
        logger.error("Database connection failed due to {}".format(e))          
