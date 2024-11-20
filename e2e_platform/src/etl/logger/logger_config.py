# logger_config.py
import logging, sys

# Configure the logger
logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(levelname)s - %(filename)s - %(message)s - %(asctime)s',  # Set the format
    datefmt = '%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler()  # Log to the console
    ]
)

# Get a logger instance
def get_logger(name):
    return logging.getLogger(name)
