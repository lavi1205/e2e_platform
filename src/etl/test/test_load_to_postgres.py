import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call
from psycopg2 import connect
from src.etl.resources.load_resource import ParquetPostgresLoader
from dagster import AssetExecutionContext

# test_load_resource.py

@pytest.fixture
def mock_context():
    """Fixture to create a mock AssetExecutionContext."""
    context = MagicMock(spec=AssetExecutionContext)
    context.log.info = MagicMock()
    context.log.error = MagicMock()
    return context

@pytest.fixture
def loader():
    """Fixture to create a ParquetPostgresLoader instance."""
    return ParquetPostgresLoader(
        host="localhost",
        port=5432,
        dbname="test_db",
        user="test_user",
        password="test_password",
        output_folder="/tmp/parquet_files"
    )

@patch("psycopg2.connect")
def test_connection_postgres(mock_connect, loader, mock_context):
    """Test connection failure to PostgreSQL."""
    mock_connect.side_effect = Exception("Connection error")
    loader.connect(mock_context)
    mock_context.log.error.assert_called_with("Connection error")

@patch("glob.glob")
@patch("pandas.read_parquet")
@patch("psycopg2.connect")
def test_load_parquet_files_success(mock_connect, mock_read_parquet, mock_glob, loader, mock_context):
    """Test successful loading of Parquet files into PostgreSQL."""
    # Mock the Parquet files
    mock_glob.return_value = ["/tmp/parquet_files/file1.parquet"]
    mock_read_parquet.return_value = pd.DataFrame({
        "uuid": ["123"],
        "pickup_date": ["2023-01-01"],
        "total_passenger_count": [2],
        "total_distance": [10.5],
        "total_fare": [25.0],
        "avg_trip_distance": [5.25],
        "avg_fare_amount": [12.5],
    })
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    loader.load_parquet_files(mock_context)

    mock_context.log.info.assert_any_call("Processing file: /tmp/parquet_files/file1.parquet")
    mock_conn.cursor.return_value.__enter__.return_value.execute.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_context.log.info.assert_any_call("Data from file /tmp/parquet_files/file1.parquet loaded successfully.")

@patch("glob.glob")
@patch("pandas.read_parquet")
@patch("psycopg2.connect")
def test_load_parquet_files_missing_columns(mock_connect, mock_read_parquet, mock_glob, loader, mock_context):
    """Test handling of Parquet files with missing columns."""
    # Mock the Parquet files
    mock_glob.return_value = ["/tmp/parquet_files/file1.parquet"]
    mock_read_parquet.return_value = pd.DataFrame({
        "uuid": ["123"],
        "pickup_date": ["2023-01-01"],
        "total_passenger_count": [2],
    })  # Missing required columns
    mock_connect.return_value = MagicMock()

    loader.load_parquet_files(mock_context)

    mock_context.log.info.assert_any_call(
        "File /tmp/parquet_files/file1.parquet is missing required columns: "
        "['total_distance', 'total_fare', 'avg_trip_distance', 'avg_fare_amount']. Skipping."
    )

@patch("glob.glob")
@patch("pandas.read_parquet")
@patch("psycopg2.connect")
def test_load_parquet_files_no_files(mock_connect, mock_read_parquet, mock_glob, loader, mock_context):
    """Test handling when no Parquet files are found."""
    mock_glob.return_value = []
    mock_connect.return_value = MagicMock()

    loader.load_parquet_files(mock_context)

    mock_context.log.info.assert_called_with("No Parquet files found in directory: /tmp/parquet_files")