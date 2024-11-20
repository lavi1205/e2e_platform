import os
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unittest import mock
from etl.transform import transform_parquet_files, process_parquet_file

@pytest.fixture
def mock_parquet_file(tmpdir):
    """
    Fixture to create a mock Parquet file for testing.
    """
    file_path = tmpdir.join("yellow_tripdata_2024-07.parquet")

    # Sample data for the Parquet file
    data = {
        'VendorID': [1, 2],
        'tpep_pickup_datetime': ['2024-07-01', '2024-07-02'],
        'tpep_dropoff_datetime': ['2024-07-01', '2024-07-02'],
        'passenger_count': [1, 2],
        'trip_distance': [1.5, 2.5],
        'fare_amount': [10.0, 20.0]
    }

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)

    # Write the DataFrame to a Parquet file
    pq.write_table(table, file_path)

    return str(file_path)

@pytest.fixture
def output_folder(tmpdir):
    """
    Fixture for creating a temporary output directory.
    """
    return tmpdir.mkdir("output")

@mock.patch('transform.logger')
def test_process_parquet_file(mock_logger, mock_parquet_file, output_folder):
    """
    Test the process_parquet_file function with a mock Parquet file.
    """
    result = process_parquet_file(mock_parquet_file, output_folder)

    # Check if the function returns the correct success message
    assert result == f"Successfully processed {os.path.basename(mock_parquet_file)}"

    # Check if the output file is created
    output_file_name = f"summary_{os.path.basename(mock_parquet_file)}"
    output_file_path = os.path.join(output_folder, output_file_name)
    assert os.path.exists(output_file_path)

    # Verify that the logger logged the correct information
    assert (f"Processing file: {os.path.basename(mock_parquet_file)}")
    assert (f"Saved summary to Parquet file: {output_file_name}")

@mock.patch('transform.logger')
def test_transform_parquet_files(mock_logger, mock_parquet_file, output_folder):
    """
    Test the transform_parquet_files function to ensure multiple files are processed correctly.
    """
    with mock.patch('os.listdir', return_value=[os.path.basename(mock_parquet_file)]):
        with mock.patch('transform.process_parquet_file', return_value="Successfully processed mock file"):
            transform_parquet_files(os.path.dirname(mock_parquet_file), output_folder, max_workers=2)

    # Check if logger detected files and processed them
    assert ("Found 1 Parquet files to process.")
    assert ("Successfully processed mock file")

def test_empty_source_folder(tmpdir, output_folder):
    """
    Test transform_parquet_files when the source folder is empty.
    """
    source_folder = tmpdir.mkdir("empty_source")

    # Mock the logger
    with mock.patch('transform.logger') as mock_logger:
        transform_parquet_files(source_folder, output_folder, max_workers=2)

    # Ensure the logger logs that no files were found
    assert ("Found 0 Parquet files to process.")

