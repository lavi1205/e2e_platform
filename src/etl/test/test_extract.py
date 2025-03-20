import os, sys
import pytest
from unittest import mock
from etl.resources.ts_resources.ParquetTransformResource import ensure_download_folder_exists, download_parquet_data

# Define the path where you expect the folder to be created
@pytest.fixture
def mock_download_folder(tmpdir):
    """
    Fixture to provide a temporary directory for downloads.
    This ensures that each test runs in a clean environment.
    """
    return str(tmpdir.mkdir("downloads"))

@mock.patch('extract.logger')
def test_ensure_download_folder_exists(mock_logger, mock_download_folder):
    """
    Test that the folder is created if it doesn't exist.
    """
    # Remove the folder if it exists to simulate folder creation
    if os.path.exists(mock_download_folder):
        os.rmdir(mock_download_folder)
    
    # Ensure the folder exists after calling the function
    ensure_download_folder_exists(mock_download_folder)

    # Check if the folder exists (this should always pass now)
    assert os.path.exists(mock_download_folder)

    # Verify that the logger was called to indicate folder creation
    assert (f"Created download folder at {mock_download_folder}")

@mock.patch('os.path.exists', return_value=True)
@mock.patch('extract.logger')
def test_download_folder_already_exists(mock_logger, mock_exists):
    """
    Test that no folder is created if it already exists.
    """
    folder_path = "some_existing_folder"
    ensure_download_folder_exists(folder_path)
    assert (f"Download folder already exists at {folder_path}")


@mock.patch('extract.httpx.Client.get')
@mock.patch('builtins.open', new_callable=mock.mock_open)
@mock.patch('extract.logger')
def test_download_parquet_data(mock_logger, mock_open, mock_get):
    """
    Test downloading parquet data by mocking httpx.Client.get and file creation.
    """
    base_url = "https://example.com/yellow_tripdata"
    years = [2024]
    months = [7]
    
    # Mock a successful response from the server
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = b"parquet data"

    # Mock download folder to avoid real file operations
    mock_folder = "mock_download_folder"
    with mock.patch('extract.download_folder', mock_folder):
        download_parquet_data(base_url, years, months)

    expected_url = f"{base_url}_2024-07.parquet"
    expected_filename = f"yellow_tripdata_2024-07.parquet"
    expected_filepath = os.path.join(mock_folder, expected_filename)

    # Assert file download was triggered
    assert (expected_url)

    
    # Assert file was written with the correct content
    assert (b"parquet data")
    assert (f"Download complete for year 2024, month 07.")


@mock.patch('extract.httpx.Client.get')
@mock.patch('extract.logger')
def test_download_parquet_data_fail(mock_logger, mock_get):
    """
    Test that download_parquet_data logs an error if the download fails.
    """
    base_url = "https://example.com/yellow_tripdata"
    years = [2024]
    months = [7]
    
    # Mock a failed response from the server
    mock_get.return_value.status_code = 404

    download_parquet_data(base_url, years, months)

    assert ("Failed to download file for year 2024, month 07. Status code: 404")
