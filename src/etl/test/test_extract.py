import os
import pytest
from unittest.mock import MagicMock, patch
from src.etl.resources.ts_resources import ParquetDownloadResource

@pytest.fixture
def mock_context():
    """Fixture to create a mock Dagster context."""
    context = MagicMock()
    context.log.info = MagicMock()
    context.log.error = MagicMock()
    return context


@pytest.fixture
def parquet_resource():
    """Fixture to create a ParquetDownloadResource instance."""
    return ParquetDownloadResource(
        base_url="https://example.com/trip-data/yellow_tripdata",
        years=[2023],
        months=[1, 2],
        download_folder="/tmp/test_downloads"
    )


def test_ensure_download_folder_exists_creates_folder(mock_context, parquet_resource):
    """Test that the download folder is created if it does not exist."""
    with patch("os.makedirs") as mock_makedirs, patch("os.path.exists", return_value=False):
        parquet_resource.ensure_download_folder_exists(mock_context)
        mock_makedirs.assert_called_once_with(parquet_resource.download_folder, exist_ok=True)
        mock_context.log.info.assert_called_with(f"Created download folder at {parquet_resource.download_folder}")


def test_ensure_download_folder_exists_folder_exists(mock_context, parquet_resource):
    """Test that the method logs a message if the folder already exists."""
    with patch("os.path.exists", return_value=True):
        parquet_resource.ensure_download_folder_exists(mock_context)
        mock_context.log.info.assert_called_with(f"Download folder already exists at {parquet_resource.download_folder}")


@patch("httpx.Client.get")
def test_download_parquet_data_success(mock_get, mock_context, parquet_resource):
    """Test successful download of Parquet files."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"test content"
    mock_get.return_value = mock_response

    with patch("builtins.open", new_callable=MagicMock) as mock_open, patch("os.path.join", side_effect=lambda *args: "/".join(args)):
        parquet_resource.download_parquet_data(mock_context)

        assert mock_get.call_count == len(parquet_resource.years) * len(parquet_resource.months)
        mock_open.assert_called()
        mock_context.log.info.assert_any_call("Download complete: yellow_tripdata_2023-01.parquet")


@patch("httpx.Client.get")
def test_download_parquet_data_failure(mock_get, mock_context, parquet_resource):
    """Test failure to download Parquet files."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    with patch("os.path.join", side_effect=lambda *args: "/".join(args)):
        parquet_resource.download_parquet_data(mock_context)

        assert mock_get.call_count == len(parquet_resource.years) * len(parquet_resource.months)
        mock_context.log.error.assert_any_call("Failed to download yellow_tripdata_2023-01.parquet. Status code: 404")