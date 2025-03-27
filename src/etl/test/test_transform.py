import os
# Removed unused import
import pandas as pd
import pyarrow as pa
import pytest
from unittest.mock import MagicMock
from src.etl.resources.transform_resource import ParquetTransformResource
from dagster import AssetExecutionContext

import pyarrow.parquet as pq


@pytest.fixture
def mock_context():
    """Fixture for mocking the Dagster AssetExecutionContext."""
    context = MagicMock(spec=AssetExecutionContext)
    context.log.info = MagicMock()
    context.log.error = MagicMock()
    return context


@pytest.fixture
def sample_parquet_file(tmp_path):
    """Fixture to create a sample Parquet file for testing."""
    data = {
        "tpep_pickup_datetime": ["2023-01-01 10:00:00", "2023-01-01 11:00:00", "2023-02-01 12:00:00"],
        "passenger_count": [1, 2, 3],
        "trip_distance": [1.5, 2.5, 3.5],
        "fare_amount": [10.0, 20.0, 30.0],
    }
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    file_path = tmp_path / "test-01.parquet"
    pq.write_table(table, file_path)
    return file_path


@pytest.fixture
def transform_resource(tmp_path):
    """Fixture to create a ParquetTransformResource instance."""
    return ParquetTransformResource(
        source_folder=str(tmp_path),
        output_folder=str(tmp_path / "output"),
        max_workers=2,
    )


def test_ensure_staging_folder_exists(transform_resource, mock_context, tmp_path):
    """Test the ensure_staging_folder_exists method."""
    output_folder = tmp_path / "output"
    transform_resource = ParquetTransformResource(
        source_folder=transform_resource.source_folder,
        output_folder=str(output_folder),
        max_workers=transform_resource.max_workers,
    )

    # Ensure folder does not exist initially
    assert not output_folder.exists()

    # Call the method
    transform_resource.ensure_staging_folder_exists(mock_context)

    # Verify folder is created
    assert output_folder.exists()
    mock_context.log.info.assert_called_with(f"Created output folder: {output_folder}")
    transform_resource = ParquetTransformResource(
        source_folder=transform_resource.source_folder,
        output_folder=str(tmp_path / "output"),
        max_workers=transform_resource.max_workers,
    )

def test_process_parquet_file(transform_resource, mock_context, sample_parquet_file, tmp_path):
    """Test the process_parquet_file method."""
    transform_resource = ParquetTransformResource(
        source_folder=transform_resource.source_folder,
        output_folder=str(tmp_path / "output"),
        max_workers=transform_resource.max_workers,
    )
    os.makedirs(transform_resource.output_folder, exist_ok=True)

    # Call the method
    result = transform_resource.process_parquet_file(str(sample_parquet_file), mock_context)

    # Verify the output file is created
    output_file_name = f"summary_{os.path.basename(sample_parquet_file)}"
    output_file_path = os.path.join(transform_resource.output_folder, output_file_name)
    assert os.path.exists(output_file_path)

    # Verify the content of the output file
    summary = pq.read_table(output_file_path).to_pandas()
    assert "uuid" in summary.columns
    assert "pickup_date" in summary.columns
    assert "total_passenger_count" in summary.columns
    assert "total_distance" in summary.columns
    assert "total_fare" in summary.columns
    assert "avg_trip_distance" in summary.columns
    assert "avg_fare_amount" in summary.columns

    transform_resource = ParquetTransformResource(
        source_folder=str(tmp_path),
        output_folder=str(tmp_path / "output"),
        max_workers=transform_resource.max_workers,
    )
    mock_context.log.info.assert_any_call(f"Saved summary to Parquet file: {output_file_name}")
    assert result == f"Successfully processed {os.path.basename(sample_parquet_file)}"
    transform_resource = ParquetTransformResource(
        source_folder=str(tmp_path),
        output_folder=str(tmp_path / "output"),
        max_workers=transform_resource.max_workers,
    )
def test_transform_parquet_files(transform_resource, mock_context, sample_parquet_file, tmp_path):
    """Test the transform_parquet_files method."""
    transform_resource = ParquetTransformResource(
        source_folder=str(tmp_path),
        output_folder=str(tmp_path / "output"),
        max_workers=transform_resource.max_workers,
    )
    os.makedirs(transform_resource.output_folder, exist_ok=True)

    # Call the method
    transform_resource.transform_parquet_files(mock_context)

    # Verify the output file is created
    output_file_name = f"summary_{os.path.basename(sample_parquet_file)}"
    output_file_path = os.path.join(transform_resource.output_folder, output_file_name)
    assert os.path.exists(output_file_path)

    # Verify log messages
    mock_context.log.info.assert_any_call(f"Found 1 Parquet files to process.")
    mock_context.log.info.assert_any_call(f"Successfully processed {os.path.basename(sample_parquet_file)}")
    mock_context.log.info.assert_any_call(f"Transformation complete. Summaries saved in {transform_resource.output_folder}")