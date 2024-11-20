import os
import pytest
from unittest.mock import MagicMock, patch
from etl.upload import upload_to_s3_batch
from botocore.exceptions import NoCredentialsError

# Fixture to set up mock S3 client
@pytest.fixture
def mock_s3_client():
    return MagicMock()

# Fixture to set up a temporary folder with mock Parquet files
@pytest.fixture
def tmp_folder(tmpdir):
    # Create mock Parquet files in the temp directory
    for i in range(15):  # Create 15 files to test batch processing
        tmpdir.join(f"file_{i}.parquet").write("content")
    return tmpdir

# Test case: Successful batch upload with multiple files
def test_upload_success(mock_s3_client, tmp_folder):
    # Patch logger to avoid logging during tests
    with patch('upload.logger') as mock_logger:
        bucket_name = 'test-bucket'
        s3_folder = 'test-folder'

        # Call the upload function with batch size of 5
        result = upload_to_s3_batch(mock_s3_client, tmp_folder, bucket_name, s3_folder, batch_size=5)

        # Check that upload_file was called 15 times (once for each file)
        assert mock_s3_client.upload_file.call_count == 15

        # Ensure each file was uploaded with correct S3 key
        for i in range(15):
            mock_s3_client.upload_file.assert_any_call(
                os.path.join(tmp_folder, f"file_{i}.parquet"),
                bucket_name,
                os.path.join(s3_folder, f"file_{i}.parquet")
            )

        # Check the final result message
        assert result == f"All batches uploaded successfully to {bucket_name}."

        # Ensure that logging occurred
        assert ("Processing batch 1: ['file_0.parquet', 'file_1.parquet', 'file_2.parquet', 'file_3.parquet', 'file_4.parquet']")
        assert ("Successfully uploaded file_0.parquet to S3 bucket test-bucket.")

# Test case: No Parquet files in the source folder
def test_no_files_to_upload(mock_s3_client, tmpdir):
    # Patch logger to avoid logging during tests
    with patch('upload.logger') as mock_logger:
        result = upload_to_s3_batch(mock_s3_client, tmpdir, 'test-bucket', 'test-folder')

        # Check that no files were uploaded
        assert mock_s3_client.upload_file.call_count == 0

        # Check the return value
        assert result == "No files to upload."

        # Ensure error logging occurred
        assert ("No Parquet files found in the source folder.")

# Test case: File not found error during upload
def test_file_not_found(mock_s3_client, tmp_folder):
    # Patch logger to avoid logging during tests
    with patch('upload.logger') as mock_logger:
        # Simulate FileNotFoundError for the first file
        mock_s3_client.upload_file.side_effect = [FileNotFoundError, None] * 14

        result = upload_to_s3_batch(mock_s3_client, tmp_folder, 'test-bucket', 'test-folder', batch_size=2)

        # Check that all files were attempted to upload
        assert mock_s3_client.upload_file.call_count == 15
    


# Test case: AWS NoCredentialsError during upload
def test_credentials_error(mock_s3_client, tmp_folder):
    # Patch logger to avoid logging during tests
    with patch('upload.logger') as mock_logger:
        # Simulate NoCredentialsError
        mock_s3_client.upload_file.side_effect = NoCredentialsError

        result = upload_to_s3_batch(mock_s3_client, tmp_folder, 'test-bucket', 'test-folder', batch_size=5)
        # Check that no files were uploaded due to credentials error
        assert result == "Credentials error occurred."
