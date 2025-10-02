import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from prefect_gcp.data.process import Process


@pytest.fixture
def process():
    """Fixture to create a Process instance."""
    return Process()


def test_extract_data(process):
    """Test the extract_data method."""
    data = process.extract_data()
    assert data == ["data1", "data2", "data3"]


def test_transform_data(process):
    """Test the transform_data method."""
    transformed = process.transform_data("hello")
    assert transformed == "HELLO"


@patch("prefect_gcp.data.process.storage.Client")
def test_load_data_local(mock_storage_client, process, tmp_path):
    """Test the load_data method for a local file."""
    # Create a temporary CSV file
    csv_file = tmp_path / "test.csv"
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    df.to_csv(csv_file, index=False)

    # Load the data
    loaded_df = process.load_data(str(csv_file))
    pd.testing.assert_frame_equal(loaded_df, df)


@patch("prefect_gcp.data.process.storage.Client")
def test_load_data_gcs(mock_storage_client, process):
    """Test the load_data method for a GCS file."""
    # Mock GCS client and blob
    mock_blob = MagicMock()
    mock_blob.name = "test.csv"
    mock_blob.download_as_text.return_value = "col1,col2\n1,a\n2,b\n3,c"
    mock_storage_client.return_value.list_blobs.return_value = [mock_blob]

    # Load the data
    loaded_df = process.load_data("gs://test-bucket/test-folder/")
    expected_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    pd.testing.assert_frame_equal(loaded_df, expected_df)