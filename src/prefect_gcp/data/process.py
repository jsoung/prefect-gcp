import logging
import os
from typing import List, Tuple

import pandas as pd
from google.cloud import storage


class Process:
    """Encapsulates core data processing logic."""

    def __init__(self) -> None:
        """Initialize the Process class."""
        self.gcs_client = storage.Client()

        # Set up logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())

        self.logger.info(f"Using Google Cloud Project: {self.gcs_client.project}")

    def extract_data(self) -> List[str]:
        """Simulate data extraction.

        Returns:
            List[str]: A list of extracted data strings.
        """
        return ["data1", "data2", "data3"]

    def transform_data(self, data: str) -> str:
        """Transform data (e.g., convert to uppercase).

        Args:
            data (str): The input data string.

        Returns:
            str: The transformed data string.
        """
        return data.upper()

    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Load data from a file path (local or GCS URL).

        Args:
            file_path (str): The file path to load data from. Can be a local path or a GCS URL.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the loaded data.
        """
        if file_path.startswith("gs://"):
            # Handle GCS URL
            bucket_name, blob_prefix = self._parse_gcs_path(file_path)
            blobs = self.gcs_client.list_blobs(bucket_name, prefix=blob_prefix)
            csv_data = []
            for blob in blobs:
                if blob.name.endswith(".csv"):
                    print(f"Reading GCS file: {blob.name}")
                    content = blob.download_as_text()
                    csv_data.append(pd.read_csv(pd.compat.StringIO(content)))
            return pd.concat(csv_data, ignore_index=True)
        else:
            # Handle local file path
            if os.path.isdir(file_path):
                # Read all part files in the directory
                csv_files = [os.path.join(file_path, f) for f in os.listdir(file_path) if f.endswith(".csv")]
                return pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
            elif os.path.isfile(file_path) and file_path.endswith(".csv"):
                # Read a single CSV file
                return pd.read_csv(file_path)
            else:
                raise ValueError(f"Invalid file path: {file_path}")

    def _parse_gcs_path(self, gcs_path: str) -> Tuple[str, str]:
        """
        Parse a GCS path into bucket name and blob prefix.

        Args:
            gcs_path (str): The GCS path (e.g., "gs://bucket-name/path/to/files").

        Returns:
            Tuple[str, str]: A tuple containing the bucket name and blob prefix.
        """
        if not gcs_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {gcs_path}")
        parts = gcs_path[5:].split("/", 1)
        bucket_name = parts[0]
        blob_prefix = parts[1] if len(parts) > 1 else ""
        return bucket_name, blob_prefix
