import logging
import tempfile
import urllib.request
from typing import List, Tuple
from urllib.parse import urlparse

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

    def transform_data(self, data: str) -> str:
        """Transform data (e.g., convert to uppercase).

        Args:
            data (str): The input data string.

        Returns:
            str: The transformed data string.
        """
        return data

    def download_data(self, url: str) -> str:
        """
        Downloads data from a URL to a temporary local file.

        Args:
            url (str): The URL to download the data from.

        Returns:
            str: The path to the temporary local file.
        """
        self.logger.info(f"Downloading data from {url}...")
        try:
            # Check if the input is a URL and download it if so.
            # This is necessary because Beam's ReadFromText doesn't directly support HTTP.
            # We add a User-Agent header to avoid 403 Forbidden errors from servers
            # that block requests from default Python user agents.
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".csv") as tmp_file:
                with urllib.request.urlopen(req) as response:
                    content = response.read().decode("utf-8")
                    tmp_file.write(content)
                self.logger.info(f"Data downloaded to temporary file: {tmp_file.name}")
                return tmp_file.name
        except Exception as e:
            self.logger.error(f"Failed to download data from {url}: {e}")
            raise
