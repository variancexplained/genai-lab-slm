#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/infra/persist/cloud/kaggle.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 12th 2024 03:09:28 pm                                              #
# Modified   : Saturday January 25th 2025 04:40:43 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import json
import os
import subprocess

import pandas as pd
from genailabslm.infra.utils.file.compress import ZipFileHandler
from genailabslm.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                     KAGGLE SERVICE                                               #
# ------------------------------------------------------------------------------------------------ #
class KaggleService:
    def __init__(
        self,
        username: str,
        io: type[IOService] = IOService,
        zip_file_handler: type[ZipFileHandler] = ZipFileHandler,
    ):
        """
        Initialize the KaggleService with the username, IOService, and ZipFileHandler.

        Args:
            username (str): Your Kaggle username.
            io_service (IOService): An instance of IOService for writing JSON.
            zip_file_handler (ZipFileHandler): An instance of ZipFileHandler for compressing and decompressing files.
        """
        self._username = username
        self._io = io
        self._zip_file_handler = zip_file_handler()

    def upload(
        self, filepath: str, title: str, dataset_name: str, private: bool = True
    ):
        """
        Upload a file to Kaggle as a dataset.

        Args:
            filepath (str): The path to the file to upload.
            title (str): The title of the dataset.
            dataset_name (str): The name of the dataset.
            private (bool): Whether the dataset should be private. Defaults to True.
        """
        # Create metadata for the dataset
        metadata = {
            "title": title,
            "id": f"{self._username}/{dataset_name}",
            "licenses": [{"name": "CC0-1.0"}],
            "isPrivate": private,
        }
        # Save the metadata to a JSON file in the same directory as the file
        metadata_filepath = os.path.join(
            os.path.dirname(filepath), "dataset-metadata.json"
        )
        self._io.write(metadata_filepath, metadata)

        # Compress the file
        zip_path = os.path.splitext(filepath)[0] + ".zip"
        self._zip_file_handler.compress_file(filepath=filepath, zippath=zip_path)

        # Remove the non-zipped file to avoid uploading it
        os.remove(path=filepath)

        # Check if the zipped file exists, then upload using subprocess
        if os.path.exists(zip_path):
            subprocess.run(
                ["kaggle", "datasets", "create", "-p", os.path.dirname(zip_path)],
                check=True,
            )
        else:
            raise FileNotFoundError(f"Compressed file {zip_path} not found for upload.")

    def download(self, dataset_name: str, filepath: str):
        """
        Download a dataset from Kaggle.

        Args:
            dataset_name (str): The name of the dataset to download (username/dataset-name).
            filepath (str): The path where the dataset should be downloaded.
        """
        # Download the dataset using the Kaggle API
        subprocess.run(
            ["kaggle", "datasets", "download", "-d", dataset_name, "-p", filepath],
            check=True,
        )

        # Check if a zipped file was downloaded and decompress it
        zip_path = os.path.join(filepath, f"{dataset_name.split('/')[-1]}.zip")
        if os.path.exists(zip_path):
            self._zip_file_handler.extract(zippath=zip_path, extract_to=filepath)
            os.remove(zip_path)  # Remove the zipped file

    def update(self, dataset_name: str, filepath: str, update_message: str):
        """
        Update an existing dataset on Kaggle.

        Args:
            dataset_name (str): The name of the dataset to update (username/dataset-name).
            filepath (str): The path to the updated file.
            update_message (str): A message describing the update.
        """
        # Compress the file
        zip_path = os.path.splitext(filepath)[0] + ".zip"
        self._zip_file_handler.compress_file(filepath=filepath, zippath=zip_path)

        # Check if the zipped file exists, then update using subprocess
        if os.path.exists(zip_path):
            subprocess.run(
                [
                    "kaggle",
                    "datasets",
                    "version",
                    "-p",
                    os.path.dirname(zip_path),
                    "-m",
                    update_message,
                ],
                check=True,
            )
        else:
            raise FileNotFoundError(f"Compressed file {zip_path} not found for update.")

    def exists(self, dataset_name: str) -> bool:
        """
        Check if a dataset exists on Kaggle.

        Args:
            dataset_name (str): The name of the dataset (username/dataset-name).

        Returns:
            bool: True if the dataset exists, False otherwise.
        """
        try:
            # Use the Kaggle API to list the dataset and check if it exists
            result = subprocess.run(
                ["kaggle", "datasets", "list", "--search", dataset_name],
                capture_output=True,
                text=True,
                check=True,
            )
            # Check if the dataset name appears in the output
            return dataset_name in result.stdout
        except subprocess.CalledProcessError:
            # If the Kaggle API command fails, return False
            return False


# ------------------------------------------------------------------------------------------------ #
#                                  KAGGLE CREATE DATASET                                           #
# ------------------------------------------------------------------------------------------------ #
def create_dataset(username: str, df: pd.DataFrame, filename: str, title: str):
    """
    Create a Kaggle dataset from a DataFrame in the Kaggle environment.

    Args:
        df (pd.DataFrame): The DataFrame to save and upload as a dataset.
        filename (str): The name of the file to save the DataFrame as (e.g., 'sentiment_analysis.csv').
        title (str): The title of the dataset.
    """

    # Save the DataFrame to a CSV file in the working directory
    csv_path = os.path.join("/kaggle/working", filename)
    df.to_csv(csv_path, index=False)

    # Get the file name without the extension for the dataset ID
    dataset_name = os.path.splitext(filename)[0]

    # Create the metadata dictionary
    metadata = {
        "title": title,
        "id": f"{username}/{dataset_name}",
        "licenses": [{"name": "CC0-1.0"}],
    }

    # Save the metadata to a JSON file in the same directory as the file
    metadata_filepath = os.path.join("/kaggle/working", "dataset-metadata.json")
    with open(metadata_filepath, "w") as f:
        json.dump(metadata, f)

    # Use the Kaggle API to create the dataset
    subprocess.run(
        ["kaggle", "datasets", "create", "-p", "/kaggle/working"], check=True
    )

    print(f"Dataset '{title}' created successfully on Kaggle.")
