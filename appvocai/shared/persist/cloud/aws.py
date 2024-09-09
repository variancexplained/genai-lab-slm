#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/utils/aws.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday June 20th 2024 05:25:35 am                                                 #
# Modified   : Thursday June 20th 2024 06:26:08 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Amazon AWS Module"""
import boto3
from botocore.config import Config
import os
from tqdm import tqdm
from dotenv import load_dotenv

# ------------------------------------------------------------------------------------------------ #
load_dotenv()


# ------------------------------------------------------------------------------------------------ #
class S3Handler:
    """
    A class to handle operations with Amazon AWS S3 buckets, including downloading and uploading files.

    Attributes:
        s3_client (boto3.client): A low-level client representing Amazon Simple Storage Service (S3).

    Example:
        Initialize the handler and use various methods:

        ```python
        handler = S3Handler(aws_access_key_id='your_access_key_id',
                            aws_secret_access_key='your_secret_access_key',
                            region_name='your_region',
                            config={'read_timeout': 60, 'retries': {'max_attempts': 10}})

        # Download a file
        handler.download_file('my-bucket', 'path/to/myfile.csv', '/local/path/to/myfile.csv')

        # Upload a file
        handler.upload_file('/local/path/to/myfile.csv', 'my-bucket', 'path/to/myfile.csv')
        ```

        Download an entire folder:

        ```python
        handler.download_folder('my-bucket', 'path/to/myfolder/', '/local/path/to/myfolder/')
        ```

        Upload an entire folder:

        ```python
        handler.upload_folder('/local/path/to/myfolder/', 'my-bucket', 'path/to/myfolder/')
        ```
    """

    def __init__(
        self,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region_name=None,
        config=None,
    ):
        """
        Initializes the S3Handler with optional AWS credentials and configuration.

        Args:
            aws_access_key_id (str, optional): AWS access key ID.
            aws_secret_access_key (str, optional): AWS secret access key.
            region_name (str, optional): AWS region name.
            config (dict, optional): Dictionary containing configuration options such as read timeout and retries.
        """
        aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY")
        aws_secret_access_key = aws_secret_access_key or os.getenv(
            "AWS_SECRET_ACCESS_KEY"
        )
        region_name = region_name or os.getenv("AWS_REGION")

        # Define default configuration settings if none are provided
        default_config = Config(
            read_timeout=60, retries={"max_attempts": 5, "mode": "standard"}
        )

        # Override default configuration with user-provided settings if available
        if config:
            custom_config = Config(**config)
            default_config = default_config.merge(custom_config)

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            config=default_config,
        )

    def download_file(
        self, bucket_name: str, s3_key: str, local_path: str, force: bool = False
    ):
        """
        Downloads a file from an S3 bucket to a local path.

        Args:
            bucket_name (str): Name of the S3 bucket.
            s3_key (str): The key of the file in the S3 bucket.
            local_path (str): The local path where the file should be saved.
            force (bool): Whether to donwload the file if it already exists.

        Raises:
            Exception: If there is an error downloading the file.
        """
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            if not os.path.exists(local_path) or force:
                self.s3_client.download_file(bucket_name, s3_key, local_path)
                print(f"Downloaded {s3_key} from {bucket_name} to {local_path}")
            else:
                print(f"{local_path} was not downloaded, as it already exists.")

        except Exception as e:
            print(f"Error downloading {s3_key} from {bucket_name}: {e}")

    def download_folder(
        self, bucket_name: str, s3_folder: str, local_folder: str, force: bool = False
    ):
        """
        Downloads a folder from an S3 bucket to a local directory.

        Args:
            bucket_name (str): Name of the S3 bucket.
            s3_folder (str): The folder path in the S3 bucket.
            local_folder (str): The local directory where the files should be saved.
            force (bool): Whether to donwload the file if it already exists.

        Raises:
            Exception: If there is an error downloading the folder.
        """
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_folder):
                if "Contents" in page:
                    for obj in tqdm(page["Contents"]):
                        s3_key = obj["Key"]
                        local_path = os.path.join(
                            local_folder, os.path.relpath(s3_key, s3_folder)
                        )
                        if not os.path.exists(local_path) or force:
                            self.download_file(bucket_name, s3_key, local_path)
                            print(
                                f"Downloaded {local_path} from {bucket_name}/{s3_key}"
                            )
                        else:
                            print(
                                f"{local_path} was not downloaded, as it already exists."
                            )

        except Exception as e:
            print(f"Error downloading folder {s3_folder} from {bucket_name}: {e}")

    def upload_file(
        self, local_path: str, bucket_name: str, s3_key: str, force: bool = False
    ):
        """
        Uploads a file to an S3 bucket.

        Args:
            local_path (str): The local file path to upload.
            bucket_name (str): Name of the S3 bucket.
            s3_key (str): The key of the file in the S3 bucket.
            force (bool): Whether to upload / overwrite the file if it already exists.

        Raises:
            Exception: If there is an error uploading the file.
        """
        try:
            if not self.file_exists(bucket_name=bucket_name, s3_key=s3_key) or force:
                self.s3_client.upload_file(local_path, bucket_name, s3_key)
                print(f"Uploaded {local_path} to {bucket_name}/{s3_key}")
            else:
                print(f"File {s3_key} already exists in {bucket_name} bucket.")
        except Exception as e:
            print(f"Error uploading {local_path} to {bucket_name}/{s3_key}: {e}")

    def upload_folder(
        self, local_folder: str, bucket_name: str, s3_folder: str, force: bool = False
    ):
        """
        Uploads a local folder and its contents to an S3 bucket.

        Args:
            local_folder (str): The local folder path to upload.
            bucket_name (str): Name of the S3 bucket.
            s3_folder (str): The folder path in the S3 bucket.
            force (bool): Whether to upload / overwrite the file if it already exists.

        Raises:
            Exception: If there is an error uploading the folder.
        """
        try:
            for root, dirs, files in os.walk(local_folder):
                for file in tqdm(files):
                    local_path = os.path.join(root, file)
                    s3_key = os.path.join(
                        s3_folder, os.path.relpath(local_path, local_folder)
                    )
                    if (
                        not self.file_exists(bucket_name=bucket_name, s3_key=s3_key)
                        or force
                    ):
                        self.upload_file(local_path, bucket_name, s3_key)
                    else:
                        print(f"File {s3_key} already exists in {bucket_name} bucket.")
        except Exception as e:
            print(
                f"Error uploading folder {local_folder} to {bucket_name}/{s3_folder}: {e}"
            )

    def create_bucket(self, bucket_name, region=None) -> bool:
        """
        Creates a new S3 bucket with the specified name.

        Args:
            bucket_name (str): Name of the S3 bucket to create.
            region (str, optional): AWS region where the bucket should be created. Defaults to None (uses the default
                                    region of the session).

        Returns:
            bool: True if the bucket was created successfully or already exists, False otherwise.
        """
        try:
            if region is None:
                region = self.s3_client.meta.region_name
            self.s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
            print(f"Bucket '{bucket_name}' created successfully in region '{region}'.")
            return True
        except Exception as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "BucketAlreadyOwnedByYou":
                print(f"Bucket '{bucket_name}' already exists and is owned by you.")
                return True
            elif error_code == "BucketAlreadyExists":
                print(
                    f"Bucket '{bucket_name}' already exists and is owned by another account."
                )
                return True
            else:
                print(f"Error creating bucket '{bucket_name}': {e}")
                return False

    def file_exists(self, bucket_name, s3_key) -> bool:
        """
        Checks if a file exists in an S3 bucket.

        Args:
            bucket_name (str): Name of the S3 bucket.
            s3_key (str): The key of the file in the S3 bucket.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            return True
        except Exception as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                return False
            else:
                print(
                    f"Error checking file existence for {s3_key} in {bucket_name}: {e}"
                )
                return False

    def folder_exists(self, bucket_name, s3_folder) -> bool:
        """
        Checks if a folder exists in an S3 bucket.

        Args:
            bucket_name (str): Name of the S3 bucket.
            s3_folder (str): The folder path in the S3 bucket.

        Returns:
            bool: True if the folder exists, False otherwise.
        """
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=bucket_name, Prefix=s3_folder, Delimiter="/"
            ):
                if "CommonPrefixes" in page or "Contents" in page:
                    return True
            return False
        except Exception as e:
            print(
                f"Error checking folder existence for {s3_folder} in {bucket_name}: {e}"
            )
            return False

    def bucket_exists(self, bucket_name) -> bool:
        """
        Checks if a bucket exists in AWS S3.

        Args:
            bucket_name (str): Name of the S3 bucket.

        Returns:
            bool: True if the bucket exists, False otherwise.
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except Exception as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                return False
            else:
                print(f"Error checking bucket existence for {bucket_name}: {e}")
                return False

    def delete_file(self, bucket_name, s3_key):
        """
        Deletes a file from an S3 bucket.

        Args:
            bucket_name (str): Name of the S3 bucket.
            s3_key (str): The key of the file in the S3 bucket.

        Returns:
            bool: True if the file was deleted successfully, False otherwise.
        """
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
            print(f"File '{s3_key}' deleted from bucket '{bucket_name}'.")
            return True
        except Exception as e:
            print(f"Error deleting file '{s3_key}' from bucket '{bucket_name}': {e}")
            return False

    def delete_folder(self, bucket_name, s3_folder):
        """
        Deletes a folder and its contents from an S3 bucket.

        Args:
            bucket_name (str): Name of the S3 bucket.
            s3_folder (str): The folder path in the S3 bucket.

        Returns:
            bool: True if the folder was deleted successfully, False otherwise.
        """
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            objects_to_delete = []
            for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_folder):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects_to_delete.append({"Key": obj["Key"]})

            if objects_to_delete:
                self.s3_client.delete_objects(
                    Bucket=bucket_name, Delete={"Objects": objects_to_delete}
                )
                print(
                    f"Folder '{s3_folder}' and its contents deleted from bucket '{bucket_name}'."
                )
            return True
        except Exception as e:
            print(
                f"Error deleting folder '{s3_folder}' from bucket '{bucket_name}': {e}"
            )
            return False
