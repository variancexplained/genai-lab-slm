#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/setup/file/download.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday July 4th 2024 05:40:36 pm                                                  #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Download File Module"""

import os

from appvocai.shared.persist.cloud.aws import S3Handler
from prefect import Task


# ------------------------------------------------------------------------------------------------ #
class DownloadFileTask(Task):
    """
    Task for downloading a file from an S3 bucket to a local directory.

    Args:
        s3h_cls (type[S3Handler]): The class for handling S3 interactions. Defaults to S3Handler.
        bucket_name (str, optional): The name of the S3 bucket. If not provided, it is loaded from environment variables.
        s3_key (str, optional): The S3 key of the file to download. If not provided, it is loaded from environment variables.
        local_download_folder (str, optional): The local directory where the file will be downloaded. If not provided, it is loaded from environment variables.
        force (bool, optional): If True, forces the download even if the file already exists locally. Defaults to False.
    """

    def __init__(
        self,
        aws_access_key: str,
        aws_secret_access_key: str,
        aws_region_name: str,
        aws_bucket_name: str,
        aws_folder: str,
        aws_s3_key: str,
        local_download_folder: str,
        local_download_filepath: str,
        s3h_cls: type[S3Handler] = S3Handler,
        force: bool = False,
    ) -> None:
        """
        Initializes the DownloadFileTask.

        Loads environment variables, sets up the S3 handler and file system handler, and configures the task parameters.
        """
        super().__init__()

        self._s3h = s3h_cls(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name,
        )
        self._bucket_name = aws_bucket_name
        self._s3_key = aws_s3_key
        self._local_download_folder = local_download_folder
        self._force = force

    def run(self) -> str:
        """Executes the task to download the file from the S3 bucket to the local directory.

        Downloads the file from the specified S3 bucket and key to the local directory.
        If `force` is True, the download occurs even if the file already exists locally.

        Returns
            The path to which the file was downloaded.
        """
        filepath = os.path.join(self._local_download_folder, self._s3_key)
        if self._force or not os.path.exists(filepath):
            self._s3h.download_file(
                bucket_name=self._bucket_name,
                s3_key=self._s3_key,
                local_path=self._local_download_folder,
                force=self._force,
            )
        return filepath
