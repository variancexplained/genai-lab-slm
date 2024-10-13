#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/incubator/dynamics/task/file/download.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday July 4th 2024 05:40:36 pm                                                  #
# Modified   : Sunday October 13th 2024 02:09:42 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Download File Module"""

import os
from typing import Optional

from discover.dynamics.base.task import Task
from discover.infra.persistence.cloud.aws import S3Handler


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

    __ESTAGE = DataPrepStageDef.RAW

    def __init__(
        self,
        aws_bucket_name: str,
        aws_folder: str,
        aws_s3_key: str,
        local_folder: str,
        local_name: Optional[str] = None,
        s3h_cls: type[S3Handler] = S3Handler,
        force: bool = False,
    ) -> None:
        """
        Initializes the DownloadFileTask.

        Loads environment variables, sets up the S3 handler and file system handler, and configures the task parameters.
        """
        super().__init__(stage=self.__STAGE)
        self._aws_bucket_name = aws_bucket_name
        self._aws_folder = aws_folder
        self._aws_s3_key = aws_s3_key
        self._local_folder = local_folder
        self._local_name = local_name
        self._s3h_cls = s3h_cls
        self._force = force

    def run(self) -> str:
        """Executes the task to download the file from the S3 bucket to the local directory.

        Downloads the file from the specified S3 bucket and key to the local directory.
        If `force` is True, the download occurs even if the file already exists locally.

        Returns
            The path to which the file was downloaded.
        """
        filename = self._local_name or self._aws_s3_key
        filepath = os.path.join(self._local_folder, filename)
        if self._force or not os.path.exists(filepath):
            self._s3h.download_file(
                bucket_name=self._aws_bucket_name,
                s3_key=self._aws_s3_key,
                local_path=filepath,
                force=self._force,
            )
            return filepath
        else:
            msg = f"Download aborted. A file at {filepath} already exists."
            self.logger.exception(msg)
            raise FileExistsError(msg)
