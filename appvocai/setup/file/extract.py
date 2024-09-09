#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/setup/file/extract.py                                                     #
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
"""Extract File Module"""
import os

from appvocai.shared.persist.file.compression import TarGzHandler
from prefect import Task


# ------------------------------------------------------------------------------------------------ #
class ExtractFileTask(Task):
    """
    Task for extracting a tar.gz file to a specified destination directory.

    Args:
        source (str): The path to the source tar.gz file.
        destination (str): The path to the destination directory where the files will be extracted.
        targz_cls (type[TarGzHandler]): The class for handling tar.gz file operations. Defaults to TarGzHandler.
        force (bool, optional): If True, forces the extraction even if the destination directory already exists. Defaults to False.
    """

    def __init__(
        self,
        source: str,
        destination: str,
        targz_cls: type[TarGzHandler] = TarGzHandler,
        force: bool = False,
    ) -> None:
        """
        Initializes the ExtractFileTask.

        Sets up the source and destination paths, initializes the TarGzHandler, and configures the task parameters.
        """
        super().__init__()
        self._source = source
        self._destination = destination
        self._targz = targz_cls()
        self._force = force

    def run(self) -> None:
        """Executes the task to extract the tar.gz file to the destination directory.

        Extracts the contents of the tar.gz file from the specified source path to the destination directory.
        If `force` is True, the extraction occurs even if the destination directory already exists.
        """
        if self._force or not os.path.exists(self._destination):
            self._targz.extract(tar_gz_path=self._source, extract_dir=self._destination)
