#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/fao/base.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 06:05:13 pm                                              #
# Modified   : Saturday October 12th 2024 09:09:06 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
import os
import shutil
from typing import Union

import pandas as pd
import pyspark.sql

from discover.infra.dal.base import DAO
from discover.infra.dal.fao.exception import FileIOException


# ------------------------------------------------------------------------------------------------ #
class FileSystemFAO(DAO):
    """
    A Data Access Object (DAO) class for interacting with the filesystem, providing methods
    to create, read, delete, and check the existence of files or directories. This class
    abstracts the underlying file operations and allows the actual read and write operations
    to be implemented by subclasses.

    Attributes:
        _logger (logging.Logger): A logger instance for logging messages.
    """

    def __init__(self) -> None:
        """
        Initializes the FileSystemFAO instance, setting up a logger.
        """
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(
        self,
        filepath: str,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        **kwargs,
    ) -> None:
        """
        Creates a file at the specified filepath with the provided data.

        This method ensures that the directory for the filepath exists before
        delegating the actual write operation to the `_write` method of a subclass.

        Args:
            filepath (str): The path where the file should be created.
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The data to be written
                to the file.
            **kwargs: Additional keyword arguments to be passed to the `_write` method.
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self._write(filepath=filepath, data=data, **kwargs)

    def read(
        self, filepath: str, **kwargs
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        """
        Reads data from a file at the specified filepath.

        This method delegates the actual reading operation to the `_read` method
        of a subclass.

        Args:
            filepath (str): The path of the file to be read.
            **kwargs: Additional keyword arguments to be passed to the `_read` method.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: The data read from the file.
        """
        return self._read(filepath=filepath, **kwargs)

    def delete(self, filepath: str, **kwargs) -> None:
        """
        Deletes a file or directory at the specified filepath.

        If the filepath is a directory, it deletes the entire directory tree. If it's
        a file, it deletes the file. Logs an exception and raises a `FileIOException`
        if an error occurs during deletion.

        Args:
            filepath (str): The path of the file or directory to be deleted.
            **kwargs: Additional keyword arguments for logging purposes.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            FileIOException: If an unknown exception occurs during deletion.
        """
        if os.path.isdir(filepath):
            try:
                shutil.rmtree(filepath)
            except Exception as e:
                msg = f"Exception occurred while deleting from {filepath}."
                self._logger.exception(msg)
                raise FileIOException(msg, e) from e
        else:
            try:
                os.remove(path=filepath)
            except FileNotFoundError as e:
                msg = f"Exception occurred while deleting from {filepath}. File does not exist.\nKeyword Arguments: {kwargs}.\n{e}"
                self._logger.exception(msg)
                raise
            except Exception as e:
                msg = f"Unknown exception occurred while deleting file from {filepath}. Keyword Arguments: {kwargs}."
                self._logger.exception(msg)
                raise FileIOException(msg, e) from e

    def exists(self, filepath: str) -> bool:
        """
        Checks if a file or directory exists at the specified filepath.

        Args:
            filepath (str): The path of the file or directory to check.

        Returns:
            bool: True if the file or directory exists, False otherwise.
        """
        return os.path.exists(filepath)
