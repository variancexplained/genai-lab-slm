#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/file/base.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 06:05:13 pm                                              #
# Modified   : Tuesday September 24th 2024 02:12:17 pm                                             #
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


# ------------------------------------------------------------------------------------------------ #
class FileSystemDAO(DAO):
    """
    A Data Access Object (DAO) responsible for interacting with a file system, allowing
    the creation, reading, and deletion of files stored in either Pandas DataFrame or
    PySpark DataFrame formats. The file paths used by the DAO are environment-specific,
    derived from environment-agnostic paths generated externally (e.g., by a builder).

    The DAO converts the agnostic file path into an environment-specific path by prepending
    the appropriate base directory for the environment, as defined in the configuration.

    Attributes:
        _logger (logging.Logger): Logger instance for error logging and tracking.

    Methods:
        create(filepath, data, **kwargs):
            Creates a new file at the specified filepath, converting the provided
            environment-agnostic path to an environment-specific path.

        read(filepath, **kwargs):
            Reads a file from the file system, converting the provided environment-agnostic
            path to an environment-specific path.

        delete(filepath, **kwargs):
            Deletes a file from the file system, converting the provided environment-agnostic
            path to an environment-specific path.

        _format_filepath(filepath):
            Converts an environment-agnostic file path into an environment-specific file path
            by prepending the base directory.
    """

    def __init__(self) -> None:
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(
        self,
        filepath: str,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        **kwargs,
    ) -> None:
        """
        Creates a new file at the specified filepath. The provided filepath is environment-agnostic
        and will be converted to an environment-specific filepath. Raises a FileExistsError if
        the file already exists.

        Args:
            filepath (str): The environment-agnostic file path.
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The data to be written, either
                as a Pandas or PySpark DataFrame.
            **kwargs: Additional keyword arguments passed to the file writing method.

        Raises:
            FileExistsError: If a file with the same name already exists in the specified location.
        """

        # Check if the file already exists before writing
        if os.path.exists(filepath):
            msg = f"File {os.path.basename(filepath)} already exists in {os.path.dirname(filepath)}."
            self._logger.error(msg)
            raise FileExistsError(msg)

        # ^ The actual writing operation is delegated to the `_write` method of subclass.
        self._write(filepath=filepath, data=data, **kwargs)

    def read(
        self, filepath: str, **kwargs
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        """
        Reads a file from the specified filepath. The provided filepath is environment-agnostic
        and will be converted to an environment-specific filepath. Raises a FileNotFoundError if
        the file does not exist.

        Args:
            filepath (str): The environment-agnostic file path.
            **kwargs: Additional keyword arguments passed to the file reading method.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: The data read from the file, either as
            a Pandas or PySpark DataFrame.

        Raises:
            FileNotFoundError: If the specified file does not exist.
        """
        # Check if the file exists before reading
        if os.path.exists(filepath):
            return self._read(filepath=filepath, **kwargs)
        else:
            msg = f"File {os.path.basename(filepath)} does not exist in {os.path.dirname(filepath)}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    def delete(self, filepath: str, **kwargs) -> None:
        """
        Deletes a file or directory from the specified filepath. The provided filepath is
        environment-agnostic and will be converted to an environment-specific filepath. If the
        filepath points to a directory, the entire directory tree is removed. Raises an exception
        if an unknown error occurs during deletion.

        Args:
            filepath (str): The environment-agnostic file path.
            **kwargs: Additional keyword arguments for the delete operation.

        Raises:
            Exception: If an unknown error occurs during file or directory deletion.
        """

        # Check if it's a directory and remove the directory tree
        if os.path.isdir(filepath):
            shutil.rmtree(filepath, ignore_errors=True)
        else:
            try:
                os.remove(path=filepath)  # Remove the file if it exists
            except Exception as e:
                msg = f"Unknown exception occurred while deleting file {os.path.basename(filepath)} in {os.path.dirname(filepath)}.\n{e}"
                self._logger.exception(msg)
                raise
