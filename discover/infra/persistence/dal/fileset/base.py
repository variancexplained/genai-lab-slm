#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/fileset/base.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 06:05:13 pm                                              #
# Modified   : Wednesday December 18th 2024 03:26:03 am                                            #
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

from discover.infra.persistence.dal.base import DAL
from discover.infra.persistence.dal.fileset.exception import FileIOException
from discover.infra.persistence.dal.fileset.location import FilesetLocationService


# ------------------------------------------------------------------------------------------------ #
class FilesetDAL(DAL):
    def __init__(
        self, storage_config: dict, location_service: FilesetLocationService
    ) -> None:
        self._storage_config = storage_config
        self._location_service = location_service
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def get_filepath(self, name: str) -> str:
        """Returns a filepath for the specified file name.

        Note, the name has no file extension. It will default to .parquet and overriden
        in subclasses as required.

        Args:
            name (str): Name of the file for which a relative filepath is returned.

        Returns:
            str: Relative filepath

        """
        filename = f"{name}.{self._storage_config.get('format', 'parquet')}"
        filepath = self._location_service.get_filepath(filename=filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        return filepath

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
