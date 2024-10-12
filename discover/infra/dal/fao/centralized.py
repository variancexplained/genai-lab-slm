#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/fao/centralized.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Saturday October 12th 2024 01:09:16 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Module for the Centralized File System (CFS) Data Access Layer"""
import pandas as pd

from discover.infra.dal.fao.base import FileSystemDAO
from discover.infra.dal.fao.exception import FileIOException


# ------------------------------------------------------------------------------------------------ #
class CentralizedFileSystemDAO(FileSystemDAO):
    """
    Data Access Object (DAO) for interacting with a centralized file system using Pandas.

    This class handles reading and writing Parquet files using Pandas DataFrames for storage
    systems that are accessible as a centralized file system (e.g., local filesystem, NFS).

    Inherits from:
        FileSystemDAO: Base class for file system-based data access operations.
    """

    def __init__(self) -> None:
        """
        Initializes the CentralizedFileSystemDAO.

        This constructor sets up the base functionality for handling file I/O
        using the centralized file system.
        """
        super().__init__()

    def _read(self, filepath: str, **kwargs) -> pd.DataFrame:
        """
        Reads a Parquet file from a specified filepath into a Pandas DataFrame.

        Uses `pd.read_parquet` to read the specified Parquet file. If an error occurs
        during the read operation, an exception is raised.

        Args:
            filepath (str): The path of the Parquet file to read.
            **kwargs: Additional keyword arguments to pass to the `pd.read_parquet` method,
                such as options for columns, engine, etc.

        Returns:
            pd.DataFrame: The DataFrame containing the data read from the Parquet file.

        Raises:
            FileIOException: If an error occurs while reading the Parquet file.
        """
        try:
            return pd.read_parquet(path=filepath, **kwargs)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}.File does not exist.\nKeyword Arguments: {kwargs}\n{e}"
            self._logger.exception(msg)
            raise
        except Exception as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}.\nKeyword Arguments: {kwargs}"
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e

    def _write(self, filepath: str, data: pd.DataFrame, **kwargs) -> None:
        """
        Writes a Pandas DataFrame to a specified filepath as a Parquet file.

        Uses `pd.DataFrame.to_parquet` to write the DataFrame to the specified file.
        If an error occurs during the write process, an exception is raised.

        Args:
            filepath (str): The path where the Parquet file will be written.
            data (pd.DataFrame): The DataFrame to write to the Parquet file.
            **kwargs: Additional keyword arguments to configure the `to_parquet` method,
                such as compression, engine, etc.

        Raises:
            FileIOException: If an error occurs while writing the Parquet file.
        """
        try:
            data.to_parquet(path=filepath, **kwargs)
        except Exception as e:
            msg = f"Exception occurred while writing a Parquet file to {filepath}.\nKeyword Arguments: {kwargs}"
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e
