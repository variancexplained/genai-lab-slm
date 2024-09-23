#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/file/centralized.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Sunday September 22nd 2024 07:38:19 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Module for the Centralized File System (CFS) Data Access Layer"""
import pandas as pd

from discover.infra.config.reader import ConfigReader
from discover.infra.dal.file.base import FileSystemDAO
from discover.infra.storage.local.io import ParquetPandasIO


# ------------------------------------------------------------------------------------------------ #
class CentralizedFileSystemDAO(FileSystemDAO):
    """
    A specialized Data Access Object (DAO) for interacting with a centralized file system,
    handling data in Pandas DataFrame format. This class inherits from the FileSystemDAO and
    implements reading and writing of Parquet files using Pandas.

    The centralized file system is environment-specific, and the paths passed to the
    create, read, and delete methods are converted from environment-agnostic paths
    using the base directory set in the configuration.

    Attributes:
        _logger (logging.Logger): Inherited from FileSystemDAO, used for logging errors and tracking operations.
        _basedir (str): Inherited from FileSystemDAO, defines the base directory for file operations.

    Methods:
        _read(filepath, **kwargs):
            Reads a Parquet file from the centralized file system into a Pandas DataFrame.

        _write(filepath, data, **kwargs):
            Writes a Pandas DataFrame to a Parquet file in the centralized file system.
    """

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
        """
        Initializes the CentralizedFileSystemDAO by calling the parent FileSystemDAO constructor.
        The base directory is set using the configuration reader class.

        Args:
            config_reader_cls (type[ConfigReader], optional): The configuration reader class
                responsible for retrieving the base directory for centralized storage. Defaults
                to ConfigReader.
        """
        super().__init__(config_reader_cls=config_reader_cls)

    def _read(self, filepath: str, **kwargs) -> pd.DataFrame:
        """
        Reads a Parquet file from the centralized file system and loads it into a Pandas DataFrame.

        Args:
            filepath (str): The full path to the Parquet file.
            **kwargs: Additional keyword arguments for reading the Parquet file.

        Returns:
            pd.DataFrame: The data read from the file, loaded into a Pandas DataFrame.
        """
        return ParquetPandasIO._read(filepath=filepath, **kwargs)

    def _write(self, filepath: str, data: pd.DataFrame, **kwargs) -> None:
        """
        Writes a Pandas DataFrame to a Parquet file in the centralized file system.

        Args:
            filepath (str): The full path where the Parquet file will be written.
            data (pd.DataFrame): The Pandas DataFrame containing the data to be written.
            **kwargs: Additional keyword arguments for writing the Parquet file.
        """
        ParquetPandasIO._write(filepath=filepath, data=data, **kwargs)
