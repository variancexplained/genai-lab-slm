#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/dataframe/base.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Tuesday December 24th 2024 12:21:43 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataFrame Persistence Base Module"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Union

import pandas as pd
import pyspark
import pyspark.sql

# ------------------------------------------------------------------------------------------------ #
DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                                           FAO                                                    #
# ------------------------------------------------------------------------------------------------ #
class FAO(ABC):
    def __init__(self, reader: DataFrameReader, writer: DataFrameWriter):
        self._reader = reader
        self._writer = writer

    @abstractmethod
    def read(self, filepath: str, **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def write(self, filepath: str, data: DataFrame, **kwargs) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                 DATAFRAME READER BASE CLASS                                      #
# ------------------------------------------------------------------------------------------------ #
class DataFrameReader(ABC):
    """
    Abstract base class for reading data into a DataFrame.

    This class defines a common interface for reading CSV and Parquet files into
    either a Pandas or PySpark DataFrame. Additional file formats can be supported
    by extending this class.

    Supported DataFrame types:
        - Pandas DataFrame
        - PySpark DataFrame

    Implementations must handle file-specific options provided via `**kwargs`.
    """

    @classmethod
    @abstractmethod
    def csv(cls, filepath: str, **kwargs) -> DataFrame:
        """
        Reads data from a CSV file into a DataFrame.

        Args:
            filepath (str): Path to the CSV file.
            **kwargs: Additional file-specific options (e.g., delimiter, header, schema).

        Returns:
            DataFrame: A Pandas or PySpark DataFrame containing the data.

        Raises:
            FileNotFoundError: If the file at the specified path does not exist.
            ValueError: If the file cannot be parsed as CSV.
        """
        pass

    @classmethod
    @abstractmethod
    def parquet(cls, filepath: str, **kwargs) -> DataFrame:
        """
        Reads data from a Parquet file into a DataFrame.

        Args:
            filepath (str): Path to the Parquet file.
            **kwargs: Additional file-specific options (e.g., schema, compression).

        Returns:
            DataFrame: A Pandas or PySpark DataFrame containing the data.

        Raises:
            FileNotFoundError: If the file at the specified path does not exist.
            ValueError: If the file cannot be parsed as Parquet.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
#                               DATAFRAME WRITER BASE CLASS                                        #
# ------------------------------------------------------------------------------------------------ #
class DataFrameWriter(ABC):
    """
    Abstract base class for writing data from a DataFrame to a file.

    This class defines a common interface for writing data to CSV and Parquet files
    from either a Pandas or PySpark DataFrame. Additional file formats can be supported
    by extending this class.

    Supported DataFrame types:
        - Pandas DataFrame
        - PySpark DataFrame
    """

    @classmethod
    @abstractmethod
    def csv(
        cls, data: DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """
        Writes data from a DataFrame to a CSV file.

        Args:
            data (DataFrame): A Pandas or PySpark DataFrame containing the data to write.
            filepath (str): Path where the CSV file will be saved.
            overwrite (bool): Whether to overwrite existing files. Default is False.
            **kwargs: Additional file-specific options (e.g., delimiter, header, compression).

        Returns:
            None

        Raises:
            FileExistsError: If ovewrite is False and the file already exists.
            IOError: If there is an issue writing to the specified file path.
            ValueError: If the DataFrame cannot be written as CSV.
        """
        if not overwrite and os.path.exists(filepath):
            raise FileExistsError(
                f"Overwrite mode is False and a file already exists at {filepath}"
            )

    @classmethod
    @abstractmethod
    def parquet(
        cls, data: DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """
        Writes data from a DataFrame to a Parquet file.

        Args:
            data (DataFrame): A Pandas or PySpark DataFrame containing the data to write.
            filepath (str): Path where the Parquet file will be saved.
            overwrite (bool): Whether to overwrite existing files. Default is False.
            **kwargs: Additional file-specific options (e.g., compression, partitioning).

        Returns:
            None

        Raises:
            FileExistsError: If ovewrite is False and the file already exists.
            IOError: If there is an issue writing to the specified file path.
            ValueError: If the DataFrame cannot be written as Parquet.
        """
        if not overwrite and os.path.exists(filepath):
            raise FileExistsError(
                f"Overwrite mode is False and a file already exists at {filepath}"
            )
