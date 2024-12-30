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
# Modified   : Sunday December 29th 2024 03:31:44 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataFrame Persistence Base Module"""
from __future__ import annotations

import os
from abc import abstractmethod
from typing import Union

import pandas as pd
import pyspark
import pyspark.sql

from discover.infra.persist.file.base import Reader, Writer

# ------------------------------------------------------------------------------------------------ #
DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                                 DATAFRAME READER BASE CLASS                                      #
# ------------------------------------------------------------------------------------------------ #
class DataFrameReader(Reader):
    """
    Base class for reading data into a DataFrame.

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
    def read(cls, filepath: str, **kwargs) -> DataFrame:
        """Defines the interface for reading data into a DataFrame.

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


# ------------------------------------------------------------------------------------------------ #
#                               DATAFRAME WRITER BASE CLASS                                        #
# ------------------------------------------------------------------------------------------------ #
class DataFrameWriter(Writer):
    """Base class for writing data from a DataFrame to a file."""

    @classmethod
    @abstractmethod
    def write(
        cls, data: DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """
        Writes data from a DataFrame to a file.

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

    @classmethod
    def validate_write(cls, filepath: str, overwrite: bool, **kwargs) -> None:
        """Validates the write operation can proceed.

        This centralizes and enforces the overwrite policy should the file exist.

        Args:
            filepath (str): Write path
            overwrite (bool): Indicates whether existing file can be overwritten.
            kwargs (dict): Arbitrary keyword arguments.

        Raises:
            FileExistsError if overwrite is False and a file exists at the
             designated filepath.

        """
        if not overwrite and os.path.exists(filepath):
            raise FileExistsError(
                f"File exists at {filepath}. To overwrite this file, set overwrite argument to True."
            )
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
