#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/persist/repo/file/base.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 26th 2024 02:37:54 pm                                             #
# Modified   : Saturday February 8th 2025 10:43:32 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Access Layer Base Module"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any, Union

import pandas as pd
import pyspark
import pyspark.sql

# ------------------------------------------------------------------------------------------------ #
DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                                       IO FACTORY                                                 #
# ------------------------------------------------------------------------------------------------ #


class IOFactory(ABC):
    """Defines the interface for factories that return objects that perform IO.

    This encapsulates IO for a range of data structures, such as DataFrames, arrays,
    and dictionaries.

    """

    @classmethod
    @abstractmethod
    def get_reader(cls, *args, **kwargs) -> Reader:
        """Returns a reader based on args, and kwargs defined in subclasses."""

    @classmethod
    @abstractmethod
    def get_writer(cls, *args, **kwargs) -> Writer:
        """Returns a writer based on args, and kwargs defined in subclasses."""


# ------------------------------------------------------------------------------------------------ #
#                                       READER                                                     #
# ------------------------------------------------------------------------------------------------ #
class Reader(ABC):

    @classmethod
    @abstractmethod
    def read(cls, filepath: str, **kwargs) -> Any:
        """Reads data from a file.

        Args:
            filepath (str): Path to the file to be read.
            **kwargs (dict): Keyword arguments passed to the underlying read mechanism.
        """


# ------------------------------------------------------------------------------------------------ #
#                                       WRITER                                                     #
# ------------------------------------------------------------------------------------------------ #
class Writer(ABC):

    @classmethod
    @abstractmethod
    def write(cls, filepath: str, data: Any, **kwargs) -> Any:
        """Writes data to a file

        Args:
            filepath (str): Path to the file to be read.
            data (Any): DAta to  be written to file.
            **kwargs (dict): Keyword arguments passed to the underlying write mechanism.
        """


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
