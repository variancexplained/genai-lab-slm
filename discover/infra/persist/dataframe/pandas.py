#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/dataframe/pandas.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Monday December 23rd 2024 08:47:37 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Pandas File Access Object Module"""
from __future__ import annotations

from typing import Type

import pandas as pd

from discover.asset.dataset.dataset import FileFormat
from discover.infra.exception.file import FileIOException
from discover.infra.persist.dataframe.base import FAO
from discover.infra.persist.dataframe.base import DataFrameReader as BaseDataFrameReader
from discover.infra.persist.dataframe.base import DataFrameWriter as BaseDataFrameWriter


# ------------------------------------------------------------------------------------------------ #
#                                     DATAFRAME READER                                             #
# ------------------------------------------------------------------------------------------------ #
class DataFrameReader(BaseDataFrameReader):
    """
    A reader class for loading data into Pandas DataFrames from various file formats.

    This class provides methods for reading Parquet and CSV files into Pandas DataFrames,
    handling exceptions and providing meaningful error messages when issues occur.

    Methods:
        parquet: Reads a Parquet file into a Pandas DataFrame.
        csv: Reads a CSV file into a Pandas DataFrame.
    """

    @classmethod
    def parquet(cls, filepath: str, **kwargs) -> pd.DataFrame:
        """
        Reads a Parquet file into a Pandas DataFrame.

        Args:
            filepath (str): The path to the Parquet file.
            **kwargs: Additional keyword arguments passed to `pandas.read_parquet`.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the data from the Parquet file.

        Raises:
            FileNotFoundError: If the specified Parquet file does not exist.
            FileIOException: If any other exception occurs while reading the file.
        """
        super().parquet(filepath=filepath, **kwargs)
        try:
            return pd.read_parquet(filepath, **kwargs)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}. File does not exist.\n{e}"
            raise FileNotFoundError(msg)
        except Exception as e:
            msg = (
                f"Exception occurred while reading a Parquet file from {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e

    @classmethod
    def csv(cls, filepath: str, **kwargs) -> pd.DataFrame:
        """
        Reads a CSV file into a Pandas DataFrame.

        Args:
            filepath (str): The path to the CSV file.
            **kwargs: Additional keyword arguments passed to `pandas.read_csv`.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the data from the CSV file.

        Raises:
            FileNotFoundError: If the specified CSV file does not exist.
            FileIOException: If any other exception occurs while reading the file.
        """
        super().csv(filepath=filepath, **kwargs)
        try:
            return pd.read_csv(filepath, **kwargs)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a CSV file from {filepath}. File does not exist.\n{e}"
            raise FileNotFoundError(msg)
        except Exception as e:
            msg = f"Exception occurred while reading a CSV file from {filepath}.\n{e}"
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
#                                    DATAFRAME WRITER                                              #
# ------------------------------------------------------------------------------------------------ #
class DataFrameWriter(BaseDataFrameWriter):
    """
    A writer class for saving Pandas DataFrames to various file formats.

    This class provides methods for writing data to Parquet and CSV files, handling
    exceptions and ensuring robust error reporting.
    """

    @classmethod
    def parquet(
        cls, data: pd.DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """
        Writes the data to a Parquet file at the designated filepath.

        Args:
            data (pd.DataFrame): The Pandas DataFrame to write to the Parquet file.
            filepath (str): The path where the Parquet file will be saved.
            overwrite (bool): Whether to overwrite existing data. Defaults to False.
            **kwargs: Additional keyword arguments passed to `pandas.DataFrame.to_parquet`.

        Raises:
            FileIOException: If an error occurs while writing the Parquet file.
        """
        super().parquet(data=data, filepath=filepath, overwrite=overwrite, **kwargs)
        try:
            data.to_parquet(filepath, **kwargs)
        except Exception as e:
            msg = (
                f"Exception occurred while creating a Parquet file at {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e

    @classmethod
    def csv(
        cls, data: pd.DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """
        Writes the data to a CSV file at the designated filepath.

        Args:
            data (pd.DataFrame): The Pandas DataFrame to write to the CSV file.
            filepath (str): The path where the CSV file will be saved.
            overwrite (bool): Whether to overwrite existing data. Defaults to False.
            **kwargs: Additional keyword arguments passed to `pandas.DataFrame.to_csv`.

        Raises:
            FileIOException: If an error occurs while writing the CSV file.
        """
        super().csv(data=data, filepath=filepath, overwrite=overwrite, **kwargs)
        try:
            data.to_csv(filepath, **kwargs)
        except Exception as e:
            msg = f"Exception occurred while creating a CSV file at {filepath}.\n{e}"
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
#                                       PANDAS FAO                                                 #
# ------------------------------------------------------------------------------------------------ #
class PandasFAO(FAO):
    """
    A File Access Object (FAO) for handling Pandas DataFrames.

    This class provides methods to read and write Pandas DataFrames in various file formats,
    including Parquet and CSV. The default `reader` and `writer` classes can be customized
    by providing subclasses of `DataFrameReader` and `DataFrameWriter`.

    Attributes:
        _reader (Type[DataFrameReader]): The class responsible for reading data.
        _writer (Type[DataFrameWriter]): The class responsible for writing data.
    """

    def __init__(
        self,
        reader: Type[DataFrameReader] = DataFrameReader,
        writer: Type[DataFrameWriter] = DataFrameWriter,
    ) -> None:
        """
        Initializes the PandasFAO with a reader and writer.

        Args:
            reader (Type[DataFrameReader]): A class implementing the interface for reading data.
                Defaults to `DataFrameReader`.
            writer (Type[DataFrameWriter]): A class implementing the interface for writing data.
                Defaults to `DataFrameWriter`.
        """
        self._reader = reader
        self._writer = writer

    def read(
        self, filepath: str, file_format: FileFormat = FileFormat.PARQUET, **kwargs
    ) -> pd.DataFrame:
        """
        Reads data from the specified file into a Pandas DataFrame.

        Args:
            filepath (str): The path to the file to read.
            file_format (FileFormat): The format of the file to read (e.g., CSV, PARQUET).
                Defaults to `FileFormat.PARQUET`.
            **kwargs: Additional arguments passed to the reader class.

        Returns:
            pd.DataFrame: The data read from the file as a Pandas DataFrame.

        Raises:
            ValueError: If the specified `file_format` is not supported.
        """
        if file_format == FileFormat.CSV:
            return self._reader.csv(filepath=filepath, **kwargs)
        elif file_format == FileFormat.PARQUET:
            return self._reader.parquet(filepath=filepath, **kwargs)
        else:
            raise ValueError(f"Unrecognized file_format: {file_format}")

    def write(
        self,
        filepath: str,
        data: pd.DataFrame,
        file_format: FileFormat = FileFormat.PARQUET,
        **kwargs,
    ) -> None:
        """
        Writes a Pandas DataFrame to the specified file.

        Args:
            filepath (str): The path to the file to write.
            data (pd.DataFrame): The Pandas DataFrame to write.
            file_format (FileFormat): The format of the file to write (e.g., CSV, PARQUET).
                Defaults to `FileFormat.PARQUET`.
            **kwargs: Additional arguments passed to the writer class.

        Raises:
            ValueError: If the specified `file_format` is not supported.
        """
        if file_format == FileFormat.CSV:
            self._writer.csv(filepath=filepath, data=data, **kwargs)
        elif file_format == FileFormat.PARQUET:
            return self._writer.parquet(filepath=filepath, data=data, **kwargs)
        else:
            raise ValueError(f"Unrecognized file_format: {file_format}")
