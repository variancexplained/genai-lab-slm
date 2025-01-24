#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/repo/file/pandas.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Thursday January 23rd 2025 09:38:30 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Pandas File Access Object Module"""
from __future__ import annotations

import logging

import pandas as pd

from discover.core.dtypes import DTYPES
from discover.infra.exception.file import FileIOException
from discover.infra.persist.repo.file.base import DataFrameReader as BaseDataFrameReader
from discover.infra.persist.repo.file.base import DataFrameWriter as BaseDataFrameWriter


# ------------------------------------------------------------------------------------------------ #
#                                    DATAFRAME READERS                                             #
# ------------------------------------------------------------------------------------------------ #
class PandasDataFrameParquetReader(BaseDataFrameReader):
    """A reader class for loading dataframe into Pandas DataFrames from parquet files."""

    def __init__(self, kwargs: dict) -> None:
        self._kwargs = kwargs
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def read(self, filepath: str, **kwargs) -> pd.DataFrame:
        """
        Reads a Parquet file into a Pandas DataFrame.

        Args:
            filepath (str): The path to the Parquet file.
            **kwargs: Additional keyword arguments passed to `pandas.read_parquet`.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the dataframe from the Parquet file.

        Raises:
            FileNotFoundError: If the specified Parquet file does not exist.
            FileIOException: If any other exception occurs while reading the file.
        """
        try:
            df = pd.read_parquet(filepath, **self._kwargs)
            msg = f"{self.__class__.__name__} read from {filepath}"
            self._logger.debug(msg)
            return df.astype(DTYPES)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}. File does not exist.\n{e}"
            raise FileNotFoundError(msg)
        except Exception as e:
            msg = (
                f"Exception occurred while reading a Parquet file from {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
class PandasDataFrameCSVReader(BaseDataFrameReader):
    """A reader class for loading dataframe into Pandas DataFrames from csv files."""

    def __init__(self, kwargs: dict) -> None:
        self._kwargs = kwargs
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def read(self, filepath: str, **kwargs) -> pd.DataFrame:
        """
        Reads a CSV file into a Pandas DataFrame.

        Args:
            filepath (str): The path to the CSV file.
            **kwargs: Additional keyword arguments passed to `pandas.read_csv`.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the dataframe from the CSV file.

        Raises:
            FileNotFoundError: If the specified CSV file does not exist.
            FileIOException: If any other exception occurs while reading the file.
        """
        try:
            df = pd.read_csv(filepath, **self._kwargs)
            msg = f"{self.__class__.__name__} read from {filepath}"
            self._logger.debug(msg)
            return df.astype(DTYPES)

        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a CSV file from {filepath}. File does not exist.\n{e}"
            raise FileNotFoundError(msg)
        except Exception as e:
            msg = f"Exception occurred while reading a CSV file from {filepath}.\n{e}"
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
#                                  DATAFRAME WRITERS                                               #
# ------------------------------------------------------------------------------------------------ #
class PandasDataFrameParquetWriter(BaseDataFrameWriter):
    """Writes a pandas DataFrame to a parquet file."""

    def __init__(self, kwargs: dict) -> None:
        self._kwargs = kwargs
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def write(
        self,
        dataframe: pd.DataFrame,
        filepath: str,
        overwrite: bool = False,
    ) -> None:
        """
        Writes the dataframe to a Parquet file at the designated filepath.

        Args:
            dataframe (pd.DataFrame): The Pandas DataFrame to write to the Parquet file.
            filepath (str): The path where the Parquet file will be saved.
            overwrite (bool): Whether to overwrite existing dataframe. Defaults to False.
            **kwargs: Additional keyword arguments passed to `pandas.DataFrame.to_parquet`.

        Raises:
            FileIOException: If an error occurs while writing the Parquet file.
        """
        self.validate_write(filepath=filepath, overwrite=overwrite, **self._kwargs)
        try:
            dataframe.to_parquet(filepath, **self._kwargs)
            msg = f"{self.__class__.__name__} wrote to {filepath}"
            self._logger.debug(msg)
        except Exception as e:
            msg = (
                f"Exception occurred while creating a Parquet file at {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
class PandasDataFrameCSVWriter(BaseDataFrameWriter):
    """Writes a pandas DataFrame to a csv file."""

    def __init__(self, kwargs: dict) -> None:
        self._kwargs = kwargs
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def write(
        self,
        dataframe: pd.DataFrame,
        filepath: str,
        overwrite: bool = False,
    ) -> None:
        """
        Writes the dataframe to a CSV file at the designated filepath.

        Args:
            dataframe (pd.DataFrame): The Pandas DataFrame to write to the CSV file.
            filepath (str): The path where the CSV file will be saved.
            overwrite (bool): Whether to overwrite existing dataframe. Defaults to False.
            **kwargs: Additional keyword arguments passed to `pandas.DataFrame.to_csv`.

        Raises:
            FileIOException: If an error occurs while writing the CSV file.
        """
        self.validate_write(filepath=filepath, overwrite=overwrite, **self._kwargs)
        try:
            dataframe.to_csv(filepath, **self._kwargs)
            msg = f"{self.__class__.__name__} wrote to {filepath}"
            self._logger.debug(msg)
        except Exception as e:
            msg = f"Exception occurred while creating a CSV file at {filepath}.\n{e}"
            raise FileIOException(msg, e) from e
