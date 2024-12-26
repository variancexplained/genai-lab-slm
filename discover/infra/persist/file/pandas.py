#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/file/pandas.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Thursday December 26th 2024 01:39:28 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Pandas File Access Object Module"""
from __future__ import annotations

import pandas as pd

from discover.infra.exception.file import FileIOException
from discover.infra.persist.file.base import FAO
from discover.infra.persist.file.base import DataFrameReader as BaseDataFrameReader
from discover.infra.persist.file.base import DataFrameWriter as BaseDataFrameWriter


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
#                                        PANDAS FAO                                                #
# ------------------------------------------------------------------------------------------------ #
class PandasFAO(FAO):
    """File Access Object (FAO) implementation for Pandas-based environments.

    This class extends the base `FAO` to manage data files using Pandas for local,
    in-memory data processing. It supports reading and writing CSV and Parquet files
    and is suited for environments that do not require distributed computing.

    Args:
        workspace_location (str): Base directory for the FAO workspace.
        directory (str): Subdirectory for data management.
        fao_config (dict): Configuration dictionary containing CSV and Parquet settings.
        reader (DataFrameReader): Object responsible for reading data files (default is `DataFrameReader`).
        writer (DataFrameWriter): Object responsible for writing data files (default is `DataFrameWriter`).
        **kwargs: Additional keyword arguments for customization.
    """

    def __init__(
        self,
        workspace_location: str,
        directory: str,
        fao_config: dict,
        reader: DataFrameReader = DataFrameReader,
        writer: DataFrameWriter = DataFrameWriter,
        **kwargs,
    ) -> None:
        super().__init__(
            workspace_location=workspace_location,
            directory=directory,
            fao_config=fao_config,
            reader=reader,
            writer=writer,
            **kwargs,
        )
