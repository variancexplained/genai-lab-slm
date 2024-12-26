#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/file/base.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Thursday December 26th 2024 07:54:01 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataFrame Persistence Base Module"""
from __future__ import annotations

import logging
import os
import shutil
from abc import ABC, abstractmethod
from typing import Union

import pandas as pd
import pyspark
import pyspark.sql

from discover.core.file import FileFormat

# ------------------------------------------------------------------------------------------------ #
DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                                           FAO                                                    #
# ------------------------------------------------------------------------------------------------ #
class FAO(ABC):
    """File Access Object (FAO) for reading, writing, and managing data files.

    This abstract base class provides methods for creating, reading, checking the existence of,
    and deleting files in various formats (e.g., CSV, Parquet). It abstracts the underlying
    read/write logic via injected reader and writer dependencies.

    Args:
        config (dict): Configuration dictionary containing settings for CSV and Parquet read/write operations.
        reader (DataFrameReader): Object responsible for reading data files.
        writer (DataFrameWriter): Object responsible for writing data files.
        **kwargs: Additional keyword arguments for customization.
    """

    def __init__(
        self,
        fao_config: dict,
        reader: DataFrameReader,
        writer: DataFrameWriter,
        **kwargs,
    ):
        self._csv_config = fao_config["csv"]
        self._parquet_config = fao_config["parquet"]
        self._reader = reader
        self._writer = writer
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(
        self,
        filepath: str,
        data: DataFrame,
        file_format: FileFormat = FileFormat.PARQUET,
        **kwargs,
    ) -> None:
        """Creates a data file in the specified format.

        Args:
            filepath (str): Path where the file should be created.
            data (DataFrame): The data to be written to the file.
            file_format (FileFormat): The format of the file to create (default is Parquet).

        Raises:
            ValueError: If an unsupported file format is provided.

        Logs:
            Error: If an invalid file format is specified.
        """
        if file_format == FileFormat.PARQUET:
            self._writer.parquet(
                data=data, filepath=filepath, **self._parquet_config["write_kwargs"]
            )
        elif file_format == FileFormat.CSV:
            self._writer.csv(
                data=data, filepath=filepath, **self._csv_config["write_kwargs"]
            )
        else:
            msg = f"Invalid file_format {file_format}. Supported formats include .csv and .parquet"
            self._logger.error(msg)
            raise ValueError(msg)

    def read(
        self, filepath: str, file_format: FileFormat = FileFormat.PARQUET, **kwargs
    ) -> DataFrame:
        """Reads a data file in the specified format.

        Args:
            filepath (str): Path to the file to read.
            file_format (FileFormat): The format of the file to read (default is Parquet).

        Returns:
            DataFrame: The data read from the file.

        Raises:
            ValueError: If an unsupported file format is provided.

        Logs:
            Error: If an invalid file format is specified.
        """
        if file_format == FileFormat.PARQUET:
            return self._reader.parquet(
                filepath=filepath, **self._parquet_config["read_kwargs"]
            )
        elif file_format == FileFormat.CSV:
            return self._reader.csv(
                filepath=filepath, **self._csv_config["read_kwargs"]
            )
        else:
            msg = f"Invalid file_format {file_format}. Supported formats include .csv and .parquet"
            self._logger.error(msg)
            raise ValueError(msg)

    def exists(self, filepath: str) -> bool:
        """Checks if a file or directory exists at the given path.

        Args:
            filepath (str): The path to check for existence.

        Returns:
            bool: True if the file or directory exists, False otherwise.
        """
        return os.path.exists(filepath)

    def delete(self, filepath: str) -> None:
        """Deletes a file or directory at the specified path.

        Args:
            filepath (str): The path to the file or directory to delete.

        Raises:
            ValueError: If the filepath is neither a valid file nor directory.

        Logs:
            Error: If the filepath is invalid.
        """
        try:
            os.remove(filepath)
        except FileNotFoundError:
            msg = f"File {filepath} not found."
            self._logger.warning(msg)
        except OSError:
            shutil.rmtree(filepath)
        except Exception as e:
            msg = f"Unexpected exception occurred.\n{e}"
            self._logger.error(msg)
            raise Exception(msg)

    def reset(self, verified: bool = False) -> None:
        if verified:
            shutil.rmtree(self._basedir)
            self._logger.warning(f"{self.__class__.__name__} has been reset.")
        else:
            proceed = input(
                f"Resetting the {self.__class__.__name__} object database is irreversible. To proceed, type 'YES'."
            )
            if proceed == "YES":
                shutil.rmtree(self._basedir)
                self._logger.warning(f"{self.__class__.__name__} has been reset.")
            else:
                self._logger.info(f"{self.__class__.__name__} reset has been aborted.")


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
