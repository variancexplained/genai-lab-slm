#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/dataframe/spark.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Monday December 23rd 2024 08:52:19 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark File Access Object Module"""
from typing import Type

from pyspark.sql import DataFrame, SparkSession

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
    A reader class for loading data into DataFrames from various file formats.

    This class provides methods for reading Parquet and CSV files into Spark DataFrames,
    handling exceptions and providing meaningful error messages when issues occur.

    Methods:
        parquet: Reads a Parquet file into a Spark DataFrame.
        csv: Reads a CSV file into a Spark DataFrame.
    """

    @classmethod
    def parquet(cls, filepath: str, spark: SparkSession, **kwargs) -> DataFrame:
        """
        Reads a Parquet file into a Spark DataFrame.

        Args:
            filepath (str): The path to the Parquet file.
            spark (SparkSession): The Spark session to use for reading the file.
            **kwargs: Additional keyword arguments passed to Spark's `read.parquet` method.

        Returns:
            DataFrame: A PySpark DataFrame containing the data from the Parquet file.

        Raises:
            FileNotFoundError: If the specified Parquet file does not exist.
            FileIOException: If any other exception occurs while reading the file.
        """
        super().parquet(filepath=filepath, **kwargs)
        try:
            return spark.read.parquet(filepath, **kwargs)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}. File does not exist.\n{e}"
            raise FileNotFoundError(msg)
        except Exception as e:
            msg = (
                f"Exception occurred while reading a Parquet file from {filepath}.\n{e}"
            )
            raise FileIOException(msg, e) from e

    @classmethod
    def csv(cls, filepath: str, spark: SparkSession, **kwargs) -> DataFrame:
        """
        Reads a CSV file into a Spark DataFrame.

        Args:
            filepath (str): The path to the CSV file.
            spark (SparkSession): The Spark session to use for reading the file.
            **kwargs: Additional keyword arguments passed to Spark's `read.csv` method.

        Returns:
            DataFrame: A PySpark DataFrame containing the data from the CSV file.

        Raises:
            FileNotFoundError: If the specified CSV file does not exist.
            FileIOException: If any other exception occurs while reading the file.
        """
        super().csv(filepath=filepath, **kwargs)
        try:
            return spark.read.csv(filepath, **kwargs)
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
    A writer class for saving DataFrames to various file formats.

    This class provides methods for writing data to Parquet and CSV file formats.
    It supports options for overwriting files and specifying additional write parameters.

    Methods:
        parquet: Writes data to a Parquet file.
        csv: Writes data to a CSV file.
    """

    def parquet(
        self, data: DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """
        Writes the data to a Parquet file at the designated filepath.

        Args:
            data (DataFrame): The DataFrame to write to the Parquet file.
            filepath (str): The path where the Parquet file will be saved.
            overwrite (bool): Whether to overwrite existing data. Defaults to False.
            **kwargs: Additional keyword arguments passed to the PySpark write command.
                Common options include:
                - mode (str): Write mode (e.g., "overwrite", "append").
                - partitionBy (list): List of columns to partition the data by.

        Raises:
            FileIOException: If an error occurs while writing the Parquet file.
        """
        super().parquet(data=data, filepath=filepath, overwrite=overwrite, **kwargs)
        # Extract arguments from kwargs
        mode = kwargs.get("mode", None)
        partition_cols = kwargs.get("partitionBy", None)

        # Construct pyspark write command based upon kwargs
        try:
            if mode and partition_cols:
                data.write.mode(mode).partitionBy(partition_cols).parquet(filepath)
            elif mode:
                data.write.mode(mode).parquet(filepath)
            elif partition_cols:
                data.write.partitionBy(partition_cols).parquet(filepath)
            else:
                data.write.parquet(filepath)
        except Exception as e:
            msg = f"Exception occurred while writing a Parquet file to {filepath}.\nKeyword Arguments: {kwargs}"
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e

    def csv(
        self, data: DataFrame, filepath: str, overwrite: bool = False, **kwargs
    ) -> None:
        """
        Writes the data to a CSV file at the designated filepath.

        Args:
            data (DataFrame): The DataFrame to write to the CSV file.
            filepath (str): The path where the CSV file will be saved.
            overwrite (bool): Whether to overwrite existing data. Defaults to False.
            **kwargs: Additional keyword arguments passed to the PySpark write command.

        Raises:
            FileIOException: If an error occurs while writing the CSV file.
        """
        super().csv(data=data, filepath=filepath, overwrite=overwrite, **kwargs)
        try:
            data.write.csv(filepath, **kwargs)
        except Exception as e:
            msg = f"Exception occurred while writing a CSV file to {filepath}.\nKeyword Arguments: {kwargs}"
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
#                                       SPARK FAO                                                  #
# ------------------------------------------------------------------------------------------------ #
class SparkFAO(FAO):
    """
    A File Access Object (FAO) for handling Spark DataFrames.

    This class provides methods to read and write Spark DataFrames in various file formats,
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
        Initializes the SparkFAO with a reader and writer.

        Args:
            reader (Type[DataFrameReader]): A class implementing the interface for reading data.
                Defaults to `DataFrameReader`.
            writer (Type[DataFrameWriter]): A class implementing the interface for writing data.
                Defaults to `DataFrameWriter`.
        """
        self._reader = reader
        self._writer = writer

    def read(
        self,
        filepath: str,
        spark: SparkSession,
        file_format: FileFormat = FileFormat.PARQUET,
        **kwargs,
    ) -> DataFrame:
        """
        Reads data from the specified file into a Spark DataFrame.

        Args:
            filepath (str): The path to the file to read.
            spark (SparkSession): The SparkSession instance to use for reading.
            file_format (FileFormat): The format of the file to read (e.g., CSV, PARQUET).
                Defaults to `FileFormat.PARQUET`.
            **kwargs: Additional arguments passed to the reader class.

        Returns:
            DataFrame: The data read from the file as a Spark DataFrame.

        Raises:
            ValueError: If the specified `file_format` is not supported.
        """
        if file_format == FileFormat.CSV:
            return self._reader.csv(filepath=filepath, spark=spark, **kwargs)
        elif file_format == FileFormat.PARQUET:
            return self._reader.parquet(filepath=filepath, spark=spark, **kwargs)
        else:
            raise ValueError(f"Unrecognized file_format: {file_format}")

    def write(
        self,
        filepath: str,
        data: DataFrame,
        file_format: FileFormat = FileFormat.PARQUET,
        **kwargs,
    ) -> None:
        """
        Writes a Spark DataFrame to the specified file.

        Args:
            filepath (str): The path to the file to write.
            data (DataFrame): The Spark DataFrame to write.
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
