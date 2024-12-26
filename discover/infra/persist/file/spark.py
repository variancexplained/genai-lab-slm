#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/file/spark.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Thursday December 26th 2024 03:56:32 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark File Access Object Module"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from discover.core.file import FileFormat
from discover.infra.exception.file import FileIOException
from discover.infra.persist.file.base import FAO
from discover.infra.persist.file.base import DataFrameReader as BaseDataFrameReader
from discover.infra.persist.file.base import DataFrameWriter as BaseDataFrameWriter


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
            data = spark.read.parquet(filepath, **kwargs)
            logging.debug(f"Read Spark DataFrame from parquet file {filepath}")
            return data
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
            data = spark.read.csv(filepath, **kwargs)
            logging.debug(f"Read Spark DataFrame from csv file {filepath}")
            return data
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

    @classmethod
    def parquet(
        cls, data: DataFrame, filepath: str, overwrite: bool = False, **kwargs
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
                logging.debug(
                    f"Writing spark DataFrame to partitioned parquet file at {filepath}"
                )
            elif mode:
                data.write.mode(mode).parquet(filepath)
                logging.debug(f"Writing spark DataFrame to parquet file at {filepath}")
            elif partition_cols:
                data.write.partitionBy(partition_cols).parquet(filepath)
                logging.debug(
                    f"Writing spark DataFrame to partitioned parquet file at {filepath}"
                )
            else:
                data.write.parquet(filepath)
                logging.debug(f"Writing spark DataFrame to parquet file at {filepath}")
        except Exception as e:
            msg = f"Exception occurred while writing a Parquet file at {filepath}.\nKeyword Arguments: {kwargs}"
            raise FileIOException(msg, e) from e

    @classmethod
    def csv(
        cls, data: DataFrame, filepath: str, overwrite: bool = False, **kwargs
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
            data.coalesce(1).write.csv(filepath, **kwargs)
            logging.debug(f"Writing partitioned spark csv file to {filepath}")
        except Exception as e:
            msg = f"Exception occurred while writing a CSV file to {filepath}.\nKeyword Arguments: {kwargs}"
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
#                                       SPARK FAO                                                  #
# ------------------------------------------------------------------------------------------------ #
class SparkFAO(FAO):
    """File Access Object (FAO) implementation for Apache Spark environments.

    This class extends the base `FAO` to integrate with Apache Spark for managing data files.
    It leverages a Spark session pool for handling Parquet and CSV files in distributed
    environments and supports switching between Spark and SparkNLP session contexts.

    Args:
        fao_config (dict): Configuration dictionary containing CSV and Parquet settings.
        reader (DataFrameReader): Object responsible for reading data files (default is `DataFrameReader`).
        writer (DataFrameWriter): Object responsible for writing data files (default is `DataFrameWriter`).
        **kwargs: Additional keyword arguments for customization.
    """

    def __init__(
        self,
        fao_config: dict,
        reader: DataFrameReader = DataFrameReader,
        writer: DataFrameWriter = DataFrameWriter,
        **kwargs,
    ) -> None:
        super().__init__(
            fao_config=fao_config,
            reader=reader,
            writer=writer,
        )

    def read(
        self,
        filepath: str,
        spark: SparkSession,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> None:
        """Reads a data file in the specified format using Spark.

        Args:
            filepath (str): Path to the file to read.
            spark (SparkSession): A Spark or SparkNLP Session
            file_format (FileFormat): The format of the file to read (default is Parquet).

        Raises:
            ValueError: If an unsupported file format or dataframe structure is provided.

        Logs:
            Error: If an invalid file format or dataframe structure is specified.
        """

        if file_format == FileFormat.PARQUET:
            return self._reader.parquet(
                filepath=filepath,
                spark=spark,
                **self._parquet_config["read_kwargs"],
            )
        elif file_format == FileFormat.CSV:
            return self._reader.csv(
                filepath=filepath,
                spark=spark,
                **self._csv_config["read_kwargs"],
            )
        else:
            msg = f"Invalid file_format {file_format}. Supported formats include .csv and .parquet"
            self._logger.error(msg)
            raise ValueError(msg)
