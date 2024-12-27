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
# Modified   : Thursday December 26th 2024 08:45:58 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark File Access Object Module"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from discover.infra.exception.file import FileIOException
from discover.infra.persist.dataframe.base import DataFrameReader as BaseDataFrameReader
from discover.infra.persist.dataframe.base import DataFrameWriter as BaseDataFrameWriter


# ------------------------------------------------------------------------------------------------ #
#                                    DATAFRAME READERS                                             #
# ------------------------------------------------------------------------------------------------ #
class SparkDataFrameParquetReader(BaseDataFrameReader):
    """A reader class for loading data into Spark DataFrames from parquet files."""

    @classmethod
    def read(cls, filepath: str, spark: SparkSession, **kwargs) -> DataFrame:
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


# ------------------------------------------------------------------------------------------------ #
class SparkDataFrameCSVReader(BaseDataFrameReader):
    """A reader class for loading data into Spark DataFrames from csv files."""

    @classmethod
    def read(cls, filepath: str, spark: SparkSession, **kwargs) -> DataFrame:
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
#                                   DATAFRAME WRITERS                                              #
# ------------------------------------------------------------------------------------------------ #
class SparkDataFrameParquetWriter(BaseDataFrameWriter):
    """Writes a pandas DataFrame to a parquet file."""

    @classmethod
    def write(
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
        super().validate_write(filepath=filepath, overwrite=overwrite, **kwargs)
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


# ------------------------------------------------------------------------------------------------ #
class SparkDataFrameCSVWriter(BaseDataFrameWriter):
    """Writes a pandas DataFrame to a csv file."""

    @classmethod
    def write(
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
        cls.validate_write()
        try:
            data.repartition(1).write.csv(filepath, **kwargs)
            logging.debug(f"Writing partitioned spark csv file to {filepath}")
        except Exception as e:
            msg = f"Exception occurred while writing a CSV file to {filepath}.\nKeyword Arguments: {kwargs}"
            raise FileIOException(msg, e) from e
