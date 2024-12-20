#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/data/fao/spark.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:35 pm                                              #
# Modified   : Thursday December 19th 2024 11:10:43 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark File Access Object Module"""
import pyspark

from discover.infra.data.dal.base import FAO
from discover.infra.data.fao.exception import FileIOException
from discover.infra.service.spark.session import SparkSessionPool


# ------------------------------------------------------------------------------------------------ #
#                                      PARQUET                                                     #
# ------------------------------------------------------------------------------------------------ #
class SparkParquetFAO(FAO):
    """
    Spark File Access Object (FAO) for parquet files."""

    def __init__(self, config: dict, spark_session_pool: SparkSessionPool) -> None:
        self._config = config
        self._spark_session_pool = spark_session_pool
        self._spark_session = None

    # -------------------------------------------------------------------------------------------- #
    def stop_spark_session(self) -> None:
        try:
            self._spark_session.stop()
        except TypeError:
            pass
        except Exception as e:
            raise (f"Unexpected error occurred while shutting down spark session.\n{e}")

    # -------------------------------------------------------------------------------------------- #
    def create(
        self,
        data: pyspark.sql.DataFrame,
        filepath: str,
        **kwargs,
    ) -> None:
        """
        Writes a PySpark DataFrame to a specified filepath as a Parquet file.

        Configures the write operation based on provided keyword arguments such as `mode` and
        `partitionBy` to control how the data is written. If an error occurs during the write
        process, an exception is raised.

        Args:
            filepath (str): The path where the Parquet file will be written.
            data (pyspark.sql.DataFrame): The PySpark DataFrame to write to the Parquet file.
            **kwargs: Additional keyword arguments to configure the write operation, such as:
                - mode (str): The write mode (e.g., 'overwrite', 'append').
                - partitionBy (list): Columns to partition the data by.

        Raises:
            FileIOException: If an error occurs while writing the Parquet file.
        """
        # Extract kwargs from storage config
        mode = self._config["write_kwargs"].get("mode", None)
        partition_cols = self._config["write_kwargs"].get("partitionBy", None)

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

    # -------------------------------------------------------------------------------------------- #
    def read(self, filepath: str, nlp: bool = False) -> pyspark.sql.DataFrame:
        """
        Reads a Parquet file from a specified filepath using PySpark.

        Obtains a Spark session from the session pool and attempts to read the specified
        Parquet file into a PySpark DataFrame. If the file does not exist or if there is an
        error during the read process, an exception is raised.

        Args:
            filepath (str): The path of the Parquet file to read.
            nlp (bool): Whether to use a session configured for NLP processing (default is False).

        Returns:
            pyspark.sql.DataFrame: The PySpark DataFrame containing the data from the Parquet file.

        Raises:
            FileIOException: If an error occurs while creating the Spark session or reading the
                Parquet file, including cases where the file is not found.
        """
        try:
            self._spark_session = self._spark_session_pool.get_or_create(nlp=nlp)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}.File does not exist.\n{e}"
            self._logger.error(msg)
            raise
        except Exception as e:
            msg = f"Exception occurred while reading Parquet file from {filepath}. Unable to create a spark session."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e

        try:
            return self._spark_session.read.parquet(filepath)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}. The file does not exist."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e
        except Exception as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e


# ------------------------------------------------------------------------------------------------ #
#                                         CSV                                                      #
# ------------------------------------------------------------------------------------------------ #
class SparkCSVFAO(FAO):

    def __init__(self, config: dict, session_pool: SparkSessionPool) -> None:
        self._config = config
        self._session_pool = session_pool

    # -------------------------------------------------------------------------------------------- #
    def create(
        self,
        data: pyspark.sql.DataFrame,
        filepath: str,
        **kwargs,
    ) -> None:
        """
        Writes a PySpark DataFrame to a specified filepath as a CSV file.

        Configures the write operation based on provided keyword arguments such as `mode` and
        `partitionBy` to control how the data is written. If an error occurs during the write
        process, an exception is raised.

        Args:
            filepath (str): The path where the CSV file will be written.
            data (pyspark.sql.DataFrame): The PySpark DataFrame to write to the CSV file.
            **kwargs: Additional keyword arguments to configure the write operation, such as:
                - mode (str): The write mode (e.g., 'overwrite', 'append').
                - partitionBy (list): Columns to partition the data by.

        Raises:
            FileIOException: If an error occurs while writing the CSV file.
        """
        # Extract kwargs from storage config
        mode = self._config["write_kwargs"].get("mode", None)
        partition_cols = self._config["write_kwargs"].get("partitionBy", None)

        # Construct pyspark write command based upon kwargs
        try:
            if mode and partition_cols:
                data.write.mode(mode).partitionBy(partition_cols).csv(filepath)
            elif mode:
                data.write.mode(mode).csv(filepath)
            elif partition_cols:
                data.write.partitionBy(partition_cols).csv(filepath)
            else:
                data.write.csv(filepath)
        except Exception as e:
            msg = f"Exception occurred while writing a CSV file to {filepath}.\nKeyword Arguments: {kwargs}"
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
    def read(self, filepath: str, nlp: bool = False) -> pyspark.sql.DataFrame:
        """
        Reads a CSV file from a specified filepath using PySpark.

        Obtains a Spark session from the session pool and attempts to read the specified
        CSV file into a PySpark DataFrame. If the file does not exist or if there is an
        error during the read process, an exception is raised.

        Args:
            filepath (str): The path of the CSV file to read.
            nlp (bool): Whether to use a session configured for NLP processing (default is False).

        Returns:
            pyspark.sql.DataFrame: The PySpark DataFrame containing the data from the CSV file.

        Raises:
            FileIOException: If an error occurs while creating the Spark session or reading the
                CSV file, including cases where the file is not found.
        """
        try:
            spark_session = self._session_pool.get_or_create(nlp=nlp)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a CSV file from {filepath}.File does not exist.\n{e}"
            self._logger.error(msg)
            raise
        except Exception as e:
            msg = f"Exception occurred while reading CSV file from {filepath}. Unable to create a spark session."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e

        try:
            return spark_session.read.csv(filepath)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a CSV file from {filepath}. The file does not exist."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e
        except Exception as e:
            msg = f"Exception occurred while reading a CSV file from {filepath}."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e
