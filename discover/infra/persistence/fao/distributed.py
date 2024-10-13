#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/fao/distributed.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:42 pm                                              #
# Modified   : Sunday October 13th 2024 02:07:14 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from typing import Dict

import pyspark

from discover.infra.persistence.fao.base import FileSystemFAO
from discover.infra.persistence.fao.exception import FileIOException
from discover.infra.service.spark.session import SparkSessionPool


# ------------------------------------------------------------------------------------------------ #
class DistributedFileSystemFAO(FileSystemFAO):
    """
    Data Access Object (DAO) for interacting with a distributed file system using PySpark.

    This class manages reading from and writing to a distributed file system (e.g., HDFS, S3)
    using PySpark DataFrames. It coordinates with a SparkSessionPool to obtain or create
    Spark sessions for executing distributed data operations.

    Args:
        storage_config (Dict): File persistence configuration
        session_pool (SparkSessionPool): A Spark session pool for obtaining or creating
                Spark sessions

    """

    def __init__(
        self,
        storage_config: Dict,
        session_pool: SparkSessionPool,
    ) -> None:
        """
        Initializes the DistributedFileSystemFAO with a provided Spark session pool.

        Args:
            session_pool (SparkSessionPool): A Spark session pool for obtaining or creating
                Spark sessions, injected by the dependency injection framework.
        """
        super().__init__()
        self._storage_config = storage_config
        self._session_pool = session_pool

    def _read(self, filepath: str, nlp: bool = False) -> pyspark.sql.DataFrame:
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
            spark_session = self._session_pool.get_or_create(nlp=nlp)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}.File does not exist.\n{e}"
            self._logger.error(msg)
            raise
        except Exception as e:
            msg = f"Exception occurred while reading Parquet file from {filepath}. Unable to create a spark session."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e

        try:
            return spark_session.read.parquet(filepath)
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}. The file does not exist."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e
        except Exception as e:
            msg = f"Exception occurred while reading a Parquet file from {filepath}."
            self._logger.exception(msg)
            raise FileIOException(msg, e) from e

    def _write(
        self,
        filepath: str,
        data: pyspark.sql.DataFrame,
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
        mode = self._storage_config["write_kwargs"].get("mode", None)
        partition_cols = self._storage_config["write_kwargs"].get("partitionBy", None)

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
