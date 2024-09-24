#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/file/distributed.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:36:42 pm                                              #
# Modified   : Tuesday September 24th 2024 01:49:31 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import pyspark
from dependency_injector.wiring import Provide, inject

from discover.container import DiscoverContainer
from discover.infra.dal.file.base import FileSystemDAO
from discover.infra.frameworks.spark.nlp import SparkSessionPoolNLP
from discover.infra.frameworks.spark.standard import SparkSessionPoolStandard


# ------------------------------------------------------------------------------------------------ #
class DistributedFileSystemDAO(FileSystemDAO):
    """
    A specialized Data Access Object (DAO) for interacting with a distributed file system,
    handling data in PySpark DataFrame format. This class inherits from the FileSystemDAO
    and implements reading and writing of Parquet files using PySpark.

    The distributed file system is environment-specific, and the file paths passed to the
    create, read, and delete methods are converted from environment-agnostic paths using
    the base directory set in the configuration.

    Attributes:
        _logger (logging.Logger): Inherited from FileSystemDAO, used for logging errors and tracking operations.
        _basedir (str): Inherited from FileSystemDAO, defines the base directory for file operations.

    Methods:
        _read(filepath, **kwargs):
            Reads a Parquet file from the distributed file system into a PySpark DataFrame.

        _write(filepath, data, **kwargs):
            Writes a PySpark DataFrame to a Parquet file in the distributed file system.
    """

    @inject
    def __init__(
        self,
        session_pool: SparkSessionPoolStandard = Provide[
            DiscoverContainer.spark.standard
        ],
    ) -> None:
        """
        Initializes the DistributedFileSystemDAO by calling the parent FileSystemDAO constructor.
        The base directory is set using the configuration reader class, which is responsible for
        retrieving environment-specific settings.

        Args:
            config_reader_cls (type[ConfigReader], optional): The configuration reader class
                responsible for retrieving the base directory for distributed storage. Defaults
                to ConfigReader.
        """
        super().__init__()
        self._session_pool = session_pool

    def _read(
        self, filepath: str, session_name: str, **kwargs
    ) -> pyspark.sql.DataFrame:
        """
        Reads a Parquet file from the distributed file system and loads it into a PySpark DataFrame.

        Args:
            filepath (str): The full path to the Parquet file in the distributed file system.
            **kwargs: Additional keyword arguments for reading the Parquet file.

        Returns:
            pyspark.sql.DataFrame: The data read from the file, loaded into a PySpark DataFrame.
        """
        # Obtain a spark session from the pool
        spark_session = self._session_pool.get_or_create(session_name=session_name)
        return spark_session.read.parquet(filepath, **kwargs)

    def _write(
        self, filepath: str, data: pyspark.sql.DataFrame, session_name: str, **kwargs
    ) -> None:
        """
        Writes a PySpark DataFrame to a Parquet file in the distributed file system.

        Args:
            filepath (str): The full path where the Parquet file will be written in the distributed system.
            data (pyspark.sql.DataFrame): The PySpark DataFrame containing the data to be written.
            **kwargs: Additional keyword arguments for writing the Parquet file.
        """
        # Extract arguments from kwargs
        mode = kwargs.get("mode", None)
        partition_cols = kwargs.get("partition_cols", None)

        # Construct pyspark write command based upon kwargs
        if mode and partition_cols:
            data.write.mode(mode).partitionBy(partition_cols).parquet(filepath)
        elif mode:
            data.write.mode(mode).parquet(filepath)
        elif partition_cols:
            data.write.partitionBy(partition_cols).parquet(filepath)


# ------------------------------------------------------------------------------------------------ #
class DistributedFileSystemNLPDAO(FileSystemDAO):
    """
    A specialized Data Access Object (DAO) for interacting with a distributed file system
    optimized for NLP work loads, handling data in PySpark DataFrame format.
    This class inherits from the FileSystemDAO and implements reading and writing of
    Parquet files using PySpark.

    The distributed file system is environment-specific, and the file paths passed to the
    create, read, and delete methods are converted from environment-agnostic paths using
    the base directory set in the configuration.

    Attributes:
        _logger (logging.Logger): Inherited from FileSystemDAO, used for logging errors and tracking operations.
        _basedir (str): Inherited from FileSystemDAO, defines the base directory for file operations.

    Methods:
        _read(filepath, **kwargs):
            Reads a Parquet file from the distributed file system into a PySpark DataFrame.

        _write(filepath, data, **kwargs):
            Writes a PySpark DataFrame to a Parquet file in the distributed file system.
    """

    @inject
    def __init__(
        self,
        session_pool: SparkSessionPoolNLP = Provide[DiscoverContainer.spark.nlp],
    ) -> None:
        """
        Initializes the DistributedFileSystemDAO by calling the parent FileSystemDAO constructor.
        The base directory is set using the configuration reader class, which is responsible for
        retrieving environment-specific settings.

        Args:
            config_reader_cls (type[ConfigReader], optional): The configuration reader class
                responsible for retrieving the base directory for distributed storage. Defaults
                to ConfigReader.
        """
        super().__init__()
        self._session_pool = session_pool

    def _read(
        self, filepath: str, session_name: str, **kwargs
    ) -> pyspark.sql.DataFrame:
        """
        Reads a Parquet file from the distributed file system and loads it into a PySpark DataFrame.

        Args:
            filepath (str): The full path to the Parquet file in the distributed file system.
            **kwargs: Additional keyword arguments for reading the Parquet file.

        Returns:
            pyspark.sql.DataFrame: The data read from the file, loaded into a PySpark DataFrame.
        """
        # Obtain a spark session from the pool
        spark_session = self._session_pool.get_or_create(session_name=session_name)
        return spark_session.read.parquet(filepath, **kwargs)

    def _write(
        self, filepath: str, data: pyspark.sql.DataFrame, session_name: str, **kwargs
    ) -> None:
        """
        Writes a PySpark DataFrame to a Parquet file in the distributed file system.

        Args:
            filepath (str): The full path where the Parquet file will be written in the distributed system.
            data (pyspark.sql.DataFrame): The PySpark DataFrame containing the data to be written.
            **kwargs: Additional keyword arguments for writing the Parquet file.
        """
        # Extract arguments from kwargs
        mode = kwargs.get("mode", None)
        partition_cols = kwargs.get("partition_cols", None)

        # Construct pyspark write command based upon kwargs
        if mode and partition_cols:
            data.write.mode(mode).partitionBy(partition_cols).parquet(filepath)
        elif mode:
            data.write.mode(mode).parquet(filepath)
        elif partition_cols:
            data.write.partitionBy(partition_cols).parquet(filepath)
