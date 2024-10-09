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
# Modified   : Wednesday October 9th 2024 12:52:29 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os

import pyspark
from dependency_injector.wiring import Provide, inject

from discover.container import DiscoverContainer
from discover.infra.dal.file.base import FileSystemDAO
from discover.infra.frameworks.spark.nlp import SparkSessionPoolNLP
from discover.infra.frameworks.spark.standard import SparkSessionPoolStandard


# ------------------------------------------------------------------------------------------------ #
class DistributedFileSystemDAO(FileSystemDAO):
    """
    Data Access Object (DAO) for distributed file systems, supporting read and write operations for Spark and Pandas DataFrames.

    This class manages file I/O operations, such as reading and writing parquet files, leveraging a session pool to manage
    Spark sessions efficiently. The class supports both Pandas and Spark DataFrames for reading and writing.

    Attributes:
    -----------
    _session_pool : SparkSessionPoolStandard
        A pool of Spark sessions, injected to manage Spark session lifecycle.

    Methods:
    --------
    __init__(session_pool: SparkSessionPoolStandard):
        Initializes the DAO with a session pool for Spark sessions.

    read(filepath: str, spark_session_name: str, **kwargs) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        Reads a parquet file from the specified file path using the provided Spark session. Raises a FileNotFoundError if the file does not exist.

    _read(filepath: str, spark_session_name: str, **kwargs) -> pyspark.sql.DataFrame:
        Internal method to perform the actual Spark DataFrame read operation from a parquet file.

    _write(filepath: str, data: pyspark.sql.DataFrame, spark_session_name: str, **kwargs) -> None:
        Internal method to write a Spark DataFrame to a parquet file. Supports partitioning and various write modes.
    """

    @inject
    def __init__(
        self,
        session_pool: SparkSessionPoolStandard = Provide[
            DiscoverContainer.spark.standard
        ],
    ) -> None:
        """"""
        super().__init__()
        self._session_pool = session_pool

    def read(
        self, filepath: str, spark_session_name: str, **kwargs
    ) -> pyspark.sql.DataFrame:
        """"""
        # Check if the file exists before reading
        if os.path.exists(filepath):
            return self._read(
                filepath=filepath, spark_session_name=spark_session_name, **kwargs
            )
        else:
            msg = f"File {os.path.basename(filepath)} does not exist in {os.path.dirname(filepath)}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    def _read(
        self, filepath: str, spark_session_name: str, **kwargs
    ) -> pyspark.sql.DataFrame:
        """"""
        # Obtain a spark session from the pool
        spark_session = self._session_pool.get_or_create(
            spark_session_name=spark_session_name
        )
        return spark_session.read.parquet(filepath, **kwargs)

    def _write(
        self,
        filepath: str,
        data: pyspark.sql.DataFrame,
        **kwargs,
    ) -> None:
        """"""
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
class DistributedFileSystemNLPDAO(DistributedFileSystemDAO):
    """"""

    @inject
    def __init__(
        self,
        session_pool: SparkSessionPoolNLP = Provide[DiscoverContainer.spark.nlp],
    ) -> None:
        """"""
        super().__init__(session_pool=session_pool)
