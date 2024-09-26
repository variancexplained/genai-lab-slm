#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/frameworks/spark/standard.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 03:33:40 pm                                            #
# Modified   : Wednesday September 25th 2024 08:00:06 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark Utils Module"""

import os
import time

from pyspark.sql import SparkSession

from discover.infra.frameworks.spark.base import SparkSessionPool


# ------------------------------------------------------------------------------------------------ #
class SparkSessionPoolStandard(SparkSessionPool):
    """
    Factory class for creating and managing standard Spark sessions.

    This class is responsible for creating Spark sessions with general configurations
    tailored for non-NLP workloads. It reuses Spark sessions if available or creates a
    new one using the specified configurations such as memory size, row group size,
    and retry logic in case of failures.

    Methods:
        create_session(name: str, memory: str, parquet_block_size: int, retries: int) -> SparkSession:
            Creates and returns a Spark session for standard workloads.
    """

    def __init__(self) -> None:
        super().__init__()

    def create_session(
        self, name: str, memory: str, parquet_block_size: int, retries: int
    ) -> SparkSession:
        """
        Creates and returns a standard Spark session with specified configurations.

        This method attempts to create a Spark session using the provided configurations
        for memory, row group size, and other standard settings. It includes retry logic
        with exponential backoff in case of failures. The method will attempt the specified
        number of retries before raising a `RuntimeError` if session creation ultimately fails.

        Args:
            name (str): The name of the Spark session.
            memory (str): The memory allocation for the driver and executor (e.g., "32g").
            parquet_block_size (int): The size of the Parquet block size for row groups.
            retries (int): The number of retries to attempt in case of failure.

        Returns:
            SparkSession: A Spark session configured for standard workloads.

        Raises:
            RuntimeError: If the session creation fails after the specified number of retries.

        Example usage:
            >>> pool = SparkSessionPoolStandard()
            >>> session = pool.create_session(
            ...     name="standard-session",
            ...     memory="16g",
            ...     parquet_block_size=128 * 1024 * 1024,
            ...     retries=3
            ... )
            >>> print(session)
            <pyspark.sql.session.SparkSession object ...>

        Retry mechanism:
            This method implements a retry mechanism with exponential backoff. If session creation
            fails, it logs the error and retries after waiting 2^attempt seconds between retries,
            up to the specified number of retries. If all attempts fail, a `RuntimeError` is raised.
        """
        attempts = 0
        error = None
        while attempts < retries:
            try:
                log4j_conf_path = "file:" + os.path.abspath("log4j.properties")
                return (
                    SparkSession.builder.appName(name)
                    .master("local[*]")
                    .config("spark.driver.memory", memory)
                    .config("spark.executor.memory", memory)
                    .config("spark.sql.parquet.block.size", parquet_block_size)
                    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                    .config(
                        "spark.sql.execution.arrow.pyspark.fallback.enabled", "false"
                    )
                    .config(
                        "spark.driver.extraJavaOptions",
                        f"-Dlog4j.configuration={log4j_conf_path}",
                    )
                    .config(
                        "spark.executor.extraJavaOptions",
                        f"-Dlog4j.configuration={log4j_conf_path}",
                    )
                    .getOrCreate()
                )
            except Exception as e:
                attempts += 1
                error = e
                self._logger.warning(
                    f"Error creating Spark session: {e}. Retrying {attempts} of {retries}..."
                )
                time.sleep(2**attempts)

        msg = f"Retries exhausted. Unable to create Spark session.\n{error}"
        self._logger.exception(msg)
        raise RuntimeError(msg)
