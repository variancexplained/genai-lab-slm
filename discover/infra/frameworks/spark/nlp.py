#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/frameworks/spark/nlp.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 07:31:28 pm                                             #
# Modified   : Tuesday September 24th 2024 02:20:15 am                                             #
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
class SparkSessionPoolNLP(SparkSessionPool):
    """
    Factory class for creating and managing Spark sessions specifically tailored for
    natural language processing (NLP) workloads.
    """

    def create_session(
        self, name: str, memory: str, row_group_size: int, retries: int
    ) -> SparkSession:
        """
        Creates and returns a Spark session for NLP workloads with specified configurations.

        This method attempts to create a Spark session with the provided configurations for
        memory, row group size, and other parameters necessary for handling NLP tasks.
        It uses Kryo serialization and includes the necessary NLP libraries (e.g.,
        spark-nlp). If session creation fails, it retries the specified number of times
        with exponential backoff before raising a `RuntimeError` if all retries fail.

        Args:
            name (str): The name of the Spark session.
            memory (str): The memory allocation for the driver and executor (e.g., "32g").
            row_group_size (int): The size of Parquet block size for row groups.
            retries (int): The number of times to retry session creation if it fails.

        Returns:
            SparkSession: A configured Spark session for NLP workloads.

        Raises:
            RuntimeError: If the session creation fails after the specified number of retries.

        Example usage:
            >>> pool = SparkSessionPoolNLP()
            >>> session = pool.create_session(
            ...     name="nlp-session",
            ...     memory="32g",
            ...     row_group_size=256 * 1024 * 1024,
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
                    .config("spark.sql.parquet.block.size", row_group_size)
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                    .config(
                        "spark.sql.execution.arrow.pyspark.fallback.enabled", "false"
                    )
                    .config(
                        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                    )
                    .config("spark.kryoserializer.buffer.max", "2000M")
                    .config("spark.driver.maxResultSize", "0")
                    .config(
                        "spark.jars.packages",
                        "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3",
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
