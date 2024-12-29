#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/spark/pool.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 12:50:08 am                                             #
# Modified   : Saturday December 28th 2024 08:15:16 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import atexit
import logging
import os
import time
from typing import Dict

from pyspark.sql import SparkSession

from discover.core.data_structure import NestedNamespace

# ------------------------------------------------------------------------------------------------ #
# Set up root logger to only log errors
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)
logging.getLogger("com.johnsnowlabs").setLevel(logging.ERROR)
logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.hadoop").setLevel(logging.ERROR)


# ------------------------------------------------------------------------------------------------ #
# Register shutdown hook
def shutdown(session: SparkSession):
    print("Shutting down Spark session...")
    session.stop()


# ------------------------------------------------------------------------------------------------ #
class SparkSessionPool:
    """
    Manages the creation and retrieval of Spark sessions, with support for
    standard and NLP-specific configurations.

    This class abstracts the setup and retry mechanism for creating a SparkSession,
    utilizing configurations from a specified configuration reader. It supports
    both standard Spark sessions and sessions pre-configured for NLP tasks.

    Attributes:
        _config_reader (AppConfigReader): Instance of the configuration reader used
            to retrieve file and Spark-specific settings.
        _parquet_block_size (int): Block size configuration for writing Parquet files.
        _spark_config (dict): Configuration details for setting up the Spark session,
            including memory allocation and retry attempts.
        _logger (Logger): Logger instance for logging Spark session creation and errors.

    Methods:
        get_or_create(nlp: bool = False) -> SparkSession:
            Retrieves an existing Spark session or creates a new one based on
            the specified type (standard or NLP). Registers the session for
            graceful shutdown at program exit.

        _create_session(memory: str, parquet_block_size: int, retries: int) -> SparkSession:
            Creates a standard SparkSession with specified memory and Parquet
            configuration, retrying if creation fails.

        _create_nlp_session(memory: str, parquet_block_size: int, retries: int) -> SparkSession:
            Creates a SparkSession configured for NLP tasks with additional settings
            such as Kryo serialization and specific library dependencies, retrying if
            creation fails.
    """

    def __init__(self, spark_config: Dict) -> None:
        """
        Initializes a SparkSessionPool with the given configuration reader class.

        Args:
            config_reader_cls (type[AppConfigReader]): A class for reading configurations.
                Defaults to AppConfigReader, allowing for custom configuration reader
                classes if needed.
        """
        self._spark_config = NestedNamespace(spark_config)
        self._spark = None  # Spark Session
        self._sparknlp = None  # Spark NLP Session
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = self.get_or_create(nlp=False)
        return self._spark

    @property
    def sparknlp(self) -> SparkSession:
        if self._sparknlp is None:
            self._sparknlp = self.get_or_create(nlp=True)
        return self._sparknlp

    def stop(self) -> None:
        if self._sparknlp is not None:
            self._sparknlp.stop()
            self._sparknlp = None
        elif self._spark is not None:
            self._spark.stop()
            self._spark = None

    def get_or_create(self, nlp: bool = False) -> SparkSession:
        """
        Retrieves or creates a Spark session based on the specified type.

        If `nlp` is True, creates a Spark session with configurations optimized
        for NLP processing. Otherwise, creates a standard Spark session.

        Args:
            nlp (bool): Indicates whether to create an NLP-optimized Spark session.
                Defaults to False.

        Returns:
            SparkSession: The created or retrieved Spark session.

        Raises:
            RuntimeError: If unable to create a Spark session after retrying.
        """
        if not nlp:
            spark_session = self._create_session(
                memory=self._spark_config.memory,
                parquet_block_size=self._spark_config.parquet_block_size,
                retries=self._spark_config.retries,
            )
        else:
            spark_session = self._create_nlp_session(
                memory=self._spark_config.memory,
                parquet_block_size=self._spark_config.parquet_block_size,
                retries=self._spark_config.retries,
            )
        atexit.register(shutdown, spark_session)
        return spark_session

    def _create_session(
        self, memory: str, parquet_block_size: int, retries: int
    ) -> SparkSession:
        """
        Creates a standard Spark session with the specified configuration.

        Attempts to create a Spark session with the given memory allocation,
        Parquet block size, and retry attempts. If an error occurs, retries
        using an exponential backoff strategy until the maximum number of
        retries is reached.

        Args:
            memory (str): The amount of memory to allocate for the Spark session.
            parquet_block_size (int): The block size for Parquet files in bytes.
            retries (int): The number of times to retry creating the Spark session
                if an error occurs.

        Returns:
            SparkSession: The created Spark session.

        Raises:
            RuntimeError: If unable to create a Spark session after retrying.
        """
        self._logger.debug("Creating a spark session.")
        attempts = 0
        error = None
        while attempts < retries:
            try:
                log4j_conf_path = "file:" + os.path.abspath("log4j.properties")
                self._logger.debug(
                    f"Creating an Spark session. log4j Configuration: {log4j_conf_path}"
                )
                spark = (
                    SparkSession.builder.appName("appvocai-discover")
                    .master("local[*]")
                    .config("spark.sql.session.timeZone", "UTC")
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
                        f"-Dlog4j.configurationFile={log4j_conf_path}",
                    )
                    .config(
                        "spark.executor.extraJavaOptions",
                        f"-Dlog4j.configurationFile={log4j_conf_path}",
                    )
                    .getOrCreate()
                )
                spark.sparkContext.setLogLevel("ERROR")
                return spark
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

    def _create_nlp_session(
        self, memory: str, parquet_block_size: int, retries: int
    ) -> SparkSession:
        """
        Creates a Spark session optimized for NLP tasks.

        Similar to `_create_session`, but includes additional configurations
        for NLP processing, such as Kryo serialization, extended result size,
        and necessary dependencies for NLP libraries. Retries session creation
        upon failure using exponential backoff.

        Args:
            memory (str): The amount of memory to allocate for the Spark session.
            parquet_block_size (int): The block size for Parquet files in bytes.
            retries (int): The number of times to retry creating the Spark session
                if an error occurs.

        Returns:
            SparkSession: The created Spark session optimized for NLP.

        Raises:
            RuntimeError: If unable to create a Spark session after retrying.
        """
        self._logger.debug("Creating a spark nlp session.")
        attempts = 0
        error = None
        while attempts < retries:
            try:
                log4j_conf_path = "file:" + os.path.abspath("log4j.properties")
                self._logger.debug(
                    f"Creating an SparkNLP session. log4j Configuration: {log4j_conf_path}"
                )
                spark = (
                    SparkSession.builder.appName("appvocai-discover-nlp")
                    .master("local[*]")
                    .config("spark.sql.session.timeZone", "UTC")
                    .config("spark.driver.memory", memory)
                    .config("spark.executor.memory", memory)
                    .config("spark.sql.parquet.block.size", parquet_block_size)
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
                        f"-Dlog4j.configurationFile={log4j_conf_path}",
                    )
                    .config(
                        "spark.executor.extraJavaOptions",
                        f"-Dlog4j.configurationFile={log4j_conf_path}",
                    )
                    .getOrCreate()
                )
                spark.sparkContext.setLogLevel("ERROR")
                return spark
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
