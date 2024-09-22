#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/frameworks/spark/factory.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 07:31:28 pm                                             #
# Modified   : Saturday September 21st 2024 03:30:00 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark Utils Module"""

import logging

from pyspark.sql import SparkSession


# ------------------------------------------------------------------------------------------------ #
class SparkSessionFactory:
    """Factory returning Spark Session objects."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build(
        self, name: str = "AppVoCAI-Discover", memory: str = "32g", nlp: bool = False
    ) -> SparkSession:
        """Returns a Spark Sesion for the given parameters.

        Args:
            name (str): The Spark Session name
            memory (str): The executor and driver memory parameters.
            nlp (bool): Whether the Spark Session will be used for Spark NLP operations.

        """
        if nlp:
            return self._nlp_session(name=name, memory=memory)
        else:
            return self._session(name=name, memory=memory)

    def _session(
        self, name: str = "AppVoCAI-Discover", memory: str = "32g"
    ) -> SparkSession:
        """Returns a standard Spark Session"""
        try:
            return (
                SparkSession.builder.appName(name)
                .master("local[*]")
                .config("spark.driver.memory", memory)
                .config("spark.executor.memory", memory)
                .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
                .getOrCreate()
            )
        except Exception as e:
            self.logger.error(f"Error creating Spark session: {e}")
            raise

    def _nlp_session(
        self, name: str = "AppVoCAI-Discover", memory: str = "32g"
    ) -> SparkSession:
        """Returns a Spark Session configured for Spark NLP operations."""
        try:
            return (
                SparkSession.builder.appName(name)
                .master("local[*]")
                .config("spark.driver.memory", memory)
                .config("spark.executor.memory", memory)
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
                .config(
                    "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                )
                .config("spark.kryoserializer.buffer.max", "2000M")
                .config("spark.driver.maxResultSize", "0")
                .config(
                    "spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3"
                )
                .getOrCreate()
            )
        except Exception as e:
            self.logger.error(f"Error creating Spark session: {e}")
            raise
