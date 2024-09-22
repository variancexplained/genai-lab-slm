#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/frameworks/spark/session.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 03:33:40 pm                                            #
# Modified   : Saturday September 21st 2024 03:52:13 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark Utils Module"""

import logging

from pyspark.sql import SparkSession


# ------------------------------------------------------------------------------------------------ #
class SparkSessionProvider:
    """Factory returning Spark Session objects."""

    def __init__(self, memory: str = "32g") -> None:
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._spark = self._build(memory=memory)

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def _build(
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
