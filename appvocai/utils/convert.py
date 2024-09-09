#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/utils/convert.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 29th 2024 12:30:01 am                                                 #
# Modified   : Tuesday August 27th 2024 10:54:13 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark Utility Module"""
import logging
from typing import Callable

import pandas as pd
from appvocai.utils.base import Converter
from appvocai.utils.tempfile import TempFileMgr
from pyspark.sql import DataFrame, SparkSession


# ------------------------------------------------------------------------------------------------ #
#                                         TO SPARK                                                 #
# ------------------------------------------------------------------------------------------------ #
class ToSpark(Converter):
    def __init__(
        self,
        spark: SparkSession,
        task_cls: Callable,
        tempfile_manager_cls: type[TempFileMgr] = TempFileMgr,
    ) -> None:
        super().__init__()
        self._spark = spark
        self._task_cls = task_cls
        self._tempfile_manager = tempfile_manager_cls()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def convert(
        self, data: pd.DataFrame, viaspark: bool = False, **kwargs
    ) -> DataFrame:
        """Converts a pandas DataFrame to a Spark DataFrame

        Args:
            data (pd.DataFrame): DataFrame in Pandas format
            viaspark (bool): Whether to convert via spark. May be computationally expensive.
        """
        if viaspark:
            return self._convert_via_spark(data=data)
        else:
            return self._convert_spark(data=data)

    def _convert_spark(self, data: pd.DataFrame) -> DataFrame:
        """Converts a pandas DataFrame to PySpark"""
        filename = f"to_spark_tempfile_{self._task_cls.__name__}.parquet"
        try:
            filepath = self._tempfile_manager.add_temp_file(filename=filename)
            data.to_parquet(filepath)
            return self._spark.read.parquet(filepath)
        except Exception as e:
            self.logger.error(
                f"Error converting Pandas DataFrame to Spark DataFrame: \n{e}"
            )
            raise

    def _convert_via_spark(self, data: pd.DataFrame) -> DataFrame:
        """Converts a Pandas Dataframe to PySpark format."""

        try:
            return self._spark.createDataFrame(data)
        except Exception as e:
            self.logger.exception(
                f"Exception while converting Pandas DataFrame to PySpark. \n{e}"
            )
            raise


# ------------------------------------------------------------------------------------------------ #
#                                         TO PANDAS                                                #
# ------------------------------------------------------------------------------------------------ #
class ToPandas(Converter):
    def __init__(
        self, task_cls: Callable, tempfile_manager_cls: type[TempFileMgr] = TempFileMgr
    ) -> None:
        super().__init__()
        self._task_cls = task_cls
        self._tempfile_manager = tempfile_manager_cls()

    def convert(self, data: pd.DataFrame, viaspark: bool = False) -> DataFrame:
        """Converts a pandas DataFrame to a Spark DataFrame

        Args:
            data (pd.DataFrame): DataFrame in Pandas format
            viaspark (bool): Whether to convert via spark. May be computationally expensive.
        """
        if viaspark:
            return self._convert_via_spark(data=data)
        else:
            return self._convert_pandas(data=data)

    def _convert_pandas(self, data: DataFrame) -> DataFrame:
        filename = f"to_pandas_tempfile_{self._task_cls.__name__}.parquet"
        """Converts a pandas DataFrame to PySpark"""
        try:
            filepath = self._tempfile_manager.add_temp_file(filename)
            data.write.mode("overwrite").parquet(filepath)
            return pd.read_parquet(filepath)
        except Exception as e:
            self.logger.error(f"Error converting DataFrame to Spark DataFrame.\n {e}")
            raise

    def _convert_via_spark(self, data: pd.DataFrame) -> DataFrame:
        """Converts a Pandas Dataframe to PySpark format."""
        try:
            return data.toPandas()
        except Exception as e:
            self.logger.exception(
                f"Exception while converting PySpark to Pandas. \n{e}"
            )
            raise
