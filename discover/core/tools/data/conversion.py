#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/core/tools/data/converter.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 29th 2024 12:30:01 am                                                 #
# Modified   : Friday September 20th 2024 05:51:24 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark Utility Module"""
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from discover.core.config.reader import ConfigReader
from discover.core.tools.file.tempfile import TempFileMgr


# ------------------------------------------------------------------------------------------------ #
#                                     CONVERTER                                                    #
# ------------------------------------------------------------------------------------------------ #
class Converter(ABC):
    """Abstract base class for dataframe converters."""

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
        self.tempdir = os.getenv(key="TEMPDIR")

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def convert(
        self, data: Union[pd.DataFrame, DataFrame], *args, **kwargs
    ) -> Union[pd.DataFrame, DataFrame]:
        """Abstract method for methods that perform the dataframe conversion

        Args:
            data (Union[pd.DataFrame, DataFrame]): Data to be converted.
        """


# ------------------------------------------------------------------------------------------------ #
#                                         TO SPARK                                                 #
# ------------------------------------------------------------------------------------------------ #
class ToSpark(Converter):
    def __init__(
        self,
        spark: SparkSession,
        tempfile_manager_cls: type[TempFileMgr] = TempFileMgr,
    ) -> None:
        super().__init__()
        self._spark = spark
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
        """Converts a pandas DataFrame to PySpark

        Converts a pandas DataFrame to a Spark DataFrame by first converting
        the pandas DataFrame to a parquet file, which is then read
        using Spark, converting the contents to a Spark DataFrame. This
        may be faster than creating a DataFrame object directly from a
        pandas DataFrame for large files.

        """
        filename = (
            f"to_spark_tempfile_{datetime.now().strftime('%Y%m%d-%H%M%S')}.parquet"
        )
        try:
            with self._tempfile_manager as tfm:
                filepath = tfm.add_temp_file(filename=filename)
                data.to_parquet(filepath)
                return self._spark.read.parquet(filepath)
        except Exception as e:
            self.logger.error(
                f"Error converting Pandas DataFrame to Spark DataFrame: \n{e}"
            )
            raise

    def _convert_via_spark(self, data: pd.DataFrame) -> DataFrame:
        """Converts a Pandas Dataframe to PySpark format.

        Converts a Pandas DataFrame to a Spark DataFrame with
        spark createDataFrame method directly.

        """

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
    def __init__(self, tempfile_manager_cls: type[TempFileMgr] = TempFileMgr) -> None:
        super().__init__()

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
        filename = (
            f"to_pandas_tempfile_{datetime.now().strftime('%Y%m%d-%H%M%S')}.parquet"
        )
        """Converts a pandas DataFrame to PySpark"""
        try:
            with self._tempfile_manager as tfm:
                filepath = tfm.add_temp_file(filename)
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
