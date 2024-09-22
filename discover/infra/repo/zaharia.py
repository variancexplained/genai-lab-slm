#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/repo/zaharia.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 02:58:50 pm                                               #
# Modified   : Saturday September 21st 2024 11:05:53 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Zaharia Review Repo Module"""
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from discover.infra.config.reader import ConfigReader
from discover.infra.repo.base import ReviewRepo


# ------------------------------------------------------------------------------------------------ #
class ZahariaRepo(ReviewRepo):
    """
    A repository for managing review data using Apache Spark. This class supports
    reading and writing of PySpark DataFrames in Parquet format, designed for
    distributed processing in a Spark environment. It extends the base `ReviewRepo`
    class and implements Spark-specific file operations.

    Attributes:
    -----------
    _spark : SparkSession
        The Spark session used for reading and writing PySpark DataFrames.
    _logger : logging.Logger
        Logger for handling log messages during file operations.

    Methods:
    --------
    _read(filepath: str) -> DataFrame
        Reads review data from a Parquet file using Spark and returns it as a PySpark DataFrame.

    _write(data: DataFrame, filepath: str) -> None
        Writes review data to a Parquet file using Spark.
    """

    def __init__(
        self,
        config_reader_cls: type[ConfigReader] = ConfigReader,
        spark: Optional[SparkSession] = None,
    ) -> None:
        """
        Initializes the ZahariaRepo with a file format, configuration, and an optional Spark session.
        If no Spark session is provided, a new session will be created automatically. In production
        environments, a Spark session must be provided explicitly.

        Parameters:
        -----------
        file_format : StorageFormat
            The format in which the review data will be stored. Expected to be PARQUET or PARQUET_PARTITIONED.
        config_reader_cls : type[ConfigReader], optional
            The configuration class that provides environment-specific settings, by default ConfigReader.
        spark : Optional[SparkSession], optional
            The Spark session to use for reading and writing data. If not provided, a new session will
            be created. In production, this parameter is required.

        Raises:
        -------
        TypeError:
            If no Spark session is provided in a production environment.
        """
        super().__init__(config_reader_cls=config_reader_cls)
        self._spark = spark
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        env = self._config_reader.get_environment()
        if env == "prod" and self._spark is None:
            msg = "A Spark Session is required in production environments."
            self._logger.exception(msg)
            raise TypeError(msg)
        elif self._spark is None:
            self._spark = (
                spark or SparkSession.builder.appName("AppVoCAI-D").getOrCreate()
            )

    def _read(self, filepath: str, **kwargs) -> DataFrame:
        """
        Reads review data from a Parquet file using Spark.

        Parameters:
        -----------
        filepath : str
            The full file path to the Parquet file to be read.

        Returns:
        --------
        pyspark.sql.DataFrame:
            The review data read from the file as a PySpark DataFrame.
        """
        return self._spark.read.parquet(filepath, kwargs=kwargs)

    def _write(self, data: DataFrame, filepath: str, **kwargs) -> None:
        """
        Writes the given review data to a Parquet file using Spark.

        Parameters:
        -----------
        data : pyspark.sql.DataFrame
            The review data to be written to the file.
        filepath : str
            The full file path where the Parquet file will be saved.

        Raises:
        -------
        FileExistsError:
            If the file already exists in the specified path.
        """
        data.write.parquet(filepath, kwargs=kwargs)
