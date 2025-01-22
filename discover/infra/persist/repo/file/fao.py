#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/repo/file/fao.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 26th 2024 04:10:40 pm                                             #
# Modified   : Wednesday January 22nd 2025 12:37:39 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Access Module"""
import logging
import os
import shutil
from typing import Optional, Union

import pandas as pd
import pyspark
from pyspark.sql import SparkSession

from discover.core.dtypes import DFType
from discover.infra.persist.repo.base import DAL
from discover.infra.persist.repo.file.factory import DataFrameIOFactory
from discover.infra.utils.file.fileset import FileFormat

DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                                           FAO                                                    #
# ------------------------------------------------------------------------------------------------ #
class FAO(DAL):
    """
    A class for handling file access operations (FAO) for datasets.

    This class manages reading and writing datasets in various formats and structures,
    leveraging an IO factory for specific implementations based on the dataframe type
    and file format.

    Args:
        iofactory (DataFrameIOFactory): Factory for creating readers and writers
            specific to dataframe types and file formats.

    Methods:
        create(dftype, filepath, file_format, created, data, overwrite):
            Writes a dataset to the specified file path with the given format and type.
        read(filepath, file_format, dftype, spark):
            Reads a dataset from the specified file path with the given format and type.
    """

    def __init__(self, iofactory: DataFrameIOFactory):
        self._iofactory = iofactory
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(
        self,
        dftype: DFType,
        filepath: str,
        file_format: FileFormat,
        dataframe: Union[pd.DataFrame, DataFrame],
        overwrite: bool = False,
    ) -> None:
        """
        Writes a dataset to the specified file path with the given format and type.

        Args:
            dftype (DFType): The type of the dataframe (e.g., PANDAS, SPARK).
            filepath (str): The destination file path.
            file_format (FileFormat): The file format (e.g., CSV, PARQUET).
            created (datetime): The timestamp of the dataset creation.
            data (Union[pd.DataFrame, DataFrame]): The dataset to be written.
            overwrite (bool): Whether to overwrite the file if it already exists.
                Defaults to False.

        Raises:
            Exception: If the write operation fails due to configuration or IO issues.
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        writer = self._iofactory.get_writer(
            dftype=dftype,
            file_format=file_format,
        )
        writer.write(
            dataframe=dataframe,
            filepath=filepath,
            overwrite=overwrite,
        )
        if not os.path.exists(filepath):
            msg = f"Failure to write dataframe to {filepath}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    def read(
        self,
        filepath: str,
        dftype: DFType,
        file_format: Optional[FileFormat] = None,
        spark: Optional[SparkSession] = None,
    ) -> DataFrame:
        """
        Reads a dataset from the specified file path with the given format and type.

        Args:
            filepath (str): The path to the dataset file.
            dftype (DFType): The type of the dataframe (e.g., PANDAS, SPARK).
            spark (Optional[SparkSession]): A Spark session, required for Spark-based
                operations. Defaults to None.

        Returns:
            DataFrame: The loaded dataset as a Pandas or Spark dataframe.

        Raises:
            Exception: If the read operation fails due to configuration or IO issues.
        """
        file_format = file_format or FileFormat.PARQUET

        reader = self._iofactory.get_reader(
            dftype=dftype,
            file_format=file_format,
        )

        if dftype == DFType.PANDAS:
            return reader.read(
                filepath=filepath,
            )
        else:
            if spark is None:
                msg = "Unable to read spark dataframe. Spark session is None. When reading spark dataframes a spark session must be provided."
                self._logger.error(msg)
                raise RuntimeError(msg)
            return reader.read(
                filepath=filepath,
                spark=spark,
            )

    def exists(self, filepath: str) -> bool:
        """
        Checks whether a file exists at the specified file path.

        Args:
            filepath (str): The path to the file.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        return os.path.exists(filepath)

    def delete(self, filepath: str) -> None:
        """
        Deletes a file or directory at the specified file path.

        Args:
            filepath (str): The path to the file or directory.

        Raises:
            Exception: If an unexpected error occurs during deletion.
        """
        if os.path.isfile(filepath):
            os.remove(filepath)
            msg = f"File {os.path.basename(filepath)} successfully removed from the repository."
            self._logger.debug(msg)
        elif os.path.isdir(filepath):
            shutil.rmtree(filepath, ignore_errors=True)
            msg = f"Directory {os.path.basename(filepath)} successfully removed from the repository."
            self._logger.debug(msg)
        elif not os.path.exists(filepath):
            msg = f"Filepath {filepath} does not exist."
            self._logger.warning(msg)
        else:
            msg = "Unexpected exception occurred."
            self._logger.exception(msg)
            raise Exception(msg)
