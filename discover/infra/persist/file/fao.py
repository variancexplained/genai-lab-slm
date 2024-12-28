#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/file/fao.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 26th 2024 04:10:40 pm                                             #
# Modified   : Saturday December 28th 2024 12:52:27 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Access Module"""
import logging
import os
import shutil
from datetime import datetime
from typing import Optional, Union

import pandas as pd
import pyspark
from pyspark.sql import SparkSession

from discover.asset.dataset import DFType, FileFormat
from discover.infra.persist.dataframe.factory import DataFrameIOFactory

DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                                           FAO                                                    #
# ------------------------------------------------------------------------------------------------ #
class FAO:
    """
    A class for handling file access operations (FAO) for datasets.

    This class manages reading and writing datasets in various formats and structures,
    leveraging an IO factory for specific implementations based on the dataframe type
    and file format.

    Args:
        fao_config (dict): Configuration dictionary specifying read/write options
            for different dataframe types and file formats.
        io_factory (DataFrameIOFactory): Factory for creating readers and writers
            specific to dataframe types and file formats.

    Methods:
        create(dftype, filepath, file_format, created, data, overwrite):
            Writes a dataset to the specified file path with the given format and type.
        read(filepath, file_format, dftype, spark):
            Reads a dataset from the specified file path with the given format and type.
    """

    def __init__(
        self,
        fao_config: dict,
        io_factory: DataFrameIOFactory,
    ):
        self._fao_config = fao_config
        self._io_factory = io_factory
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(
        self,
        dftype: DFType,
        filepath: str,
        file_format: FileFormat,
        created: datetime,
        data: Union[pd.DataFrame, DataFrame],
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
        writer = self._io_factory.get_writer(
            dftype=dftype,
            file_format=file_format,
        )
        writer.write(
            data=data,
            filepath=filepath,
            overwrite=overwrite,
            **self._fao_config[dftype.value][file_format.value]["write_kwargs"],
        )

    def read(
        self,
        filepath: str,
        file_format: FileFormat,
        dftype: DFType,
        spark: Optional[SparkSession] = None,
    ) -> DataFrame:
        """
        Reads a dataset from the specified file path with the given format and type.

        Args:
            filepath (str): The path to the dataset file.
            file_format (FileFormat): The file format (e.g., CSV, PARQUET).
            dftype (DFType): The type of the dataframe (e.g., PANDAS, SPARK).
            spark (Optional[SparkSession]): A Spark session, required for Spark-based
                operations. Defaults to None.

        Returns:
            DataFrame: The loaded dataset as a Pandas or Spark dataframe.

        Raises:
            Exception: If the read operation fails due to configuration or IO issues.
        """
        reader = self._io_factory.get_reader(
            dftype=dftype,
            file_format=file_format,
        )

        if dftype == DFType.PANDAS:
            return reader.read(
                filepath=filepath,
                **self._fao_config[dftype.value][file_format.value]["read_kwargs"],
            )
        else:
            return reader.read(
                filepath=filepath,
                spark=spark,
                **self._fao_config[dftype.value][file_format.value]["read_kwargs"],
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
        try:
            os.remove(filepath)
        except FileNotFoundError:
            msg = f"File {filepath} not found."
            self._logger.warning(msg)
        except OSError:
            shutil.rmtree(filepath)
        except Exception as e:
            msg = f"Unexpected exception occurred.\n{e}"
            self._logger.error(msg)
            raise Exception(msg)

    def reset(self, verified: bool = False) -> None:
        """
        Resets the FAO database directory by deleting all files.

        Args:
            verified (bool): Whether the reset action is pre-approved. If False, prompts
                for confirmation before proceeding.

        Raises:
            Exception: If an unexpected error occurs during reset.
        """
        if verified:
            shutil.rmtree(self._basedir)
            self._logger.warning(f"{self.__class__.__name__} has been reset.")
        else:
            proceed = input(
                f"Resetting the {self.__class__.__name__} object database is irreversible. To proceed, type 'YES'."
            )
            if proceed == "YES":
                shutil.rmtree(self._basedir)
                self._logger.warning(f"{self.__class__.__name__} has been reset.")
            else:
                self._logger.info(f"{self.__class__.__name__} reset has been aborted.")
