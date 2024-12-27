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
# Modified   : Thursday December 26th 2024 07:37:56 pm                                             #
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

from discover.core.data_structure import DataFrameStructureEnum
from discover.core.file import FileFormat
from discover.infra.persist.dataframe.factory import DataFrameIOFactory

DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                                           FAO                                                    #
# ------------------------------------------------------------------------------------------------ #
class FAO:
    """File Access Object (FAO) for reading, writing, and managing data files.

    Args:
        fao_config (dict): Configuration dictionary containing settings read/write operations.
        io_factory (IOFactory): IOFactory providing readers and writers
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
        filepath: str,
        data: DataFrame,
        dataframe_structure: DataFrameStructureEnum,
        file_format: FileFormat = FileFormat.PARQUET,
        overwrite: bool = False,
    ) -> None:
        """Creates a file for the designated dataframe structure and file format.

        Args:
            filepath (str): Path where the file should be created.
            data (DataFrame): The data to be written to the file.
            dataframe_structure (DataFrameStructureEnum): The Enum for pandas, spark, or sparknlp dataframes.
            file_format (FileFormat): The format of the file to create (default is Parquet).
            overwrite (bool): Whether existing files can be overwritten.

        Raises:
            ValueError: If an unsupported file format or dataframe structure is provided.

        Logs:
            Error: If an invalid file format is specified.
        """
        writer = self._io_factory.get_writer(
            dataframe_structure=dataframe_structure, file_format=file_format
        )
        writer.write(
            data=data,
            filepath=filepath,
            overwrite=overwrite,
            **self._fao_config[dataframe_structure.value][file_format.value][
                "write_kwargs"
            ],
        )

    def read(
        self,
        filepath: str,
        dataframe_structure: DataFrameStructureEnum,
        file_format: FileFormat = FileFormat.PARQUET,
        spark: Optional[SparkSession] = None,
    ) -> DataFrame:
        """Reads a data file in the specified dataframe structure and file format.

        Args:
            filepath (str): Path to the file to read.
            dataframe_structure (DataFrameStructureEnum): The Enum for pandas, spark, or sparknlp dataframes.
            file_format (FileFormat): The format of the file to read (default is Parquet).

        Returns:
            DataFrame: The data read from the file.

        Raises:
            ValueError: If an unsupported file format is provided.

        Logs:
            Error: If an invalid file format is specified.
        """
        # Inspect arguments.
        msg = f"\n\nFAO read arguments:\ndataframe_structure: {dataframe_structure.value}\nfile_format: {file_format.value}"
        self._logger.debug(msg)

        reader = self._io_factory.get_reader(
            dataframe_structure=dataframe_structure, file_format=file_format
        )

        msg = f"Returned {reader.__class__.__name__} from DataFrameIOFActory"
        self._logger.debug(msg)

        if dataframe_structure == DataFrameStructureEnum.PANDAS:
            return reader.read(
                filepath=filepath,
                **self._fao_config[dataframe_structure.value][file_format.value][
                    "read_kwargs"
                ],
            )
        else:
            return reader.read(
                filepath=filepath,
                spark=spark,
                **self._fao_config[dataframe_structure.value][file_format.value][
                    "read_kwargs"
                ],
            )

    def exists(self, filepath: str) -> bool:
        """Checks if a file or directory exists at the given path.

        Args:
            filepath (str): The path to check for existence.

        Returns:
            bool: True if the file or directory exists, False otherwise.
        """
        return os.path.exists(filepath)

    def delete(self, filepath: str) -> None:
        """Deletes a file or directory at the specified path.

        Args:
            filepath (str): The path to the file or directory to delete.

        Raises:
            ValueError: If the filepath is neither a valid file nor directory.

        Logs:
            Error: If the filepath is invalid.
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
