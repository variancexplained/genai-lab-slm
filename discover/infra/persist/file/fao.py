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
# Modified   : Friday December 27th 2024 09:21:46 am                                               #
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

from discover.core.data_structure import (
    DataEnvelope,
    DataEnvelopeConfig,
    DataFrameStructureEnum,
)
from discover.infra.persist.dataframe.factory import DataFrameIOFactory

DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
#                                           FAO                                                    #
# ------------------------------------------------------------------------------------------------ #
class FAO:
    """
    File Access Object (FAO) for managing dataset file operations such as creation,
    reading, deletion, and validation.

    This class uses an IO factory to dynamically select appropriate readers and writers
    based on the dataset's structure and format.

    Attributes:
        _fao_config (dict): Configuration for file access operations, including arguments
            for readers and writers.
        _io_factory (DataFrameIOFactory): Factory for creating appropriate readers and writers.
        _logger (logging.Logger): Logger instance for the FAO class.

    Methods:
        create(data_envelope, overwrite): Writes a dataset to a file.
        read(data_envelope_config, spark): Reads a dataset from a file.
        exists(filepath): Checks if a file exists.
        delete(filepath): Deletes a file or directory.
        reset(verified): Resets the FAO database directory.
    """

    def __init__(
        self,
        fao_config: dict,
        io_factory: DataFrameIOFactory,
    ):
        """
        Initializes the FAO object with configuration and IO factory.

        Args:
            fao_config (dict): Configuration for file access operations, including read/write
                arguments for various dataframe structures and formats.
            io_factory (DataFrameIOFactory): Factory instance for obtaining readers and writers.
        """
        self._fao_config = fao_config
        self._io_factory = io_factory
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(
        self,
        data_envelope: DataEnvelope,
        overwrite: bool = False,
    ) -> None:
        """
        Writes a dataset to a file using the appropriate writer from the IO factory.

        Args:
            data_envelope (DataEnvelope): The dataset to be written, including its data,
                file path, structure, and format.
            overwrite (bool): Whether to overwrite the file if it exists. Defaults to False.

        Raises:
            Exception: If the writer encounters an error during the write operation.
        """
        writer = self._io_factory.get_writer(
            dataframe_structure=data_envelope.dataframe_structure,
            file_format=data_envelope.file_format,
        )
        writer.write(
            data=data_envelope.data,
            filepath=data_envelope.filepath,
            overwrite=overwrite,
            **self._fao_config[data_envelope.dataframe_structure.value][
                data_envelope.file_format.value
            ]["write_kwargs"],
        )

    def read(
        self,
        data_envelope_config: DataEnvelopeConfig,
        spark: Optional[SparkSession] = None,
    ) -> DataFrame:
        """
        Reads a dataset from a file using the appropriate reader from the IO factory.

        Args:
            data_envelope_config (DataEnvelopeConfig): Configuration specifying the dataset's
                file path, structure, and format.
            spark (Optional[SparkSession]): Spark session required for reading Spark DataFrames.
                Defaults to None.

        Returns:
            DataFrame: The loaded dataset as a Pandas or Spark DataFrame.

        Raises:
            Exception: If the reader encounters an error during the read operation.
        """
        reader = self._io_factory.get_reader(
            dataframe_structure=data_envelope_config.dataframe_structure,
            file_format=data_envelope_config.file_format,
        )

        if data_envelope_config.dataframe_structure == DataFrameStructureEnum.PANDAS:
            return reader.read(
                filepath=data_envelope_config.filepath,
                **self._fao_config[data_envelope_config.dataframe_structure.value][
                    data_envelope_config.file_format.value
                ]["read_kwargs"],
            )
        else:
            return reader.read(
                filepath=data_envelope_config.filepath,
                spark=spark,
                **self._fao_config[data_envelope_config.dataframe_structure.value][
                    data_envelope_config.file_format.value
                ]["read_kwargs"],
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
