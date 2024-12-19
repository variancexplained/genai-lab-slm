#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/repo/fileset.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday October 8th 2024 07:31:47 pm                                                #
# Modified   : Wednesday December 18th 2024 07:17:16 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repository Module"""
import logging
import os
import shutil
from datetime import datetime
from typing import Optional, Union

import pandas as pd
import pyspark

from discover.asset.workspace.repo import Repo
from discover.core.data_structure import DataFrameType
from discover.infra.persistence.dal.fileset.centralized import (
    CentralizedFilesetDAL as FAOCFS,
)
from discover.infra.persistence.dal.fileset.distributed import (
    DistributedFilesetDAL as FAODFS,
)

DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
class FilesetRepo(Repo):
    """Repository for managing datasets in a fileset.

    This class provides methods to add, retrieve, delete, and check the existence
    of datasets stored as files. It supports both pandas and PySpark DataFrames.

    Args:
        fao_cfs (FAOCFS): File access object for managing files in a centralized file system.
        fao_dfs (FAODFS): File access object for managing files in a distributed file system.
    """

    def __init__(self, fao_cfs: FAOCFS, fao_dfs: FAODFS) -> None:
        self._fao_cfs = fao_cfs
        self._fao_dfs = fao_dfs
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add(self, asset_id: str, data: DataFrame) -> Optional[str]:
        """Adds a dataset to the file set repository.

        Saves the provided data to the appropriate file system based on the data type.

        Args:
            asset_id (str): The unique identifier for the dataset.
            data (DataFrame): The dataset to be added, either as a pandas DataFrame
                or a PySpark DataFrame.

        Returns:
            Optional[str]: String containing the file location

        Raises:
            TypeError: If the data is not a pandas or PySpark DataFrame.
            Exception: If an error occurs during the save operation.
        """
        # Obtain the file access object for the data type
        fao = self._get_write_fao(data=data)
        # Filepath assignment is delegated to the file access layer.
        filepath = fao.get_filepath(name=asset_id)

        if os.path.exists(filepath):
            msg = f"Unable to add Fileset name {asset_id}. It already exists at {filepath}."
            raise FileExistsError(msg)

        fao.create(filepath=filepath, data=data)

        msg = f"Fileset {asset_id} successfully added to the repository at {filepath}."
        self._logger.info(msg)

        return filepath

    def get(
        self, asset_id: str, dataframe_type: DataFrameType = DataFrameType.PANDAS
    ) -> Optional[Union[pd.DataFrame, DataFrame]]:
        """Retrieves a dataset from the repository.

        Loads the dataset as the specified type of DataFrame.

        Args:
            asset_id (str): The unique identifier for the dataset.
            dataframe_type (DataFrameType): The type of DataFrame to load (PANDAS, SPARK, or SPARKNLP).

        Returns:
            Optional[Dataset]: The loaded dataset.

        Raises:
            ValueError: If the specified dataframe_type is invalid.
            FileNotFoundError: If the dataset does not exist in the repository.
            Exception: If an error occurs during the load operation.
        """
        # Obtain the file access object for the dataframe type
        fao = self._get_read_fao(dataframe_type=dataframe_type)
        # Filepath assignment is delegated to the file access layer.
        filepath = fao.get_filepath(name=asset_id)

        return fao.read(filepath=filepath)

    def remove(self, asset_id: str, ignore_errors: bool = False) -> None:
        """Removes a dataset from the repository.

        Deletes the file associated with the given asset ID.

        Args:
            asset_id (str): The unique identifier for the dataset.
            ignore_errors (bool): Whether to ignore errors when removing directories.

        Raises:
            FileNotFoundError: If the dataset file does not exist.
            Exception: If an error occurs during the removal operation.
        """

        filepath = self._fao_cfs.get_filepath(name=asset_id)

        try:
            os.remove(filepath)
        except FileNotFoundError:
            msg = f"Unable to remove file {asset_id}. File {filepath} not found."
            raise FileNotFoundError(msg)
        except IsADirectoryError:
            shutil.rmtree(filepath, ignore_errors=ignore_errors)
        except Exception as e:
            msg = f"Unable to remove file {asset_id}. Unknown exception.\n{e}"
            raise Exception(msg)

    def get_filepath(self, asset_id: str) -> str:
        return self._fao_cfs.get_filepath(name=asset_id)

    def exists(self, asset_id: str) -> bool:
        """Checks if a dataset exists in the repository.

        Args:
            asset_id (str): The unique identifier for the dataset.

        Returns:
            bool: True if the dataset exists, False otherwise.
        """
        filepath = self._fao_cfs.get_filepath(name=asset_id)
        return os.path.exists(filepath)

    def persisted(self, asset_id: str) -> Optional[datetime]:
        """Returns the datetime the file was persisted.

        Args:
            asset_id (str): The unique identifier for the dataset.

        Returns:
            datetime: Datetime the file was persisted
        """
        dt_persisted = None
        filepath = self._fao_cfs.get_filepath(name=asset_id)
        if os.path.exists(filepath):
            dt_persisted = datetime.fromtimestamp(os.path.getctime(filepath))
        return dt_persisted

    def _get_read_fao(self, dataframe_type: DataFrameType) -> Union[FAOCFS, FAODFS]:
        """Returns a file access object based on the dataframe_type"""
        if dataframe_type == DataFrameType.PANDAS:
            return self._fao_cfs
        elif dataframe_type in (DataFrameType.SPARK, DataFrameType.SPARKNLP):
            return self._fao_dfs
        else:
            msg = f"Invalid dataframe_type. Expected DataFrameType.PANDAS, DataFrameType.SPARK, or DataFrameType.SPARKNLP. Received {dataframe_type}"
            raise TypeError(msg)

    def _get_write_fao(self, data: DataFrame) -> Union[FAOCFS, FAODFS]:
        """Returns a file access object based on the data type"""
        if isinstance(data, (pd.DataFrame, pd.core.frame.DataFrame)):
            return self._fao_cfs
        elif isinstance(data, pyspark.sql.DataFrame):
            return self._fao_dfs
        else:
            msg = f"Invalid data type. Expected a pandas or PySpark DataFrame, received {type(data)}"
            raise TypeError(msg)
