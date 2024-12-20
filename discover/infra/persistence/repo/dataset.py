#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/repo/dataset.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday October 8th 2024 07:31:47 pm                                                #
# Modified   : Thursday December 19th 2024 11:47:17 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repository Module"""
import logging
from typing import Optional

import pandas as pd
import pyspark
from git import Union

from discover.assets.data.dataset import Dataset
from discover.assets.workspace.repo import Repo
from discover.core.data_structure import DataStructure
from discover.infra.data.dal.dataset import DatasetDAO
from discover.infra.data.fao.pandas import PandasParquetFAO
from discover.infra.data.fao.spark import SparkParquetFAO
from discover.infra.persistence.repo.exception import (
    DatasetIOError,
    DatasetRemovalError,
)

DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """Repository for managing dataset objects.

    This class provides methods to add, retrieve, remove, and check the existence
    of datasets. It integrates with a DatasetDAL for data access operations and
    a FilesetRepo for managing dataset storage.

    Args:
        pandas_fao (DatasetDAL): Data Access Layer for managing dataset persistence.
        fileset_repo (FilesetRepo): Repository for managing dataset file storage.
    """

    def __init__(
        self,
        dataset_dao: DatasetDAO,
        pandas_fao: PandasParquetFAO,
        spark_fao: SparkParquetFAO,
    ) -> None:
        self._dataset_dao = dataset_dao
        self._pandas_fao = pandas_fao
        self._spark_fao = spark_fao

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def stop_spark(self) -> None:
        self._spark_fao.stop_spark_session()

    def add(self, dataset: Dataset) -> None:
        """Adds a dataset to the repository.

        Args:
            dataset (Dataset): The dataset to add.

        Raises:
            Exception: If an error occurs during the operation.
        """
        try:
            self._write(dataset=dataset)
            self._dataset_dao.create(dataset=dataset)
        except Exception as e:
            msg = f"Exception occurred while saving dataset {dataset.asset_id} to object storage.\n{e}"
            raise Exception(msg)

    def get(self, asset_id: str) -> Optional[Dataset]:
        """Lazily retrieves a dataset object by its ID.

        Args:
            asset_id (str): The unique identifier of the dataset.

        Returns:
            Optional[Dataset]: The retrieved dataset, or None if not found.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            DatasetIOError: If an I/O error occurs during the operation.
        """
        try:
            return self._dataset_dao.read(asset_id=asset_id)
        except Exception as e:
            msg = f"Exception occurred while reading the dataset {asset_id} object."
            self._logger.exception(msg)
            raise DatasetIOError(msg, e) from e

    def remove(self, asset_id: str, ignore_errors: bool = False) -> None:
        """Removes a dataset by its ID.

        Args:
            asset_id (str): The unique identifier of the dataset to remove.
            ignore_errors (bool): If True, suppresses errors when the dataset does not exist.

        Raises:
            DatasetRemovalError: If an error occurs during the removal and ignore_errors is False.
        """

        try:
            dataset = self._dataset_dao.read(asset_id=asset_id)
            self._dataset_dao.delete(filepath=dataset.filepath)
        except KeyError:
            if ignore_errors:
                msg = f"Warning: Dataset {asset_id} does not exist."
                self._logger.warning(msg)
            else:
                msg = f"Exception: Dataset {asset_id} does not exist"
                self._logger.exception(msg)
                raise DatasetRemovalError(msg)
        except Exception as e:
            if ignore_errors:
                msg = f"Warning: Unknown exception occurred while removing Dataset {asset_id}.\n{e}"
                self._logger.warning(msg)
            else:
                msg = f"Unknown exception occurred while removing Dataset {asset_id}.\n{e}"
                self._logger.exception(msg)
                raise DatasetRemovalError(msg)

    def exists(self, asset_id: str) -> bool:
        """Checks if a dataset exists by its ID.

        Args:
            asset_id (str): The unique identifier of the dataset.

        Returns:
            bool: True if the dataset exists, False otherwise.
        """
        return self._dataset_dao.exists(asset_id=asset_id)

    def read(self, filepath: str, data_structure: DataStructure) -> Optional[Dataset]:
        """Reads the designated data structure from file."""
        if data_structure == DataStructure.PANDAS:
            return self._pandas_fao.read(filepath=filepath)
        elif (
            data_structure == DataStructure.SPARK
            or data_structure == DataStructure.SPARKNLP
        ):
            return self._spark_fao.read(filepath=filepath, nlp=data_structure.nlp)
        else:
            ValueError(f"Invalid data_structure value.\n{data_structure}")

    def _write(self, dataset: Dataset) -> None:
        """Writes a dataset to file."""
        if dataset.data_structure == DataStructure.PANDAS:
            self._pandas_fao.write(filepath=dataset.filepath)
        elif (
            dataset.data_structure == DataStructure.SPARK
            or dataset.data_structure == DataStructure.SPARKNLP
        ):
            self._spark_fao.write(
                filepath=dataset.filepath, nlp=dataset.data_structure.nlp
            )
        else:
            ValueError(f"Invalid data_structure value.\n{dataset.data_structure}")
