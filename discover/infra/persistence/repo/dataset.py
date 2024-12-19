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
# Modified   : Thursday December 19th 2024 05:00:17 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repository Module"""
import logging
from typing import Optional

from discover.assets.data.dataset import Dataset
from discover.assets.workspace.repo import Repo
from discover.infra.persistence.dal.dao.dataset import DatasetDAL
from discover.infra.persistence.dal.dao.exception import ObjectNotFoundError
from discover.infra.persistence.repo.exception import (
    DatasetIOError,
    DatasetNotFoundError,
    DatasetRemovalError,
)
from discover.infra.persistence.repo.fileset import FilesetRepo


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """Repository for managing dataset objects.

    This class provides methods to add, retrieve, remove, and check the existence
    of datasets. It integrates with a DatasetDAL for data access operations and
    a FilesetRepo for managing dataset storage.

    Args:
        dataset_dal (DatasetDAL): Data Access Layer for managing dataset persistence.
        fileset_repo (FilesetRepo): Repository for managing dataset file storage.
    """

    def __init__(
        self,
        dataset_dal: DatasetDAL,
        fileset_repo: FilesetRepo,
    ) -> None:
        self._dataset_dal = dataset_dal
        self._fileset_repo = fileset_repo
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add(self, dataset: Dataset) -> None:
        """Adds a dataset to the repository.

        Persists the dataset using the DatasetDAL.

        Args:
            dataset (Dataset): The dataset to add.

        Raises:
            Exception: If an error occurs during the operation.
        """
        try:
            self._dataset_dal.create(dataset=dataset)
        except Exception as e:
            msg = f"Exception occurred while saving dataset {dataset.asset_id} to object storage.\n{e}"
            raise Exception(msg)

    def get(self, asset_id: str) -> Optional[Dataset]:
        """Retrieves a dataset by its ID.

        Args:
            asset_id (str): The unique identifier of the dataset.

        Returns:
            Optional[Dataset]: The retrieved dataset, or None if not found.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            DatasetIOError: If an I/O error occurs during the operation.
        """
        try:
            dataset = self._dataset_dal.read(asset_id=asset_id)
            dataset.repo = self._fileset_repo
            return dataset
        except ObjectNotFoundError:
            msg = f"Dataset {asset_id} does not exist."
            self._logger.exception(msg)
            raise DatasetNotFoundError(msg)
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
            self._dataset_dal.delete(asset_id=asset_id)
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
        return self._dataset_dal.exists(asset_id=asset_id)
