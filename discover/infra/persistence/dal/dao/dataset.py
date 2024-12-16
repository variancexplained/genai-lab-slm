#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Fileid   : /discover/infra/persistence/dao/dataset.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 07:41:04 pm                                              #
# Modified   : Monday December 16th 2024 03:53:30 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset DAO Module"""
import logging
import os
import shelve
from typing import Any, List, Optional

import pandas as pd

from discover.assets.dataset import DatasetMeta
from discover.core.flow import PhaseDef
from discover.infra.persistence.dal.dao.base import DAO
from discover.infra.persistence.dal.dao.exception import (
    ObjectDatabaseNotFoundError,
    ObjectIOException,
    ObjectNotFoundError,
)
from discover.infra.persistence.dal.dao.location import DAOLocationService


# ------------------------------------------------------------------------------------------------ #
class DatasetDAO(DAO):
    """
    A Data Access Object (DAO) for managing Dataset objects within a key-value store.

    This class is responsible for interacting with a shelve-based object database to
    create, read, update, and delete Dataset objects. It manages dataset metadata
    persistence and provides methods for various operations like checking dataset
    existence and filtering datasets by specific attributes.

    Args:
        location_service (LocationService): Service encapsulating persistence locations.
    """

    def __init__(self, location_service: DAOLocationService):
        self._db_path = location_service.get_filepath()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(self, dataset_meta: DatasetMeta) -> None:
        """
        Creates a new dataset entry in the object database.

        Serializes the dataset object (excluding its content) and stores it in the
        key-value store. Raises an error if the object database is not found.

        Args:
            dataset_meta (DatasetMeta): The serialized dataset metadata object to be created.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            Exception: For any other exceptions encountered during the process.
        """

        # Create a serializable DatasetMeta object
        serialized_dataset_meta = dataset_meta.__getstate__()

        # Store the serializable version in the key value store.
        try:
            with shelve.open(self._db_path, writeback=True) as db:
                db[dataset_meta.asset_id] = serialized_dataset_meta
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception occurred while creating dataset {dataset_meta.asset_id}.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def read(self, asset_id: str) -> Optional[DatasetMeta]:
        """
        Reads dataset metadata into a DatasetMeta object.

        Args:
            asset_id (str): The id of the dataset to retrieve.

        Returns:
            Optional[DatasetMeta]: The retrieved state as a dictionary object or None if not found.

        Raises:
            ObjectNotFoundError: If the dataset does not exist.
            ObjectDatabaseNotFoundError: If the database file is not found.
            Exception: For any other exceptions encountered during the process.
        """
        try:
            with shelve.open(self._db_path) as db:
                return DatasetMeta.deserialize(db[asset_id])
        except KeyError:
            msg = f"Dataset object {asset_id} was not found."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while reading dataset {asset_id} from the dataset object database.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def read_all(self) -> List[DatasetMeta]:
        """
        Reads all DatasetMeta objects from the object database and returns them as a List.

        Returns:
            List[DatasetMeta]: A list of DatasetMeta objects.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            Exception: For any other exceptions encountered during the process.
        """
        dataset_meta_list = []
        try:
            with shelve.open(self._db_path) as db:
                for asset_id in db:
                    dataset_meta = DatasetMeta.deserialize(db[asset_id])
                    dataset_meta_list.append(dataset_meta)

        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while reading from the dataset object database.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

        if len(dataset_meta_list) == 0:
            self._logger.info("No datasets found.")

        return dataset_meta_list

    def read_by_phase(self, phase: PhaseDef) -> pd.DataFrame:
        """
        Reads datasets filtered by a specific phase.

        Args:
            phase (PhaseDef): The phase to filter datasets by.

        Returns:
            pd.DataFrame: A DataFrame containing datasets that match the specified phase.
        """
        return self._read_by_attribute("phase", phase)

    def update(self, dataset_meta: DatasetMeta) -> None:
        """Updates the Dataset in the object database

        Args:
            dataset_meta (DatasetMeta): DatasetMeta object
        """
        self.create(dataset_meta=dataset_meta)

    def exists(self, asset_id: str) -> bool:
        """
        Checks if a dataset with the given id exists in the object database.

        Args:
            asset_id (str): The id of the dataset to check.

        Returns:
            bool: True if the dataset exists, False otherwise.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            Exception: For any other exceptions encountered during the process.
        """
        try:
            with shelve.open(self._db_path) as db:
                return asset_id in db
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while checking existence of the {asset_id} dataset."
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def delete(self, asset_id: str) -> None:
        """
        Deletes a dataset by its id from the object database.

        Args:
            asset_id (str): The id of the dataset to delete.

        Raises:
            ObjectNotFoundError: If the dataset does not exist.
            ObjectDatabaseNotFoundError: If the database file is not found.
            Exception: For any other exceptions encountered during the process.
        """
        try:
            with shelve.open(self._db_path, writeback=True) as db:
                del db[asset_id]
        except KeyError:
            msg = f"Dataset object {asset_id} was not found."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while deleting dataset {asset_id}."
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def reset(self, force: bool = False) -> None:
        """
        Resets the dataset storage, deleting all persisted datasets.

        Args:
            force (bool): If True, bypasses user confirmation for reset.
                          Default is False, which prompts for confirmation.
        """
        if (
            force
            or "y"
            in input("Resetting the DatasetDAO is permanent. Confirm. [Y/N] ").lower()
        ):
            self._reset()

    def _reset(self) -> None:
        """
        Internal method to perform the reset of the dataset storage.

        Deletes the object database files associated with the shelve storage.
        """
        for ext in (".db", ".bak", ".dat", ".dir"):
            try:
                os.remove(self._db_path + ext)
            except FileNotFoundError:
                pass

    def _read_by_attribute(self, attribute: str, value: Any) -> pd.DataFrame:
        """
        Reads datasets filtered by a specific attribute and value.

        Args:
            attribute (str): The attribute to filter datasets by.
            value (Any): The value of the attribute to match.

        Returns:
            pd.DataFrame: A DataFrame containing datasets that match the specified attribute and value.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            Exception: For any other exceptions encountered during the process.
        """
        dataset_meta_list = []
        try:
            with shelve.open(self._db_path) as db:
                for asset_id in db:
                    dataset_meta = DatasetMeta.deserialize(db[asset_id])
                    if getattr(dataset_meta, attribute) == value:
                        dataset_meta_list.append(dataset_meta)
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception occurred while reading from dataset object database at {self._db_path}."
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

        if len(dataset_meta_list) == 0:
            self._logger.info(f"No datasets found for {attribute}={value}.")

        return dataset_meta_list
