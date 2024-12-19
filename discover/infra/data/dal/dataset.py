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
# Modified   : Thursday December 19th 2024 01:40:50 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset DAL Module"""
import logging
import shelve

from discover.infra.data.dal.base import DAO
from discover.infra.data.dal.exception import (
    ObjectDatabaseNotFoundError,
    ObjectIOException,
    ObjectNotFoundError,
)


# ------------------------------------------------------------------------------------------------ #
class DatasetDAO(DAO):
    """
    A Data Access Object (DAL) for managing Dataset objects within a key-value store.

    This class is responsible for interacting with a shelve-based object database to
    create, read, update, and delete Dataset objects. It manages dataset metadata
    persistence and provides methods for various operations like checking dataset
    existence and filtering datasets by specific attributes.

    Args:
        location_service (LocationService): Service encapsulating persistence locations.
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def create(self, dataset) -> None:
        try:
            with shelve.open(self._db_path) as db:
                db[dataset.asset_id] = dataset
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception occurred while creating dataset {dataset.asset_id}.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def read(self, asset_id: str):
        """
        Reads dataset metadata into a Dataset object.

        Args:
            asset_id (str): The id of the dataset to retrieve.

        Returns:
            Optional[Dataset]: The retrieved state as a dictionary object or None if not found.

        Raises:
            ObjectNotFoundError: If the dataset does not exist.
            ObjectDatabaseNotFoundError: If the database file is not found.
            Exception: For any other exceptions encountered during the process.
        """
        try:
            with shelve.open(self._db_path) as db:
                return db[asset_id]
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
