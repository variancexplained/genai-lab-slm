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
# Modified   : Friday December 27th 2024 04:40:05 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset DAL Module"""
import logging
import os
import shelve
import shutil
from typing import Dict

from discover.asset.base import Asset
from discover.asset.core import AssetType
from discover.infra.exception.object import (
    ObjectDatabaseNotFoundError,
    ObjectIOException,
    ObjectNotFoundError,
)
from discover.infra.persist.object.base import DAO


# ------------------------------------------------------------------------------------------------ #
#                              SHELVE DATA ACCESS OBJECT                                           #
# ------------------------------------------------------------------------------------------------ #
class ShelveDAO(DAO):
    """Data Access Object (DAO) implementation using Python's `shelve` module.

    This class provides methods for managing assets in a persistent object database.
    It supports CRUD operations and handles exceptions to ensure robust data access
    and error reporting.

    Args:
        location (str): Directory where the database file is stored.
        db_path (str): Name of the database file.
        asset_type (AssetType): Type of asset managed by this DAO.
    """

    def __init__(self, location: str, db_path: str, asset_type: AssetType):
        self._db_path = os.path.join(location, db_path)
        self._asset_type = asset_type
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def count(self) -> int:
        """Gets the count of all assets in the database.

        Returns:
            int: Number of assets in the database.
        """
        return len(self.read_all())

    @property
    def size(self) -> int:
        """Gets the size of the database file in bytes.

        Returns:
            int: Size of the database file in bytes.
        """
        return os.path.getsize(self._db_path)

    def create(self, asset: Asset) -> None:
        """Creates a new asset in the database.

        Args:
            asset (Asset): The asset to be added.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during creation.
        """
        try:
            with shelve.open(self._db_path) as db:
                db[asset.asset_id] = asset
        except FileNotFoundError as e:
            msg = f"The object database for {self._asset_type.value} was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception occurred while creating {self._asset_type.value} asset_id: {asset.asset_id}.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def read(self, asset_id: str) -> Asset:
        """Reads an asset by its ID from the database.

        Args:
            asset_id (str): The unique identifier of the asset to retrieve.

        Returns:
            Asset: The retrieved asset.

        Raises:
            ObjectNotFoundError: If the asset is not found in the database.
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during reading.
        """
        try:
            with shelve.open(self._db_path) as db:
                return db[asset_id]
        except KeyError:
            msg = f"Asset {asset_id} was not found."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except FileNotFoundError as e:
            msg = f"The object database for {self._asset_type.value} was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while reading {self._asset_type.value} asset_id: {asset_id} from the object database.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def read_all(self) -> Dict[str, Asset]:
        """Reads all assets from the database.

        Returns:
            Dict[str, Asset]: A dictionary of all assets, with keys as asset IDs
            and values as Asset objects.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during reading.
        """
        try:
            with shelve.open(self._db_path) as db:
                return dict(db.items())
        except FileNotFoundError as e:
            msg = f"The object database for {self._asset_type.value} was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while reading from {self._asset_type.value} database.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def exists(self, asset_id: str) -> bool:
        """Checks if an asset exists in the database.

        Args:
            asset_id (str): The unique identifier of the asset to check.

        Returns:
            bool: True if the asset exists, False otherwise.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during the check.
        """
        try:
            with shelve.open(self._db_path) as db:
                return asset_id in db
        except FileNotFoundError as e:
            msg = f"The object database for {self._asset_type.value} was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while checking existence of {self._asset_type.value} asset_id: {asset_id}."
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def delete(self, asset_id: str) -> None:
        """Deletes an asset by its ID from the database.

        Args:
            asset_id (str): The unique identifier of the asset to delete.

        Raises:
            ObjectNotFoundError: If the asset is not found in the database.
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during deletion.
        """
        try:
            with shelve.open(self._db_path, writeback=True) as db:
                del db[asset_id]
        except KeyError:
            msg = f"{self._asset_type.label} asset_id: {asset_id} was not found."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except FileNotFoundError as e:
            msg = f"The object database for {self._asset_type.value} was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while deleting {self._asset_type.value} asset_id: {asset_id}."
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def reset(self, verified: bool = False) -> None:
        """Resets the database by deleting all its contents.

        Args:
            verified (bool): If True, performs the reset immediately. If False,
                prompts the user for confirmation.

        Logs:
            Warning: Logs a warning if the reset is performed.
            Info: Logs information if the reset operation is aborted.
        """
        if verified:
            shutil.rmtree(os.path.dirname(self._db_path))
            self._logger.warning(f"{self.__class__.__name__} has been reset.")
        else:
            proceed = input(
                f"Resetting the {self.__class__.__name__} object database is irreversible. To proceed, type 'YES'."
            )
            if proceed == "YES":
                shutil.rmtree(os.path.dirname(self._db_path))
                self._logger.warning(f"{self.__class__.__name__} has been reset.")
            else:
                self._logger.info(f"{self.__class__.__name__} reset has been aborted.")
