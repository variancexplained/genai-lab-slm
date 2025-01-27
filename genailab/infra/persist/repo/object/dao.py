#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Fileid   : /genailab/infra/persistence/dao/dataset.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 07:41:04 pm                                              #
# Modified   : Sunday January 26th 2025 10:38:16 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset DAL Module"""
import logging
import os
import shelve
import shutil
from typing import Optional

from genailab.asset.base.asset import Asset
from genailab.infra.exception.object import (
    ObjectDatabaseNotFoundError,
    ObjectExistsError,
    ObjectIOException,
    ObjectNotFoundError,
)
from genailab.infra.persist.repo.base import DAL


# ------------------------------------------------------------------------------------------------ #
#                              SHELVE DATA ACCESS OBJECT                                           #
# ------------------------------------------------------------------------------------------------ #
class DAO(DAL):
    """Data Access Object (DAO) implementation using Python's `shelve` module.

    This class provides methods for managing assets in a persistent object database.
    It supports CRUD operations and handles exceptions to ensure robust data access
    and error reporting.

    Args:
        db_path (str): Path to the database file.
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._logger.debug(f"DAO created at {db_path}")

    @property
    def location(self) -> str:
        """Returns the location of the data access layer."""
        return self._db_path

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
        """Adds an asset object to the repository if it doesn't already exist.

        Args:
            asset (Asset): The asset to add to the repository.

        Raises:
            ObjectExistsError: If the asset already exiss in the repository.
        """
        if not self.exists(asset_id=asset.asset_id):
            self._write(asset=asset)
        else:
            msg = f"Error adding asset {asset.asset_id} to the repository. This asset already exists."
            self._logger.error(msg)
            raise ObjectExistsError(msg)

    def read(self, asset_id: str) -> Asset:
        """Reads an asset by its ID from the database.

        Args:
            asset_id (str): The unique identifier of the asset to retrieve.

        Returns:
            Optional[Asset]: The retrieved asset if it exists.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during reading.
        """
        return self._read(asset_id=asset_id)

    def update(self, asset: Asset) -> None:
        """Updates an asset object in the repository.

        Args:
            asset (Asset): The asset to add to the repository.

        Raises:
            ObjectNotFoundError: If the asset does not exiss in the repository.
        """
        if self.exists(asset_id=asset.asset_id):
            self._write(asset=asset)
        else:
            msg = f"Error updating asset {asset.asset_id}. It does not exist in the repository."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)

    def exists(self, asset_id: str) -> bool:
        """Checks if an asset exists in the database.

        Args:
            asset_id (str): The unique identifier of the asset to check.

        Returns:
            bool: True if the asset exists, False otherwise.

        """
        try:
            _ = self._read(asset_id=asset_id)
            return True
        except ObjectNotFoundError:
            return False

    def delete(self, asset_id: str, not_exists_ok: bool = False) -> None:
        """Deletes an asset by its ID from the database.

        Args:
            asset_id (str): The unique identifier of the asset to delete.
            not_exists_ok (bool): Whether ObjectNotFoundError should be ignored. Default is False.

        Raises:
            ObjectNotFoundError: If the asset is not found in the database.
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during deletion.
        """
        try:
            with shelve.open(self._db_path, writeback=True) as db:
                del db[asset_id]
                msg = f"Dataset object {asset_id} removed from object storage."
                self._logger.debug(msg)
        except KeyError:
            if not not_exists_ok:
                msg = f"asset_id: {asset_id} was not found."
                self._logger.error(msg)
                raise ObjectNotFoundError(msg)
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while deleting asset_id: {asset_id}."
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

    def _read(self, asset_id: str) -> Optional[Asset]:
        """Reads an asset by its ID from the database.

        Args:
            asset_id (str): The unique identifier of the asset to retrieve.

        Returns:
            Optional[Asset]: The retrieved asset if it exists.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during reading.
        """
        try:
            with shelve.open(self._db_path) as db:
                return db[asset_id]
        except KeyError:
            msg = f"Dataset {asset_id} was not found."
            raise ObjectNotFoundError(msg)
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while reading asset_id: {asset_id} from the object database.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def _write(self, asset: Asset) -> None:
        """Creates a new asset in the database.

        Args:
            asset (Asset): The asset to be added.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during creation.
        """

        df = asset.serialize()

        try:
            with shelve.open(self._db_path) as db:
                db[asset.asset_id] = asset
        except FileNotFoundError as e:
            msg = f"The object database was not found at {self._db_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception occurred while creating asset_id: {asset.asset_id}.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

        # Ensure that the dataframe is not None before deserializing
        if df is not None:
            asset.deserialize(dataframe=df)
        else:
            self._logger.error(
                "Attempted to deserialize with a None dataframe. Asset deserialization may be incomplete."
            )
