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
# Modified   : Wednesday January 22nd 2025 02:42:49 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset DAL Module"""
import logging
import os
import shelve
import shutil

import pandas as pd

from discover.asset.base.asset import Asset
from discover.infra.exception.object import (
    ObjectDatabaseNotFoundError,
    ObjectIOException,
    ObjectNotFoundError,
)
from discover.infra.persist.repo.base import DAL


# ------------------------------------------------------------------------------------------------ #
#                              REGISTRY ACCESS OBJECT                                              #
# ------------------------------------------------------------------------------------------------ #
class RAO(DAL):
    """Registry Access Object (RAO) implementation using Python's `shelve` module.

    This class provides access to the repository registry in which a reacord of all assets is
    in the repository is maintained for record keeping keeping and inventory management purposes.
    It supports CRUD operations and handles exceptions to ensure robust data access
    and error reporting.

    Args:
        registry_path (str): Path to the registry database file.
    """

    __REGKEY = "dataset_registry"

    def __init__(self, registry_path: str):
        self._registry_path = registry_path
        self._regkey = self.__REGKEY
        os.makedirs(os.path.dirname(self._registry_path), exist_ok=True)

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def count(self) -> int:
        """Gets the count of all assets in the registry.

        Returns:
            int: Number of assets in the registry.
        """
        return len(self.read())

    def create(self, asset: Asset) -> None:
        """Creates a new asset in the registry.

        Args:
            asset (Asset): The asset to be added.

        Raises:
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during creation.
        """
        # Set state of the Dataset to `PUBLISHED` and obtain the registration entry
        entry = asset.publish()
        entry = pd.DataFrame.from_dict(entry)

        # Obtain the registry if available.
        registry = self.read()

        # Drop the old registration if it exists and insert the new  entry into the registry.
        registry = registry.loc[registry["asset_id" != asset.asset_id]]
        registry = pd.concat([registry, entry], axis=0)

        # Save the registry.
        self.write(registry)

    def read(self) -> pd.DataFrame:
        """Reads an asset by its ID from the database.

        Returns:
            pd.DataFrame: A DataFramwe containing dataset registration information.

        Raises:
            ObjectNotFoundError: If the asset is not found in the database.
            ObjectDatabaseNotFoundError: If the database file is not found.
            ObjectIOException: If an unknown exception occurs during reading.
        """
        try:
            with shelve.open(self._registry_path) as db:
                return db.get(self._regkey, pd.DataFrame())
        except KeyError:
            msg = f"No registry was found in the registry database at {self._registry_path}."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except FileNotFoundError as e:
            msg = f"The registration database was not found at {self._registry_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg, e) from e
        except Exception as e:
            msg = f"Unknown exception occurred while reading the registry at {self._registry_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def read_asset(self, asset_id: str) -> pd.DataFrame:
        """Reads the registry and returns the entry for the designated asset, if it exists.

        Args:
            asset_id (str): Asset identifier

        Returns:
            Optional[pd.DataFrame]. Returns the registry entry of the asset if it exists.

        """
        registry = self.read()
        return registry.loc[registry["asset_id"] == asset_id]

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
        return len(self.read_asset(asset_id=asset_id)) > 0

    def delete(self, asset_id: str) -> None:
        """Deletes an asset by its asest_id from the registry

        Args:
            asset_id (str): The unique identifier of the asset to delete.
        """
        # Get the registry
        registry = self.read()
        # Filter the asset
        registry = registry.loc[registry["asset_id" != asset_id]]
        # Persist the registry
        self.write(registry=registry)

    def write(self, registry: pd.DataFrame) -> None:
        """Writes the registry to the database.

        Args:
            registry (pd.DataFrame): The registry

        Returns None
        """

        try:
            with shelve.open(self._registry_path) as db:
                db[self._regkey] = registry
        except FileNotFoundError as e:
            msg = f"The registry was not found at {self._registry_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectDatabaseNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception occurred while writing to thee registry at {self._registry_path}.\n{e}"
            self._logger.exception(msg)
            raise ObjectIOException(msg, e) from e

    def reset(self, verified: bool = False) -> None:
        """Resets the registry by deleting all its contents.

        Args:
            verified (bool): If True, performs the reset immediately. If False,
                prompts the user for confirmation.

        Logs:
            Warning: Logs a warning if the reset is performed.
            Info: Logs information if the reset operation is aborted.
        """
        if verified:
            self.write(registry=None)
            shutil.rmtree(os.path.dirname(self._registry_path))
            self._logger.warning(f"{self.__class__.__name__} has been reset.")
        else:
            self._logger.info(f"{self.__class__.__name__} reset has been aborted.")
