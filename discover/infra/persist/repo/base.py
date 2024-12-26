#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/repo/base.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 02:33:35 pm                                               #
# Modified   : Wednesday December 25th 2024 10:25:30 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from typing import Dict

import pandas as pd

from discover.asset.base import Asset
from discover.asset.repo import Repo
from discover.infra.persist.object.base import DAO


# ------------------------------------------------------------------------------------------------ #
class AssetRepo(Repo):
    """Repository for managing assets with data persistence and logging.

    This class provides a high-level interface for interacting with assets stored
    in a data access object (DAO). It supports adding, retrieving, checking,
    removing, and resetting assets, while maintaining logs of critical operations.

    Args:
        dao (DAO): The data access object used for persistence of asset data.
    """

    def __init__(self, dao: DAO) -> None:
        self._dao = dao
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add(self, asset: Asset) -> None:
        """Adds an asset to the repository.

        Args:
            asset (Asset): The asset object to be added to the repository.
        """
        self._dao.create(asset=asset)

    def get(self, asset_id: str) -> Asset:
        """Retrieves an asset by its ID.

        Args:
            asset_id (str): The unique identifier of the asset to retrieve.

        Returns:
            Asset: The retrieved asset object.
        """
        return self._dao.read(asset_id=asset_id)

    def get_all(self) -> Dict[str, Asset]:
        """Retrieves all assets in the repository.

        Returns:
            Dict[str, Asset]: A dictionary of all assets, where keys are asset IDs
            and values are Asset objects.
        """
        return self._dao.read_all()

    def remove(self, asset_id: str) -> None:
        """Removes an asset by its ID.

        Args:
            asset_id (str): The unique identifier of the asset to remove.
        """
        self._dao.delete(asset_id=asset_id)

    def exists(self, asset_id: str) -> bool:
        """Checks if an asset exists in the repository.

        Args:
            asset_id (str): The unique identifier of the asset to check.

        Returns:
            bool: True if the asset exists, False otherwise.
        """
        return self._dao.exists(asset_id=asset_id)

    def registry(self) -> pd.DataFrame:
        """Creates a registry of all assets in the repository as a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame where each row represents an asset and columns
            are the asset's attributes.
        """
        assets = self.get_all()
        asset_list = [v.as_dict() for v in assets.values()]
        return pd.DataFrame(data=asset_list)

    def reset(self) -> None:
        """Resets the repository, removing all assets.

        The reset operation is irreversible and requires confirmation from the user.

        Raises:
            Warning: Logs a warning if the repository is reset.
            Info: Logs information if the reset operation is aborted.
        """
        proceed = input(
            "Resetting the repository is irreversible. To proceed, type 'YES'."
        )
        if proceed == "YES":
            self._dao.reset()
            self._logger.warning(f"{self.__class__.__name__} has been reset.")
        else:
            self._logger.info(f"{self.__class__.__name__} reset has been aborted.")
