#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/repo/dataset.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 02:46:53 pm                                               #
# Modified   : Wednesday December 25th 2024 10:25:22 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""

import os
import shutil

from discover.infra.persist.object.base import DAO
from discover.infra.persist.repo.base import AssetRepo


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(AssetRepo):
    """Repository for managing dataset assets with file handling.

    This repository extends `AssetRepo` to include operations for managing datasets,
    ensuring that associated files on disk are properly removed when a dataset is deleted
    or the repository is reset.

    Args:
        dao (DAO): The data access object used for persistence of dataset data.
    """

    def __init__(self, dao: DAO) -> None:
        super().__init__(dao=dao)

    def remove(self, asset_id: str) -> None:
        """Removes a dataset and its associated file from the repository.

        Args:
            asset_id (str): The unique identifier of the dataset to remove.

        Logs:
            Info: Logs the successful removal of the dataset and its file.

        Raises:
            ValueError: If the file or directory specified by the dataset's filepath
            does not exist or cannot be identified.
        """
        asset = self.get(asset_id=asset_id)
        self._remove_file(filepath=asset.filepath)
        self._dao.delete(asset_id=asset_id)
        self._logger.info(
            f"Dataset {asset.asset_id}, including its file at {asset.filepath} has been removed from the repository."
        )

    def reset(self) -> None:
        """Resets the repository by removing all datasets and their associated files.

        The reset operation is irreversible and requires user confirmation.

        Logs:
            Warning: Logs a warning if the repository is successfully reset.
            Info: Logs information if the reset operation is aborted.

        Raises:
            ValueError: If any dataset's filepath does not exist or cannot be identified.
        """
        proceed = input(
            "Resetting the repository is irreversible. To proceed, type 'YES'."
        )
        if proceed == "YES":
            assets = self.get_all()
            for asset_id, asset in assets.items():
                self._remove_file(filepath=asset.filepath)
            self._dao.reset()
            self._logger.warning(f"{self.__class__.__name__} has been reset.")
        else:
            self._logger.info(f"{self.__class__.__name__} reset has been aborted.")

    def _remove_file(self, filepath: str) -> None:
        """Removes a file or directory at the specified filepath.

        Args:
            filepath (str): The path to the file or directory to remove.

        Raises:
            ValueError: If the filepath does not point to a valid file or directory.
        """
        if os.path.isfile(filepath):
            os.remove(filepath)
        elif os.path.isdir(filepath):
            shutil.rmtree(filepath)
        else:
            raise ValueError(
                f"The filepath argument {filepath} is neither a file nor a directory."
            )
