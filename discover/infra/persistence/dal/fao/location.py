#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/fao/location.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 01:04:56 pm                                             #
# Modified   : Monday December 16th 2024 03:58:38 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""FilePath Service Module"""

import os
from abc import abstractmethod

from discover.assets.base import AssetMeta
from discover.infra.persistence.dal.base.location import LocationService


# ------------------------------------------------------------------------------------------------ #
class FAOLocationService(LocationService):
    """
    Manages the generation and reconstruction of file paths for different entities, phases, and stages
    within a workspace. It ensures that directories are created as needed and provides a consistent
    naming convention for files based on provided parameters.

    Attributes:
        _workspace (str): The base directory where all entities and related files are stored.
    """

    def __init__(self, workspace: str) -> None:
        """
        Initializes the LocationService with the specified workspace.

        Args:
            workspace (str): The base directory where entities and related files will be stored.
        """
        self._workspace = workspace

    @abstractmethod
    def get_filepath(
        self,
        asset_meta: AssetMeta,
        partitioned: bool = True,
    ) -> str:
        """
        Constructs the file path for a given asset.

        Args:
            asset_meta (AssetMeta): The Asset metadata for which a file location is to be returned.

        Returns:
            str: The complete file path for the specified asset.
        """
        filename = f"appvocai_discover-{asset_meta.phase.directory}-{asset_meta.stage.directory}-{asset_meta.name}-{asset_meta.asset_type}.parquet"
        filepath = os.path.join(
            self._workspace, asset_meta.asset_type, asset_meta.phase.directory, filename
        )
        os.makedirs(filepath, exist_ok=True)
        return filepath
