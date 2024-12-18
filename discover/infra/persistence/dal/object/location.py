#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/object/location.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday October 16th 2024 10:24:35 pm                                             #
# Modified   : Wednesday December 18th 2024 12:07:56 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os

from discover.assets.base import AssetMeta
from discover.infra.persistence.dal.base import LocationService


# ------------------------------------------------------------------------------------------------ #
class DALLocationService(LocationService):
    """
    A concrete implementation of the LocationService for datasets. This class constructs file paths
    for datasets based on the specified workspace and location.

    Attributes:
        _workspace (str): The base directory where datasets are stored.
        _location (str): The specific location or subdirectory within the workspace where the dataset files
                         will be stored.
    """

    def __init__(self, workspace: str, location: str) -> None:
        """
        Initializes the DALLocationService with the specified workspace and location.

        Args:
            workspace (str): The base directory where dataset files will be stored.
            location (str): The subdirectory or specific location within the workspace for the dataset files.
        """
        super().__init__()
        self._workspace = workspace
        self._location = location

    def get_filepath(self, asset_meta: AssetMeta, **kwargs):
        """
        Constructs and returns the file path for a object based on the workspace, location, and asset.

        Args:
            asset_meta(AssetMeta): Metadata for the asset for which a filepath is to be returned.
            **kwargs: Arbitrary keyword arguments (unused).

        Returns:
            str: The file path for the dataset as a string.
        """
        filepath = os.path.join(self._workspace, self._location)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        return filepath
