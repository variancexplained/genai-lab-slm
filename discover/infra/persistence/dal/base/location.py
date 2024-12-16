#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/base/location.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday October 16th 2024 10:22:35 pm                                             #
# Modified   : Monday December 16th 2024 03:52:19 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod

from discover.assets.base import AssetMeta


# ------------------------------------------------------------------------------------------------ #
class LocationService(ABC):
    """
    Abstract base class for defining a location service that provides file path generation.
    This class is intended to be inherited by concrete implementations that define how file
    paths are constructed based on specific parameters.
    """

    @abstractmethod
    def get_filepath(self, asset_meta: AssetMeta, **kwargs) -> str:
        """
        Abstract method to generate a file path for a given AssetMeta object.
        This method should be implemented by subclasses to define the logic for constructing
        file paths according to the needs of the specific implementation.

        Args:
            asset_meta(AssetMeta): Object containing the asset metadata.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            str: The generated file path as a string.
        """
        pass
