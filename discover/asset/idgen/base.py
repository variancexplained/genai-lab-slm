#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/idgen/base.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 10:21:05 pm                                            #
# Modified   : Wednesday December 18th 2024 02:56:37 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base ID Generator Module"""
from abc import ABC, abstractmethod


# ------------------------------------------------------------------------------------------------ #
class AssetIDGen(ABC):
    """Abstract base class for generating asset IDs.

    Defines the contract for asset ID generation. Subclasses must implement the `generate_asset_id` method.

    Methods:
        generate_asset_id: Abstract method for generating asset IDs.
    """

    @abstractmethod
    def generate_asset_id(**kwargs) -> str:
        """Abstract method for generating asset IDs.

        Args:
            **kwargs: Keyword arguments required for ID generation.

        Returns:
            str: The generated asset ID.
        """
