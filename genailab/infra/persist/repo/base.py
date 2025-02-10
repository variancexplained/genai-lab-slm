#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/persist/repo/base.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Saturday February 8th 2025 10:43:31 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Object Persistence Base Module"""

from abc import ABC, abstractmethod

from genailab.asset.base.asset import Asset


# ------------------------------------------------------------------------------------------------ #
#                               DATA ACCESS LAYER                                                  #
# ------------------------------------------------------------------------------------------------ #
class DAL(ABC):
    """Abstract base class for the Data Access Layer."""

    @abstractmethod
    def create(self, asset: Asset, **kwargs) -> None:
        """Saves an asset to the data store.
        Args:
            asset (Asset): Asset object
            **kwargs: Arbitrary keyword arguments.
        """

    @abstractmethod
    def read(self, asset_id: str, **kwargs) -> Asset:
        """Reads the asset from the data store.

        Args:
            asset_id (str): Asset identifier.
            **kwargs: Arbitrary keyword arguments.
        """

    @abstractmethod
    def exists(self, asset_id: str, **kwargs) -> bool:
        """Evaluates existence of an asset in the data store.

        Args:
            asset_id (str): Asset identifier.
            **kwargs: Arbitrary keyword arguments.
        """

    @abstractmethod
    def delete(self, asset_id: str, **kwargs) -> None:
        """Deletes the asset from the data store.

        Args:
            asset_id (str): Asset identifier.
            **kwargs: Arbitrary keyword arguments.
        """
