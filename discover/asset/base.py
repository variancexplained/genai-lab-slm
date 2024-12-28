#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/base.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:01:02 pm                                            #
# Modified   : Friday December 27th 2024 08:45:22 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Union

from pydantic.dataclasses import dataclass

from discover.asset.identity import Passport
from discover.core.dtypes import IMMUTABLE_TYPES, SEQUENCE_TYPES


# ------------------------------------------------------------------------------------------------ #
#                                        ASSET                                                     #
# ------------------------------------------------------------------------------------------------ #
class Asset(ABC):

    def __init__(
        self,
        passport: Passport,
        **kwargs,
    ) -> None:
        self._passport = passport

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __eq__(self, other: object) -> bool:
        """
        Compares this asset with another asset for equality based on the asset_id.

        Args:
            other (object): Another object to compare with.

        Returns:
            bool: True if the asset IDs are equal, False otherwise.
        """
        if not isinstance(other, Asset):
            return NotImplemented
        return self.asset_id == other.asset_id

    def __repr__(self) -> str:
        """
        Returns a string representation of the Asset object, including class name and immutable attributes.

        Returns:
            str: The string representation of the Asset object.
        """
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                f"{k}={v!r}"
                for k, v in vars(self).items()
                if isinstance(v, IMMUTABLE_TYPES)
            ),
        )

    def __str__(self) -> str:
        """
        Returns a formatted string representing the Asset, displaying its immutable attributes.

        Returns:
            str: The string representation of the Asset, including attribute names and values.
        """
        width = 32
        breadth = width * 2
        s = f"\n\n{self.__class__.__name__.center(breadth, ' ')}"
        d = self.as_dict()
        for k, v in d.items():
            if type(v) in IMMUTABLE_TYPES:
                k = k.strip("_")
                s += f"\n{k.rjust(width,' ')} | {v}"
        s += "\n\n"
        return s

    def __getstate__(self):
        """
        Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized, ensuring compatibility with serialization
        libraries and allowing the asset's state to be stored or transmitted.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        # Exclude non-serializable or private attributes if necessary
        return {key: value for key, value in self.__dict__.items()}

    def __setstate__(self, state):
        """
        Restores the object's state during deserialization.

        This method reinitializes the object with the state provided,
        typically created by `__getstate__`.

        Args:
            state (dict): The state dictionary to restore.

        Raises:
            ValueError: If an attempt is made to set invalid attributes.
        """
        for key, value in state.items():
            object.__setattr__(self, key, value)

    @property
    def passport(self) -> Passport:
        return self._passport

    def as_dict(self) -> Dict[str, Union[str, int, float, datetime, None]]:
        """Returns a dictionary representation of the the Config object."""
        return {
            k: self._export_config(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    @classmethod
    def _export_config(cls, v: Any) -> Any:
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, dict):
            return v
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        elif isinstance(v, Enum):
            if hasattr(v, "description"):
                return v.description
            else:
                return v.value
        elif isinstance(v, datetime):
            return v.isoformat()
        else:
            return dict()


# ------------------------------------------------------------------------------------------------ #
#                                   ASSET BUILDER                                                  #
# ------------------------------------------------------------------------------------------------ #
class AssetBuilder(ABC):
    """
    Abstract base class for building assets with phases, stages, and persistence
    configurations.
    """

    @abstractmethod
    def reset(self) -> None:
        """
        Resets the builder to be ready to construct another Dataset object.
        """
        pass

    @abstractmethod
    def build(self) -> Asset:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
#                                    ASSET COMPONENT                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AssetComponent(ABC):
    """Base class for asset component subclasses."""


# ------------------------------------------------------------------------------------------------ #
#                                   COMPONENT BUILDER                                              #
# ------------------------------------------------------------------------------------------------ #
class AssetComponentBuilder(ABC):
    """
    Abstract base class for building components of an asset.

    This class provides the structure for builders that work with asset components.
    It requires implementation of methods to reset the builder and construct the final asset component.

    Args:
        asset_builder (AssetBuilder): The main asset builder to which this component builder is associated.
    """

    def __init__(self, asset_builder: AssetBuilder) -> None:
        self._asset_builder = asset_builder

    @abstractmethod
    def reset(self) -> None:
        """
        Resets the builder to its initial state, preparing it to construct a new asset component.
        """
        pass

    @abstractmethod
    def build(self) -> AssetComponentBuilder:
        """
        Constructs and returns the final asset component.

        Returns:
            Dataset: The constructed asset component, ready for use.
        """
        pass
