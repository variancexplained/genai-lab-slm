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
# Modified   : Monday December 23rd 2024 08:30:01 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

import logging
from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Union

from discover.core.asset import AssetType
from discover.core.dtypes import IMMUTABLE_TYPES, SEQUENCE_TYPES
from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
#                                        ASSET                                                     #
# ------------------------------------------------------------------------------------------------ #
class Asset(ABC):

    def __init__(
        self,
        asset_type,
        name: str,
        phase: PhaseDef,
        stage: StageDef,
        description: Optional[str] = None,
        **kwargs,
    ) -> None:
        self._asset_type = asset_type
        self._name = name
        self._phase = phase
        self._stage = stage
        self._description = description
        self._created = datetime.now()

        self._asset_id = None

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
        """
        self.__dict__.update(state)

    @property
    def asset_type(self) -> AssetType:
        """
        Returns the type of asset

        Returns:
            AssetType: An AssetType Enum instance.
        """
        return self._asset_type

    @property
    def asset_id(self) -> str:
        """
        Returns the unique asset identifier.

        Returns:
            str: The asset ID.
        """
        return self._asset_id

    @property
    def name(self) -> str:
        """
        Returns the name of the asset.

        Returns:
            str: The name of the asset.
        """
        return self._name

    @property
    def description(self) -> str:
        """
        Custom description functionality defined in subclasses.

        Returns:
            str: The description of the asset.
        """
        return self._description

    @property
    def phase(self) -> PhaseDef:
        """
        Returns the phase for which the asset was created.

        Returns:
            PhaseDef: Phase.
        """
        return self._phase

    @property
    def stage(self) -> StageDef:
        """
        Returns the stage for which the asset was created.

        Returns:
            StageDef: Stage.
        """
        return self._stage

    @property
    def created(self) -> datetime:
        """
        Returns the creation timestamp of the asset.

        Returns:
            datetime: The creation time of the asset.
        """
        return self._created

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
#                                          FACTORY                                                 #
# ------------------------------------------------------------------------------------------------ #
class Factory(ABC):

    def set_asset_id(self, asset: Asset) -> Asset:
        asset_id = f"{asset.asset_type.value}-{asset.phase.value}-{asset.stage.value}-{asset.name}"
        setattr(asset, "_asset_id", asset_id)
        return asset
