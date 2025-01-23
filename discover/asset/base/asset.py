#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/base/asset.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:01:02 pm                                            #
# Modified   : Thursday January 23rd 2025 05:35:21 am                                              #
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
from typing import Any, Dict, Union

from pydantic.dataclasses import dataclass

from discover.asset.base.identity import AssetPassport
from discover.core.dtypes import IMMUTABLE_TYPES, SEQUENCE_TYPES
from discover.core.flow import PhaseDef, StageDef
from discover.infra.utils.file.fileset import FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                    ASSET CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AssetConfig:
    """Base class for asset configurations.

    Represents the configuration details of an asset, including its
    phase, stage, name, and file format.

    Args:
        phase (PhaseDef): The phase associated with the asset.
        stage (StageDef): The stage associated with the asset.
        name (str): The name of the asset.
        file_format (FileFormat): The file format of the asset.

    Attributes:
        phase (PhaseDef): The phase associated with the asset.
        stage (StageDef): The stage associated with the asset.
        name (str): The name of the asset.
        file_format (FileFormat): The file format of the asset.
    """

    phase: PhaseDef
    stage: StageDef
    name: str
    file_format: FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                     ASSET TYPE                                                   #
# ------------------------------------------------------------------------------------------------ #
class AssetType(Enum):
    """Enumerates the different types of assets.

    This enum defines the possible types of assets that can be managed
    by the system.

    Attributes:
        DATASET (str): Represents a dataset asset.
        MODEL (str): Represents a machine learning model asset.
        EXPERIMENT (str): Represents an experiment asset.

    """

    DATASET = "dataset"
    MODEL = "model"
    EXPERIMENT = "experiment"


# ------------------------------------------------------------------------------------------------ #
#                                        ASSET                                                     #
# ------------------------------------------------------------------------------------------------ #
class Asset(ABC):
    """
    Abstract base class for representing an asset with hierarchical metadata and utility methods.

    This class provides a foundational structure for assets with attributes such as asset ID, type,
    phase, stage, name, description, and creation time. It includes methods for serialization,
    equality comparison, and dictionary representation.

    Args:
        passport (Passport): The passport object containing metadata about the asset.
        **kwargs: Additional arguments for customization.

    Attributes:
        asset_id (str): Unique identifier for the asset.
        phase (PhaseDef): Phase to which the asset belongs.
        stage (StageDef): Stage to which the asset belongs.
        name (str): Name of the asset.
        description (str): Description of the asset.

    Methods:
        as_dict() -> Dict[str, Union[str, int, float, datetime, None]]:
            Returns a dictionary representation of the asset, including attributes suitable for serialization.

        _export_config(v: Any) -> Any:
            Converts Config objects and other attributes into a serializable format recursively.
    """

    def __init__(
        self,
        passport: AssetPassport,
        **kwargs,
    ) -> None:
        self._passport = passport
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __eq__(self, other: object) -> bool:
        """Checks equality between two Asset objects based on their asset ID."""
        if not isinstance(other, Asset):
            return NotImplemented
        return self.asset_id == other.asset_id

    def __repr__(self) -> str:
        """Returns a string representation of the Asset object for debugging."""
        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(
                f"{k}={v!r}"
                for k, v in vars(self).items()
                if isinstance(v, IMMUTABLE_TYPES)
            ),
        )

    def __str__(self) -> str:
        """Returns a formatted string representation of the Asset object."""
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
        """Returns the object's state for serialization, excluding non-serializable attributes."""
        return {key: value for key, value in self.__dict__.items()}

    def __setstate__(self, state):
        """Restores the object's state during deserialization."""
        for key, value in state.items():
            object.__setattr__(self, key, value)

    @property
    def passport(self) -> AssetPassport:
        """Returns the Asset Passport."""
        return self._passport

    @property
    def asset_id(self) -> str:
        """str: Unique identifier for the asset."""
        return self._passport.asset_id

    @property
    def phase(self) -> PhaseDef:
        """PhaseDef: Phase to which the asset belongs."""
        return self._passport.phase

    @property
    def stage(self) -> StageDef:
        """StageDef: Stage to which the asset belongs."""
        return self._passport.stage

    @property
    def name(self) -> str:
        """str: Name of the asset."""
        return self._passport.name

    @property
    def description(self) -> str:
        """str: Description of the asset."""
        return self._passport.description

    def as_dict(self) -> Dict[str, Union[str, int, float, datetime, None]]:
        """
        Returns a dictionary representation of the Asset object.

        Returns:
            Dict[str, Union[str, int, float, datetime, None]]: A dictionary containing
            the asset's attributes in a serializable format.
        """
        return {
            k: self._export_config(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    @classmethod
    def _export_config(cls, v: Any) -> Any:
        """
        Converts an object into a serializable format recursively.

        Args:
            v (Any): The object to be converted.

        Returns:
            Any: The converted object in a serializable format.
        """
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
