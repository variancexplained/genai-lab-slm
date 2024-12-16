#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/base.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 10:21:05 pm                                            #
# Modified   : Monday December 16th 2024 03:00:41 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from discover.assets.idgen import AssetIDGen
from discover.core.dtypes import IMMUTABLE_TYPES
from discover.core.flow import PhaseDef, StageDef
from discover.infra.utils.date_time.format import ThirdDateFormatter


# ------------------------------------------------------------------------------------------------ #
#                                         ASSET                                                    #
# ------------------------------------------------------------------------------------------------ #
class Asset(ABC):
    def __init__(self, meta: AssetMeta, content: Any) -> None:
        self._meta = meta
        self._content = content

    @property
    def meta(self) -> AssetMeta:
        return self._meta

    @property
    def content(self) -> Any:
        return self._content


# ------------------------------------------------------------------------------------------------ #
#                                    ASSET METADATA                                                #
# ------------------------------------------------------------------------------------------------ #
class AssetMeta(ABC):
    """
    Represents an asset metadata such as phase, stage, and name. Provides methods for generating a unique asset ID,
    and retrieving asset details such as description, phase, stage, and creation date.

    Args:
        phase (PhaseDef): The phase the asset is associated with.
        stage (StageDef): The stage the asset is in.
        name (str): The name of the asset.

    Attributes:
        asset_id (str): Unique identifier for the asset.
        name (str): Name of the asset.
        description (str): Description of the asset including phase, stage, and creation date.
        phase (PhaseDef): The phase of the asset.
        stage (StageDef): The stage of the asset.
        created (datetime): The creation timestamp of the asset.
    """

    def __init__(self, phase: PhaseDef, stage: StageDef, name: str) -> None:
        self._phase = phase
        self._stage = stage
        self._name = name
        self._created = datetime.now()
        self._location = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._asset_id = self._genid()

    def __eq__(self, other: object) -> bool:
        """
        Compares this asset with another asset for equality based on the asset_id.

        Args:
            other (object): Another object to compare with.

        Returns:
            bool: True if the asset IDs are equal, False otherwise.
        """
        if not isinstance(other, AssetMeta):
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
    @abstractmethod
    def asset_type(self) -> str:
        """
        Returns the type of the asset.

        Returns:
            str: The asset type.
        """

    @property
    def description(self) -> str:
        """
        Returns a description of the asset including phase, stage, and creation date.

        Returns:
            str: The description of the asset.
        """
        return f"{self.phase.description} - {self.stage.description} {self.asset_type} created on {ThirdDateFormatter().to_HTTP_format(self._created)}."

    @property
    def phase(self) -> PhaseDef:
        """
        Returns the phase of the asset.

        Returns:
            PhaseDef: The phase of the asset.
        """
        return self._phase

    @property
    def stage(self) -> StageDef:
        """
        Returns the stage of the asset.

        Returns:
            StageDef: The stage of the asset.
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

    @property
    def location(self) -> str:
        """
        Returns a string representing the Asset's persistence location.

        Returns:
            str: String containing a location
        """
        return self._location

    @location.setter
    def location(self, location: str) -> None:
        self._location = location

    def _genid(self) -> str:
        """
        Generates a unique ID for the asset based on its phase, stage, and name.

        Returns:
            str: The generated asset ID.

        Raises:
            Exception: If there is an error in generating the asset ID.
        """
        try:
            return AssetIDGen.get_asset_id(
                asset_type=self.asset_type,
                phase=self._phase,
                stage=self._stage,
                name=self._name,
            )

        except Exception as e:
            msg = f"Unable to create {self.description}\n{e}"
            self._logger.exception(msg)
