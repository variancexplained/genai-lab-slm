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
# Modified   : Thursday October 17th 2024 01:45:51 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

from dataclasses import fields
from datetime import datetime
from typing import Any, Optional

from pydantic.dataclasses import dataclass

from discover.assets.idgen import AssetIDGen
from discover.core.data_class import DataClass
from discover.core.flow import PhaseDef, StageDef
from discover.infra.utils.date_time.format import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Asset(DataClass):
    """
    A base class representing an asset in a data pipeline, such as models, datasets, or other entities.

    This class encapsulates common attributes and behaviors associated with assets,
    including tracking their phase, stage, name, content, and creation time.
    It also provides methods for serialization and deserialization, along with
    properties for generating unique IDs and descriptions.

    Attributes:
        phase (PhaseDef): The phase to which the asset belongs.
        stage (StageDef): The stage within the phase.
        name (str): The name of the asset.
        content (Any): The actual content or object the asset represents.
        created (Optional[datetime]): The timestamp when the asset was created.
            If not provided, it is set to the current datetime during initialization.
    """

    phase: PhaseDef
    stage: StageDef
    name: str
    content: Any
    created: Optional[datetime] = None

    def __post_init__(self) -> None:
        """
        Initializes the `created` attribute with the current datetime if it was not provided.

        This ensures that the asset's creation time is recorded upon instantiation,
        providing a timestamp for when the asset was initialized.
        """
        self.created = self.created or datetime.now()

    def __getstate__(self):
        """
        Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized, ensuring compatibility with serialization
        libraries and allowing the asset's state to be stored or transmitted.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        return {field.name: getattr(self, field.name) for field in fields(self)}

    def __setstate__(self, state):
        """
        Restores the object's state from a serialized format.

        This method takes a dictionary representation of the object's state,
        applying it to restore the object's attributes, effectively reconstructing
        the asset after it has been deserialized.

        Args:
            state (dict): The state dictionary used to restore the object.
        """
        for key, value in state.items():
            setattr(self, key, value)

    @property
    def asset_id(self) -> str:
        """
        Generates a unique identifier for the asset based on its asset_type type, phase, stage, and name.

        The identifier combines the values of the phase, stage, and name attributes,
        forming a string that serves as a unique reference for the asset.

        Returns:
            str: A string combining the phase, stage, and name values, acting as a unique ID.
        """
        return AssetIDGen.get_asset_id(
            asset_type=self.__class__.__name__.lower(),
            phase=self.phase,
            stage=self.stage,
            name=self.name,
        )

    @property
    def description(self) -> str:
        """
        Provides a detailed description of the asset.

        The description includes information such as the phase, stage, class name,
        and the creation timestamp formatted in HTTP date format. It offers a
        human-readable summary of the asset's key details.

        Returns:
            str: A detailed description of the asset including its phase, stage,
                 class name, and creation time.
        """
        return f"{self.phase.description} - {self.stage.description} {self.__class__.__name__} created on {dt4mtr.to_HTTP_format(self.created)}."
