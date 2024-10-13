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
# Modified   : Sunday October 13th 2024 01:57:37 am                                                #
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

from discover.core.data_class import DataClass
from discover.core.flow import PhaseDef, StageDef
from discover.infra.utils.date_time.format import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Asset(DataClass):
    """
    Represents an element within a specific phase and stage of processing,
    encapsulating a payload and its associated storage configuration.

    Attributes:
        phase (PhaseDef): The phase of the process to which this element belongs.
        stage (StageDef): The stage within the phase that the element is currently in.
        content (Any): The payload or data that this element carries.
        storage_config (StorageConfig): Configuration details for how this element
            should be stored, including parameters like partitioning and compression.
        created (Optional[datetime]): The datetime when the element was instantiated.
            Defaults to None and is set to the current datetime during initialization.

    Methods:
        __post_init__() -> None:
            Sets the `created` attribute to the current datetime upon initialization.

        __getstate__() -> dict:
            Prepares the object's state for serialization. Converts the object's
            attributes into a dictionary that can be serialized.

        __setstate__(state: dict) -> None:
            Restores the object's state from a serialized format by applying
            the provided dictionary to the object's attributes.

        name() -> str:
            Generates a name for the element based on its phase and stage values.

        description() -> str:
            Provides a descriptive string of the element, including its phase,
            stage, type, and creation timestamp in HTTP date format.
    """

    phase: PhaseDef
    stage: StageDef
    content: Any
    created: Optional[datetime] = None

    def __post_init__(self) -> None:
        """
        Initializes the `created` attribute with the current datetime.
        This ensures that the element's creation time is recorded upon instantiation.
        """
        self.created = self.created or datetime.now()

    def __getstate__(self):
        """
        Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        return {field.name: getattr(self, field.name) for field in fields(self)}

    def __setstate__(self, state):
        """
        Restores the object's state from a serialized format.

        This method takes a dictionary representation of the object's state
        and applies it to restore the object's attributes.

        Args:
            state (dict): The state to restore the object from.
        """
        for key, value in state.items():
            setattr(self, key, value)

    @property
    def name(self) -> str:
        """
        Generates a name for the element based on its phase and stage values.

        Returns:
            str: A string combining the phase and stage values.
        """
        return f"{self.phase.value}_{self.stage.value}"

    @property
    def description(self) -> str:
        """
        Provides a descriptive string for the element.

        The description includes the phase, stage, class name, and the
        creation timestamp in HTTP date format.

        Returns:
            str: A detailed description of the element.
        """
        return f"{self.phase.description} - {self.stage.description} {self.__class__.__name__} created on {dt4mtr.to_HTTP_format(self.created)}."
