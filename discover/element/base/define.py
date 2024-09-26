#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/base/define.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 10:21:05 pm                                            #
# Modified   : Wednesday September 25th 2024 10:56:46 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Element Dimension"""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Optional

from discover.core.data_class import DataClass
from discover.core.flow import PhaseDef, StageDef
from discover.element.base.store import StorageConfig
from discover.infra.tools.date_time.format import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ElementMetadata(DataClass):
    """
    Represents metadata for an element in the system.

    Attributes:
        id (int): Unique identifier for the element.
        name (str): Name of the element.
        phase (PhaseDef): Phase in which the element is created.
        stage (StageDef): Stage in which the element resides.
        description (Optional[str]): Description of the element.
        created (Optional[datetime]): Timestamp when the element was created.
        persisted (Optional[datetime]): Timestamp when the element was persisted.
        build_duration (float): Time taken to build the element.
        element_type (Optional[str]): Type of the element, automatically set on creation.
    """

    id: int
    name: str
    phase: PhaseDef
    stage: StageDef
    description: Optional[str] = None
    created: Optional[datetime] = None
    persisted: Optional[datetime] = None
    build_duration: float = 0
    element_type: Optional[str] = None

    def __post_init__(self) -> None:
        """
        Initializes the created timestamp and element type,
        and constructs the description based on phase and stage.
        """
        self.created = datetime.now()
        self.description = (
            f"{self.phase.description} - {self.stage.description} {self.name} "
            f"{self.element_type} created on {dt4mtr.to_HTTP_format(datetime.now())}."
        )

    def __getstate__(self):
        """
        Returns the state of the object for serialization.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        return {field.name: getattr(self, field.name) for field in fields(self)}

    def __setstate__(self, state):
        """
        Restores the state of the object from serialization.

        Args:
            state (dict): The state to restore the object from.
        """
        for key, value in state.items():
            setattr(self, key, value)

    def persist(self) -> None:
        """
        Updates the persisted timestamp and calculates the build duration.

        This method is called when the Dataset is saved to record the time
        of persistence and the duration taken to build the element.
        """
        self.persisted = datetime.now()
        self.build_duration = (self.persisted - self.created).total_seconds()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Element(DataClass):
    """
    Represents an element containing its metadata and content.

    Attributes:
        metadata (ElementMetadata): Metadata associated with the element.
        content (Any): The actual data of the element.
        storage_config (StorageConfig): Configuration for how the element's data is stored.
    """

    metadata: ElementMetadata
    content: Any
    storage_config: StorageConfig
