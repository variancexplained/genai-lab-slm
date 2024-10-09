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
# Modified   : Tuesday October 8th 2024 09:43:18 pm                                                #
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
class Element(DataClass):
    """
    Represents an element in the system, encapsulating its ID, name, phase, stage,
    and associated content. Additionally, tracks storage configuration and timestamps
    for creation and persistence.

    Attributes:
        id (int): The unique identifier of the element.
        name (str): The name of the element.
        phase (PhaseDef): Defines the current phase of the element.
        stage (StageDef): Defines the current stage of the element.
        content (Any): The payload associated with the element.
        storage_config (StorageConfig): Configuration for storing the element.
        description (Optional[str]): A description of the element, default is None.
        cost (float): The duration in seconds between the creation and persistence of the element. Default is 0.0.
        created (Optional[datetime]): The timestamp when the element was created. Default is None.
        persisted (Optional[datetime]): The timestamp when the element was persisted. Default is None.
    """

    id: int
    name: str
    phase: PhaseDef
    stage: StageDef
    content: Any  # The element payload
    storage_config: StorageConfig  # Specifies storage configuration
    description: Optional[str] = None
    cost: float = 0.0  # number of seconds between created and persisted
    created: Optional[datetime] = None  # DT instantiated
    persisted: Optional[datetime] = None  # DT persisted
    element_type: Optional[str] = None  # Type of element.

    def __post_init__(self) -> None:
        """
        Initializes the created timestamp and constructs the element description
        based on the phase and stage.

        This method is called automatically after the class is instantiated. It sets
        the `created` timestamp and generates a description string based on the
        phase, stage, and name of the element.
        """
        self.created = datetime.now()
        self.element_type = self.__class__.__name__
        self.description = (
            f"{self.phase.description} - {self.stage.description} {self.name} "
            f"{self.element_type} created on {dt4mtr.to_HTTP_format(self.created)}."
        )

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

    def persist(self) -> None:
        """
        Marks the element as persisted and calculates the time taken to persist.

        This method sets the `persisted` timestamp and updates the `cost` attribute,
        representing the time difference between creation and persistence of the
        element in seconds.
        """
        self.persisted = datetime.now()
        self.cost = (self.persisted - self.created).total_seconds()
