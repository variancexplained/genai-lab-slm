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
# Modified   : Monday September 23rd 2024 02:36:00 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Element Dimension"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from discover.core.data_class import DataClass
from discover.core.flow import PhaseDef, StageDef
from discover.infra.tools.date_time.format import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Element(DataClass):
    """
    A base class representing a generic element in the system.

    Attributes:
    - id (int): Unique sequential numeric identifier for the element.
    - name (str): The name of the element.
    - phase (PhaseDef): The phase in which the element is created.
    - stage (StageDef): The stage in which the element resides.
    - created (datetime): The timestamp when the element was created.
    - persisted (datetime): The timestamp when the element was persisted.
    - accessed (datetime): The timestamp when the element was last accessed.
    - build_duration (float): Time taken to build the element, defaults to 0.
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
        self.created = datetime.now()
        self.element_type = self.__class__.__name__
        self.description = f"{self.phase.description} - {self.stage.description} {self.name} {self.__class__.__name__} created on {dt4mtr.to_HTTP_format(datetime.now())}."
